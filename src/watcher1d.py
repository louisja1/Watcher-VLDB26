import random
import time
import black_box
import time
import numpy as np
from error_metrics import get_average_q_error, get_percentile_q_error
from pg_hint_utility import get_single_table_pointer, inject_single_table_sels_and_get_cost, gen_final_hint, get_real_latency

class Watcher1D:
    def __init__(
        self,
        name=None,
        conn=None,
        cursor=None,
        sql=None,
        watchset_size=black_box.config["training_size"],
        random_seed=None,
        threshold=None,
        error_metric="90th",
        info_id=None,
        sel_by_domain=None,
        assigned_alias=None,
    ):
        self.name = name
        if assigned_alias is not None:
            self.abbrev = assigned_alias
        else:
            self.abbrev = ""
            for piece in name.strip().split("_"):
                self.abbrev += piece[0]

        self.conn = conn
        self.cursor = cursor

        self.dbname_prefix = ""

        self.watchset_size = watchset_size
        self.sql = sql
        assert self.sql is not None
        self.n_tabs_in_from = sql.lower().split("from")[1].split("where")[0].count(",") + 1

        self.error_metric = error_metric
        assert error_metric in ["avg", "90th"]

        self.threshold = threshold
        if random_seed is not None:
            random.seed(random_seed)

        self.info_id = info_id
        self.sel_by_domain = sel_by_domain

        self._query_id = 0 # the 0-based query id for all the queries that have been seen so far
        self.watch_set = []  # a list of [RangeSelection, true_card]
        self.preds = [] # a list of model's pred, where each one is corresponding to the 

    def load_table(self, file, sel_col, jk_cols, pgtype, timestamp_col=None):
        self.sel_col = sel_col
        self.jk_cols = jk_cols
        self.attrs = list(pgtype.keys())
        self.pgtype = pgtype

        dbname = self.dbname_prefix + self.name

        print(f"Load Table {self.name} to {dbname}")
        self.cursor.execute(f"drop table if exists {dbname};")
        if " " in sel_col:
            # for the case where selection is on an expression, e.g., "ss_sales_price / ss_list_price"
            for each in sel_col.split(" "):
                if each.startswith(dbname[0]): # share the same first bit as the table_name
                    self.cursor.execute(f"drop index if exists idx_{dbname}_{each};")
        else: 
            self.cursor.execute(f"drop index if exists idx_{dbname}_{sel_col};")
        for jk_col in jk_cols:
            self.cursor.execute(f"drop index if exists idx_{dbname}_{jk_col};")

        fake_table_name = "fake_" + dbname
        q = f"create table {fake_table_name} ("
        q += ", ".join(k + " " + v for k, v in pgtype.items())
        q += ");"
        self.cursor.execute(q)
        q = f"copy {fake_table_name} from stdin delimiter ',' csv header"
        self.cursor.copy_expert(q, open(file, "r"))

        q = f"create table {dbname} ("
        q += "row_id serial, "
        q += ", ".join(k + " " + v for k, v in pgtype.items())
        q += ");"
        self.cursor.execute(q)

        q = f"insert into {dbname} select nextval('{dbname}_row_id_seq'), * from {fake_table_name}"
        if timestamp_col is not None:
            q += f" order by {timestamp_col} asc;"
        self.cursor.execute(q)
        self.cursor.execute(f"drop table if exists {fake_table_name};")

        if " " in sel_col:
            # for the case where selection is on an expression, e.g., "ss_sales_price / ss_list_price"
            for each in sel_col.split(" "):
                if each.startswith(dbname[0]): # share the same first bit as the table_name
                    self.cursor.execute(f"create index idx_{dbname}_{each} on {dbname}({each});")
        else:
            self.cursor.execute(
                f"create index idx_{dbname}_{sel_col} on {dbname}({sel_col});"
            )
        for jk_col in jk_cols:
            self.cursor.execute(f"create index idx_{dbname}_{jk_col} on {dbname}({jk_col});")
        self.conn.commit()

        self.cursor.execute(f"select count(*) from {dbname};")
        self.cur_table_size = self.cursor.fetchone()[0]


    def clean(self):
        # dbname = self.dbname_prefix + self.name
        # self.cursor.execute(f"drop table if exists {dbname};")
        # self.cursor.execute(f"drop index if exists idx_{dbname}_{self.sel_col};")
        # for jk_col in self.jk_cols:
        #     self.cursor.execute(f"drop index if exists idx_{dbname}_{jk_col};")
        self.cursor.close()
        self.conn.commit()
        self.conn.close()

    def setup_single_table_pointer(self):
        _map = get_single_table_pointer(
            sql=self.get_sql_with_range(
                rs=self.sel_by_domain
            )
        )
        if self.abbrev in _map:
            self.pointer = _map[self.abbrev]
        elif self.name in _map:
            self.pointer = _map[self.name]
        else:
            assert True == False

    def _get_training_set(self):
        start_time = time.time()
        
        # MSCN's inteface for retraining: [tables], [joins], [lb,ub,card]
        training_set = [
            (self.watch_set[i][0].lb, self.watch_set[i][0].ub, self.watch_set[i][1])
            for i in range(len(self.watch_set))
        ]

        return (
            time.time() - start_time, 
            (
                [f"{self.name} {self.abbrev}"],
                [""],
                training_set,
            )
        )
    
    def check_retrain(self):
        check_start_time = time.time()
        info = ""
        
        err = self.get_error()
        check_time = time.time() - check_start_time

        if err is not None and err > self.threshold:
            retrain_time = 0.
            while err > self.threshold:
                training_set_collecting_time, training_info = self._get_training_set()
                retrain_time += training_set_collecting_time

                retrain_start_time = time.time()
                black_box.retrain(self.name, *training_info)
                self.preds = []
                for i in range(len(self.watch_set)):
                    lb, ub = self.watch_set[i][0].lb, self.watch_set[i][0].ub
                    self.preds.append(
                        black_box.predict_one(
                            self.name,
                            [lb, ub],
                            [f"{self.name} {self.abbrev}"],
                            [""],
                        )
                    )
                if len(info) == 0:
                    info = ","
                    
                info += "retrain_"
                err = self.get_error()
                retrain_time += time.time() - retrain_start_time
                if err > self.threshold:
                    info += (
                        f"failed{self.info_id}=" + str(err) + ">" + str(self.threshold)
                    )
                    self.threshold *= 1.5
                    info += f",adjust_threshold{self.info_id}=" + str(self.threshold) + ","
                else:
                    info += (
                        f"successful{self.info_id}="
                        + str(err)
                        + "<="
                        + str(self.threshold)
                        + ","
                    )
                    break
            return check_time, True, info, retrain_time
        return check_time, False, None, None

    def insert(self, table_name, attr_in_str):
        assert table_name == self.name
        start_time = time.time()
        
        attr_vals = []
        for x in attr_in_str.split(","):
            if x == "None":
                attr_vals.append(None)
            else:
                attr_vals.append(x)
        attr_vals = tuple(attr_vals)
        dbname = self.dbname_prefix + self.name

        q = (
            f"insert into {dbname} ("
            + ",".join(self.attrs)
            + ") values ("
            + ",".join(["%s"] * len(self.attrs))
            + f") returning {self.sel_col};"
        )
        self.cursor.execute(q, attr_vals)
        sel_val = self.cursor.fetchone()[0]
        self.cur_table_size += 1
        
        watcher_start_time = time.time()
        if len(self.watch_set) > 0:
            for i in range(len(self.watch_set)):
                # self.watch_set[i] = (RangeSelection, card)
                if (
                    sel_val is not None
                    and self.watch_set[i][0].lb <= sel_val
                    and sel_val <= self.watch_set[i][0].ub
                ):  # pass the predicate?
                    self.watch_set[i][1] += 1
        watcher_time = time.time() - watcher_start_time

        check_time, if_retrain, retrain_info, retrain_time = self.check_retrain()

        if if_retrain:
            watcher_time += check_time
        info = f"watchset_size{self.info_id}={len(self.watch_set)},time{self.info_id}={round(time.time() - start_time, 6)},watcher_time{self.info_id}={round(watcher_time, 6):.6f}"
        if if_retrain:
            info += retrain_info + f"retrain_time{self.info_id}={round(retrain_time, 6):.6f}"
        return info

    def delete(self, table_name, id):
        assert table_name == self.name
        start_time = time.time()

        dbname = self.dbname_prefix + self.name
        q = f"delete from {dbname} where row_id = {id} returning {self.sel_col};"
        self.cursor.execute(q)
        sel_val = self.cursor.fetchone()[0]
        self.cur_table_size -= 1

        watcher_start_time = time.time()
        if len(self.watch_set) > 0:
            for i in range(len(self.watch_set)):
                # self.watch_set[i] = (RangeSelection, card)
                if (
                    sel_val is not None
                    and self.watch_set[i][0].lb <= sel_val
                    and sel_val <= self.watch_set[i][0].ub
                ):  # pass the predicate?
                    self.watch_set[i][1] -= 1
        watcher_time = time.time() - watcher_start_time

        check_time, if_retrain, retrain_info, retrain_time = self.check_retrain()

        if if_retrain:
            watcher_time += check_time
        info = f"watchset_size{self.info_id}={len(self.watch_set)},time{self.info_id}={round(time.time() - start_time, 6)},watcher_time{self.info_id}={round(watcher_time, 6):.6f}"
        if if_retrain:
            info += retrain_info + f"retrain_time{self.info_id}={round(retrain_time, 6):.6f}"
        return info

    def get_error(self):
        true_cards = [self.watch_set[i][1] for i in range(len(self.watch_set))]
        if len(true_cards) == 0:
            return 0
        if self.error_metric == "avg":
            return get_average_q_error(true_cards, self.preds)
        elif self.error_metric == "90th":
            return get_percentile_q_error(true_cards, self.preds, 90)
        else:
            raise NotImplementedError

    def get_sql_with_range(self, rs):
        return self.sql.replace("{lb" + str(self.info_id) + "}", str(rs.lb)).replace("{ub" + str(self.info_id) + "}", str(rs.ub))

    def insert_to_watchset(self, rs, true_card, model_pred):
        if true_card > 0:
            if len(self.watch_set) < self.watchset_size:
                self.watch_set.append([rs, true_card])
                self.preds.append(model_pred)
            else:
                i = random.randint(0, self._query_id)
                if i < self.watchset_size:
                    self.watch_set[i] = [rs, true_card]
                    self.preds[i] = model_pred
        self._query_id += 1

    def query(self, rs, true_card):
        start_time = time.time()
        assert rs.table_name == self.name

        model_pred = black_box.predict_one(
            self.name,
            [rs.lb, rs.ub],
            [f"{self.name} {self.abbrev}"],
            [""],
        )
        watcher_time = time.time() - start_time
        q = self.get_sql_with_range(rs)

        # inject the "model_pred" and get the plan info
        _, join_order, scan_mtd = inject_single_table_sels_and_get_cost(
            pointer=self.pointer,
            sel=model_pred / self.cur_table_size,
            n_tabs_in_from=self.n_tabs_in_from,
            q=q,
            hint=None,
            plan_info=True,
        ) 

        # construct the plan by the info
        inj_plan = gen_final_hint(scan_mtd=scan_mtd, str=join_order)

        # lets run it!
        _, execution_time, planning_time, model_name_to_have = get_real_latency(
            sql=q,
            hint=inj_plan,
            if_have=[self.name, self.abbrev],
        )

        # we cost the plan by the true_card, which is not counted as the cost 
        # of Watcher1D. Because this info is just for reporting, but not utilized
        # by the algorithm
        plan_cost = inject_single_table_sels_and_get_cost(
            pointer=self.pointer,
            sel=true_card / self.cur_table_size,
            n_tabs_in_from=self.n_tabs_in_from,
            q = q,
            hint=inj_plan,
            plan_info=False,
        )

        start_time_2 = time.time()
        self.insert_to_watchset(rs, true_card, model_pred)
        watcher_time += time.time() - start_time_2

        info = f"watchset_size{self.info_id}={len(self.watch_set)}" 
        info += f",plan_cost{self.info_id}={plan_cost}"
        info += f",execution_time{self.info_id}={round(execution_time / 1000., 6):.6f}"
        info += f",watcher_time{self.info_id}={round(watcher_time, 6):0.6f}"
        info += f",got_true_card{self.info_id}="
        if (self.name in model_name_to_have and model_name_to_have[self.name]) or (self.abbrev in model_name_to_have and model_name_to_have[self.abbrev]):
            info += "True"
        else:
            info += "False"

        hint_wo_newline = inj_plan.replace('\n', '')
        info += f",plan{self.info_id}={hint_wo_newline}"

        return (
            np.round(model_pred, 6),
            get_average_q_error([true_card], [model_pred]),
            info,
        )
