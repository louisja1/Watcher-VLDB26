import random
import black_box
import logger
import time
import numpy as np
from index import Table
from error_metrics import get_average_q_error, get_percentile_q_error
from pg_hint_utility import get_two_way_join_info, inject_join_sel_and_get_cost, gen_final_hint, get_real_latency

random.seed(2023)


class Watcher2d:
    def __init__(
        self,
        conn=None,
        cursor=None,
        sql=None,
        watchset_size=black_box.config["training_size"],
        random_seed=None,
        threshold=None,
        error_metric="95th",
        pgtypes=None,
        jk_cols=None,
        sel_cols=None,
    ):
        self.dbname_prefix = ""

        self.conn = conn
        self.cursor = cursor
        self.sql = sql
        self.watchset_size = watchset_size
        self.threshold = threshold
        self.error_metric = error_metric

        self.pgtypes = pgtypes
        self.jk_cols = jk_cols
        self.sel_cols = sel_cols

        self.tableset = list(sel_cols.keys())
        self.tableset.sort()
        self.tableset = tuple(self.tableset)

        self.tab_to_jk_to_watch_and_idx = {} # tab -> (jk_to_watch, jk_to_watch_idx)
        self.tab_to_sel_to_watch_and_idx = {} # tab -> (sel_to_watch, sel_to_watch_idx)

        self._query_id = 0
        if random_seed is not None:
            random.seed(random_seed)
        self.tables = {}
        self.watch_set = [] # [rs1, rs2, true_card]
        self.preds = []

    def clean(self):
        for tab in self.tables:
            self.tables[tab].clean(close=True)
        self.cursor.close()
        self.conn.commit()
        self.conn.close()

    def _get_leveldb_field_type(self, tab, col):
        # todo: support float, string or others
        # remember to x100 because we assume 2 decimal places
        if "/" in col or self.pgtypes[tab][col].lower() == "real" or self.pgtypes[tab][col].lower().startswith("decimal"):
            return "float"
        elif self.pgtypes[tab][col].lower() == "integer":
            return "int32"
        else:
            raise NotImplementedError

    # jk_bound, sel_bound: remember to x100
    def init_metadata(self, tab, table_filename, jk_to_watch, sel_to_watch, jk_bound, sel_bound, jk_delta, sel_delta, max_level=5, fan_out=5, jk_theta=10):
        pgtypes_in_list = list(self.pgtypes[tab].keys())
        for idx in range(len(pgtypes_in_list)):
            if pgtypes_in_list[idx] == jk_to_watch:
                self.tab_to_jk_to_watch_and_idx[tab] = (jk_to_watch, idx)
            if pgtypes_in_list[idx] == sel_to_watch:
                self.tab_to_sel_to_watch_and_idx[tab] = (sel_to_watch, idx)
        assert tab in self.tab_to_jk_to_watch_and_idx and tab in self.tab_to_sel_to_watch_and_idx

        self.tables[tab] = Table(
            tab,
            jk_idx=self.tab_to_jk_to_watch_and_idx[tab][1],
            sel_idx=self.tab_to_sel_to_watch_and_idx[tab][1],
            jk_field_type=self._get_leveldb_field_type(tab, jk_to_watch),
            sel_field_type=self._get_leveldb_field_type(tab, sel_to_watch),
            max_level=max_level,
            fan_out=fan_out,
            jk_theta=jk_theta,
            jk_delta=jk_delta,
            sel_delta=sel_delta,
        )

        self.tables[tab].set_jk_bound(jk_bound[0], jk_bound[1])
        self.tables[tab].set_sel_bound(sel_bound[0], sel_bound[1])

    def load_table(self, tab, file, timestamp_col=None):
        sel_col = list(self.sel_cols[tab].keys())[0]
        jk_cols = self.jk_cols[tab]
        pgtype = self.pgtypes[tab]

        dbname = self.dbname_prefix + tab
        
        print(f"Load Table {tab} to {dbname}")
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

        # load the data to the external index
        self.cursor.execute(f"select {', '.join(list(self.pgtypes[tab].keys()))} from {dbname} order by row_id asc;")
        while True:
            row = self.cursor.fetchone()
            if row is None:
                break
            self.tables[tab].insert(",".join([str(x) for x in row]))
        self.tables[tab].load_table(filename=None)

    def setup_black_box(
        self, model_name, model_tables, model_joins
    ):
        self.model_name = model_name
        self.model_tables = model_tables
        self.model_joins = model_joins

    def insert_to_watchset(self, rs1, rs2, true_card, model_pred):
        if true_card > 0:
            if len(self.watch_set) < self.watchset_size:
                self.watch_set.append([rs1, rs2, true_card])
                self.preds.append(model_pred)
            else:
                i = random.randint(0, self._query_id)
                if i < self.watchset_size:
                    self.watch_set[i] = [rs1, rs2, true_card]
                    self.preds[i] = model_pred
        self._query_id += 1

    def _get_training_set(self): 
        start_time = time.time()

        training_set = [
            (
                self.watch_set[i][0].lb, 
                self.watch_set[i][0].ub, 
                self.watch_set[i][1].lb,
                self.watch_set[i][1].ub,
                self.watch_set[i][2],
            )
            for i in range(len(self.watch_set))
        ]
        return (
            time.time() - start_time,
            (
                self.model_tables,
                self.model_joins,
                training_set,
            )
        )
    
    def get_error(self):
        true_cards = [self.watch_set[i][2] for i in range(len(self.watch_set))]
        if len(true_cards) == 0:
            return 0
        if self.error_metric == "avg":
            return get_average_q_error(true_cards, self.preds)
        elif self.error_metric == "90th":
            return get_percentile_q_error(true_cards, self.preds, 90)
        elif self.error_metric == "95th":
            return get_percentile_q_error(true_cards, self.preds, 95)
        else:
            raise NotImplementedError

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
                black_box.retrain(self.model_name, *training_info)
                self.preds = []
                for i in range(len(self.watch_set)):
                    lb1, ub1 = self.watch_set[i][0].lb, self.watch_set[i][0].ub
                    lb2, ub2 = self.watch_set[i][1].lb, self.watch_set[i][1].ub

                    self.preds.append(
                        black_box.predict_one(
                            self.model_name,
                            [lb1, ub1, lb2, ub2],
                            self.model_tables,
                            self.model_joins,
                        )
                    )
                if len(info) == 0:
                    info = ","
                    
                info += "retrain_"
                err = self.get_error()
                retrain_time += time.time() - retrain_start_time
                if err > self.threshold:
                    info += (
                        f"failed=" + str(err) + ">" + str(self.threshold)
                    )
                    self.threshold *= 1.5
                    info += f",adjust_threshold=" + str(self.threshold) + ","
                else:
                    info += (
                        f"successful="
                        + str(err)
                        + "<="
                        + str(self.threshold)
                        + ","
                    )
                    break
            return check_time, True, info, retrain_time
        return check_time, False, None, None

    def insert(self, tab, attr_in_str):
        start_time = time.time()
        attrs = list(self.pgtypes[tab].keys())
        jk_to_watch = self.tab_to_jk_to_watch_and_idx[tab][0]
        sel_to_watch = self.tab_to_sel_to_watch_and_idx[tab][0]

        # insert to the database
        attr_vals = []
        for x in attr_in_str.split(","):
            if x == "None":
                attr_vals.append(None)
            else:
                attr_vals.append(x)
        attr_vals = tuple(attr_vals)
        dbname = self.dbname_prefix + tab

        q = (
            f"insert into {dbname} ("
            + ",".join(attrs)
            + ") values ("
            + ",".join(["%s"] * len(attrs))
            + f") returning {jk_to_watch}, {sel_to_watch};"
        )
        self.cursor.execute(q, attr_vals)
        jk_val, sel_val = self.cursor.fetchone()

        # insert to the Watcher2D.tables
        watcher_start_time = time.time()
        _ = self.tables[tab].insert(attr_in_str)
        if len(self.watch_set) > 0 and sel_val is not None:
            # jk = self.tables[tab].get_jk_by_id(id)
            two_tabs = list(self.tables.keys())
            other_tab = two_tabs[0] if tab != two_tabs[0] else two_tabs[1]

            bounds = []
            qid = 0
            for i in range(len(self.watch_set)):
                cur = 0 if self.watch_set[i][0].table_name == tab else 1
                other = 1 - cur
                if sel_val == self.watch_set[i][cur].lb:
                    pass # To resolve a precision-related bugs
                elif self.watch_set[i][cur].check(sel_val):
                    bounds.append((qid, (self.watch_set[i][other].lb, self.watch_set[i][other].ub)))
                    logger.n_affected_queries += 1
                qid += 1

            query_bound_start_time = time.time()
            m = self.tables[other_tab].query_bounds(jk_val, bounds)
            logger.query_bounds_time += time.time() - query_bound_start_time

            for qid, delta in m.items():
                self.watch_set[qid][2] += delta

            # correctness check:
            # qid = 0
            # for q in self.watch_set:
            #     bound = q.get_bounds_if_pass_local_sel(table_name, id)
            #     if bound is not None:
            #         delta = tables[oth_table_name].query(jk, bound[0], bound[1])
            #         if delta != m[qid]:
            #             print(qid, delta, m[qid])
            #         assert qid in m and delta == m[qid]
            #     qid += 1
        watcher_time = time.time() - watcher_start_time

        check_time, if_retrain, retrain_info, retrain_time = self.check_retrain()

        if if_retrain:
            watcher_time += check_time
        info = f"watchset_size={len(self.watch_set)},time={round(time.time() - start_time, 6)},watcher_time={round(watcher_time, 6):.6f}"
        if if_retrain:
            info += retrain_info + f"retrain_time={round(retrain_time, 6):.6f}"
        return info

    def delete(self, tab, id):
        start_time = time.time()
        jk_to_watch = self.tab_to_jk_to_watch_and_idx[tab][0]
        sel_to_watch = self.tab_to_sel_to_watch_and_idx[tab][0]

        # delete from the database
        dbname = self.dbname_prefix + tab
        q = f"delete from {dbname} where row_id = {id} returning {jk_to_watch}, {sel_to_watch};"
        self.cursor.execute(q)
        jk_val, sel_val = self.cursor.fetchone()

        # delete from the Watcher2D.Table
        watcher_start_time = time.time()
        if len(self.watch_set) > 0 and sel_val is not None:
            # jk = tables[table_name].get_jk_by_id(id)
            two_tabs = list(self.tables.keys())
            other_tab = two_tabs[0] if tab != two_tabs[0] else two_tabs[1]

            bounds = []
            qid = 0
            for i in range(len(self.watch_set)):
                cur = 0 if self.watch_set[i][0].table_name == tab else 1
                other = 1 - cur
                if sel_val == self.watch_set[i][cur].lb:
                    pass # To resolve a precision-related bugs
                elif self.watch_set[i][cur].check(sel_val):
                    bounds.append((qid, (self.watch_set[i][other].lb, self.watch_set[i][other].ub)))
                    logger.n_affected_queries += 1
                qid += 1

            m = self.tables[other_tab].query_bounds(jk_val, bounds)
            for qid, delta in m.items():
                self.watch_set[qid][2] -= delta

        self.tables[tab].delete(id)
        watcher_time = time.time() - watcher_start_time

        check_time, if_retrain, retrain_info, retrain_time = self.check_retrain()

        if if_retrain:
            watcher_time += check_time
        info = f"watchset_size={len(self.watch_set)},time={round(time.time() - start_time, 6)},watcher_time={round(watcher_time, 6):.6f}"
        if if_retrain:
            info += retrain_info + f"retrain_time={round(retrain_time, 6):.6f}"
        return info

    def query(self, rs1, rs2, true_card):
        start_time = time.time()
        assert rs1.table_name in self.sel_cols
        assert rs2.table_name in self.sel_cols

        model_pred = black_box.predict_one(
            self.model_name,
            [rs1.lb, rs1.ub, rs2.lb, rs2.ub],
            self.model_tables,
            self.model_joins,
        )
        watcher_time = time.time() - start_time
        q = self.sql.replace("{lb1}", str(rs1.lb)).replace("{ub1}", str(rs1.ub)).replace("{lb2}", str(rs2.lb)).replace("{ub2}", str(rs2.ub))

        two_way_join_pointer, two_way_join_rawrows, min_join_qid, max_join_qid = get_two_way_join_info(
            sql=q,
            tableset=self.tableset,
        )

        # inject the "model_pred" and get the plan info
        _, join_order, scan_mtd = inject_join_sel_and_get_cost(
            join_pointer=two_way_join_pointer,
            join_sel=model_pred / two_way_join_rawrows,
            min_join_qid=min_join_qid, 
            max_join_qid=max_join_qid,
            q=q,
            hint=None,
            plan_info=True,
        )

        # construct the plan by the info
        inj_plan = gen_final_hint(scan_mtd=scan_mtd, str=join_order)

        # lets run it!
        _, execution_time, _ = get_real_latency(
            sql=q,
            hint=inj_plan,
        )

        # we cost the plan by the true_card, which is not counted as the cost 
        # of RountineBaseline2D. Because this info is just for reporting, but not utilized
        # by the algorithm
        plan_cost = inject_join_sel_and_get_cost(
            join_pointer=two_way_join_pointer,
            join_sel=true_card / two_way_join_rawrows,
            min_join_qid=min_join_qid, 
            max_join_qid=max_join_qid,
            q=q,
            hint=inj_plan,
            plan_info=False,
        )

        start_time_2 = time.time()
        self.insert_to_watchset(rs1, rs2, true_card, model_pred)
        watcher_time += time.time() - start_time_2

        info = f"watchset_size={len(self.watch_set)}" 
        info += f",plan_cost={plan_cost}"
        info += f",execution_time={round(execution_time / 1000., 6):.6f}"
        info += f",watcher_time={round(watcher_time, 6):0.6f}"

        hint_wo_newline = inj_plan.replace('\n', '')
        info += f",plan={hint_wo_newline}"

        return (
            np.round(model_pred, 6),
            get_average_q_error([true_card], [model_pred]),
            info,
        )
