from tqdm import tqdm
import time
import numpy as np
import black_box
import random
import json
import sys
from operation import TupleUpdateType, RangeSelection
import warnings
from connection import get_connection, close_connection
from error_metrics import get_average_q_error
from pg_hint_utility import get_single_table_pointer, inject_single_table_sels_and_get_cost, gen_final_hint, get_real_latency

warnings.filterwarnings("ignore")

class RoutineBaseline1D:
    def __init__(
        self,
        name=None,
        conn=None,
        cursor=None,
        query_pool_size=black_box.config["training_size"],
        sql=None,
        random_seed=None,
        based_on=None,
        X=None,
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

        self.query_pool_size = query_pool_size
        self.sql = sql
        assert self.sql is not None
        self.n_tabs_in_from = sql.lower().split("from")[1].split("where")[0].count(",") + 1

        if random_seed is not None:
            random.seed(random_seed)
        
        self.based_on = based_on
        assert self.based_on in ["tuples"]
        self.X = X
        assert isinstance(self.X, list)

        self.info_id = info_id
        self.sel_by_domain = sel_by_domain

        self.query_pool = []
        self._query_id = 0
        self.op_cnts = {"tuples": 0, "queries": 0}
        self.xid = 0

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
        # self.cursor.execute(f"drop table if exists {self.dbname};")
        # self.cursor.execute(f"drop index if exists idx_{self.dbname}_{self.sel_col};")
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

    def check_retrain(self):
        check_start_time = time.time()
        info = ""

        cur_n_ops = None
        if self.X is not None and self.xid < len(self.X):
            cur_n_ops = self.op_cnts[self.based_on]
        check_time = time.time() - check_start_time

        if cur_n_ops is not None and cur_n_ops == self.X[self.xid]:
            self.xid += 1
            retrain_time = 0.
            training_set_collecting_time, training_info = self._get_training_set()
            retrain_time += training_set_collecting_time

            retrain_start_time = time.time()
            black_box.retrain(self.name, *training_info)
            retrain_time += time.time() - retrain_start_time
            return check_time, True, info, retrain_time
        return check_time, False, None, None

    def _get_training_set(self):
        start_time = time.time()

        training_set = []
        dbname = self.dbname_prefix + self.name
        for x in self.query_pool:
            # we only support selection on a single column for now
            q = (
                f"select count(*) from "
                + dbname
                + " where "
                + f"{dbname}.{self.sel_col} >= {x.lb}"
                + " and "
                + f"{dbname}.{self.sel_col} <= {x.ub}"
                + ";"
            )
            self.cursor.execute(q)
            training_set.append((x.lb, x.ub, self.cursor.fetchone()[0]))
        return (
            time.time() - start_time,
            (
                [f"{self.name} {self.abbrev}"],
                [""],
                training_set,
            )
        )

    def insert(self, name, attr_in_str):
        assert name == self.name
        start_time = time.time()

        self.op_cnts["tuples"] += 1
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
        self.cur_table_size += 1
        
        check_time, if_retrain, retrain_info, retrain_time = self.check_retrain()

        baseline_time = 0.
        if if_retrain:
            baseline_time += check_time
        info = f"time{self.info_id}={round(time.time() - start_time, 6)},baseline_time{self.info_id}={round(baseline_time, 6):.6f}"
        if if_retrain:
            info += retrain_info + f",retrain_time{self.info_id}={round(retrain_time, 6):.6f}"
        return info

    def delete(self, name, id):
        assert name == self.name
        start_time = time.time()

        self.op_cnts["tuples"] += 1
        dbname = self.dbname_prefix + self.name
        q = f"delete from {dbname} where row_id = {id} returning {self.sel_col};"
        self.cursor.execute(q)
        self.cur_table_size -= 1

        check_time, if_retrain, retrain_info, retrain_time = self.check_retrain()

        baseline_time = 0.
        if if_retrain:
            baseline_time += check_time
        info = f"time{self.info_id}={round(time.time() - start_time, 6)},baseline_time{self.info_id}={round(baseline_time, 6):.6f}"
        if if_retrain:
            info += retrain_info + f",retrain_time{self.info_id}={round(retrain_time, 6):.6f}"
        return info

    def get_sql_with_range(self, rs):
        return self.sql.replace("{lb" + str(self.info_id) + "}", str(rs.lb)).replace("{ub" + str(self.info_id) + "}", str(rs.ub))

    def insert_to_query_pool(self, rs, true_card):
        if true_card > 0:
            if len(self.query_pool) < self.query_pool_size:
                self.query_pool.append(rs)
            else:
                i = random.randint(0, self._query_id)
                if i < self.query_pool_size:
                    self.query_pool[i] = rs
        self._query_id += 1

    def query(self, rs, true_card):
        start_time = time.time()
        assert rs.table_name == self.name
        self.op_cnts["queries"] += 1

        model_pred = black_box.predict_one(
            self.name,
            [rs.lb, rs.ub],
            [f"{self.name} {self.abbrev}"],
            [""],
        )
        baseline_time = time.time() - start_time
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
        _, execution_time, planning_time = get_real_latency(
            sql=q,
            hint=inj_plan,
        )

        # we cost the plan by the true_card, which is not counted as the cost 
        # of Baseline1D. Because this info is just for reporting, but not utilized
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
        self.insert_to_query_pool(rs, true_card)
        baseline_time += time.time() - start_time_2

        info = f"plan_cost{self.info_id}={plan_cost}"
        info += f",execution_time{self.info_id}={round(execution_time / 1000., 6):.6f}"
        info += f",baseline_time{self.info_id}={round(baseline_time, 6):0.6f}"
        
        hint_wo_newline = inj_plan.replace('\n', '')
        info += f",plan{self.info_id}={hint_wo_newline}"

        return (
            np.round(model_pred, 6),
            get_average_q_error([true_card], [model_pred]),
            info,
        )

def load_trainingset(
    trainingset_filename, 
    n_tabs, 
    tabs_for_baseline1d, 
    sel_cols, baseline1ds, 
    pgtypes,
    tab_to_alias,
):
    print("Load Trainingset")
    with open(trainingset_filename, "r") as fin:
        tab_to_training_set = {}
        for tab in tabs_for_baseline1d:
            tab_to_training_set[tab] = []

        lines = list(fin.readlines())

        for i in range(len(lines)):
            row = lines[i].strip().split(",")
            for j in range(n_tabs):
                if row[1 + j] in tabs_for_baseline1d:
                    tab = row[1 + j]
                    lb_id = 1 + n_tabs + 2 * j
                    ub_id = lb_id + 1
                    card_id = 1 + n_tabs * 3 + j

                    _col = list(sel_cols[tab].keys())[0]

                    if (
                        int(row[card_id]) >= 1
                        and len(tab_to_training_set[tab]) < black_box.config["training_size"]
                    ):
                        if "/" in _col:
                            lb, ub = float(row[lb_id]), float(row[ub_id])
                        elif pgtypes[tab][_col].lower() == "real" or pgtypes[tab][_col].lower().startswith("decimal"):
                            lb, ub = float(row[lb_id]), float(row[ub_id])
                        elif pgtypes[tab][_col].lower() == "integer" or pgtypes[tab][_col].lower() == "smallint":
                            lb, ub = int(row[lb_id]), int(row[ub_id])
                        else:
                            raise NotImplementedError
                        tab_to_training_set[tab].append((lb, ub, int(row[card_id])))
                        
        print("Finish loading training set:")
        for tab in tab_to_training_set:
            print(f"Training set size of [{tab}]: {len(tab_to_training_set[tab])}")
            if tab in tab_to_alias:
                tab_abbrev = tab_to_alias[tab]
            else:
                tab_abbrev = "".join(x[0] for x in tab.strip().split("_"))
            black_box.retrain(
                tab,
                [f"{tab} {tab_abbrev}"],
                [""],
                tab_to_training_set[tab],
                f"select * from {tab} {tab_abbrev} where {tab_abbrev}.{list(sel_cols[tab].keys())[0]} > {{lb1}} and {tab_abbrev}.{list(sel_cols[tab].keys())[0]} < {{ub1}};",
            )

        print("Load query pool by training set")
        for tab in tab_to_training_set:
            ii = 0
            for lb, ub, card in tab_to_training_set[tab]:
                # note that we only support selection on one column for now
                _col = list(sel_cols[tab].keys())[0]
                rs = RangeSelection(tab, lb, ub)
                rs.update_by(sel_cols[tab][_col]["min"], sel_cols[tab][_col]["max"])
                baseline1ds[tab].insert_to_query_pool(rs, card)
                ii += 1
            print(f"Finish loading query pool ({len(baseline1ds[tab].query_pool)} items)")


def load_log(
    conn,
    cursor,
    log_filename,
    output_prefix,
    n_tabs,
    sel_cols,
    tabs_for_baseline1d,
    baseline1ds,
    pgtypes,
):
    print("Load Log")
    n_tu = 0
    total_tu_time = 0.0
    total_q_time = 0.0

    to_insert_buffer = []
    to_delete_range = [None, None]

    last_op_type = None
    last_tab = None

    with open(log_filename, "r") as fin, open(
        output_prefix + "_answer.txt", "w"
    ) as fout:
        for row_num, line in enumerate(tqdm(fin)):
            op_type = line[0]
            if op_type == "Q":
                tab = None
            else:
                tab = line.split(",")[1]
            # we flush the tuple updates for non-baseline tables
            if (last_op_type != op_type or last_tab != tab) and (last_tab not in tabs_for_baseline1d) and (last_op_type != "Q"):
                if last_op_type == "I" and len(to_insert_buffer) > 0:
                    cursor.execute(f"insert into {last_tab} ({', '.join(list(pgtypes[last_tab].keys()))}) values " + ','.join([x.decode('utf-8') for x in to_insert_buffer]))
                    # print(f"flush {len(to_insert_buffer)} insertions to {last_tab}")
                    to_insert_buffer = []
                if last_op_type == "D" and to_delete_range[0] is not None:
                    cursor.execute(f"delete from {last_tab} where row_id >= {to_delete_range[0]} and row_id <= {to_delete_range[1]};")
                    # print(f"flush {to_delete_range[1] - to_delete_range[0] + 1} deletions to {last_tab}")
                    to_delete_range = [None, None]
            if last_op_type != "Q" and op_type == "Q":
                # # TODO
                # pass
                cursor.execute("vacuum (full, analyze);")
            last_op_type, last_tab = op_type, tab

            ret = str(row_num) + ":"
            if op_type in ["I", "D"]:
                start_tu_ts = time.time()
                n_tu += 1
                _type = TupleUpdateType.from_str(line.split(",")[0])
                t = line.split(",")[1]
                field = line[2 + len(t) + 1 :].strip()
                if t not in tabs_for_baseline1d:
                    total_tu_time += time.time() - start_tu_ts
                    if _type == TupleUpdateType.INSERT:
                        # q = (
                        #     f"insert into {t} ("
                        #     + ",".join(pgtypes[t])
                        #     + ") values ("
                        #     + ",".join(["%s"] * len(pgtypes[t]))
                        #     + f");"
                        # )
                        attr_vals = []
                        for x in field.split(","):
                            if x == "None":
                                attr_vals.append(None)
                            else:
                                attr_vals.append(x)
                        attr_vals = tuple(attr_vals)
                        to_insert_buffer.append(cursor.mogrify("(" + ",".join(['%s'] * len(pgtypes[t])) + ")", attr_vals))
                    elif _type == TupleUpdateType.DELETE:
                        # q = f"delete from {t} where row_id = {int(field)};"
                        # cursor.execute(q)
                        row_id = int(field)
                        if to_delete_range[0] is None or to_delete_range[0] > row_id:
                            to_delete_range[0] = row_id
                        if to_delete_range[1] is None or to_delete_range[1] < row_id:
                            to_delete_range[1] = row_id
                    fout.write(ret + f"time={round(time.time() - start_tu_ts, 6):.6f}\n")
                    continue

                if _type == TupleUpdateType.INSERT:
                    ret += baseline1ds[t].insert(t, field)
                elif _type == TupleUpdateType.DELETE:
                    ret += baseline1ds[t].delete(t, int(field))
                total_tu_time += time.time() - start_tu_ts

            elif op_type == "Q":
                row = line.strip().split(",")
                for jj in range(n_tabs):
                    if row[1 + jj] in tabs_for_baseline1d:
                        tab = row[1 + jj]
                        lb_id = 1 + n_tabs + 2 * jj
                        ub_id = lb_id + 1
                        card_id = 1 + n_tabs * 3 + jj
                        _col = list(sel_cols[tab].keys())[0]
                        if "/" in _col:
                            lb, ub = float(row[lb_id]), float(row[ub_id])
                        elif pgtypes[tab][_col].lower() == "real" or pgtypes[tab][_col].lower().startswith("decimal"):
                            lb, ub = float(row[lb_id]), float(row[ub_id])
                        elif pgtypes[tab][_col].lower() == "integer" or pgtypes[tab][_col].lower() == "smallint":
                            lb, ub = int(row[lb_id]), int(row[ub_id])
                        else:
                            raise NotImplementedError
                        true_card = int(row[card_id])

                        rs = RangeSelection(tab, lb, ub)
                        rs.update_by(
                            sel_cols[tab][_col]["min"], sel_cols[tab][_col]["max"]
                        )

                        start_q_ts = time.time()
                        est, single_err, info = baseline1ds[tab].query(rs, true_card)
                        total_q_time += time.time() - start_q_ts

                        if ret[-1] != ":":
                            ret += ","
                        info_id = baseline1ds[tab].info_id
                        ret += f"est{info_id}=" + str(est) + ","
                        ret += f"observed_err{info_id}=" + str(single_err)

                        if len(info) > 0:
                            ret += "," + info
            else:
                raise NotImplementedError
            fout.write(ret + "\n")

        with open(output_prefix + "_time.txt", "w") as fout:
            fout.write(f"Total TU time: {total_tu_time}\n")
            fout.write(f"Total Q time: {total_q_time}\n")

def load_non_watch_table(conn, cursor, table_name, file, jk_cols, pgtypes, timestamp_col=None):
    cursor.execute(f"drop table if exists {table_name};")
    for jk_col in jk_cols:
        cursor.execute(f"drop index if exists idx_{table_name}_{jk_col};")

    fake_table_name = "fake_" + table_name
    q = f"create table {fake_table_name} ("
    q += ", ".join(k + " " + v for k, v in pgtypes.items())
    q += ");"
    cursor.execute(q)
    q = f"copy {fake_table_name} from stdin delimiter ',' csv header"
    cursor.copy_expert(q, open(file, "r"))

    q = f"create table {table_name} ("
    q += "row_id serial, "
    q += ", ".join(k + " " + v for k, v in pgtypes.items())
    q += ");"
    cursor.execute(q)

    q = f"insert into {table_name} select nextval('{table_name}_row_id_seq'), * from {fake_table_name}"
    if timestamp_col is not None:
        q += f" order by {timestamp_col} asc;"
    cursor.execute(q)
    cursor.execute(f"drop table if exists {fake_table_name};")

    for jk_col in jk_cols:
        cursor.execute(
            f"create index idx_{table_name}_{jk_col} on {table_name}({jk_col});"
        )

if __name__ == "__main__":
    json_filename = sys.argv[1]
    with open(json_filename, "r") as json_fin:
        config = json.load(json_fin)
        tabs = config["tables"]
        if "static_tables" in config:
            static_tabs = config["static_tables"]
        else:
            static_tabs = []
        if "training_size" in config:
            training_size = config["training_size"]
            black_box.config["training_size"] = training_size
        if "algorithm" in config:
            algorithm = config["algorithm"]
            black_box.config["algorithm"] = algorithm
        sel_cols = config["sel_cols"]
        timestamp_cols = config["timestamp_cols"]
        jk_cols = config["jk_cols"]
        sql = config["sql"]
        pgtypes = config["pgtypes"]

        update_based_on = config["update_based_on"]
        update_after = config["update_after"]
        # for example, update_based_on = "tuples" or "queries"
        # update_after = [X1, X2, X3, ...]

        debug = config["debug"]
        table_filenames = config["table_filenames"]
        log_filename = config["log_filename"]
        trainingset_filename = config["trainingset_filename"]

        output_prefix = config["output_prefix"]
        if debug:
            output_prefix = "/".join(output_prefix.split("/")[:-1]) + "/test"
        else:
            output_prefix += (
                "_routinely["
                + ",".join(
                    [f"{k}-after-" + "|".join([str(x) for x in update_after[k]]) + f"-{update_based_on}" for k in update_after]
                )
                + "]"
            )
            if "algorithm" in config:
                output_prefix += f"_{algorithm}"

        # get db connection
        conn, cursor = get_connection()
        # load the static tables
        for static_tab in static_tabs:
            load_non_watch_table(
                conn=conn,
                cursor=cursor,
                table_name=static_tab,
                file=table_filenames[static_tab],
                jk_cols=jk_cols[static_tab],
                pgtypes=pgtypes[static_tab],
                timestamp_col=None,
            )

        # we only need to build RoutineBaseline1D for tables with selections
        tabs_for_baseline1d = list(sel_cols.keys())
        baseline1ds = {}
        tab_to_alias = {}
        for ii in range(len(tabs)):
            if (" " in tabs[ii] or "as" in tabs[ii]):
                # there is an assigned alias name
                _tab = tabs[ii].split(" ")[0]
                assigned_alias = tabs[ii].split(" ")[-1]
                tabs[ii] = _tab
                tab_to_alias[_tab] = assigned_alias
            else:
                assigned_alias = None
            tab = tabs[ii]
            if tab in tabs_for_baseline1d:
                baseline1ds[tab] = RoutineBaseline1D(
                    name=tab,
                    conn=conn,
                    cursor=cursor,
                    query_pool_size=black_box.config["training_size"],
                    sql=sql,
                    random_seed=2023,
                    based_on=update_based_on,
                    X=update_after[tab],
                    info_id=ii + 1,  # 1-base info ids
                    sel_by_domain=RangeSelection( # only utilized to get the pointer for single table
                        table_name=tab,
                        lb=sel_cols[tab][list(sel_cols[tab].keys())[0]]["min"],
                        ub=sel_cols[tab][list(sel_cols[tab].keys())[0]]["max"]
                    ),
                    assigned_alias=assigned_alias,
                )
                baseline1ds[tab].load_table(
                    file=table_filenames[tab],
                    # we only support one sel col per table for now
                    sel_col=list(sel_cols[tab].keys())[0],
                    jk_cols=jk_cols[tab],
                    pgtype=pgtypes[tab],
                    timestamp_col=timestamp_cols[tab],
                )
            else:
                load_non_watch_table(
                    conn=conn,
                    cursor=cursor,
                    table_name=tab,
                    file=table_filenames[tab],
                    jk_cols=jk_cols[tab],
                    pgtypes=pgtypes[tab],
                    timestamp_col=timestamp_cols[tab],
                )

        # analyze
        cursor.execute("vacuum (full, analyze);")

        # setup the single table pointers
        for tab in baseline1ds:
            baseline1ds[tab].setup_single_table_pointer()

        # setup the black box
        for tab in tabs_for_baseline1d:
            if tab in tab_to_alias:
                tab_abbrev = tab_to_alias[tab]
            else:
                tab_abbrev = "".join(x[0] for x in tab.strip().split("_"))
            black_box.default_column_min_max_vals[tab] = {}
            for col in sel_cols[tab]:
                black_box.default_column_min_max_vals[tab][tab_abbrev + "." + col] = [
                    sel_cols[tab][col]["min"],
                    sel_cols[tab][col]["max"],
                ]
            black_box.default_predicate_templates[tab] = []
            for col in sel_cols[tab]:
                black_box.default_predicate_templates[tab].append(
                    tab_abbrev + "." + col
                )
                black_box.default_predicate_templates[tab].append(">")
                black_box.default_predicate_templates[tab].append("")
                black_box.default_predicate_templates[tab].append(
                    tab_abbrev + "." + col
                )
                black_box.default_predicate_templates[tab].append("<")
                black_box.default_predicate_templates[tab].append("")

        load_trainingset(
            trainingset_filename,
            len(tabs),
            tabs_for_baseline1d,
            sel_cols,
            baseline1ds,
            pgtypes,
            tab_to_alias,
        )

        load_log(
            conn,
            cursor,
            log_filename,
            output_prefix,
            len(tabs),
            sel_cols,
            tabs_for_baseline1d,
            baseline1ds,
            pgtypes,
        )

        for tab in tabs_for_baseline1d:
            baseline1ds[tab].clean()

        close_connection(conn=conn, cursor=cursor)
