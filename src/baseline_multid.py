from tqdm import tqdm
import time
import numpy as np
import black_box
import random
import json
import sys
import copy
from operation import TupleUpdateType, RangeSelection
import warnings
from connection import get_connection, close_connection
from error_metrics import get_average_q_error
from pg_hint_utility import get_two_way_join_info, inject_join_sel_and_get_cost, gen_final_hint, get_real_latency

warnings.filterwarnings("ignore")

class RoutineBaseline2D:
    def __init__(
        self,
        model_name=None,
        tabs_for_baseline2d=[],
        tab_to_alias=None,
        conn=None,
        cursor=None,
        query_pool_size=black_box.config["training_size"],
        sql=None,
        counting_sql=None,
        random_seed=None,
        based_on=None,
        X=None,
    ):
        self.model_name = model_name
        self.tabs_for_baseline2d = tabs_for_baseline2d
        assert len(tabs_for_baseline2d) in [2, 3, 4]

        self.tableset = copy.deepcopy(tabs_for_baseline2d)
        self.tableset.sort()
        for i in range(len(self.tableset)):
            if self.tableset[i] in tab_to_alias:
                self.tableset[i] = tab_to_alias[self.tableset[i]]
        self.tableset = tuple(self.tableset)

        self.conn = conn
        self.cursor = cursor

        self.dbname_prefix = ""

        self.query_pool_size = query_pool_size
        self.sql = sql
        assert self.sql is not None
        self.counting_sql = counting_sql
        assert self.counting_sql is not None

        if random_seed is not None:
            random.seed(random_seed)
        
        self.based_on = based_on
        assert self.based_on in ["tuples"]
        self.X = X
        assert isinstance(self.X, list)

        self.query_pool = [] # a tuple of (RangeSelection1, RangeSelection2)
        self._query_id = 0
        self.op_cnts = {"tuples": 0, "queries": 0}
        self.xid = 0

        self.sel_cols = {}
        self.attrs = {}

    def load_table(self, tab, file, sel_col, jk_cols, pgtype, timestamp_col=None):
        self.sel_cols[tab] = sel_col
        self.attrs[tab] = list(pgtype.keys())
        self.pgtype = pgtype

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

        self.cursor.execute(f"select count(*) from {dbname};")
        self.cur_table_size = self.cursor.fetchone()[0]

    def clean(self):
        # self.cursor.execute(f"drop table if exists {self.dbname};")
        # self.cursor.execute(f"drop index if exists idx_{self.dbname}_{self.sel_col};")
        self.cursor.close()
        self.conn.commit()
        self.conn.close()
    
    def setup_black_box(
        self,
        model_name,
        model_tables,
        model_joins,
    ):
        self.model_name = model_name
        self.model_tables = model_tables
        self.model_joins = model_joins

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
            black_box.retrain(
                self.model_name,
                *training_info
            )
            retrain_time += time.time() - retrain_start_time
            return check_time, True, info, retrain_time
        return check_time, False, None, None

    def _get_training_set(self):
        start_time = time.time()

        training_set = []
        for rss in self.query_pool:
            counting_q = self.counting_sql
            bounds = []
            for i, rs in enumerate(rss):
                # we only support selection on a single column in either table for now
                counting_q = counting_q\
                    .replace("{lb" + str(i + 1) + "}", str(rs.lb))\
                        .replace("{ub" + str(i + 1) + "}", str(rs.ub))
                bounds.append(rs.lb)
                bounds.append(rs.ub)

            self.cursor.execute(counting_q)
            training_set.append(tuple(bounds + [self.cursor.fetchone()[0]]))
        return (
            time.time() - start_time,
            (
                self.model_tables,
                self.model_joins,
                training_set,
            )
        )

    def insert(self, tab, attr_in_str):
        assert tab in self.tabs_for_baseline2d
        start_time = time.time()

        self.op_cnts["tuples"] += 1
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
            + ",".join(self.attrs[tab])
            + ") values ("
            + ",".join(["%s"] * len(self.attrs[tab]))
            + f") returning {self.sel_cols[tab]};"
        )
        self.cursor.execute(q, attr_vals)
        
        check_time, if_retrain, retrain_info, retrain_time = self.check_retrain()

        baseline_time = 0.
        if if_retrain:
            baseline_time += check_time
        info = f"time={round(time.time() - start_time, 6)},baseline_time={round(baseline_time, 6):.6f}"
        if if_retrain:
            info += retrain_info + f",retrain_time={round(retrain_time, 6):.6f}"
        return info

    def delete(self, tab, id):
        assert tab in self.tabs_for_baseline2d
        start_time = time.time()

        self.op_cnts["tuples"] += 1
        dbname = self.dbname_prefix + tab
        q = f"delete from {dbname} where row_id = {id} returning {self.sel_cols[tab]};"
        self.cursor.execute(q)

        check_time, if_retrain, retrain_info, retrain_time = self.check_retrain()

        baseline_time = 0.
        if if_retrain:
            baseline_time += check_time
        info = f"time={round(time.time() - start_time, 6)},baseline_time={round(baseline_time, 6):.6f}"
        if if_retrain:
            info += retrain_info + f",retrain_time={round(retrain_time, 6):.6f}"
        return info

    def insert_to_query_pool(self, list_of_rs, true_card):
        if true_card > 0:
            if len(self.query_pool) < self.query_pool_size:
                self.query_pool.append(tuple(list_of_rs))
            else:
                i = random.randint(0, self._query_id)
                if i < self.query_pool_size:
                    self.query_pool[i] = tuple(list_of_rs)
        self._query_id += 1

    def query(self, list_of_rs, true_card):
        start_time = time.time()
        flatten_list_of_rs = []
        for rs in list_of_rs:
            assert rs.table_name in self.tabs_for_baseline2d
            flatten_list_of_rs.append(rs.lb)
            flatten_list_of_rs.append(rs.ub)
        self.op_cnts["queries"] += 1

        model_pred = black_box.predict_one(
            self.model_name,
            flatten_list_of_rs,
            self.model_tables,
            self.model_joins,
        )
        baseline_time = time.time() - start_time
        
        q = self.sql
        for i, rs in enumerate(list_of_rs):
            q = q.replace("{lb" + str(i + 1) + "}", str(rs.lb))\
                .replace("{ub" + str(i + 1) + "}", str(rs.ub))

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
        self.insert_to_query_pool(list_of_rs, true_card)
        baseline_time += time.time() - start_time_2

        info = f"plan_cost={plan_cost}"
        info += f",execution_time={round(execution_time / 1000., 6):.6f}"
        info += f",baseline_time={round(baseline_time, 6):0.6f}"
        
        hint_wo_newline = inj_plan.replace('\n', '')
        info += f",plan={hint_wo_newline}"

        return (
            np.round(model_pred, 6),
            get_average_q_error([true_card], [model_pred]),
            info,
        )

def load_trainingset(
    trainingset_filename, 
    n_tabs, 
    tabs_for_baseline2d, 
    sel_cols, 
    pgtypes,
    baseline2d,
):
    print("Load Trainingset")
    with open(trainingset_filename, "r") as fin:
        tab_to_training_set = []

        lines = list(fin.readlines())

        for i in range(len(lines)):
            row = lines[i].strip().split(",")
            bounds = []
            for j in range(n_tabs):
                if row[1 + j] in tabs_for_baseline2d and row[1 + j] == tabs_for_baseline2d[len(bounds) // 2]:
                    tab = row[1 + j]
                    lb_id = 1 + n_tabs + 2 * j
                    ub_id = lb_id + 1
                    _col = list(sel_cols[tab].keys())[0]
                    if "/" in _col:
                        lb, ub = float(row[lb_id]), float(row[ub_id])
                    elif pgtypes[tab][_col].lower() == "real" or pgtypes[tab][_col].lower().startswith("decimal"):
                        lb, ub = float(row[lb_id]), float(row[ub_id])
                    elif pgtypes[tab][_col].lower() == "integer" or pgtypes[tab][_col].lower() == "smallint":
                        lb, ub = int(row[lb_id]), int(row[ub_id])
                    else:
                        raise NotImplementedError
                    bounds.append(lb)
                    bounds.append(ub)
            
            true_card_id = 1 + n_tabs * 4
            true_card = int(row[true_card_id])
            if (
                true_card >= 1
                and len(tab_to_training_set) < black_box.config["training_size"]
            ):
                tab_to_training_set.append(tuple(bounds + [true_card]))
                        
        print("Finish loading training set:")
        print(f"Training set size of [{tab}]: {len(tab_to_training_set)}")

        training_query_template = f"select * from " \
            + ", ".join(baseline2d.model_tables) \
                + " where " + " and ".join(baseline2d.model_joins)
        for i in range(len(baseline2d.model_tables)):
            training_query_template += \
                f" and {list(sel_cols[baseline2d.model_tables[i]].keys())[0]} between "\
                    + "{lb" + str(i + 1) + "} and {ub" + str(i + 1) + "}"
        training_query_template += ";"

        black_box.retrain(
            baseline2d.model_name,
            baseline2d.model_tables,
            baseline2d.model_joins,
            tab_to_training_set,
            training_query_template
        )

        print("Load query pool by training set")
        for bounds_and_card in tab_to_training_set:
            bounds = bounds_and_card[:-1]
            card = bounds_and_card[-1]
            list_of_rs = []
            for i in range(0, len(bounds), 2):
                list_of_rs.append(RangeSelection(
                    tabs_for_baseline2d[i // 2], 
                    bounds[i], 
                    bounds[i + 1]
                ))
            baseline2d.insert_to_query_pool(list_of_rs, card)
        print(f"Finish loading query pool ({len(baseline2d.query_pool)} items)")

def load_log(
    conn,
    cursor,
    log_filename,
    output_prefix,
    n_tabs,
    sel_cols,
    pgtypes,
    tabs_for_baseline2d,
    baseline2d,
):
    assert n_tabs in [2, 3, 4]
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
            if (last_op_type != op_type or last_tab != tab) and (last_tab not in tabs_for_baseline2d) and (last_op_type != "Q"):
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
                if t not in tabs_for_baseline2d:
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
                    ret += baseline2d.insert(t, field)
                elif _type == TupleUpdateType.DELETE:
                    ret += baseline2d.delete(t, int(field))
                total_tu_time += time.time() - start_tu_ts

            elif op_type == "Q":
                row = line.strip().split(",")
                two_rs = []
                for jj in range(n_tabs):
                    if row[1 + jj] in tabs_for_baseline2d and row[1 + jj] == tabs_for_baseline2d[len(two_rs)]:
                        tab = row[1 + jj]
                        lb_id = 1 + n_tabs + 2 * jj
                        ub_id = lb_id + 1
                        _col = list(sel_cols[tab].keys())[0]
                        if "/" in _col:
                            lb, ub = float(row[lb_id]), float(row[ub_id])
                        elif pgtypes[tab][_col].lower() == "real" or pgtypes[tab][_col].lower().startswith("decimal"):
                            lb, ub = float(row[lb_id]), float(row[ub_id])
                        elif pgtypes[tab][_col].lower() == "integer" or pgtypes[tab][_col].lower() == "smallint":
                            lb, ub = int(row[lb_id]), int(row[ub_id])
                        else:
                            raise NotImplementedError
                        two_rs.append(RangeSelection(tab, lb, ub))

                assert len(two_rs) in [2, 3, 4]
                true_card_id = 1 + n_tabs * 4
                true_card = int(row[true_card_id])
                start_q_ts = time.time()
                est, single_err, info = baseline2d.query(
                    two_rs,
                    true_card
                )
                total_q_time += time.time() - start_q_ts

                if ret[-1] != ":":
                    ret += ","
                ret += f"est=" + str(est) + ","
                ret += f"observed_err=" + str(single_err)

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
        join_to_watch=config["join_to_watch"]
        sql = config["sql"]
        counting_sql = config["counting_sql"]
        pgtypes = config["pgtypes"]

        update_based_on = config["update_based_on"]
        update_after = config["update_after"]
        # for example, update_based_on = "tuples" or "queries"
        # update_after = [X1, X2, X3, ...]
        # Xi means the number of tuple updates happen on any table in "join_to_watch"

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
                + "-join-".join(tabs) + "-after-" + "|".join([str(x) for x in update_after]) + f"-{update_based_on}" 
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

        # we only need to build RoutineBaseline2D for the two-way join
        tabs_for_baseline2d = list(sel_cols.keys())
        model_name = "-".join(tabs_for_baseline2d)
        baseline2d = RoutineBaseline2D(
            model_name=model_name,
            tabs_for_baseline2d=tabs_for_baseline2d,
            tab_to_alias=tab_to_alias,
            conn=conn,
            cursor=cursor,
            query_pool_size=black_box.config["training_size"],
            sql=sql,
            counting_sql=counting_sql,
            random_seed=2023,
            based_on=update_based_on,
            X=update_after,
        )

        for tab in tabs:
            if tab in tabs_for_baseline2d:
                baseline2d.load_table(
                    tab=tab,
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

        # setup the black box
        model_tables = []
        black_box.default_column_min_max_vals[model_name] = {}
        black_box.default_predicate_templates[model_name] = []
        for tab in sel_cols:
            tab_abbrev = "".join(x[0] for x in tab.strip().split("_"))
            model_tables.append(tab + " " + tab_abbrev)
            for col in sel_cols[tab]:
                black_box.default_column_min_max_vals[model_name][tab_abbrev + "." + col] = [
                    sel_cols[tab][col]["min"],
                    sel_cols[tab][col]["max"],
                ]

            for col in sel_cols[tab]:
                black_box.default_predicate_templates[model_name].append(
                    tab_abbrev + "." + col
                )
                black_box.default_predicate_templates[model_name].append(">")
                black_box.default_predicate_templates[model_name].append("")
                black_box.default_predicate_templates[model_name].append(
                    tab_abbrev + "." + col
                )
                black_box.default_predicate_templates[model_name].append("<")
                black_box.default_predicate_templates[model_name].append("")

        # set the model_tables and model_joins for baseline2d
        baseline2d.setup_black_box(
            model_name=model_name,
            model_tables=model_tables,
            model_joins=[join_to_watch],
        )

        load_trainingset(
            trainingset_filename=trainingset_filename,
            n_tabs=len(tabs),
            tabs_for_baseline2d=tabs_for_baseline2d,
            sel_cols=sel_cols,
            pgtypes=pgtypes,
            baseline2d=baseline2d,
        )

        load_log(
            conn=conn,
            cursor=cursor,
            log_filename=log_filename,
            output_prefix=output_prefix,
            n_tabs=len(tabs),
            sel_cols=sel_cols,
            pgtypes=pgtypes,
            tabs_for_baseline2d=tabs_for_baseline2d,
            baseline2d=baseline2d,
        )

        baseline2d.clean()
        close_connection(conn=conn, cursor=cursor)
