from operation import TupleUpdateType, RangeSelection, QueryInfo
from index import Table
from tqdm import tqdm
import time
import warnings
import black_box
import json
import sys
import copy
import random
import numpy as np
from error_metrics import get_average_q_error, get_percentile_q_error
from connection import get_connection, close_connection
from pg_hint_utility import get_single_table_pointer, gen_final_hint, inject_single_table_sels_and_get_cost, inject_true_cards_and_get_cost, inject_single_and_join_sels_and_get_cost, inject_multiple_single_table_sel_and_get_cost, get_real_latency, load_pg_join_subquery_info

warnings.filterwarnings("ignore")

################################## Duplicates of RoutineBaseline1D, ...., ##################################
# But do not execute the query until collaborate both the estimates
# So we implement pre_query() and post_query()

class RoutineBaseline1D:
    def __init__(
        self,
        name=None,
        conn=None,
        cursor=None,
        query_pool_size=black_box.config["training_size"],
        random_seed=None,
        based_on=None,
        X=None,
        info_id=None,
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

        if random_seed is not None:
            random.seed(random_seed)
        
        self.based_on = based_on
        assert self.based_on in ["tuples"]
        self.X = X
        assert self.X is None or isinstance(self.X, list)
        self.info_id = info_id
        assert self.info_id is not None

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

    def insert_to_query_pool(self, rs, true_card):
        if true_card > 0:
            if len(self.query_pool) < self.query_pool_size:
                self.query_pool.append(rs)
            else:
                i = random.randint(0, self._query_id)
                if i < self.query_pool_size:
                    self.query_pool[i] = rs
        self._query_id += 1

    # Output: model_pred, baseline_time
    def pre_query(self, rs):
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
        return model_pred, baseline_time
    
    def post_query(self, rs, true_card):
        start_time_2 = time.time()
        self.insert_to_query_pool(rs, true_card)
        baseline_time = time.time() - start_time_2

        return baseline_time

class RoutineBaseline2D:
    def __init__(
        self,
        tabs_for_baseline2d=[],
        tab_to_alias=None,
        conn=None,
        cursor=None,
        query_pool_size=black_box.config["training_size"],
        counting_sql=None,
        random_seed=None,
        based_on=None,
        X=None,
    ):
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
        self.counting_sql = counting_sql
        assert self.counting_sql is not None

        if random_seed is not None:
            random.seed(random_seed)
        
        self.based_on = based_on
        assert self.based_on in ["tuples"]
        self.X = X
        assert self.X is None or isinstance(self.X, list)

        self.query_pool = [] # a tuple of (RangeSelection1, RangeSelection2)
        self._query_id = 0
        self.op_cnts = {"tuples": 0, "queries": 0}
        self.xid = 0

        self.sel_cols = {}
        self.attrs = {}

    def load_table(self, tab, file, sel_col, jk_cols, pgtype, timestamp_col=None, skip_pg_modification=False):
        self.sel_cols[tab] = sel_col
        self.attrs[tab] = list(pgtype.keys())
        self.pgtype = pgtype

        if skip_pg_modification:
            return

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

    def insert(self, tab, attr_in_str, skip_pg_modification=False):
        assert tab in self.tabs_for_baseline2d
        start_time = time.time()

        self.op_cnts["tuples"] += 1

        if not skip_pg_modification:
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
        info = f"join_time={round(time.time() - start_time, 6):.6f},join_baseline_time={round(baseline_time, 6):.6f}"
        if if_retrain:
            info += retrain_info + f",join_retrain_time={round(retrain_time, 6):.6f}"
        return info

    def delete(self, tab, id, skip_pg_modification=False):
        assert tab in self.tabs_for_baseline2d
        start_time = time.time()

        self.op_cnts["tuples"] += 1
        
        if not skip_pg_modification:
            dbname = self.dbname_prefix + tab
            q = f"delete from {dbname} where row_id = {id} returning {self.sel_cols[tab]};"
            self.cursor.execute(q)

        check_time, if_retrain, retrain_info, retrain_time = self.check_retrain()

        baseline_time = 0.
        if if_retrain:
            baseline_time += check_time
        info = f"join_time={round(time.time() - start_time, 6):.6f},join_baseline_time={round(baseline_time, 6):.6f}"
        if if_retrain:
            info += retrain_info + f",join_retrain_time={round(retrain_time, 6):.6f}"
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

    # Output: model_pred, baseline_time
    def pre_query(self, list_of_rs):
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
        return model_pred, baseline_time

    def post_query(self, list_of_rs, true_card):
        start_time_2 = time.time()
        self.insert_to_query_pool(list_of_rs, true_card)
        baseline_time = time.time() - start_time_2

        return baseline_time

class Watcher1D:
    def __init__(
        self,
        name=None,
        conn=None,
        cursor=None,
        watchset_size=black_box.config["training_size"],
        random_seed=None,
        threshold=None,
        error_metric="90th",
        info_id=None,
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

        self.error_metric = error_metric
        assert error_metric in ["avg", "90th", "95th"]

        self.threshold = threshold
        if random_seed is not None:
            random.seed(random_seed)

        self.info_id = info_id

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
        
        if self.threshold is None:
            err = None
        else:
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

    def insert(self, table_name, attr_in_str, return_attrs=None):
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

        if return_attrs is None:
            q = (
                f"insert into {dbname} ("
                + ",".join(self.attrs)
                + ") values ("
                + ",".join(["%s"] * len(self.attrs))
                + f") returning {self.sel_col};"
            )
            self.cursor.execute(q, attr_vals)
            sel_val = self.cursor.fetchone()[0]
        else:
            assert isinstance(return_attrs, list)
            assert len(return_attrs) == 2
            assert return_attrs[1] == self.sel_col

            q = (
                f"insert into {dbname} ("
                + ",".join(self.attrs)
                + ") values ("
                + ",".join(["%s"] * len(self.attrs))
                + f") returning {return_attrs[0]}, {return_attrs[1]};"
            )
            self.cursor.execute(q, attr_vals)
            jk_val, sel_val = self.cursor.fetchone()
            
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
        
        if return_attrs is None:
            return info
        else:
            return info, jk_val, sel_val

    def delete(self, table_name, id, return_attrs=None):
        assert table_name == self.name
        start_time = time.time()

        dbname = self.dbname_prefix + self.name
        if return_attrs is None:
            q = f"delete from {dbname} where row_id = {id} returning {self.sel_col};"
            self.cursor.execute(q)
            sel_val = self.cursor.fetchone()[0]
        else:
            assert isinstance(return_attrs, list)
            assert len(return_attrs) == 2
            assert return_attrs[1] == self.sel_col
            q = f"delete from {dbname} where row_id = {id} returning {return_attrs[0]}, {return_attrs[1]};"
            self.cursor.execute(q)
            jk_val, sel_val = self.cursor.fetchone()

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
        
        if return_attrs is None:
            return info
        else:
            return info, jk_val, sel_val

    def get_error(self):
        true_cards = [self.watch_set[i][1] for i in range(len(self.watch_set))]
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

    def pre_query(self, rs):
        start_time = time.time()
        assert rs.table_name == self.name

        model_pred = black_box.predict_one(
            self.name,
            [rs.lb, rs.ub],
            [f"{self.name} {self.abbrev}"],
            [""],
        )
        watcher_time = time.time() - start_time
        return model_pred, watcher_time
    
    def post_query(self, rs, true_card, model_pred):
        start_time_2 = time.time()
        self.insert_to_watchset(rs, true_card, model_pred)
        watcher_time = time.time() - start_time_2
        return watcher_time

class Watcher2d:
    def __init__(
        self,
        conn=None,
        cursor=None,
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

    def load_table(self, tab, file, timestamp_col=None, skip_pg_modification=False):
        sel_col = list(self.sel_cols[tab].keys())[0]
        jk_cols = self.jk_cols[tab]
        pgtype = self.pgtypes[tab]

        dbname = self.dbname_prefix + tab

        if not skip_pg_modification:
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
        
        if self.threshold is None:
            err = None
        else:
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
                    
                info += "join_retrain_"
                err = self.get_error()
                retrain_time += time.time() - retrain_start_time
                if err > self.threshold:
                    info += (
                        f"failed=" + str(err) + ">" + str(self.threshold)
                    )
                    self.threshold *= 1.5
                    info += f",join_adjust_threshold=" + str(self.threshold) + ","
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

    def insert(self, tab, attr_in_str, skip_pg_modification=False, external_jk_val=None, external_sel_val=None):
        start_time = time.time()
        attrs = list(self.pgtypes[tab].keys())
        jk_to_watch = self.tab_to_jk_to_watch_and_idx[tab][0]
        sel_to_watch = self.tab_to_sel_to_watch_and_idx[tab][0]

        if not skip_pg_modification:
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
        else:
            jk_val, sel_val = external_jk_val, external_sel_val

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
                qid += 1

            m = self.tables[other_tab].query_bounds(jk_val, bounds)

            for qid, delta in m.items():
                self.watch_set[qid][2] += delta

        watcher_time = time.time() - watcher_start_time

        check_time, if_retrain, retrain_info, retrain_time = self.check_retrain()

        if if_retrain:
            watcher_time += check_time
        info = f"join_watchset_size={len(self.watch_set)},join_time={round(time.time() - start_time, 6):.6f},join_watcher_time={round(watcher_time, 6):.6f}"
        if if_retrain:
            info += retrain_info + f"join_retrain_time={round(retrain_time, 6):.6f}"
        return info

    def delete(self, tab, id, skip_pg_modification=False, external_jk_val=None, external_sel_val=None):
        start_time = time.time()
        jk_to_watch = self.tab_to_jk_to_watch_and_idx[tab][0]
        sel_to_watch = self.tab_to_sel_to_watch_and_idx[tab][0]

        if not skip_pg_modification:
            # delete from the database
            dbname = self.dbname_prefix + tab
            q = f"delete from {dbname} where row_id = {id} returning {jk_to_watch}, {sel_to_watch};"
            self.cursor.execute(q)
            jk_val, sel_val = self.cursor.fetchone()
        else:
            jk_val, sel_val = external_jk_val, external_sel_val

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
                    pass # To resolve a precision-related bug
                elif self.watch_set[i][cur].check(sel_val):
                    bounds.append((qid, (self.watch_set[i][other].lb, self.watch_set[i][other].ub)))
                qid += 1

            m = self.tables[other_tab].query_bounds(jk_val, bounds)
            for qid, delta in m.items():
                self.watch_set[qid][2] -= delta

        self.tables[tab].delete(id)
        watcher_time = time.time() - watcher_start_time

        check_time, if_retrain, retrain_info, retrain_time = self.check_retrain()

        if if_retrain:
            watcher_time += check_time
        info = f"join_watchset_size={len(self.watch_set)},join_time={round(time.time() - start_time, 6):.6f},join_watcher_time={round(watcher_time, 6):.6f}"
        if if_retrain:
            info += retrain_info + f"join_retrain_time={round(retrain_time, 6):.6f}"
        return info

    def pre_query(self, rs1, rs2):
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
        return model_pred, watcher_time
        
    def post_query(self, rs1, rs2, true_card, model_pred):
        start_time_2 = time.time()
        self.insert_to_watchset(rs1, rs2, true_card, model_pred)
        watcher_time = time.time() - start_time_2

        return watcher_time

class TwoTierWatcher1D:
    def __init__(
        self,
        name=None,
        conn=None,
        cursor=None,
        n_tabs_in_from=None,
        query_pool_size=black_box.config["training_size"], # this is the size of query_pool.
        training_size=black_box.config["training_size"],
        triggers=None,
        random_seed=None,
        execution_time_lb=None, # Watchset: execution_time >= execution_time_lb; If None, this check always fails;
        planning_time_ub=None, # Watchset: 1. if planning_time_ub is a 'int': planning_time <= planning_time_ub; 2. If None, this checks always fails; 3. if planning_time_ub is a percentage: planning_time <= execution_time * planning_time_ub
        # And we need to make sure that execution_time_lb >= planning_time_ub
        plancost_check_ratio=None, # Watchset: pc(pred) >= pc(true) * plancost_check_ratio; If None, this check always fails; And we need to ensure that plancost_check_ratio >= 1
        overlap_ratio_lb=None, # We will do the correction only when overlap_ratio, i.e., overlap_ratio >= overlap_ratio_lb; And we need to ensure that overlap_ratio in [0., 1.] 
        enable_lazy_tuple_updates=False, # If True: TwoTierWatcher1D will postpone the tuple updates till we really need them (a retraining is triggered, a refill is needed, different type of tuple updates show up)
        info_id=None,
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

        self.n_tabs_in_from = n_tabs_in_from

        self.dbname_prefix = ""
        self.query_pool_size = query_pool_size
        self.training_size = training_size
        if random_seed is not None:
            random.seed(random_seed)
        self.triggers = triggers

        self.execution_time_lb = execution_time_lb
        self.planning_time_ub = planning_time_ub
        if isinstance(self.planning_time_ub, str):
            assert self.planning_time_ub.endswith("%")
            try:
                self.planning_time_ub = float(self.planning_time_ub.split("%")[0]) / 100.
            except ValueError:
                print("planning_time_ub is not a percentage")
                assert True == False
            self.planning_time_ub_is_percentage = True
        else:
            self.planning_time_ub_is_percentage = False

        assert (self.execution_time_lb is None and self.planning_time_ub is None) \
            or (not self.planning_time_ub_is_percentage and self.execution_time_lb >= self.planning_time_ub) \
                or (self.planning_time_ub_is_percentage and self.planning_time_ub <= 100.)
        
        self.plancost_check_ratio = plancost_check_ratio
        assert self.plancost_check_ratio is None or self.plancost_check_ratio >= 1.
        self.overlap_ratio_lb = overlap_ratio_lb
        assert self.overlap_ratio_lb is not None and self.overlap_ratio_lb >= 0. and self.overlap_ratio_lb <= 1.

        self.enable_lazy_tuple_updates = enable_lazy_tuple_updates
        if self.enable_lazy_tuple_updates:
            self._update_type_to_flush = None # it will be either TupleUpdateType.INSERT or TupleUpdateType.DELETE
            self._updates_to_flush = None # if it is TupleUpdateType.INSERT, it will be a list of insertion info; otherwise, it will be a list [lb, ub] to delete.

        self.info_id = info_id

        self.query_pool = [] # [rs, ...]
        self.watch_set = {} # {rs -> QueryInfo}

        self.query_pool_query_id = 0

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

    def setup_single_table_pointer(self, qid):
        self.pointer = qid

    def _flush_tuple_updates(self):
        if self._update_type_to_flush is not None and self._updates_to_flush is not None:
            dbname = self.dbname_prefix + self.name
            if self._update_type_to_flush == TupleUpdateType.INSERT:
                assert isinstance(self._updates_to_flush, list)
                self.cursor.execute(f"insert into {dbname} ({', '.join(list(self.pgtype.keys()))}) values " + ','.join([x.decode('utf-8') for x in self._updates_to_flush]))
            elif self._update_type_to_flush == TupleUpdateType.DELETE:
                assert isinstance(self._updates_to_flush, list) and len(self._updates_to_flush) == 2
                self.cursor.execute(f"delete from {dbname} where row_id >= {self._updates_to_flush[0]} and row_id <= {self._updates_to_flush[1]};")
            else:
                raise NotImplementedError
            self._update_type_to_flush = None
            self._updates_to_flush = None

    def _add_tuple_update_to_flush(self, update_type, update):
        if update_type != self._update_type_to_flush:
            self._flush_tuple_updates()
        if self._update_type_to_flush is None:
            self._update_type_to_flush = update_type
            if update_type == TupleUpdateType.INSERT:
                self._updates_to_flush = []
            else:
                self._updates_to_flush = [None, None]
        if update_type == TupleUpdateType.INSERT:
            self._updates_to_flush.append(update)
        else:
            if self._updates_to_flush[0] is None or self._updates_to_flush[0] > update:
                self._updates_to_flush[0] = update
            if self._updates_to_flush[1] is None or self._updates_to_flush[1] < update:
                self._updates_to_flush[1] = update

    def _get_training_set(self):
        if self.enable_lazy_tuple_updates:
            self._flush_tuple_updates()
        
        start_time = time.time()

        # MSCN's inteface for retraining: [tables], [joins], [lb,ub,card]
        training_set = []

        if len(self.watch_set) > 0:
            all_rss_from_watchset = list(self.watch_set.keys())
            random.shuffle(all_rss_from_watchset)
            for i in range(min(len(self.watch_set), self.training_size)):
                rs = all_rss_from_watchset[i]
                training_set.append((rs.lb, rs.ub, self.watch_set[rs].true_card))
            
        if len(training_set) < self.training_size:
            remaining_size = self.training_size - len(training_set)
            sample = random.sample(self.query_pool, remaining_size)
            dbname = self.dbname_prefix + self.name
            for rs in sample:
                # we only support selection on a single column for now
                q = (
                    f"select count(*) from "
                    + dbname
                    + " where "
                    + f"{dbname}.{self.sel_col} >= {rs.lb}"
                    + " and "
                    + f"{dbname}.{self.sel_col} <= {rs.ub}"
                    + ";"
                )
                self.cursor.execute(q)
                training_set.append((rs.lb, rs.ub, self.cursor.fetchone()[0]))
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

        err = None
        cur_watchset_size = None

        for trigger in self.triggers:
            if trigger == "watchset_err":
                err = self.get_error()
            elif trigger == "watchset_size":
                cur_watchset_size = len(self.watch_set)
        check_time = time.time() - check_start_time

        if err is not None and err > self.threshold:
            retrain_time = 0.
            while err > self.threshold:
                training_set_collecting_time, training_info = self._get_training_set()
                retrain_time += training_set_collecting_time

                retrain_start_time = time.time()
                black_box.retrain(self.name, *training_info)
                for rs in self.watch_set:
                    self.watch_set[rs].pred = black_box.predict_one(
                        self.name,
                        [rs.lb, rs.ub],
                        [f"{self.name} {self.abbrev}"],
                        [""],
                    )
                
                if len(info) == 0:
                    info = ","
                    
                info += "retrain_"
                err = self.get_error()
                if err > self.threshold:
                    info += (
                        f"failed{self.info_id}=" + str(err) + ">" + str(self.threshold)
                    )
                    self.threshold *= 1.1
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
                retrain_time += time.time() - retrain_start_time
            refill_time = self.refill_watchset([(rs, self.watch_set[rs]) for rs in self.watch_set])
            return check_time + refill_time, True, info, retrain_time
        
        if cur_watchset_size is not None and cur_watchset_size >= self.size_budget:
            retrain_time = 0.
            refill_time = 0.
            offset = 0. # this is to correct the time needed for refillin the watchset
            while cur_watchset_size >= self.size_budget:
                training_set_collecting_time, training_info = self._get_training_set()
                retrain_time += training_set_collecting_time

                retrain_start_time = time.time()
                black_box.retrain(self.name, *training_info)
                for rs in self.watch_set:
                    self.watch_set[rs].pred = black_box.predict_one(
                        self.name,
                        [rs.lb, rs.ub],
                        [f"{self.name} {self.abbrev}"],
                        [""],
                    )
                if len(info) == 0:
                    info = ","
                    
                info += "retrain_"

                _ts = time.time()
                refill_time += self.refill_watchset([(rs, self.watch_set[rs]) for rs in self.watch_set])
                offset -= (time.time() - _ts)

                cur_watchset_size = len(self.watch_set)

                retrain_time += time.time() - retrain_start_time
                if cur_watchset_size >= self.size_budget:
                    info += (
                        f"failed{self.info_id}=" + str(cur_watchset_size) + ">" + str(self.size_budget)
                    )
                    self.size_budget *= 2
                    info += f",adjust_size_budget{self.info_id}=" + str(self.size_budget) + ","
                else:
                    info += (
                        f"successful{self.info_id}="
                        + str(cur_watchset_size)
                        + "<="
                        + str(self.size_budget)
                        + ","
                    )
                    break
            return check_time + refill_time, True, info, retrain_time + offset

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

        if not self.enable_lazy_tuple_updates:
            q = (
                f"insert into {dbname} ("
                + ",".join(self.attrs)
                + ") values ("
                + ",".join(["%s"] * len(self.attrs))
                + f") returning {self.sel_col};"
            )
            self.cursor.execute(q, attr_vals)
            sel_val = self.cursor.fetchone()[0]
        else:
            self.cursor.execute(f"select {self.sel_col} from {dbname};")
            sel_val = self.cursor.fetchone()[0]
            self._add_tuple_update_to_flush(
                update_type=TupleUpdateType.INSERT, 
                update=self.cursor.mogrify("(" + ",".join(['%s'] * len(self.pgtype)) + ")", attr_vals)
            )

        self.cur_table_size += 1

        watcher_start_time = time.time()
        if len(self.watch_set) > 0:
            for rs in self.watch_set:
                if (
                    sel_val is not None
                    and rs.lb <= sel_val
                    and sel_val <= rs.ub
                ):  # pass the predicate? card ++ 
                    self.watch_set[rs].true_card += 1
        watcher_time = time.time() - watcher_start_time

        if self.triggers is not None and len(self.triggers) > 0:
            check_time_and_refill_time, if_retrain, retrain_info, retrain_time = self.check_retrain()
        else:
            if_retrain = False
        
        if if_retrain:
            watcher_time += check_time_and_refill_time
        info = f"watchset_size{self.info_id}={len(self.watch_set)},time{self.info_id}={round(time.time() - start_time, 6)},watcher_time{self.info_id}={round(watcher_time, 6):.6f}"
        if if_retrain:
            info += retrain_info + f"retrain_time{self.info_id}={round(retrain_time, 6):.6f}"
        return info

    def delete(self, table_name, id):
        assert table_name == self.name
        start_time = time.time()

        dbname = self.dbname_prefix + self.name
        if not self.enable_lazy_tuple_updates:
            q = f"delete from {dbname} where row_id = {id} returning {self.sel_col};"
            self.cursor.execute(q)
            sel_val = self.cursor.fetchone()[0]
        else:
            self.cursor.execute(f"select {self.sel_col} from {dbname};")
            sel_val = self.cursor.fetchone()[0]
            self._add_tuple_update_to_flush(
                update_type=TupleUpdateType.DELETE,
                update=id,
            )

        self.cur_table_size -= 1

        watcher_start_time = time.time()
        if len(self.watch_set) > 0:
            for rs in self.watch_set:
                if (
                    sel_val is not None
                    and rs.lb <= sel_val
                    and sel_val <= rs.ub
                ):  # pass the predicate?
                    self.watch_set[rs].true_card -= 1
        watcher_time = time.time() - watcher_start_time

        if self.triggers is not None and len(self.triggers) > 0:
            check_time_and_refill_time, if_retrain, retrain_info, retrain_time = self.check_retrain()
        else:
            if_retrain = False
        
        if if_retrain:
            watcher_time += check_time_and_refill_time
        info = f"watchset_size{self.info_id}={len(self.watch_set)},time{self.info_id}={round(time.time() - start_time, 6)},watcher_time{self.info_id}={round(watcher_time, 6):.6f}"
        if if_retrain:
            info += retrain_info + f"retrain_time{self.info_id}={round(retrain_time, 6):.6f}"
        return info

    def get_error(self):
        true_cards = []
        preds = []
        for query_info in self.watch_set.values():
            true_cards.append(query_info.true_card)
            preds.append(query_info.pred)
        if len(true_cards) == 0:
            return 0
        if self.error_metric == "avg":
            return get_average_q_error(true_cards, preds)
        elif self.error_metric == "95th":
            return get_percentile_q_error(true_cards, preds, 95)
        elif self.error_metric == "25th":
            return get_percentile_q_error(true_cards, preds, 25)
        elif self.error_metric == "50th":
            return get_percentile_q_error(true_cards, preds, 50)
        else:
            raise NotImplementedError

    def _check_the_cache(self, rs):
        max_overlap_ratio, which = None, None

        for watch_rs in self.watch_set:
            len_intersection = (max(0, min(rs.ub, watch_rs.ub) - max(rs.lb, watch_rs.lb)))
            len_union = (max(rs.ub, watch_rs.ub) - min(rs.lb, watch_rs.lb))
            # length of intersection / length of union
            if len_union == 0.:
                overlap_ratio = 1.0
            else:
                overlap_ratio = 1.0 * len_intersection / len_union 
            # 0 means non-overlapping at all; 1 means identical
            if which is None or max_overlap_ratio < overlap_ratio:
                which = watch_rs
                max_overlap_ratio = overlap_ratio
        
        answer = black_box.predict_one(
            self.name,
            [rs.lb, rs.ub],
            [f"{self.name} {self.abbrev}"],
            [""],
        )
        pred = copy.copy(answer)

        if which is not None and max_overlap_ratio >= self.overlap_ratio_lb:
            # let's do the correction: replace the overlap with the true card
            # answer += 1. * (max(0, min(rs.ub, which.ub) - max(rs.lb, which.lb))) / (which.ub - which.lb) * (self.watch_set[which].true_card - self.watch_set[which].pred)

            answer = max(0, answer - black_box.predict_one(self.name, [max(rs.lb, which.lb), min(rs.ub, which.ub)], [f"{self.name} {self.abbrev}"], [""])) 
            if which.ub == which.lb:
                answer += 1. * self.watch_set[which].true_card
            else:
                answer += 1. * (max(0, min(rs.ub, which.ub) - max(rs.lb, which.lb))) / (which.ub - which.lb) * self.watch_set[which].true_card
            answer = round(answer)
            answer = max(0, min(answer, self.cur_table_size))

        return max_overlap_ratio, answer, pred, which

    def _load_pg_join_subquery_info(self):
        tableset_to_joinid = {}
        tableset_to_sel = {}
        tableset_to_rawrows = {}
        tableset_to_estrows = {}
        tableset_to_outerrel = {}
        min_join_qid = None
        max_join_qid = None

        with open("/winhomes/yl762/watcher_data/join_est_record_job.txt", "r") as test_fin:
            lines = list(test_fin.readlines())
            query_id = None
            relations = []
            outerrel = []
            selectivity = None
            est_rows = None
            raw_rows = None
            for line_id in range(len(lines)):
                line = lines[line_id].strip()
                if line.startswith("query:"):
                    query_id = int(line.split(" ")[-1])
                    if min_join_qid is None or min_join_qid > query_id:
                        min_join_qid = query_id
                    if max_join_qid is None or max_join_qid < query_id:
                        max_join_qid = query_id
                    relations = []
                    outerrel = []
                    selectivity = None
                    est_rows = None
                    raw_rows = None
                elif line.startswith("RELOPTINFO"):
                    if len(relations) > 0:
                        # the outer part
                        outerrel.extend(line.split("(")[1].split(")")[0].split(" "))
                    relations.extend(line.split("(")[1].split(")")[0].split(" "))
                elif line.startswith("Selectivity"):
                    selectivity = float(line.split(" ")[-1])
                elif line.startswith("Estimated Rows"):
                    est_rows = int(float(line.split(": ")[-1]))
                elif line.startswith("Raw Rows"):
                    raw_rows = int(float(line.split(": ")[-1]))
                elif len(line) == 0 and query_id is not None and (self.abbrev in relations or self.name in relations) and selectivity is not None:
                    relations.sort()
                    tableset = tuple(relations)
                    outerrel.sort()


                    tableset_to_joinid[tableset] = query_id
                    tableset_to_sel[tableset] = selectivity
                    tableset_to_rawrows[tableset] = raw_rows
                    tableset_to_estrows[tableset] = est_rows
                    tableset_to_outerrel[tableset] = tuple(outerrel)

                    query_id = None
                    relations = []
                    outerrel = []
                    selectivity = None
                    est_rows = None
                    raw_rows = None
    
        return tableset_to_joinid, tableset_to_sel, tableset_to_rawrows, tableset_to_estrows, min_join_qid, max_join_qid, tableset_to_outerrel

    def _debug_check_hint(self, hint_to_inject):
        assert len([line for line in hint_to_inject.split('\n') if line.strip() != '']) == self.n_tabs_in_from + (self.n_tabs_in_from - 1) + 1 + 1 
        # N scan methods, (N - 1) join methods, 1 for header, 1 for leading, i.e., join order and join direction

    def pre_query(self, rs):
        if self.enable_lazy_tuple_updates:
            self._flush_tuple_updates()

        start_time = time.time()
        assert rs.table_name == self.name

        self._overlap_ratio, self._answer, self._model_pred, self._which = self._check_the_cache(rs)
        watcher_time = time.time() - start_time

        return self._answer, watcher_time

    def post_query(
        self, 
        rs, 
        true_card, 
        q, 
        pg_plan_cost, 
        execution_time, 
        planning_time,
        tableset_to_truecard,
    ): #TODO
        if rs in self.watch_set:
            hit = True
        else:
            hit = False

        if self._overlap_ratio is not None and self._overlap_ratio >= self.overlap_ratio_lb:
            correction = True
        else:
            correction = False

        # # inject the "answer" and get the plan info
        # _, join_order, scan_mtd = inject_single_table_sels_and_get_cost(
        #     pointer=self.pointer,
        #     sel=self._answer / self.cur_table_size,
        #     n_tabs_in_from=self.n_tabs_in_from,
        #     q=q,
        #     hint=None,
        #     plan_info=True,
        # ) # this plan_cost if useful only when answer == true_card, otherwise we need to pay extra 1x planning_time to cost the plan

        # # construct the plan by the info
        # hint = gen_final_hint(scan_mtd=scan_mtd, str=join_order)
        # # self._debug_check_hint(hint)
        
        # tableset_to_joinid, tableset_to_sel, tableset_to_rawrows, tableset_to_estrows, min_join_qid, max_join_qid, tableset_to_outerrel = self._load_pg_join_subquery_info()

        # flag_for_injecting_join_sels = False
        # if self.enable_correct_the_joins \
        #     and self._overlap_ratio is not None \
        #         and self._overlap_ratio >= self.overlap_ratio_lb \
        #             and self._which is not None \
        #                 and self.watch_set[self._which].tableset_to_correction is not None:
        #     n_single = min_join_qid
        #     n_all = max_join_qid + 1

        #     pointers = [self.pointer]
        #     single_sels = [self._answer / self.cur_table_size]
        #     join_sels = []
        #     tableset_to_correctedrows = {}
        #     for tableset, factor in self.watch_set[self._which].tableset_to_correction.items():
        #         if tableset in tableset_to_joinid and len(tableset) == self.correct_exactly_ntabsjoin:
        #             pointers.append(tableset_to_joinid[tableset])
        #             new_join_sel = max(min(tableset_to_sel[tableset] * factor, 1.0), 0.0)
        #             join_sels.append(new_join_sel)
        #             tableset_to_correctedrows[tableset] = round(tableset_to_rawrows[tableset] * new_join_sel)

        #     if len(join_sels) > 0:
        #         _, corrected_join_order, corrected_scan_mtd = inject_single_and_join_sels_and_get_cost(
        #             pointers=pointers,
        #             single_sels=single_sels,
        #             n_single=n_single,
        #             join_sels=join_sels,
        #             n_all=n_all,
        #             q=q,
        #             hint=None,
        #             plan_info=True,
        #         ) 
        #         flag_for_injecting_join_sels = True
        #         # get the "corrected" plan
        #         corrected_hint = gen_final_hint(scan_mtd=corrected_scan_mtd, str=corrected_join_order)
        #         self._debug_check_hint(corrected_hint)

        # # lets run it!    
        # if self.enable_correct_the_joins:
        #     pg_plan_cost, execution_time, planning_time, tableset_to_truecard = get_real_latency(
        #         sql=q,
        #         hint=corrected_hint if flag_for_injecting_join_sels else hint, # if we have corrected plan, we use the correct one; otherwise, use the one by single-card injection
        #         extract_plan_info=True,
        #         focus_on=[self.abbrev, self.name],
        #         enable_join_sel_injection=flag_for_injecting_join_sels,
        #         debug_print_the_monitored_part=self.debug_print_the_monitored_part,
        #     ) # the first item is "plan cost" based on Postgres's own stats, but it is not the plan cost based on the injected cardinality
        #     tableset_to_correction = {}
        #     for tableset in tableset_to_truecard:
        #         if tableset in tableset_to_estrows and len(tableset) == self.correct_exactly_ntabsjoin:
        #             tableset_to_correction[tableset] = tableset_to_truecard[tableset] / tableset_to_estrows[tableset]
        # else:
        #     pg_plan_cost, execution_time, planning_time, tableset_to_truecard = get_real_latency(
        #         sql=q,
        #         hint=hint,
        #         extract_plan_info=True,
        #         focus_on=[self.abbrev, self.name],
        #         debug_print_the_monitored_part=self.debug_print_the_monitored_part,
        #     ) # the first item is "plan cost" based on Postgres's own stats, but it is not the plan cost based on the injected cardinality
        #     tableset_to_correction = None

        # if self.enable_correct_the_joins and correction and which is not None and tableset_to_correction is not None:
        #     self.watch_set[which].merge_new_join_correction(tableset_to_correction)

        ######## TODO: !!!! this is just for single table correction
        tableset_to_correction = None

        watcher_time = 0.
        start_time = time.time()
        # let's see if we have a new member for the watchset
        query_info = QueryInfo(
            sql=q,
            true_card=true_card,
            pred=self._model_pred,
            plan_cost=pg_plan_cost,
            execution_time=execution_time,
            execution_time_based_on_model_pred=None,
            planning_time=planning_time,
            pc_pred=None,
            pc_true=None,
            tableset_to_correction=tableset_to_correction,
        )
        if hit:
            del self.watch_set[rs]

        checks_info = [
            "fail_long_exectime_but_short_optime_check",
            "fail_plan_cost_check",
            "fail_extrapolated_exectime_check",
            "pass",
        ]
        checks_id = 0

        if self.pass_long_exectime_but_short_optime_check(query_info):
            checks_id += 1

            # get the plan by model_pred
            inj_start_ts = time.time()
            _, model_pred_inj_join_order, model_pred_inj_scan_method = inject_single_table_sels_and_get_cost(
                pointer=self.pointer,
                sel=query_info.pred / self.cur_table_size,
                n_tabs_in_from=self.n_tabs_in_from,
                q=q,
                hint=None,
                plan_info=True,
            )
            model_pred_inj_plan = gen_final_hint(model_pred_inj_join_order, model_pred_inj_scan_method)
            # self._debug_check_hint(model_pred_inj_plan)
            watcher_time = watcher_time - (time.time() - inj_start_ts) + (planning_time / 1000.) # here we replace the time cost for "injection" with 1x planning_time
            #############################################

            # get the plan by true_card
            inj_start_ts = time.time()
            _, true_card_inj_join_order, true_card_inj_scan_method = inject_single_table_sels_and_get_cost(
                pointer=self.pointer,
                sel=query_info.true_card / self.cur_table_size,
                n_tabs_in_from=self.n_tabs_in_from,
                q=q,
                hint=None,
                plan_info=True,
            )
            true_card_tableset_to_joinid, _, true_card_tableset_to_rawrows, true_card_tableset_to_estrows, true_card_min_join_qid, true_card_max_join_qid, true_card_tableset_to_outerrel = self._load_pg_join_subquery_info()
            true_card_inj_plan = gen_final_hint(true_card_inj_join_order, true_card_inj_scan_method)
            # self._debug_check_hint(true_card_inj_plan)
            watcher_time = watcher_time - (time.time() - inj_start_ts) + (planning_time / 1000.) # here we replace the time cost for "injection" with 1x planning_time
            #############################################

            # we cost both plans by true_card
            inj_start_ts = time.time()
            query_info.pc_pred = inject_true_cards_and_get_cost(
                pointer=self.pointer,
                sel=query_info.true_card / self.cur_table_size,
                join_pointer=true_card_tableset_to_joinid,
                join_true_cards=tableset_to_truecard,
                join_est_rows=true_card_tableset_to_estrows,
                join_raw_rows=true_card_tableset_to_rawrows,
                join_outerrel=true_card_tableset_to_outerrel,
                n_single=true_card_min_join_qid,
                n_all=true_card_max_join_qid + 1,
                q = q,
                hint=model_pred_inj_plan,
                plan_info=False,
            )
            watcher_time = watcher_time - (time.time() - inj_start_ts) + (planning_time / 1000.) # here we replace the time cost for "injection" with 1x planning_time

            inj_start_ts = time.time()
            query_info.pc_true = inject_true_cards_and_get_cost(
                pointer=self.pointer,
                sel=query_info.true_card / self.cur_table_size,
                join_pointer=true_card_tableset_to_joinid,
                join_true_cards=tableset_to_truecard,
                join_est_rows=true_card_tableset_to_estrows,
                join_raw_rows=true_card_tableset_to_rawrows,
                join_outerrel=true_card_tableset_to_outerrel,
                n_single=true_card_min_join_qid,
                n_all=true_card_max_join_qid + 1,
                q = q,
                hint=true_card_inj_plan,
                plan_info=False,
            )
            # we extroplate the excution_time_based_on_model_pred, based on the current plan cost and current plan execution time
            query_info.execution_time_based_on_model_pred = query_info.execution_time / query_info.plan_cost * query_info.pc_pred
            watcher_time = watcher_time - (time.time() - inj_start_ts) + (planning_time / 1000.) # here we replace the time cost for "injection" with 1x planning_time
            #############################################

            if self.pass_cost_check(query_info):
                checks_id += 1
                if self.pass_execution_time_check(query_info):
                    checks_id += 1  
                    self.watch_set[rs] = query_info

        # update query_pool
        self.insert_to_query_pool(rs)
        watcher_time += time.time() - start_time

        info = ""
        if query_info.execution_time_based_on_model_pred is None:
            info += f",expected_execution_time_based_on_pred{self.info_id}=None"
        else:
            info += f",expected_execution_time_based_on_pred{self.info_id}={round(query_info.execution_time_based_on_model_pred / 1000., 6):.6f}"
        if query_info.pc_true is None:
            info += f",pc_true{self.info_id}=None"
        else:
            info += f",pc_true{self.info_id}={query_info.pc_true}"
        if query_info.pc_pred is None:
            info += f",pc_pred{self.info_id}=None"
        else:
            info += f",pc_pred{self.info_id}={query_info.pc_pred}"
        info += f",sensitivity_check={self.info_id}={checks_info[checks_id]}"
        if self._overlap_ratio is None: 
            info += f",max_overlap_ratio{self.info_id}={None},correction{self.info_id}?={False}"
        else:
            info += f",max_overlap_ratio{self.info_id}={round(self._overlap_ratio, 2):.2f},correction{self.info_id}?={correction}"
            if correction:
                info += f",pred{self.info_id}={self._model_pred}"
                if self._which is not None:
                    info += f",corrected_by{self.info_id}={self._which.lb}-{self._which.ub}"

        return watcher_time, info
    
    def insert_to_query_pool(self, rs):
        if len(self.query_pool) < self.query_pool_size:
            self.query_pool.append(rs)
        else:
            i = random.randint(0, self.query_pool_query_id)
            if i < self.query_pool_size:
                self.query_pool[i] = rs
        self.query_pool_query_id += 1

    def pass_long_exectime_but_short_optime_check(self, query_info):
        if self.planning_time_ub is None:
            return False
        if self.execution_time_lb is None:
            return False
        assert query_info.execution_time is not None
        assert query_info.planning_time is not None
        
        if query_info.execution_time < self.execution_time_lb / self.plancost_check_ratio:
            # because we have another check on query_info.execution_time_based_on_model_pred
            # so we approximately want query_info.execution_time >= self.execution_time_lb / self.plancost_check_ratio
            # meaning that we don't want to let those queries which are "relatively fast" to pass this check
            return False
        
        if not self.planning_time_ub_is_percentage:
            if query_info.planning_time > self.planning_time_ub:
                return False
        else:
            if query_info.planning_time > query_info.execution_time * self.planning_time_ub:
                return False
        return True
    
    def pass_execution_time_check(self, query_info):
        if self.execution_time_lb is None:
            return False
        assert query_info.execution_time_based_on_model_pred is not None
        return query_info.execution_time_based_on_model_pred >= self.execution_time_lb
    
    def pass_cost_check(self, query_info):
        if self.plancost_check_ratio is None:
            return False
        assert query_info.pc_true is not None
        assert query_info.pc_pred is not None

        return query_info.pc_pred >= query_info.pc_true * self.plancost_check_ratio

    def refill_watchset(self, list_of_rs_and_query_info):
        if self.enable_lazy_tuple_updates:
            self._flush_tuple_updates()

        # we run this function only at the beginning (after we get the first model) or right after we retrain the model
        # for this implementation: for every query that passes the time check, we collect the new prediction and re-computes their pc(true) and pc(pred)
        refill_start_time = time.time()
        offset = 0. # the offset caused by replacing TimeCost("inject_...") with 1xplanning_time

        self.watch_set = {}
        qualified = []
        
        for _item in list_of_rs_and_query_info:
            rs, query_info = _item[0], _item[1]
            q = query_info.sql

            if self.pass_long_exectime_but_short_optime_check(query_info):
                # get prediction
                query_info.pred = black_box.predict_one(
                    self.name,
                    [rs.lb, rs.ub],
                    [f"{self.name} {self.abbrev}"],
                    [""],
                )
                
                inj_start_ts = time.time()
                query_info.pc_true = inject_single_table_sels_and_get_cost(
                    pointer=self.pointer,
                    sel=query_info.true_card / self.cur_table_size,
                    n_tabs_in_from=self.n_tabs_in_from,
                    q=q,
                    hint=None,
                    plan_info=False,
                )
                offset += (query_info.planning_time / 1000.) - (time.time() - inj_start_ts) # replace TimeCost("inject_...") with 1x planning_time

                # 1. get the join order and scan method for the plan generated by injecting query_info.pred
                inj_start_ts = time.time()
                _, inj_join_order, inj_scan_method = inject_single_table_sels_and_get_cost(
                    pointer=self.pointer,
                    sel=query_info.pred / self.cur_table_size,
                    n_tabs_in_from=self.n_tabs_in_from,
                    q=q,
                    hint=None,
                    plan_info=True,
                )
                #2. generate the hint
                inj_hint = gen_final_hint(inj_join_order, inj_scan_method)
                # self._debug_check_hint(inj_hint)
                offset += (query_info.planning_time / 1000.) - (time.time() - inj_start_ts) # replace TimeCost("inject_...") with 1x planning_time
                #3. cost it by query_info.true_card
                inj_start_ts = time.time()
                query_info.pc_pred = inject_single_table_sels_and_get_cost(
                    pointer=self.pointer,
                    sel=query_info.true_card / self.cur_table_size,
                    n_tabs_in_from=self.n_tabs_in_from,
                    q=q,
                    hint=inj_hint,
                    plan_info=False,
                )
                offset += (query_info.planning_time / 1000.) - (time.time() - inj_start_ts) # replace TimeCost("inject_...") with 1x planning_time

                query_info.execution_time_based_on_model_pred = query_info.execution_time / query_info.plan_cost * query_info.pc_pred
                if self.pass_execution_time_check(query_info) and self.pass_cost_check(query_info):
                    qualified.append((rs, query_info))
            
        for i in range(len(qualified)):
            self.watch_set[qualified[i][0]] = qualified[i][1]

        refill_time = time.time() - refill_start_time + offset
        return refill_time 

#########################################################################################################

class JoinCardCorrector:
    def __init__(self, n_tabs, overlap_ratio):
        self.n_tabs = n_tabs
        assert len(overlap_ratio) == n_tabs
        self.overlap_ratio = overlap_ratio # {tab : overlap_ratio}
        self.correction_factors = {} # {(rs1, rs2, ...) : {tableset : correction_factor}}
    def insert(self, rss, factors):
        assert len(rss) == self.n_tabs
        if tuple(rss) not in self.correction_factors:
            self.correction_factors[tuple(rss)] = factors
        else:
            for tableset, factor in factors.items():
                self.correction_factors[tuple(rss)][tableset] = factor
    def _compute_overlap_ratio(self, rs1, rs2):
        len_intersection = (max(0, min(rs1.ub, rs2.ub) - max(rs1.lb, rs2.lb)))
        len_union = (max(rs1.ub, rs2.ub) - min(rs1.lb, rs2.lb))
        if len_union == 0:
            return 1.0
        else:
            return 1.0 * len_intersection / len_union
    def search(self, rss):
        assert len(rss) == self.n_tabs
        _key = tuple(rss)
        if _key in self.correction_factors:
            return self.correction_factors[_key]
        else:
            max_overlap_ratio, which = None, None
            for k in self.correction_factors:
                cur_max_overlap_ratio = None
                for i in range(self.n_tabs):
                    if k[i] is None:
                        continue
                    assert k[i].table_name == rss[i].table_name
                    cur_dimension_overlap_ratio = self._compute_overlap_ratio(k[i], rss[i])
                    if cur_dimension_overlap_ratio >= self.overlap_ratio[k[i].table_name]:
                        if cur_max_overlap_ratio is None or cur_max_overlap_ratio < cur_dimension_overlap_ratio:
                            cur_max_overlap_ratio = cur_dimension_overlap_ratio
                if cur_max_overlap_ratio is not None:
                    if max_overlap_ratio is None or cur_max_overlap_ratio > max_overlap_ratio:
                        max_overlap_ratio = cur_max_overlap_ratio
                        which = k
            if which is None:
                return None
            else:
                return self.correction_factors[which]
            
#########################################################################################################

def load_trainingset(
    trainingset_filename, 
    sql,
    n_tabs, 
    tabs_with_sel, 
    sel_cols, 
    pgtypes,
    tab_to_alias,
    deployment,
    deployments, # model_name -> deployment
    two_way_model_name, 
    two_way_model_tables, 
    two_way_model_joins,
    watcher_init_theta=None,
    twotier_triggers=None,
):
    assert deployment != "watcher" or watcher_init_theta is not None
    assert deployment != "twotier" or twotier_triggers is not None

    print("Load Trainingset")
    with open(trainingset_filename, "r") as fin:
        two_way_training_set = []
        tab_to_single_table_training_set = {}
        for tab in tabs_with_sel:
            tab_to_single_table_training_set[tab] = []
        
        if deployment == "twotier":
            query_info = {
                tab : [] for tab in tabs_with_sel
            }

        lines = list(fin.readlines())

        for i in range(len(lines)):
            row = lines[i].strip().split(",")
            bounds = []
            query_info_id = {} # tab -> id
            for j in range(n_tabs):
                if row[1 + j] in tabs_with_sel:
                    tab = row[1 + j]
                    lb_id = 1 + n_tabs + 2 * j
                    ub_id = lb_id + 1
                    single_tab_card_id = 1 + n_tabs * 3 + j

                    if deployment == "twotier":
                        cost_id = 3 + n_tabs * 4
                        latency_id = cost_id + 1
                        planning_time_id = cost_id + 2

                    _col = list(sel_cols[tab].keys())[0]
                    single_tab_card = int(row[single_tab_card_id])

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

                    if (
                        single_tab_card >= 1
                        and len(tab_to_single_table_training_set[tab]) < black_box.config["training_size"]
                    ):
                        tab_to_single_table_training_set[tab].append((lb, ub, single_tab_card))

                    if deployment == "twotier":
                        rs = RangeSelection(tab, lb, ub)
                        rs.update_by(sel_cols[tab][_col]["min"], sel_cols[tab][_col]["max"])
                        query_info[tab].append(
                            [
                                rs, 
                                QueryInfo(
                                    sql=None,
                                    true_card=int(row[single_tab_card_id]),
                                    pred=None,
                                    plan_cost=float(row[cost_id]),
                                    execution_time=float(row[latency_id]),
                                    execution_time_based_on_model_pred=None,
                                    planning_time=float(row[planning_time_id]),
                                    pc_pred=None,
                                    pc_true=None,
                                )
                            ]
                        )
                        query_info_id[tab] = len(query_info[tab]) - 1

            if two_way_model_name is not None:
                assert len(bounds) == n_tabs * 2
                two_way_true_card_id = 1 + n_tabs * 4
                two_way_true_card = int(row[two_way_true_card_id])
                if (
                    two_way_true_card >= 1
                    and len(two_way_training_set) < black_box.config["training_size"]
                ):
                    two_way_training_set.append(tuple(bounds + [two_way_true_card]))
            
            if deployment == "twotier":
                assert len(query_info_id) in [2, 3, 4]
                cur_sql = copy.deepcopy(sql)
                for tab_id, tab in enumerate(tabs_with_sel):
                    cur_sql = cur_sql.replace(
                        "{lb" + str(tab_to_info_id[tab]) + "}",
                        str(bounds[tab_id * 2])
                    ).replace(
                        "{ub" + str(tab_to_info_id[tab]) + "}",
                        str(bounds[tab_id * 2 + 1])
                    )
                for tab in tabs_with_sel:
                    query_info[tab][query_info_id[tab]][1].sql = cur_sql

        print("Finish loading training set:")
        for tab in tab_to_single_table_training_set:
            print(f"Training set size of [{tab}]: {len(tab_to_single_table_training_set[tab])}")
            if tab in tab_to_alias:
                tab_abbrev = tab_to_alias[tab]
            else:
                tab_abbrev = "".join(x[0] for x in tab.strip().split("_"))
            black_box.retrain(
                tab,
                [f"{tab} {tab_abbrev}"],
                [""],
                tab_to_single_table_training_set[tab],
                f"select * from {tab} {tab_abbrev} where {tab_abbrev}.{list(sel_cols[tab].keys())[0]} > {{lb1}} and {tab_abbrev}.{list(sel_cols[tab].keys())[0]} <{{ub1}};",
            )
        
        if two_way_model_name is not None:
            print(f"Training set size of [{two_way_model_name}]: {len(two_way_training_set)}")
            two_way_training_query_template = f"select * from " \
                + ", ".join(two_way_model_tables) \
                    + " where " + " and ".join(two_way_model_joins)
            for jj in range(len(two_way_model_tables)):
                two_way_training_query_template += \
                    f" and {list(sel_cols[two_way_model_tables[jj].split(' ')[0]].keys())[0]} between "\
                        + "{lb" + str(jj + 1) + "} and {ub" + str(jj + 1) + "}"
            two_way_training_query_template += ";"
            black_box.retrain(
                two_way_model_name,
                two_way_model_tables,
                two_way_model_joins,
                two_way_training_set,
                two_way_training_query_template,
            )

        if deployment == "routinely":
            print("Load query pool by training set")
            for tab in tab_to_single_table_training_set:
                ii = 0
                for lb, ub, card in tab_to_single_table_training_set[tab]:
                    # note that we only support selection on one column for now
                    _col = list(sel_cols[tab].keys())[0]
                    rs = RangeSelection(tab, lb, ub)
                    rs.update_by(sel_cols[tab][_col]["min"], sel_cols[tab][_col]["max"])
                    deployments[tab].insert_to_query_pool(rs, card)
                    ii += 1
                print(f"Finish loading query pool for [{tab}] ({len(deployments[tab].query_pool)} items)")
            for bounds_and_card in two_way_training_set:
                bounds = bounds_and_card[:-1]
                card = bounds_and_card[-1]
                list_of_rs = []
                for jj in range(0, len(bounds), 2):
                    list_of_rs.append(RangeSelection(
                        tabs_with_sel[jj // 2], 
                        bounds[jj], 
                        bounds[jj + 1]
                    ))
                deployments[two_way_model_name].insert_to_query_pool(
                    list_of_rs, card
                )
            print(f"Finish loading query pool for [{two_way_model_name}] ({len(deployments[two_way_model_name].query_pool)} items)")
        elif deployment == "watcher":
            print("Load watchset by training set")
            tab_to_model_preds = {}
            for tab in tab_to_single_table_training_set:
                if tab in tab_to_alias:
                    tab_abbrev = tab_to_alias[tab]
                else:
                    tab_abbrev = "".join(x[0] for x in tab.strip().split("_"))
                # get the preds for watchset
                tab_to_model_preds[tab] = [
                    black_box.predict_one(
                        tab,
                        [x[0], x[1]],
                        [f"{tab} {tab_abbrev}"],
                        [""],
                    )
                    for x in tab_to_single_table_training_set[tab]
                ] 

                # get the init theta
                if watcher_init_theta[tab] in ["90th", "95th"]:
                    pth = watcher_init_theta[tab]
                    deployments[tab].threshold = get_percentile_q_error(
                        [x[2] for x in tab_to_single_table_training_set[tab]],
                        tab_to_model_preds[tab],
                        int(watcher_init_theta[tab].rstrip("th")),
                    )
                    print(f"Training set {pth}-percentile relative error of [{tab}]: {deployments[tab].threshold}")
                    watcher_init_theta[tab] = deployments[tab].threshold
                else:
                    assert watcher_init_theta[tab] is None or isinstance(watcher_init_theta[tab], float)
                

                # load the watchset
                ii = 0
                for lb, ub, card in tab_to_single_table_training_set[tab]:
                    # note that we only support selection on one column for now
                    _col = list(sel_cols[tab].keys())[0]
                    rs = RangeSelection(tab, lb, ub)
                    rs.update_by(sel_cols[tab][_col]["min"], sel_cols[tab][_col]["max"])
                    deployments[tab].insert_to_watchset(rs, card, tab_to_model_preds[tab][ii])
                    ii += 1
                print(f"Finish loading watchset on [{tab}] (intially {len(deployments[tab].watch_set)} items)")
        
            two_way_model_preds = [
                black_box.predict_one(
                    two_way_model_name,
                    list(tmp[:-1]),
                    two_way_model_tables,
                    two_way_model_joins,
                )
                for tmp in two_way_training_set
            ]
            if watcher_init_theta[two_way_model_name] in ["90th", "95th"]:
                pth = watcher_init_theta[two_way_model_name]
                deployments[two_way_model_name].threshold = get_percentile_q_error(
                    [tmp[-1] for tmp in two_way_training_set],
                    two_way_model_preds,
                    int(watcher_init_theta[two_way_model_name].rstrip("th")),
                )
                print(f"Training set {pth}-percentile relative error of [{two_way_model_name}]: {deployments[two_way_model_name].threshold}")
                watcher_init_theta[two_way_model_name] = deployments[two_way_model_name].threshold
            else:
                assert isinstance(watcher_init_theta[two_way_model_name], float)

            for i in range(len(two_way_training_set)):
                t1 = tabs_with_sel[0]
                t2 = tabs_with_sel[1]
                lb1 = two_way_training_set[i][0]
                ub1 = two_way_training_set[i][1]
                lb2 = two_way_training_set[i][2]
                ub2 = two_way_training_set[i][3]
                card = two_way_training_set[i][4]

                rs1 = RangeSelection(t1, lb1, ub1)
                rs1.update_by(deployments[two_way_model_name].tables[t1].metadata.sel_min, deployments[two_way_model_name].tables[t1].metadata.sel_max)
                rs2 = RangeSelection(t2, lb2, ub2)
                rs2.update_by(deployments[two_way_model_name].tables[t2].metadata.sel_min, deployments[two_way_model_name].tables[t2].metadata.sel_max)

                deployments[two_way_model_name].insert_to_watchset(rs1, rs2, card, two_way_model_preds[i])
            print(f"Finish loading Watcher2D's watchset on [{two_way_model_name}] (intially {len(deployments[two_way_model_name].watch_set)} items)")
        elif deployment == "twotier":
            print("Load query pool by training set")
            for tab in tabs_with_sel:
                for i in range(len(query_info[tab])):
                    deployments[tab].insert_to_query_pool(query_info[tab][i][0]) # insert the rs
                print(f"Finish loading Watcher's query pool on {tab} (intially {len(deployments[tab].query_pool)} queries)")

            print("Load watchset by training set")
            for tab in tabs_with_sel:
                deployments[tab].refill_watchset(query_info[tab])
                to_print = f"Finish loading watchset on {tab} (initially {len(deployments[tab].watch_set)} items)"
                if "watchset_err" in twotier_triggers:
                    to_print += f" with watchset error = {deployments[tab].get_error()})"
                print(to_print)
        else:
            raise NotImplementedError

def load_log(
    conn,
    cursor,
    sql,
    log_filename,
    output_prefix,
    n_tabs,
    sel_cols,
    tabs_with_sel,
    tab_to_info_id,
    pgtypes,
    deployments,
    single_tab_to_qid,
    two_way_model_name,
    tab_to_alias,
    twotier_enable_correct_the_joins,
    twotier_correct_exactly_ntabsjoin,
):
    assert n_tabs in [2, 3, 4]
    print("Load Log")

    def index_to_single_tab_to_qid(tab):
        if tab in tab_to_alias:
            return single_tab_to_qid[tab_to_alias[tab]]
        return single_tab_to_qid[tab]

    two_way_tableset = copy.deepcopy(tabs_with_sel)
    two_way_tableset.sort()
    for i in range(len(two_way_tableset)):
        if two_way_tableset[i] in tab_to_alias:
            two_way_tableset[i] = tab_to_alias[two_way_tableset[i]]
    two_way_tableset = tuple(two_way_tableset)

    to_insert_buffer = []
    to_delete_range = [None, None]

    last_op_type = None
    last_tab = None

    n_tabs_in_from = sql.lower().split("from")[1].split("where")[0].count(",") + 1

    if deployment == "twotier" and twotier_enable_correct_the_joins == True:
        join_corrector = JoinCardCorrector(
            n_tabs=n_tabs,
            overlap_ratio={
                tab : deployments[tab].overlap_ratio_lb
                for tab in tabs_with_sel
            }
        )

    with open(log_filename, "r") as fin, open(
        output_prefix + "_answer.txt", "w"
    ) as fout:
        for row_num, line in enumerate(tqdm(fin)):
            op_type = line[0]
            if op_type == "Q":
                tab = None
            else:
                tab = line.split(",")[1]
            # we flush the tuple updates for tables
            if (last_op_type != op_type or last_tab != tab) and (last_op_type != "Q"):
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

            start_ts = time.time()
            ret = str(row_num) + ":"
            if op_type in ["I", "D"]:
                _type = TupleUpdateType.from_str(line.split(",")[0])
                t = line.split(",")[1]
                assert t == tab
                field = line[2 + len(t) + 1 :].strip()
                if t in tabs_with_sel:
                    # for tabs with selection
                    if _type == TupleUpdateType.INSERT:
                        if ret[-1] != ":":
                            ret += ","
                        if deployment == "routinely":
                            ret += deployments[t].insert(t, field)
                            ret += "," + deployments[two_way_model_name].insert(t, field, skip_pg_modification=True)
                        elif deployment == "watcher":
                            info, external_jk_val, external_sel_val = deployments[t].insert(
                                t, 
                                field, 
                                return_attrs=[
                                    deployments[two_way_model_name].tab_to_jk_to_watch_and_idx[tab][0],
                                    deployments[two_way_model_name].tab_to_sel_to_watch_and_idx[tab][0]
                                ]
                            )
                            ret += info
                            ret += "," + deployments[two_way_model_name].insert(t, field, skip_pg_modification=True, external_jk_val=external_jk_val, external_sel_val=external_sel_val)
                        elif deployment == "twotier":
                            ret += deployments[t].insert(t, field)
                        else:
                            raise NotImplementedError
                    elif _type == TupleUpdateType.DELETE:
                        if ret[-1] != ":":
                            ret += ","
                        if deployment == "routinely":         
                            ret += deployments[t].delete(t, int(field))
                            ret += "," + deployments[two_way_model_name].delete(t, int(field), skip_pg_modification=True)
                        elif deployment == "watcher":
                            info, external_jk_val, external_sel_val = deployments[t].delete(
                                t, 
                                int(field), 
                                return_attrs=[
                                    deployments[two_way_model_name].tab_to_jk_to_watch_and_idx[tab][0],
                                    deployments[two_way_model_name].tab_to_sel_to_watch_and_idx[tab][0]
                                ]
                            )
                            ret += info
                            ret += "," + deployments[two_way_model_name].delete(t, int(field), skip_pg_modification=True, external_jk_val=external_jk_val, external_sel_val=external_sel_val)
                        elif deployment == "twotier":
                            ret += deployments[t].delete(t, int(field))
                        else:
                            raise NotImplementedError
                else:
                    # other tabs
                    if _type == TupleUpdateType.INSERT:
                        attr_vals = []
                        for x in field.split(","):
                            if x == "None":
                                attr_vals.append(None)
                            else:
                                attr_vals.append(x)
                        attr_vals = tuple(attr_vals)
                        to_insert_buffer.append(cursor.mogrify("(" + ",".join(['%s'] * len(pgtypes[t])) + ")", attr_vals))
                    elif _type == TupleUpdateType.DELETE:
                        row_id = int(field)
                        if to_delete_range[0] is None or to_delete_range[0] > row_id:
                            to_delete_range[0] = row_id
                        if to_delete_range[1] is None or to_delete_range[1] < row_id:
                            to_delete_range[1] = row_id
                if ret[-1] != ":":
                    ret += ","
                ret += f"time={round(time.time() - start_ts, 6):.6f}"
            elif op_type == "Q":
                row = line.strip().split(",")
                if deployment in ["routinely", "watcher", "twotier"]:
                    tab_to_pre_query_info = {}
                    # stage: pre-query
                    if two_way_model_name is not None:
                        tab_to_pre_query_info[two_way_model_name] = {"rs": []}

                    for jj in range(n_tabs):
                        if row[1 + jj] in tabs_with_sel:
                            tab = row[1 + jj]

                            lb_id = 1 + n_tabs + 2 * jj
                            ub_id = lb_id + 1
                            single_tab_card_id = 1 + n_tabs * 3 + jj

                            _col = list(sel_cols[tab].keys())[0]
                            single_tab_card = int(row[single_tab_card_id])

                            if "/" in _col:
                                lb, ub = float(row[lb_id]), float(row[ub_id])
                            elif pgtypes[tab][_col].lower() == "real" or pgtypes[tab][_col].lower().startswith("decimal"):
                                lb, ub = float(row[lb_id]), float(row[ub_id])
                            elif pgtypes[tab][_col].lower() == "integer" or pgtypes[tab][_col].lower() == "smallint":
                                lb, ub = int(row[lb_id]), int(row[ub_id])
                            else:
                                raise NotImplementedError
                            
                            rs = RangeSelection(tab, lb, ub)
                            rs.update_by(
                                sel_cols[tab][_col]["min"], sel_cols[tab][_col]["max"]
                            )
                            tab_to_pre_query_info[tab] = {}
                            tab_to_pre_query_info[tab]["rs"] = rs
                            tab_to_pre_query_info[tab]["true_card"] = single_tab_card
                            tab_to_pre_query_info[tab]["model_pred"], tab_to_pre_query_info[tab]["baseline_time"] = deployments[tab].pre_query(rs)

                            if two_way_model_name is not None:
                                tab_to_pre_query_info[two_way_model_name]["rs"].append(rs)

                    true_join_card_id = 1 + n_tabs * 4
                    if two_way_model_name is not None:
                        assert len(tab_to_pre_query_info[two_way_model_name]["rs"]) == n_tabs
                        tab_to_pre_query_info[two_way_model_name]["true_card"] = int(row[true_join_card_id])
                        (
                            tab_to_pre_query_info[two_way_model_name]["model_pred"], 
                            tab_to_pre_query_info[two_way_model_name]["baseline_time"]
                        ) = deployments[two_way_model_name].pre_query(
                            tab_to_pre_query_info[two_way_model_name]["rs"]
                        )

                    ########### stage: execute the query
                    cur_sql = copy.deepcopy(sql)
                    for tab in tabs_with_sel:
                        cur_sql = cur_sql.replace(
                            "{lb" + str(tab_to_info_id[tab]) + "}",
                            str(tab_to_pre_query_info[tab]["rs"].lb)
                        ).replace(
                            "{ub" + str(tab_to_info_id[tab]) + "}",
                            str(tab_to_pre_query_info[tab]["rs"].ub)
                        )

                    extra_twotier_watcher_time = 0.
                    if deployment == "twotier" and enable_correct_the_joins:
                        # inject single-table estimates and get the plan
                        _, single_only_join_order, single_only_scan_mtd = inject_multiple_single_table_sel_and_get_cost(
                            pointers_and_sels=[(index_to_single_tab_to_qid(tab), tab_to_pre_query_info[tab]["model_pred"] / deployments[tab].cur_table_size) for tab in tabs_with_sel],
                            n_tabs_in_from=n_tabs_in_from,
                            q=cur_sql,
                            hint=None,
                            plan_info=True,
                        )
                        # construct the plan by the info
                        single_only_hint = gen_final_hint(scan_mtd=single_only_scan_mtd, str=single_only_join_order)
                        tableset_to_joinid, tableset_to_sel, tableset_to_rawrows, tableset_to_estrows, min_join_qid, max_join_qid, _ = load_pg_join_subquery_info()
                        twotier_start_time = time.time()
                        correction_factor = join_corrector.search(
                            rss=[tab_to_pre_query_info[tab]["rs"] for tab in tabs_with_sel]
                        )
                        if correction_factor is None:
                            flag_for_correcting_join = False
                        else:
                            _join_qids = []
                            _join_sels = []
                            for tableset, factor in correction_factor.items():
                                if tableset in tableset_to_joinid and len(tableset) == twotier_correct_exactly_ntabsjoin:
                                    _join_qids.append(tableset_to_joinid[tableset])
                                    _join_sels.append(max(min(tableset_to_sel[tableset] * factor, 1.0), 0.0))
                            if len(_join_qids) > 0:
                                flag_for_correcting_join = True
                            else:
                                flag_for_correcting_join = False
                        extra_twotier_watcher_time += time.time() - twotier_start_time
                        if not flag_for_correcting_join:
                            hint = single_only_hint
                            # cost the plan by true cards
                            plan_cost = inject_multiple_single_table_sel_and_get_cost(
                                pointers_and_sels=[(index_to_single_tab_to_qid(tab), tab_to_pre_query_info[tab]["true_card"] / deployments[tab].cur_table_size) for tab in tabs_with_sel],
                                n_tabs_in_from=n_tabs_in_from,
                                q=cur_sql,
                                hint=hint,
                                plan_info=False,
                            )
                        else:
                            # inject single-table and two-way join estimates and get the plan
                            _, join_order, scan_mtd = inject_single_and_join_sels_and_get_cost(
                                pointers=[index_to_single_tab_to_qid(tab) for tab in tabs_with_sel] + _join_qids,
                                single_sels=[tab_to_pre_query_info[tab]["model_pred"] / deployments[tab].cur_table_size for tab in tabs_with_sel],
                                n_single=min_join_qid,
                                join_sels=_join_sels,
                                n_all=max_join_qid + 1,
                                q=cur_sql,
                                hint=None,
                                plan_info=True,
                            )
                            # construct the plan by the info
                            hint = gen_final_hint(scan_mtd=scan_mtd, str=join_order)
                            # cost the plan by true cards
                            # but firstly, we need to get the join information after injecting true cards to both the single table
                            _ = inject_multiple_single_table_sel_and_get_cost(
                                pointers_and_sels=[(index_to_single_tab_to_qid(tab), tab_to_pre_query_info[tab]["true_card"] / deployments[tab].cur_table_size) for tab in tabs_with_sel],
                                n_tabs_in_from=n_tabs_in_from,
                                q=cur_sql,
                                hint=None,
                                plan_info=False,
                            )
                            _, _, truecard_tableset_to_rawrows, _, _, _, _ = load_pg_join_subquery_info()
                            plan_cost = inject_single_and_join_sels_and_get_cost(
                                pointers=[index_to_single_tab_to_qid(tab) for tab in tabs_with_sel] + [tableset_to_joinid[two_way_tableset]],
                                single_sels=[tab_to_pre_query_info[tab]["true_card"] / deployments[tab].cur_table_size for tab in tabs_with_sel],
                                n_single=min_join_qid,
                                join_sels=[int(row[true_join_card_id]) / truecard_tableset_to_rawrows[two_way_tableset]],
                                n_all=max_join_qid + 1,
                                q=cur_sql,
                                hint=hint,
                                plan_info=False,
                            )
                    elif two_way_model_name is None:
                        # inject single-table estimates and get the plan
                        _, join_order, scan_mtd = inject_multiple_single_table_sel_and_get_cost(
                            pointers_and_sels=[(index_to_single_tab_to_qid(tab), tab_to_pre_query_info[tab]["model_pred"] / deployments[tab].cur_table_size) for tab in tabs_with_sel],
                            n_tabs_in_from=n_tabs_in_from,
                            q=cur_sql,
                            hint=None,
                            plan_info=True,
                        )
                        # construct the plan by the info
                        hint = gen_final_hint(scan_mtd=scan_mtd, str=join_order)
                        # cost the plan by true cards
                        plan_cost = inject_multiple_single_table_sel_and_get_cost(
                            pointers_and_sels=[(index_to_single_tab_to_qid(tab), tab_to_pre_query_info[tab]["true_card"] / deployments[tab].cur_table_size) for tab in tabs_with_sel],
                            n_tabs_in_from=n_tabs_in_from,
                            q=cur_sql,
                            hint=hint,
                            plan_info=False,
                        )
                    else:
                        _ = inject_multiple_single_table_sel_and_get_cost(
                            pointers_and_sels=[(index_to_single_tab_to_qid(tab), tab_to_pre_query_info[tab]["model_pred"] / deployments[tab].cur_table_size) for tab in tabs_with_sel],
                            n_tabs_in_from=n_tabs_in_from,
                            q=cur_sql,
                            hint=None,
                            plan_info=False,
                        )
                        tableset_to_joinid, _, tableset_to_rawrows, _, min_join_qid, max_join_qid, _ = load_pg_join_subquery_info()

                        # inject single-table and two-way join estimates and get the plan
                        _, join_order, scan_mtd = inject_single_and_join_sels_and_get_cost(
                            pointers=[index_to_single_tab_to_qid(tab) for tab in tabs_with_sel] + [tableset_to_joinid[two_way_tableset]],
                            single_sels=[tab_to_pre_query_info[tab]["model_pred"] / deployments[tab].cur_table_size for tab in tabs_with_sel],
                            n_single=min_join_qid,
                            join_sels=[tab_to_pre_query_info[two_way_model_name]["model_pred"] / tableset_to_rawrows[two_way_tableset]],
                            n_all=max_join_qid + 1,
                            q=cur_sql,
                            hint=None,
                            plan_info=True,
                        )
                        # construct the plan by the info
                        hint = gen_final_hint(scan_mtd=scan_mtd, str=join_order)
                        # cost the plan by true cards
                        # but firstly, we need to get the join information after injecting true cards to both the single table
                        _ = inject_multiple_single_table_sel_and_get_cost(
                            pointers_and_sels=[(index_to_single_tab_to_qid(tab), tab_to_pre_query_info[tab]["true_card"] / deployments[tab].cur_table_size) for tab in tabs_with_sel],
                            n_tabs_in_from=n_tabs_in_from,
                            q=cur_sql,
                            hint=None,
                            plan_info=False,
                        )
                        _, _, truecard_tableset_to_rawrows, _, _, _, _ = load_pg_join_subquery_info()
                        plan_cost = inject_single_and_join_sels_and_get_cost(
                            pointers=[index_to_single_tab_to_qid(tab) for tab in tabs_with_sel] + [tableset_to_joinid[two_way_tableset]],
                            single_sels=[tab_to_pre_query_info[tab]["true_card"] / deployments[tab].cur_table_size for tab in tabs_with_sel],
                            n_single=min_join_qid,
                            join_sels=[tab_to_pre_query_info[two_way_model_name]["true_card"] / truecard_tableset_to_rawrows[two_way_tableset]],
                            n_all=max_join_qid + 1,
                            q=cur_sql,
                            hint=hint,
                            plan_info=False,
                        )

                    # lets run it!
                    if deployment in ["routinely", "watcher"]:
                        _, execution_time, _, model_name_to_have = get_real_latency(
                            sql=cur_sql,
                            hint=hint,
                            if_have=list(tab_to_pre_query_info.keys()), 
                        )
                    elif deployment == "twotier":
                        _focus_on = []
                        for tab in tabs_with_sel:
                            _focus_on.append(tab)
                            if tab in tab_to_alias:
                                _focus_on.append(tab_to_alias[tab])
                            else:
                                _focus_on.append("".join(x[0] for x in tab.strip().split("_")))
                        _, execution_time, planning_time, tableset_to_truecard, model_name_to_have = get_real_latency(
                            sql=cur_sql,
                            hint=hint,
                            extract_plan_info=True,
                            focus_on=_focus_on,
                            if_have=list(tab_to_pre_query_info.keys()), 
                        )
                        if twotier_enable_correct_the_joins:
                            at_least_one_notnone = False
                            rss = []
                            for tab in tabs_with_sel:
                                if deployments[tab]._which is not None:
                                    at_least_one_notnone = True
                                    rss.append(deployments[tab]._which)
                                else:
                                    rss.append(None)
                            if at_least_one_notnone:
                                join_corrector.insert(
                                    rss, 
                                    {
                                        tableset : tableset_to_truecard[tableset] / tableset_to_estrows[tableset]
                                        for tableset in tableset_to_truecard if tableset in tableset_to_estrows and len(tableset) == twotier_correct_exactly_ntabsjoin
                                    }
                                )
                    else:
                        raise NotImplementedError

                    execution_time = execution_time / 1000.

                    # stage: post-query
                    extra_info = {}
                    for tab in tabs_with_sel:
                        # add the baseline_time
                        if deployment == "routinely":
                            tab_to_pre_query_info[tab]["baseline_time"] += deployments[tab].post_query(
                                rs=tab_to_pre_query_info[tab]["rs"],
                                true_card=tab_to_pre_query_info[tab]["true_card"],
                            )
                        elif deployment == "watcher":
                            tab_to_pre_query_info[tab]["baseline_time"] += deployments[tab].post_query(
                                rs=tab_to_pre_query_info[tab]["rs"],
                                true_card=tab_to_pre_query_info[tab]["true_card"],
                                model_pred=tab_to_pre_query_info[tab]["model_pred"],
                            )
                        elif deployment == "twotier":
                            _watcher_time, extra_info[tab] = deployments[tab].post_query(
                                rs=tab_to_pre_query_info[tab]["rs"],
                                true_card=tab_to_pre_query_info[tab]["true_card"],
                                q=cur_sql,
                                pg_plan_cost=plan_cost,
                                execution_time=execution_time,
                                planning_time=planning_time,
                                tableset_to_truecard=tableset_to_truecard,
                            )
                            tab_to_pre_query_info[tab]["baseline_time"] += _watcher_time
                        else:
                            raise NotImplementedError
              
                    if deployment == "routinely":
                        tab_to_pre_query_info[two_way_model_name]["baseline_time"] += deployments[two_way_model_name].post_query(
                            tab_to_pre_query_info[two_way_model_name]["rs"],
                            tab_to_pre_query_info[two_way_model_name]["true_card"]
                        )
                    elif deployment == "watcher":
                        tab_to_pre_query_info[two_way_model_name]["baseline_time"] += deployments[two_way_model_name].post_query(
                            tab_to_pre_query_info[two_way_model_name]["rs"][0],
                            tab_to_pre_query_info[two_way_model_name]["rs"][1],
                            tab_to_pre_query_info[two_way_model_name]["true_card"],
                            tab_to_pre_query_info[two_way_model_name]["model_pred"],
                        )
                    elif deployment == "twotier":
                        pass
                    else:
                        raise NotImplementedError

                    # summarize the output
                    for tab in tabs_with_sel:
                        info_id = tab_to_info_id[tab]
                        model_pred = tab_to_pre_query_info[tab]["model_pred"]
                        true_card = tab_to_pre_query_info[tab]["true_card"]
                        baseline_time = tab_to_pre_query_info[tab]["baseline_time"]
                        if deployment == "routinely":
                            name_for_extra_time_cost = "baseline_time"
                        elif deployment in ["watcher", "twotier"]:
                            name_for_extra_time_cost = "watcher_time"
                        else:
                            raise NotImplementedError
                        if ret[-1] != ':':
                            ret += ","
                        ret += f"est{info_id}={model_pred}"
                        ret += f",observed_err{info_id}={np.round(get_average_q_error([true_card], [model_pred]), 6):.6f}"
                        ret += f",{name_for_extra_time_cost}{info_id}={round(baseline_time, 6):0.6f}"
                        if deployment == "watcher":
                            ret += f",watchset_err{info_id}={np.round(deployments[tab].get_error()):.6f}"
                            ret += f",watchset_size{info_id}={len(deployments[tab].watch_set)}"

                            ret += f",got_true_card{info_id}="
                            if tab in model_name_to_have and model_name_to_have[tab]:
                                ret += "True"
                            else:
                                ret += "False"
                        elif deployment == "twotier":
                            ret += f",watchset_size{info_id}={len(deployments[tab].watch_set)}"
                            ret += extra_info[tab]
                            ret += f",got_true_card{info_id}="
                            if tab in model_name_to_have and model_name_to_have[tab]:
                                ret += "True"
                            else:
                                ret += "False"
                    
                    if deployment == "twotier" and enable_correct_the_joins:
                        ret += f",join_correction_time={round(extra_twotier_watcher_time, 6):.6f}"
                        if flag_for_correcting_join:
                            ret += ",join_correction?=True"
                            ret += ",join_correcting_qids=" + "|".join([str(x) for x in _join_qids])
                        else:
                            ret += ",join_correction?=False"


                    if two_way_model_name is not None:
                        if deployment == "routinely":
                            name_for_extra_time_cost = "baseline_time"
                        elif deployment == "watcher":
                            name_for_extra_time_cost = "watcher_time"
                        else:
                            raise NotImplementedError
                        _join_est = tab_to_pre_query_info[two_way_model_name]["model_pred"]
                        _join_true = tab_to_pre_query_info[two_way_model_name]["true_card"]
                        ret += f",join_est={_join_est}"
                        ret += f",join_observed_err={np.round(get_average_q_error([_join_true], [_join_est]), 6):.6f}"
                        ret += f",join_{name_for_extra_time_cost}={round(tab_to_pre_query_info[two_way_model_name]['baseline_time'], 6):0.6f}"
                        if deployment == "watcher":
                            ret += f",join_watchset_err={np.round(deployments[two_way_model_name].get_error()):.6f}"
                            ret += f",join_watchset_size={len(deployments[two_way_model_name].watch_set)}"
                            ret += f",join_got_true_card="
                            if two_way_model_name in model_name_to_have and model_name_to_have[two_way_model_name]:
                                ret += "True"
                            else:
                                ret += "False"

                    ret += f",plan_cost={plan_cost}"
                    ret += f",execution_time={round(execution_time, 6):.6f}"
                    ret += f",time={round(time.time() - start_ts, 6):.6f}"

                    hint_wo_newline = hint.replace('\n', '')
                    ret += f",plan={hint_wo_newline}"
                else:
                    raise NotImplementedError
            else:
                raise NotImplementedError
            fout.write(ret + "\n")

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
        jk_cols=config["jk_cols"]
        timestamp_cols=config["timestamp_cols"]
        join_to_watch=config["join_to_watch"]
        sql = config["sql"]
        pgtypes = config["pgtypes"]
        debug = config["debug"]
        deployment = config["deployment"]
        deployment_config = config["deployment_config"]
        if deployment == "routinely":
            assert "routinely" in deployment_config
            update_based_on = deployment_config["routinely"]["update_based_on"]
            update_after = deployment_config["routinely"]["update_after"]
            counting_sql = deployment_config["routinely"]["counting_sql"]
        elif deployment == "watcher":
            assert "watcher" in deployment_config
            error_metric = deployment_config["watcher"]["error_metric"] 
            init_theta = deployment_config["watcher"]["init_theta"]
            max_level = deployment_config["watcher"]["max_level"] 
            fan_out = deployment_config["watcher"]["fan_out"] 
            jk_theta = deployment_config["watcher"]["jk_theta"] 
            jk_bounds = deployment_config["watcher"]["jk_bounds"]
        elif deployment == "twotier":
            assert "twotier" in deployment_config
            # TODO
            triggers = deployment_config["twotier"]["triggers"] # tab -> [..., ...]
            if "error_metric" in deployment_config["twotier"]:
                error_metric = deployment_config["twotier"]["error_metric"]
            if "init_theta" in deployment_config["twotier"]:
                init_theta = deployment_config["twotier"]["init_theta"] # TODO: it is init_theta instead of init_thetas
            if "size_budget" in deployment_config["twotier"]:
                size_budget = deployment_config["twotier"]["size_budget"]
            execution_time_lb = deployment_config["twotier"]["execution_time_lb"]
            planning_time_ub = deployment_config["twotier"]["planning_time_ub"]
            plancost_check_ratio = deployment_config["twotier"]["plancost_check_ratio"]
            overlap_ratio_lb = deployment_config["twotier"]["overlap_ratio_lb"]
            enable_lazy_tuple_updates = deployment_config["twotier"]["enable_lazy_tuple_updates"]
            enable_correct_the_joins = deployment_config["twotier"]["enable_correct_the_joins"]
            if enable_correct_the_joins:
                correct_exactly_ntabsjoin = deployment_config["twotier"]["correct_exactly_ntabsjoin"]
            else:
                correct_exactly_ntabsjoin = None
        else:
            raise NotImplementedError

        # if "disable_table_with_sel_insertions_in_batch" in config:
        #     disable_table_with_sel_insertions_in_batch = config["disable_table_with_sel_insertions_in_batch"]
        # else:
        #     disable_table_with_sel_insertions_in_batch = False
        table_filenames = config["table_filenames"]
        log_filename = config["log_filename"]
        trainingset_filename = config["trainingset_filename"]
        output_prefix = config["output_prefix"]

        if deployment == "routinely":
            if debug:
                output_prefix = "/".join(output_prefix.split("/")[:-1]) + "/test"
            else:
                routine_part = []
                for k, v in update_after.items():
                    if v is None:
                        routine_part.append(f"{k}-after-None-" + update_based_on[k])
                    else:
                        routine_part.append(f"{k}-after-" + "|".join([str(x) for x in v]) + "-" + update_based_on[k])
                output_prefix += "_collab_routinely[" + ",".join(routine_part) + "]"
                if "algorithm" in config:
                    output_prefix += f"_{algorithm}"
        elif deployment == "watcher":
            pass # postponed
        elif deployment == "twotier":
            if debug:
                output_prefix = "/".join(output_prefix.split("/")[:-1]) + "/test"
            else:
                output_prefix += "_collab_two-tier(" + ",".join(list(sel_cols.keys())) + ")"
                output_prefix += f"_exec-time>=(" + ",".join([str(execution_time_lb[tab]) + "ms" for tab in execution_time_lb]) + ")"
                
                output_prefix += f"_plan-time<=("
                to_add = [] 
                for tab in planning_time_ub:
                    if not planning_time_ub[tab].endswith("%"):
                        to_add.append(str(planning_time_ub[tab]) + "ms")
                    else:
                        to_add.append(str(planning_time_ub[tab]))
                output_prefix += ",".join(to_add) + ")"

                output_prefix += f"_cost-ratio=(" + ",".join([str(plancost_check_ratio[tab]) for tab in plancost_check_ratio]) + ")"
                output_prefix += f"_overlap-ratio>=(" + ",".join([str(overlap_ratio_lb[tab]) for tab in overlap_ratio_lb]) + ")"
                
                if not enable_correct_the_joins:
                    output_prefix += f"_correct-join=False"
                else:
                    output_prefix += f"_correct-join=" + str(correct_exactly_ntabsjoin)

                if len(triggers[tab]) == 0:
                    output_prefix += "_notrigger"
                else:
                    for tab in triggers:
                        if "watchset_err" in triggers[tab]: 
                            assert tab in error_metric and tab in init_theta
                        else:
                            assert tab not in error_metric and tab not in init_theta
                        if "watchset_size" in triggers[tab]: 
                            assert tab in size_budget
                        else:
                            assert tab not in size_budget

                    if "init_theta" in deployment_config["twotier"] and len(init_theta) > 0:
                        output_prefix += "_theta[" + ",".join([f"{k}({error_metric[k]})<={v}" for k, v in init_theta.items()]) + "]"
                    if "size_budget" in deployment_config["twotier"] and len(size_budget) > 0:
                        output_prefix += "_size[" + ",".join([f"{tab}<={size_budget[k]}" for tab in size_budget]) + "]"
                if "algorithm" in config:
                    output_prefix += f"_{algorithm}"
        else:
            raise NotImplementedError

        # prepare the adujustment for Watcher2D
        if deployment == "watcher":
            # prepare the jk_bounds_for_leveldb and sel_bounds_for_leveldb, as well as jk_delta and sel_delta 
            # if the corresponding column is non-integer, we x100 to preserve 2 decimal places
            jk_bounds_for_leveldb = {}
            jk_deltas = {}
            for tab in jk_bounds:
                # tab -> {col -> (lb, ub)}
                assert len(jk_bounds[tab]) == 1
                col_to_watch = list(jk_bounds[tab].keys())[0]
                if "/" in col_to_watch or pgtypes[tab][col_to_watch].lower() == "real" or pgtypes[tab][col_to_watch].lower().startswith("decimal"):
                    # it is float with 2 decimal places
                    lb, ub = jk_bounds[tab][col_to_watch]["min"], jk_bounds[tab][col_to_watch]["max"]
                    assert isinstance(lb, float) and isinstance(ub, float)
                    lb *= 100
                    ub *= 100
                elif pgtypes[tab][col_to_watch].lower() == "integer":
                    lb, ub = jk_bounds[tab][col_to_watch]["min"], jk_bounds[tab][col_to_watch]["max"]
                else:
                    raise NotImplementedError
                if lb <= 0:
                    jk_deltas[tab] = 1 - lb
                else:
                    jk_deltas[tab] = 0
                lb += jk_deltas[tab]
                ub += jk_deltas[tab]
                jk_bounds_for_leveldb[tab] = {col_to_watch : (lb, ub)}

            sel_bounds_for_leveldb = {}
            sel_deltas = {}
            for tab in sel_cols:
                # tab -> {col -> {"min": lb, "max": ub}}
                assert len(sel_cols[tab]) == 1
                col_to_watch = list(sel_cols[tab].keys())[0]
                if "/" in col_to_watch or pgtypes[tab][col_to_watch].lower() == "real" or pgtypes[tab][col_to_watch].lower().startswith("decimal"):
                    # it is float with 2 decimal places
                    assert isinstance(sel_cols[tab][col_to_watch]["min"], float) and isinstance(sel_cols[tab][col_to_watch]["max"], float)
                    lb = int(100 * sel_cols[tab][col_to_watch]["min"]) 
                    ub = int(100 * sel_cols[tab][col_to_watch]["max"])
                elif pgtypes[tab][col_to_watch].lower() == "integer":
                    lb = int(sel_cols[tab][col_to_watch]["min"]) 
                    ub = int(sel_cols[tab][col_to_watch]["max"])
                else:
                    raise NotImplementedError
                if lb < 0:
                    sel_deltas[tab] = 0 - lb
                else:
                    sel_deltas[tab] = 0
                lb += sel_deltas[tab]
                ub += sel_deltas[tab]
                sel_bounds_for_leveldb[tab] = {col_to_watch : (lb, ub)}

            print(f"The adjusted bounds for jk = {jk_bounds_for_leveldb}, and for sel = {sel_bounds_for_leveldb}")
            print(f"The delta for jk = {jk_deltas}, and for sel = {sel_deltas}")

        # get db connection
        conn, cursor = get_connection()

        # load static tables
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

        # we only need to build model for tables with selections and their two-way join
        tabs_with_sel = list(sel_cols.keys())
        if deployment in ["routinely", "watcher"]:
            two_way_model_name = "-".join(tabs_with_sel)
        elif deployment == "twotier":
            two_way_model_name = None
        else:
            raise NotImplementedError

        ii = 1
        tab_to_info_id = {}
        for tab in tabs_with_sel:
            tab_to_info_id[tab] = ii
            ii += 1

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
                
        deployments = {}
        if deployment == "routinely":
            two_way_model_name_with_alias = []
            for _tab in two_way_model_name.split("-"):
                if _tab in tab_to_alias:
                    two_way_model_name_with_alias.append(_tab + " " +tab_to_alias[_tab])
                else:
                    two_way_model_name_with_alias.append(_tab)
            two_way_model_name_with_alias = "-".join(two_way_model_name_with_alias)

            deployments[two_way_model_name] = RoutineBaseline2D(
                tabs_for_baseline2d=tabs_with_sel,
                tab_to_alias=tab_to_alias,
                conn=conn,
                cursor=cursor,
                query_pool_size=black_box.config["training_size"],
                counting_sql=counting_sql[two_way_model_name_with_alias],
                random_seed=2023,
                based_on=update_based_on[two_way_model_name_with_alias],
                X=update_after[two_way_model_name_with_alias],
            )
        elif deployment == "watcher":
            deployments[two_way_model_name] = Watcher2d(
                conn=conn,
                cursor=cursor,
                watchset_size=black_box.config["training_size"],
                random_seed=2023,
                threshold=init_theta[two_way_model_name],
                error_metric=error_metric[two_way_model_name],
                pgtypes=pgtypes,
                jk_cols=jk_cols,
                sel_cols=sel_cols,
            )
        elif deployment == "twotier":
            pass # TODO: seems like nothing to do here
        else:
            raise NotImplementedError
        
        for ii in range(len(tabs)):
            if tabs[ii] in tab_to_alias:
                assigned_alias = tab_to_alias[tabs[ii]]
            else:
                assigned_alias = None

            tab = tabs[ii]
            if tab in tabs_with_sel:
                if deployment == "routinely":
                    deployments[tab] = RoutineBaseline1D(
                        name=tab,
                        conn=conn,
                        cursor=cursor,
                        query_pool_size=black_box.config["training_size"],
                        random_seed=2023,
                        based_on=update_based_on[tab],
                        X=update_after[tab],
                        info_id=tab_to_info_id[tab],  # 1-base info ids
                        assigned_alias=assigned_alias,
                    )
                    deployments[tab].load_table(
                        file=table_filenames[tab],
                        # we only support one sel col per table for now
                        sel_col=list(sel_cols[tab].keys())[0],
                        jk_cols=jk_cols[tab],
                        pgtype=pgtypes[tab],
                        timestamp_col=timestamp_cols[tab],
                    )

                    deployments[two_way_model_name].load_table(
                        tab=tab,
                        file=table_filenames[tab],
                        # we only support one sel col per table for now
                        sel_col=list(sel_cols[tab].keys())[0],
                        jk_cols=jk_cols[tab],
                        pgtype=pgtypes[tab],
                        timestamp_col=timestamp_cols[tab],
                        skip_pg_modification=True,
                    )
                elif deployment == "watcher":
                    deployments[tab] = Watcher1D(
                        name=tab,
                        conn=conn,
                        cursor=cursor,
                        watchset_size=black_box.config["training_size"],
                        random_seed=2023,
                        error_metric=error_metric[tab],
                        threshold=init_theta[tab] if not isinstance(init_theta[tab], str) else None,
                        info_id=tab_to_info_id[tab],  # 1-base info ids
                        assigned_alias=assigned_alias,
                    )
                    deployments[tab].load_table(
                        file=table_filenames[tab],
                        # we only support one sel col per table for now
                        sel_col=list(sel_cols[tab].keys())[0],
                        jk_cols=jk_cols[tab],
                        pgtype=pgtypes[tab],
                        timestamp_col=timestamp_cols[tab],
                    )

                    jk_to_watch = list(jk_bounds_for_leveldb[tab].keys())[0]
                    sel_to_watch = list(sel_bounds_for_leveldb[tab].keys())[0]
                    deployments[two_way_model_name].init_metadata(
                        tab=tab,
                        table_filename=table_filenames[tab],
                        jk_to_watch=jk_to_watch,
                        sel_to_watch=sel_to_watch,
                        jk_bound=jk_bounds_for_leveldb[tab][jk_to_watch],
                        sel_bound=sel_bounds_for_leveldb[tab][sel_to_watch],
                        jk_delta=jk_deltas[tab],
                        sel_delta=sel_deltas[tab],
                        max_level=max_level,
                        fan_out=fan_out,
                        jk_theta=jk_theta,
                    )
                    deployments[two_way_model_name].load_table(
                        tab=tab,
                        file=table_filenames[tab],
                        timestamp_col=timestamp_cols[tab],
                        skip_pg_modification=True,
                    )
                elif deployment == "twotier":
                    deployments[tab] = TwoTierWatcher1D(
                        name=tab,
                        conn=conn,
                        cursor=cursor,
                        n_tabs_in_from=sql.lower().split("from")[1].split("where")[0].count(",") + 1,
                        query_pool_size=black_box.config["training_size"],
                        training_size=black_box.config["training_size"],
                        triggers=triggers[tab],
                        random_seed=2023,
                        execution_time_lb=execution_time_lb[tab],
                        planning_time_ub=planning_time_ub[tab],
                        plancost_check_ratio=plancost_check_ratio[tab],
                        overlap_ratio_lb=overlap_ratio_lb[tab],
                        enable_lazy_tuple_updates=enable_lazy_tuple_updates[tab],
                        info_id=tab_to_info_id[tab], # 1-base info ids
                        assigned_alias=assigned_alias,
                    )

                    for trigger in triggers:
                        if trigger == "watchsert_err":
                            deployments[tab].threshold = init_theta[tab]
                            deployments[tab].error_metric = error_metric[tab]
                        elif trigger == "watchset_size":
                            deployments[tab].size_budget = size_budget[tab]

                    deployments[tab].load_table(
                        file=table_filenames[tab],
                        # we only support one sel col per table for now
                        sel_col=list(sel_cols[tab].keys())[0],
                        jk_cols=jk_cols[tab],
                        pgtype=pgtypes[tab],
                        timestamp_col=timestamp_cols[tab],
                    )
                else:
                    raise NotImplementedError
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
        if deployment in ["routinely", "watcher"]:
            two_way_model_tables = []
            black_box.default_column_min_max_vals[two_way_model_name] = {}
            black_box.default_predicate_templates[two_way_model_name] = []
            for tab in tabs_with_sel:
                if tab in tab_to_alias:
                    tab_abbrev = tab_to_alias[tab]
                else:
                    tab_abbrev = "".join(x[0] for x in tab.strip().split("_"))
                two_way_model_tables.append(tab + " " + tab_abbrev)

                # initialize the single-tabel model
                black_box.default_column_min_max_vals[tab] = {}
                black_box.default_predicate_templates[tab] = []

                for col in sel_cols[tab]:
                    for _model_name in [two_way_model_name, tab]:
                        black_box.default_column_min_max_vals[_model_name][tab_abbrev + "." + col] = [
                            sel_cols[tab][col]["min"],
                            sel_cols[tab][col]["max"],
                        ]
                        
                for col in sel_cols[tab]:
                    for _model_name in [two_way_model_name, tab]:
                        black_box.default_predicate_templates[_model_name].append(
                            tab_abbrev + "." + col
                        )
                        black_box.default_predicate_templates[_model_name].append(">")
                        black_box.default_predicate_templates[_model_name].append("")
                        black_box.default_predicate_templates[_model_name].append(
                            tab_abbrev + "." + col
                        )
                        black_box.default_predicate_templates[_model_name].append("<")
                        black_box.default_predicate_templates[_model_name].append("")
            
            
            # set the model_tables and model_joins for baseline2d, watcher2d
            deployments[two_way_model_name].setup_black_box(
                model_name=two_way_model_name,
                model_tables=two_way_model_tables,
                model_joins=join_to_watch,
            )
        elif deployment == "twotier":
            for tab in sel_cols:
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
            two_way_model_tables = None 
        else:
            raise NotImplementedError
        
        # get all the pointers
        sql_for_getting_pointers = copy.deepcopy(sql)
        for tab in tabs_with_sel:
            col_of_sel = list(sel_cols[tab].keys())[0]
            sql_for_getting_pointers = sql_for_getting_pointers.replace(
                "{lb" + str(tab_to_info_id[tab]) + "}",
                str(sel_cols[tab][col_of_sel]["min"])
            ).replace(
                "{ub" + str(tab_to_info_id[tab]) + "}",
                str(sel_cols[tab][col_of_sel]["max"])
            )
        single_tab_to_qid = get_single_table_pointer(sql=sql_for_getting_pointers)
        if deployment == "twotier":
            def index_to_single_tab_to_qid(tab):
                if tab in tab_to_alias:
                    return single_tab_to_qid[tab_to_alias[tab]]
                return single_tab_to_qid[tab]
            for tab in tabs_with_sel:
                deployments[tab].setup_single_table_pointer(index_to_single_tab_to_qid(tab))

        load_trainingset(
            trainingset_filename=trainingset_filename,
            sql=sql,
            n_tabs=len(tabs),
            tabs_with_sel=tabs_with_sel,
            sel_cols=sel_cols,
            pgtypes=pgtypes,
            tab_to_alias=tab_to_alias,
            deployment=deployment,
            deployments=deployments, 
            two_way_model_name=two_way_model_name, 
            two_way_model_tables=two_way_model_tables, 
            two_way_model_joins=join_to_watch if deployment in ["routinely", "watcher"] else None,
            watcher_init_theta=None if deployment != "watcher" else init_theta,
            twotier_triggers=None if deployment != "twotier" else triggers,
        )

        # the postponed process to compute the output_prefix
        if deployment == "watcher":
            if debug:
                output_prefix = "/".join(output_prefix.split("/")[:-1]) + "/test"
            else:
                theta_part = []
                for k, v in init_theta.items():
                    theta_part.append(f"{k}({error_metric[k]})<={v}")
                output_prefix += "_collab_theta[" + ",".join(theta_part) + "]"
                if "algorithm" in config:
                    output_prefix += f"_{algorithm}"

        load_log(
            conn=conn,
            cursor=cursor,
            sql=sql,
            log_filename=log_filename,
            output_prefix=output_prefix,
            n_tabs=len(tabs),
            sel_cols=sel_cols,
            tabs_with_sel=tabs_with_sel,
            tab_to_info_id=tab_to_info_id,
            pgtypes=pgtypes,
            deployments=deployments, 
            single_tab_to_qid=single_tab_to_qid,
            two_way_model_name=two_way_model_name,
            tab_to_alias=tab_to_alias,
            twotier_enable_correct_the_joins=None if deployment in ["routinely", "watcher"] else enable_correct_the_joins,
            twotier_correct_exactly_ntabsjoin=None if deployment in ["routinely", "watcher"] else correct_exactly_ntabsjoin,
        )

        if deployment == "watcher":
            deployments[two_way_model_name].clean()
        close_connection(conn=conn, cursor=cursor)
