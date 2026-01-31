from operation import TupleUpdateType
from tqdm import tqdm
import time
import warnings
import black_box
import json
import sys
import copy
from error_metrics import get_average_q_error
from connection import get_connection
from pg_hint_utility import gen_final_hint, inject_single_and_join_sels_and_get_cost, inject_multiple_single_table_sel_and_get_cost, get_real_latency, get_single_table_pointer, load_pg_join_subquery_info

warnings.filterwarnings("ignore")

def load_trainingset(
    trainingset_filename, 
    n_tabs, 
    tabs_for_model, 
    sel_cols, 
    pgtypes, 
    two_way_model_name, 
    two_way_model_tables, 
    two_way_model_joins
):
    print("Load Trainingset")
    with open(trainingset_filename, "r") as fin:
        two_way_training_set = []
        tab_to_single_table_training_set = {}
        for tab in tabs_for_model:
            tab_to_single_table_training_set[tab] = []

        lines = list(fin.readlines())

        for i in range(len(lines)):
            row = lines[i].strip().split(",")
            bounds = []
            for j in range(n_tabs):
                if row[1 + j] in tabs_for_model:
                    tab = row[1 + j]
                    lb_id = 1 + n_tabs + 2 * j
                    ub_id = lb_id + 1
                    single_table_card_id = 1 + n_tabs * 3 + j
                    single_tab_card = int(row[single_table_card_id])
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
                    
                    if (
                        single_tab_card >= 1
                        and len(tab_to_single_table_training_set[tab]) < black_box.config["training_size"]
                    ):
                        tab_to_single_table_training_set[tab].append((lb, ub, single_tab_card))

            two_way_true_card_id = 1 + n_tabs * 4
            two_way_true_card = int(row[two_way_true_card_id])
            if (
                two_way_true_card >= 1
                and len(two_way_training_set) < black_box.config["training_size"]
            ):
                two_way_training_set.append(tuple(bounds + [two_way_true_card]))

        print("Finish loading training set:")
        for tab in tab_to_single_table_training_set:
            print(f"Training set size of [{tab}]: {len(tab_to_single_table_training_set[tab])}")
            tab_abbrev = "".join(x[0] for x in tab.strip().split("_"))
            black_box.retrain(
                tab,
                [f"{tab} {tab_abbrev}"],
                [""],
                tab_to_single_table_training_set[tab],
                f"select * from {tab} {tab_abbrev} where {tab_abbrev}.{list(sel_cols[tab].keys())[0]} > {{lb1}} and {tab_abbrev}.{list(sel_cols[tab].keys())[0]} < {{ub1}};",
            )
        print(f"Training set size of: {len(two_way_training_set)}")
        training_query_template = f"select * from "\
            + ", ".join(two_way_model_tables) \
            + " where " + " and ".join(two_way_model_joins)
        for i in range(len(two_way_model_tables)):
            training_query_template += \
                f" and {list(sel_cols[two_way_model_tables[i].split(' ')[0]].keys())[0]} between "\
                    + "{lb" + str(i + 1) + "} and {ub" + str(i + 1) + "}"
        training_query_template += ";"

        black_box.retrain(
            two_way_model_name,
            two_way_model_tables,
            two_way_model_joins,
            two_way_training_set,
            training_query_template,
            # we need .split(" ")[0], because it is possible that the two_way_model_table has an alias, but we only want the table name to index the sel cols
        )

def load_log(
    conn,
    cursor,
    sql,
    cur_table_sizes,
    log_filename,
    output_prefix,
    n_tabs,
    sel_cols,
    tabs_for_model,
    tab_to_alias,
    single_table_to_qid,
    pgtypes,
    no_single_table_sel_injection,
    disable_table_with_sel_insertions_in_batch,
    two_way_model_name, 
    two_way_model_tables, 
    two_way_model_joins,
):
    assert n_tabs in [2, 3, 4]
    print("Load Log")

    def index_to_single_table_to_qid(tab):
        if tab in tab_to_alias:
            return single_table_to_qid[tab_to_alias[tab]]
        return single_table_to_qid[tab]
    
    tab_to_info_id = {}
    for i in range(len(tabs_for_model)):
        tab_to_info_id[tabs_for_model[i]] = i + 1

    to_insert_buffer = []
    to_delete_range = [None, None]

    last_op_type = None
    last_tab = None

    n_tabs_in_from = sql.lower().split("from")[1].split("where")[0].count(",") + 1

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
                if disable_table_with_sel_insertions_in_batch and t in tabs_for_model:
                    if _type == TupleUpdateType.INSERT:
                        cur_table_sizes[tab] += 1
                        attr_vals = []
                        for x in field.split(","):
                            if x == "None":
                                attr_vals.append(None)
                            else:
                                attr_vals.append(x)
                        attr_vals = tuple(attr_vals)
                        insert_command = (
                            f"insert into {t} ("
                            + ",".join(pgtypes[t])
                            + ") values ("
                            + ",".join(['%s'] * len(pgtypes[t]))
                            + f");"
                        )
                        cursor.execute(insert_command, attr_vals)
                    elif _type == TupleUpdateType.DELETE:
                        cur_table_sizes[tab] -= 1
                        delete_command = f"delete from {t} where row_id = {int(field)};"
                        cursor.execute(delete_command)
                else:
                    if _type == TupleUpdateType.INSERT:
                        cur_table_sizes[tab] += 1
                        attr_vals = []
                        for x in field.split(","):
                            if x == "None":
                                attr_vals.append(None)
                            else:
                                attr_vals.append(x)
                        attr_vals = tuple(attr_vals)
                        to_insert_buffer.append(cursor.mogrify("(" + ",".join(['%s'] * len(pgtypes[t])) + ")", attr_vals))
                    elif _type == TupleUpdateType.DELETE:
                        cur_table_sizes[tab] -= 1
                        row_id = int(field)
                        if to_delete_range[0] is None or to_delete_range[0] > row_id:
                            to_delete_range[0] = row_id
                        if to_delete_range[1] is None or to_delete_range[1] < row_id:
                            to_delete_range[1] = row_id
                ret += f"time={round(time.time() - start_ts, 6):.6f}"
            elif op_type == "Q":
                row = line.strip().split(",")
                cur_sql = copy.deepcopy(sql)
                two_way_tableset = []
                bounds = []
                base_time = 0.
                tab_to_single_table = {}
                single_pointers_and_sels_and_truesels = []

                for jj in range(n_tabs):
                    if row[1 + jj] in tabs_for_model:
                        tab = row[1 + jj]
                        tab_abbrev = "".join(x[0] for x in tab.strip().split("_"))
                        info_id = tab_to_info_id[tab]

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
                        bounds.append(lb)
                        bounds.append(ub)
                        cur_sql = cur_sql.replace("{lb" + str(info_id) + "}", str(lb)).replace("{ub" + str(info_id) + "}", str(ub))
                        two_way_tableset.append(tab)

                        if not no_single_table_sel_injection:
                            base_start_ts = time.time()
                            tab_to_single_table[tab] = black_box.predict_one(
                                tab,
                                [lb, ub],
                                [f"{tab} {tab_abbrev}"],
                                [""],
                            )
                            base_time += time.time() - base_start_ts
                            single_pointers_and_sels_and_truesels.append((index_to_single_table_to_qid(tab), tab_to_single_table[tab] / cur_table_sizes[tab], single_tab_card / cur_table_sizes[tab]))

                if no_single_table_sel_injection:
                    assert len(single_pointers_and_sels_and_truesels) == 0

                base_start_ts = time.time()
                two_way_est = black_box.predict_one(
                    two_way_model_name,
                    bounds,
                    two_way_model_tables,
                    two_way_model_joins,
                )
                base_time += time.time() - base_start_ts

                two_way_tableset.sort()
                for i in range(len(two_way_tableset)):
                    if two_way_tableset[i] in tab_to_alias:
                        two_way_tableset[i] = tab_to_alias[two_way_tableset[i]]
                two_way_tableset = tuple(two_way_tableset)

                true_join_card_id = 1 + n_tabs * 4
                true_join_card = int(row[true_join_card_id])
                
                _ = inject_multiple_single_table_sel_and_get_cost(
                    pointers_and_sels=[(x[0], x[1]) for x in single_pointers_and_sels_and_truesels],
                    n_tabs_in_from=n_tabs_in_from,
                    q=cur_sql,
                    hint=None,
                    plan_info=False,
                )
                tableset_to_joinid, _, tableset_to_rawrows, _, min_join_qid, max_join_qid, _ = load_pg_join_subquery_info()

                single_pointers_and_sels_and_truesels.sort(key=lambda x:x[0])
                _, join_order, scan_mtd = inject_single_and_join_sels_and_get_cost(
                    pointers=[x[0] for x in single_pointers_and_sels_and_truesels] + [tableset_to_joinid[two_way_tableset]],
                    single_sels=[x[1] for x in single_pointers_and_sels_and_truesels],
                    n_single=min_join_qid,
                    join_sels=[two_way_est / tableset_to_rawrows[two_way_tableset]],
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
                    pointers_and_sels=[(x[0], x[2]) for x in single_pointers_and_sels_and_truesels],
                    n_tabs_in_from=n_tabs_in_from,
                    q=cur_sql,
                    hint=None,
                    plan_info=False,
                )
                _, _, truecard_tableset_to_rawrows, _, _, _, _ = load_pg_join_subquery_info()
                plan_cost = inject_single_and_join_sels_and_get_cost(
                    pointers=[x[0] for x in single_pointers_and_sels_and_truesels] + [tableset_to_joinid[two_way_tableset]],
                    single_sels=[x[2] for x in single_pointers_and_sels_and_truesels],
                    n_single=min_join_qid,
                    join_sels=[true_join_card / truecard_tableset_to_rawrows[two_way_tableset]],
                    n_all=max_join_qid + 1,
                    q=cur_sql,
                    hint=hint,
                    plan_info=False,
                )
                # lets run it!
                _, execution_time, _ = get_real_latency(
                    sql=cur_sql,
                    hint=hint,
                )

                execution_time = execution_time / 1000.
    
                ret += f"two_way_est=" + str(two_way_est)
                ret += f",observed_err={round(get_average_q_error([true_join_card], [two_way_est]), 6):.6f}"
                ret += f",plan_cost={plan_cost}"
                ret += f",execution_time={round(execution_time, 6):.6f}"
                ret += f",base_time={round(base_time, 6):.6f}"
                ret += f",time={round(time.time() - start_ts, 6):.6f}"

                hint_wo_newline = hint.replace('\n', '')
                ret += f",plan={hint_wo_newline}"
            else:
                raise NotImplementedError
            fout.write(ret + "\n")

def load_table(conn, cursor, table_name, file, jk_cols, sel_cols, pgtypes, timestamp_col=None):
    cursor.execute(f"drop table if exists {table_name};")
    for jk_col in jk_cols:
        cursor.execute(f"drop index if exists idx_{table_name}_{jk_col};")
    for sel_col in sel_cols:
        if " " in sel_col:
            for each in sel_col.split(" "):
                if each.startswith(table_name[0]): # share the same first bit as the table_name
                    cursor.execute(f"drop index if exists idx_{table_name}_{each};")
        else:
            cursor.execute(f"drop index if exists idx_{table_name}_{sel_col};")

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
    for sel_col in sel_cols:
        if " " in sel_col:
            for each in sel_col.split(" "):
                if each.startswith(table_name[0]): # share the same first bit as the table_name
                    cursor.execute(f"create index idx_{table_name}_{each} on {table_name}({each});")
        else:
            cursor.execute(
                f"create index idx_{table_name}_{sel_col} on {table_name}({sel_col});"
            )

    cursor.execute(f"select count(*) from {table_name};")
    return cursor.fetchone()[0]

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
        if "no_single_table_sel_injection" in config:
            no_single_table_sel_injection = config["no_single_table_sel_injection"]
        else:
            no_single_table_sel_injection = False
        if "disable_table_with_sel_insertions_in_batch" in config:
            disable_table_with_sel_insertions_in_batch = config["disable_table_with_sel_insertions_in_batch"]
        else:
            disable_table_with_sel_insertions_in_batch = False
        table_filenames = config["table_filenames"]
        log_filename = config["log_filename"]
        trainingset_filename = config["trainingset_filename"]
        output_prefix = config["output_prefix"]
        if debug:
            output_prefix = "/".join(output_prefix.split("/")[:-1]) + "/test"
        elif no_single_table_sel_injection:
            output_prefix += f"_no-retraining_no-cache_no-single-injections"
        else:
            output_prefix += f"_no-retraining_no-cache"
            if "algorithm" in config:
                output_prefix += f"_{algorithm}"

        # get db connection
        conn, cursor = get_connection()

        cur_table_sizes = {}
        # load static tables
        for static_tab in static_tabs:
            cur_table_sizes[static_tab] = load_table(
                conn=conn,
                cursor=cursor,
                table_name=static_tab,
                file=table_filenames[static_tab],
                jk_cols=jk_cols[static_tab],
                sel_cols={},
                pgtypes=pgtypes[static_tab],
                timestamp_col=None,
            )


        # we only need to build model for tables with selections
        tabs_for_model = list(sel_cols.keys())
        tab_to_alias = {}
        for ii in range(len(tabs)):
            if (" " in tabs[ii] or "as" in tabs[ii]):
                # there is an alias name
                _tab = tabs[ii].split(" ")[0]
                tab_to_alias[_tab] = tabs[ii].split(" ")[-1]
                tabs[ii] = _tab
            tab = tabs[ii]
            cur_table_sizes[tab] = load_table(
                conn=conn,
                cursor=cursor,
                table_name=tab,
                file=table_filenames[tab],
                jk_cols=jk_cols[tab],
                sel_cols=sel_cols[tab] if tab in sel_cols else {},
                pgtypes=pgtypes[tab],
                timestamp_col=timestamp_cols[tab],
            )

        # analyze
        cursor.execute("vacuum (full, analyze);")

        sql_for_getting_single_table_pointer = copy.deepcopy(sql)
        info_id = 0
        for tab in sel_cols:
            _col = list(sel_cols[tab].keys())[0]
            info_id += 1
            sql_for_getting_single_table_pointer = sql_for_getting_single_table_pointer.replace(
                "{lb" + str(info_id) + "}",
                str(sel_cols[tab][_col]["min"])
            ).replace(
                "{ub" + str(info_id) + "}",
                str(sel_cols[tab][_col]["max"])
            )
        single_table_to_qid = get_single_table_pointer(
            sql=sql_for_getting_single_table_pointer
        )
        
        # setup the black box
        # three models: one for two-way (or multi-way) (called two_way_model_name), two for single table (called tab respectively)

        two_way_model_name = "-".join(list(sel_cols.keys()))
        two_way_model_tables = []
        black_box.default_column_min_max_vals[two_way_model_name] = {}
        black_box.default_predicate_templates[two_way_model_name] = []
        
        for tab in sel_cols:
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

        load_trainingset(
            trainingset_filename=trainingset_filename,
            n_tabs=len(tabs),
            tabs_for_model=tabs_for_model,
            sel_cols=sel_cols,
            pgtypes=pgtypes,
            two_way_model_name=two_way_model_name, 
            two_way_model_tables=two_way_model_tables, 
            two_way_model_joins=join_to_watch,
        )

        load_log(
            conn=conn,
            cursor=cursor,
            sql=sql,
            cur_table_sizes=cur_table_sizes,
            log_filename=log_filename,
            output_prefix=output_prefix,
            n_tabs=len(tabs),
            sel_cols=sel_cols,
            tabs_for_model=tabs_for_model,
            tab_to_alias=tab_to_alias,
            single_table_to_qid=single_table_to_qid,
            pgtypes=pgtypes,
            no_single_table_sel_injection=no_single_table_sel_injection,
            disable_table_with_sel_insertions_in_batch=disable_table_with_sel_insertions_in_batch,
            two_way_model_name=two_way_model_name, 
            two_way_model_tables=two_way_model_tables, 
            two_way_model_joins=join_to_watch,
        )
