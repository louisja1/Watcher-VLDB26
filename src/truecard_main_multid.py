from operation import TupleUpdateType
from tqdm import tqdm
import time
import warnings
import json
import sys
import copy
import black_box
from error_metrics import get_average_q_error
from connection import get_connection
from pg_hint_utility import gen_final_hint, inject_multiple_single_table_sel_and_get_cost, get_real_latency, get_single_table_pointer, inject_single_and_join_sels_and_get_cost, load_pg_join_subquery_info

warnings.filterwarnings("ignore")

def load_log(
    conn,
    cursor,
    sql,
    cur_table_sizes,
    log_filename,
    output_prefix,
    n_tabs,
    tabs_with_sel,
    tab_to_alias,
    single_table_to_qid,
    sel_cols,
    pgtypes,
    no_single_table_sel_injection,
    disable_table_with_sel_insertions_in_batch,
):
    assert n_tabs in [2, 3, 4]
    print("Load Log")

    def index_to_single_table_to_qid(tab):
        if tab in tab_to_alias:
            return single_table_to_qid[tab_to_alias[tab]]
        return single_table_to_qid[tab]
    
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
                if disable_table_with_sel_insertions_in_batch and t in tabs_with_sel:
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
                single_pointers_and_sels = []

                for jj in range(n_tabs):
                    if row[1 + jj] in sel_cols:
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
                        info_id = jj + 1
                        cur_sql = cur_sql.replace("{lb" + str(info_id) + "}", str(lb)).replace("{ub" + str(info_id) + "}", str(ub))
                        two_way_tableset.append(tab)
                        if not no_single_table_sel_injection:
                            
                            single_pointers_and_sels.append((index_to_single_table_to_qid(tab), single_tab_card / cur_table_sizes[tab]))

                if no_single_table_sel_injection:
                    assert len(single_pointers_and_sels) == 0

                _ = inject_multiple_single_table_sel_and_get_cost(
                    pointers_and_sels=single_pointers_and_sels,
                    n_tabs_in_from=n_tabs_in_from,
                    q=cur_sql,
                    hint=None,
                    plan_info=False,
                )
                tableset_to_joinid, _, tableset_to_rawrows, _, min_join_qid, max_join_qid, _ = load_pg_join_subquery_info()

                two_way_tableset.sort()
                for i in range(len(two_way_tableset)):
                    if two_way_tableset[i] in tab_to_alias:
                        two_way_tableset[i] = tab_to_alias[two_way_tableset[i]]
                two_way_tableset = tuple(two_way_tableset)
                true_join_card_id = 1 + n_tabs * 4
                true_join_card = int(row[true_join_card_id])

                single_pointers_and_sels.sort(key=lambda x:x[0])

                plan_cost, join_order, scan_mtd = inject_single_and_join_sels_and_get_cost(
                    pointers=[x[0] for x in single_pointers_and_sels] + [tableset_to_joinid[two_way_tableset]],
                    single_sels=[x[1] for x in single_pointers_and_sels],
                    n_single=min_join_qid,
                    join_sels=[true_join_card / tableset_to_rawrows[two_way_tableset]],
                    n_all=max_join_qid + 1,
                    q=cur_sql,
                    hint=None,
                    plan_info=True,
                ) 
                # construct the plan by the info
                hint = gen_final_hint(scan_mtd=scan_mtd, str=join_order)
                # lets run it!
                _, execution_time, _ = get_real_latency(
                    sql=cur_sql,
                    hint=hint,
                )

                execution_time = execution_time / 1000.
    
                ret += f"true_join_card=" + str(true_join_card)
                ret += f",observed_err={round(get_average_q_error([true_join_card], [true_join_card]), 6):.6f}"
                ret += f",plan_cost={plan_cost}"
                ret += f",execution_time={round(execution_time, 6):.6f}"
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
        output_prefix = config["output_prefix"]
        if debug:
            output_prefix = "/".join(output_prefix.split("/")[:-1]) + "/test"
        elif no_single_table_sel_injection:
            output_prefix += f"_true-card_no-single-injections"
        else:
            output_prefix += f"_true-card"
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

        # load dynamic tables
        tabs_with_sel = list(sel_cols.keys())
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

        load_log(
            conn=conn,
            cursor=cursor,
            sql=sql,
            cur_table_sizes=cur_table_sizes,
            log_filename=log_filename,
            output_prefix=output_prefix,
            n_tabs=len(tabs),
            tabs_with_sel=tabs_with_sel,
            tab_to_alias=tab_to_alias,
            single_table_to_qid=single_table_to_qid,
            sel_cols=sel_cols,
            pgtypes=pgtypes,
            no_single_table_sel_injection=no_single_table_sel_injection,
            disable_table_with_sel_insertions_in_batch=disable_table_with_sel_insertions_in_batch,
        )
