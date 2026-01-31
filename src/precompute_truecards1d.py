from operation import TupleUpdateType
from tqdm import tqdm
import time
import warnings
import json
import sys
import copy
import black_box
from connection import get_connection
from pg_hint_utility import get_real_latency
warnings.filterwarnings("ignore")

def precompute_log(
    conn,
    cursor,
    counting_sqls,
    cur_table_sizes,
    log_filename,
    n_tabs,
    sel_cols,
    pgtypes,
    disable_table_with_sel_insertions_in_batch,
):
    assert len(sel_cols) == 1
    print("Precomputing the true cards for Log")

    assert "_old" in log_filename
    new_log_filename = log_filename.replace("_old", "")
    log_cost_filename = new_log_filename.replace("_log", "_log_latency_for_true_cards")

    to_insert_buffer = []
    to_delete_range = [None, None]

    last_op_type = None
    last_tab = None
    
    with open(log_filename, "r") as fin, open(new_log_filename, "w") as fout, open(log_cost_filename, "w") as flatency:
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
                if disable_table_with_sel_insertions_in_batch and t in sel_cols:
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
                bounds = []
                for jj in range(n_tabs):
                    if row[1 + jj] in sel_cols:
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
                        bounds.append((lb, ub))
                assert len(bounds) == 1
                all_latency = []
                for jj in range(n_tabs):
                    if row[1 + jj] in sel_cols:
                        assert row[1 + jj] in counting_sqls
                        counting_sql = copy.deepcopy(counting_sqls[row[1 + jj]])
                        counting_sql = counting_sql.replace("{lb1}", str(bounds[0][0])).replace("{ub1}", str(bounds[0][1]))
                        cursor.execute(counting_sql)
                        if row[1 + n_tabs * 3 + jj] == "true_card1":
                            row[1 + n_tabs * 3 + jj] = str(cursor.fetchall()[0][0])
                        _, latency, _ = get_real_latency(
                            sql=counting_sql,
                            hint=None,
                            enable_single_sel_injection=False,
                            n_repetitions=1,
                        )
                        all_latency.append(latency)

                assert len(all_latency) == 1
                line = ",".join(row) + "\n"
                flatency.write(",".join(str(x) for x in all_latency) + "\n")
            else:
                raise NotImplementedError
            fout.write(line)

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
        counting_sqls = config["counting_sqls"]
        pgtypes = config["pgtypes"]
        debug = config["debug"]
        if "disable_table_with_sel_insertions_in_batch" in config:
            disable_table_with_sel_insertions_in_batch = config["disable_table_with_sel_insertions_in_batch"]
        else:
            disable_table_with_sel_insertions_in_batch = False
        table_filenames = config["table_filenames"]
        log_filename = config["log_filename"]

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

        precompute_log(
            conn=conn,
            cursor=cursor,
            counting_sqls=counting_sqls,
            cur_table_sizes=cur_table_sizes,
            log_filename=log_filename,
            n_tabs=len(tabs),
            sel_cols=sel_cols,
            pgtypes=pgtypes,
            disable_table_with_sel_insertions_in_batch=disable_table_with_sel_insertions_in_batch,
        )
