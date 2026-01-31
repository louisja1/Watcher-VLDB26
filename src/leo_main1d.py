from operation import RangeSelection, TupleUpdateType
from tqdm import tqdm
from leo1d import Leo1D
import time
import warnings
import black_box
import json
import sys
from connection import get_connection, close_connection
from error_metrics import get_percentile_q_error

warnings.filterwarnings("ignore")

def load_trainingset(
    trainingset_filename, 
    n_tabs, 
    tabs_for_leo1d, 
    sel_cols, 
    leo1ds, 
    pgtypes,
    tab_to_alias,
):
    print("Load Trainingset")
    with open(trainingset_filename, "r") as fin:
        tab_to_training_set = {}
        for tab in tabs_for_leo1d:
            tab_to_training_set[tab] = []

        lines = list(fin.readlines())

        for i in range(len(lines)):
            row = lines[i].strip().split(",")
            for j in range(n_tabs):
                if row[1 + j] in tabs_for_leo1d:
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
                        elif pgtypes[tab][_col].lower() == "integer":
                            lb, ub = int(row[lb_id]), int(row[ub_id])
                        else:
                            raise NotImplementedError
                        tab_to_training_set[tab].append((lb, ub, int(row[card_id])))

        print("Finish loading training set:")
        tab_to_model_preds = {}
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
            tab_to_model_preds[tab] = [
                black_box.predict_one(
                    tab,
                    [x[0], x[1]],
                    [f"{tab} {tab_abbrev}"],
                    [""],
                )
                for x in tab_to_training_set[tab]
            ]

        print("Load leoset by training set")
        for tab in tab_to_training_set:
            ii = 0
            for lb, ub, card in tab_to_training_set[tab]:
                # note that we only support selection on one column for now
                _col = list(sel_cols[tab].keys())[0]
                rs = RangeSelection(tab, lb, ub)
                rs.update_by(sel_cols[tab][_col]["min"], sel_cols[tab][_col]["max"])
                leo1ds[tab].insert_to_leoset(rs, card, tab_to_model_preds[tab][ii])
                ii += 1
            print(f"Finish loading leoset on {tab} (intially {len(leo1ds[tab].leo_set)} items)")


def load_log(
    conn,
    cursor,
    log_filename,
    output_prefix,
    n_tabs,
    sel_cols,
    tabs_for_leo1d,
    leo1ds,
    pgtypes,
):
    print("Load Log")
    init_start_ts = time.time()
    total_est_time = 0.0
    total_tu_time = 0.0
    total_other_time = 0.0
    n_tu = 0

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
            # we flush the tuple updates for non-leo tables
            if (last_op_type != op_type or last_tab != tab) and (last_tab not in tabs_for_leo1d) and (last_op_type != "Q"):
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
                n_tu += 1
                _type = TupleUpdateType.from_str(line.split(",")[0])
                t = line.split(",")[1]
                field = line[2 + len(t) + 1 :].strip()
                if t not in tabs_for_leo1d:
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
                    total_other_time += time.time() - start_ts
                    fout.write(ret + f"time={round(time.time() - start_ts, 6):.6f}\n")
                    continue
                
                start_tu_ts = time.time()
                if _type == TupleUpdateType.INSERT:
                    ret += leo1ds[t].insert(t, field)
                elif _type == TupleUpdateType.DELETE:
                    ret += leo1ds[t].delete(t, int(field))
                end_tu_ts = time.time()
                total_tu_time += end_tu_ts - start_tu_ts
                total_other_time -= end_tu_ts - start_tu_ts
            elif op_type == "Q":
                row = line.strip().split(",")
                for jj in range(n_tabs):
                    if row[1 + jj] in tabs_for_leo1d:
                        tab = row[1 + jj]
                        lb_id = 1 + n_tabs + 2 * jj
                        ub_id = lb_id + 1
                        card_id = 1 + n_tabs * 3 + jj
                        _col = list(sel_cols[tab].keys())[0]
                        if "/" in _col:
                            lb, ub = float(row[lb_id]), float(row[ub_id])
                        elif pgtypes[tab][_col].lower() == "real" or pgtypes[tab][_col].lower().startswith("decimal"):
                            lb, ub = float(row[lb_id]), float(row[ub_id])
                        elif pgtypes[tab][_col].lower() == "integer":
                            lb, ub = int(row[lb_id]), int(row[ub_id])
                        else:
                            raise NotImplementedError
                        true_card = int(row[card_id])

                        rs = RangeSelection(tab, lb, ub)
                        rs.update_by(
                            sel_cols[tab][_col]["min"], sel_cols[tab][_col]["max"]
                        )

                        start_est_ts = time.time()
                        est, single_err, info = leo1ds[tab].query(rs, true_card)
                        end_est_ts = time.time()

                        total_est_time += end_est_ts - start_est_ts
                        total_other_time -= end_est_ts - start_est_ts

                        info_id = leo1ds[tab].info_id
                        ret += f"est{info_id}=" + str(est) + ","
                        ret += f"observed_err{info_id}=" + str(single_err)

                        if len(info) > 0:
                            ret += "," + info
            else:
                raise NotImplementedError

            total_other_time += time.time() - start_ts
            fout.write(ret + "\n")
    with open(output_prefix + "_time.txt", "w") as fout:
        fout.write(f"Total time: {time.time() - init_start_ts}\n")
        fout.write(f"Total est. time: {total_est_time}\n")
        fout.write(f"Total tu time: {total_tu_time}\n")
        fout.write(f"Total other time: {total_other_time}\n")


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
        jk_cols = config["jk_cols"]
        timestamp_cols = config["timestamp_cols"]
        sql = config["sql"]
        pgtypes = config["pgtypes"]
        overlap_ratio_lb = config["overlap_ratio_lb"]

        debug = config["debug"]
        table_filenames = config["table_filenames"]
        log_filename = config["log_filename"]
        trainingset_filename = config["trainingset_filename"]
        output_prefix = config["output_prefix"]

        if debug:
            output_prefix = "/".join(output_prefix.split("/")[:-1]) + "/test"
        else:
            output_prefix += "_leo"
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

        # we only need to build leo1d for tables with selections
        tabs_for_leo1d = list(sel_cols.keys())
        leo1ds = {}
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
            if tab in tabs_for_leo1d:
                leo1ds[tab] = Leo1D(
                    name=tab,
                    conn=conn,
                    cursor=cursor,
                    sql=sql,
                    overlap_ratio_lb=overlap_ratio_lb,
                    info_id=ii + 1,  # 1-base info ids
                    sel_by_domain=RangeSelection( # only utilized to get the pointer for single table
                        table_name=tab,
                        lb=sel_cols[tab][list(sel_cols[tab].keys())[0]]["min"],
                        ub=sel_cols[tab][list(sel_cols[tab].keys())[0]]["max"]
                    ),
                    assigned_alias=assigned_alias,
                )
                leo1ds[tab].load_table(
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
        for tab in leo1ds:
            leo1ds[tab].setup_single_table_pointer()

        # setup the black box
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

        load_trainingset(
            trainingset_filename,
            len(tabs),
            tabs_for_leo1d,
            sel_cols,
            leo1ds,
            pgtypes,
            tab_to_alias
        )

        load_log(
            conn,
            cursor,
            log_filename,
            output_prefix,
            len(tabs),
            sel_cols,
            tabs_for_leo1d,
            leo1ds,
            pgtypes,
        )

        for tab in tabs_for_leo1d:
            leo1ds[tab].clean()

        close_connection(conn=conn, cursor=cursor)
