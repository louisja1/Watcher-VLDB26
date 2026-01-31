from operation import TupleUpdateType, RangeSelection
from connection import get_connection, close_connection
from tqdm import tqdm
from watcher2d import Watcher2d
import time
import warnings
import black_box
import logger
import json
import sys
from error_metrics import get_percentile_q_error

warnings.filterwarnings("ignore")

def load_trainingset(
    trainingset_filename,
    n_tabs,
    tabs_for_model,
    sel_cols,
    watcher2d,
    pgtypes,
    init_theta, # if some init_theta = "90th", we will need to collect the 90th percentile error in the initial training set
    model_name, 
    model_tables, 
    model_joins,
):
    print("Load Trainingset")
    with open(trainingset_filename, "r") as fin:
        training_set = []
        lines = list(fin.readlines())

        for i in range(len(lines)):
            row = lines[i].strip().split(",")
            bounds = []
            for j in range(n_tabs):
                if row[1 + j] in tabs_for_model and row[1 + j] == tabs_for_model[len(bounds) // 2]:
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

            assert len(bounds) == 4

            true_card_id = 1 + n_tabs * 4
            true_card = int(row[true_card_id])
            if (
                true_card >= 1
                and len(training_set) < black_box.config["training_size"]
            ):
                training_set.append(tuple(bounds + [true_card]))

        print("Finish loading training set:")
        print(f"Training set size of: {len(training_set)}")
        black_box.retrain(
            model_name,
            model_tables,
            model_joins,
            training_set,
            f"select * from " + ", ".join(model_tables) + " where " + " and ".join(model_joins) + f" and {list(sel_cols[model_tables[0]].keys())[0]} between {{lb1}} and {{ub1}} and {list(sel_cols[model_tables[1]].keys())[0]} between {{lb2}} and {{ub2}};",
        )

        model_preds = [
            black_box.predict_one(
                model_name,
                list(tmp[:-1]),
                model_tables,
                model_joins,
            )
            for tmp in training_set
        ]
        if init_theta in ["90th", "95th"]:
            pth = init_theta
            watcher2d.threshold = get_percentile_q_error(
                [tmp[-1] for tmp in training_set],
                model_preds,
                int(init_theta.rstrip("th")),
            )
            print(f"Training set {pth}-percentile relative error: {watcher2d.threshold}")
        else:
            assert isinstance(init_theta, float)

        for i in range(len(training_set)):
            t1 = tabs_for_model[0]
            t2 = tabs_for_model[1]
            lb1 = training_set[i][0]
            ub1 = training_set[i][1]
            lb2 = training_set[i][2]
            ub2 = training_set[i][3]
            card = training_set[i][4]

            rs1 = RangeSelection(t1, lb1, ub1)
            rs1.update_by(watcher2d.tables[t1].metadata.sel_min, watcher2d.tables[t1].metadata.sel_max)
            rs2 = RangeSelection(t2, lb2, ub2)
            rs2.update_by(watcher2d.tables[t2].metadata.sel_min, watcher2d.tables[t2].metadata.sel_max)

            watcher2d.insert_to_watchset(rs1, rs2, card, model_preds[i])
        print(f"Finish loading watchset (intially {len(watcher2d.watch_set)} items)")

def load_log(
    conn,
    cursor,
    log_filename,
    output_prefix,
    n_tabs,
    sel_cols,
    pgtypes,
    tabs_for_model,
    watcher2d,
):
    print("Load Log")
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
            # we flush the tuple updates for non-watcher tables
            if (last_op_type != op_type or last_tab != tab) and (last_tab not in tabs_for_model) and (last_op_type != "Q"):
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
                if t not in tabs_for_model:
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
                    ret += watcher2d.insert(t, field)
                elif _type == TupleUpdateType.DELETE:
                    ret += watcher2d.delete(t, int(field))
                end_tu_ts = time.time()
                total_tu_time += end_tu_ts - start_tu_ts
                total_other_time -= end_tu_ts - start_tu_ts

            elif op_type == "Q":
                row = line.strip().split(",")
                two_rs = []
                for jj in range(n_tabs):
                    if row[1 + jj] in tabs_for_model and row[1 + jj] == tabs_for_model[len(two_rs)]:
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

                assert len(two_rs) == 2
                true_card_id = 1 + n_tabs * 4
                true_card = int(row[true_card_id])

                start_est_ts = time.time()
                est, single_err, info = watcher2d.query(two_rs[0], two_rs[1], true_card)
                end_est_ts = time.time()

                total_est_time += end_est_ts - start_est_ts
                total_other_time -= end_est_ts - start_est_ts

                ret += f"est=" + str(est) + ","
                ret += f"observed_err=" + str(single_err)

                if len(info) > 0:
                    ret += "," + info
            else:
                raise NotImplementedError
            
            total_other_time += time.time() - start_ts
            if ret[-1] != ":":
                ret += ","
            ret += "watchset_err=" + str(watcher2d.get_error())
            fout.write(ret + "\n")
    with open(output_prefix + "_time.txt", "w") as fout:
        fout.write(f"Total est. time: {total_est_time}\n")
        fout.write(f"Total tu time: {total_tu_time}\n")
        fout.write(f"Total other time: {total_other_time}\n")
        fout.write(f"Total query bounds time: {logger.query_bounds_time}\n")
        fout.write(f"Total query bounds other time: {logger.query_bounds_other_time}\n")
        fout.write(f"Total KV entries visited: {logger.n_kv_pairs}\n")
        fout.write(f"Total queries affected: {logger.n_affected_queries}\n")
        fout.write(f"Total number of ranges/sub-ranges: {logger.total_bounds}\n")

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
        jk_bounds=config["jk_bounds"]
        timestamp_cols=config["timestamp_cols"]
        join_to_watch=config["join_to_watch"]
        sql = config["sql"]
        pgtypes = config["pgtypes"]

        error_metric = config["error_metric"]
        init_theta = config["init_theta"] 
        # for example and by default
        # trigger a retrain, when the 90th-percentile Q-error in the watchset > init_theta
        # by setting error_metric = "90th", init_theta = some float 

        max_level = config["max_level"] 
        fan_out = config["fan_out"]
        jk_theta = config["jk_theta"]

        debug = config["debug"]
        table_filenames = config["table_filenames"]
        log_filename = config["log_filename"]
        trainingset_filename = config["trainingset_filename"]
        output_prefix = config["output_prefix"]
        # # we delay the output_prefix construction, because init_theta can have a threshold like "90th", 
        # # which are based on the predictions by the initial model

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

        # initialize Watcher2D
        watcher2d = Watcher2d(
            conn=conn,
            cursor=cursor,
            sql=sql,
            watchset_size=black_box.config["training_size"],
            random_seed=2023,
            threshold=init_theta,
            error_metric=error_metric,
            pgtypes=pgtypes,
            jk_cols=jk_cols,
            sel_cols=sel_cols,
        )

        # we only need to build watcher1d for tables with selections
        tabs_for_model = list(sel_cols.keys())
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
            if tab in sel_cols:
                jk_to_watch = list(jk_bounds_for_leveldb[tab].keys())[0]
                sel_to_watch = list(sel_bounds_for_leveldb[tab].keys())[0]
                watcher2d.init_metadata(
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
                watcher2d.load_table(
                    tab=tab,
                    file=table_filenames[tab],
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
        model_name = "-".join(list(sel_cols.keys()))
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
        watcher2d.setup_black_box(
            model_name=model_name,
            model_tables=model_tables,
            model_joins=[join_to_watch],
        )

        load_trainingset(
            trainingset_filename=trainingset_filename,
            n_tabs=len(tabs),
            tabs_for_model=tabs_for_model,
            sel_cols=sel_cols,
            watcher2d=watcher2d,
            pgtypes=pgtypes,
            init_theta=init_theta, # if some init_theta = "90th", we will need to collect the 90th percentile error in the initial training set
            model_name=model_name, 
            model_tables=model_tables, 
            model_joins=[join_to_watch],
        )

        # re-construct the output_prefix, since the init_theta might be updated based on the initial model
        if debug:
            output_prefix = "/".join(output_prefix.split("/")[:-1]) + "/test"
        else:
            output_prefix += "_theta[" + "-join-".join(tabs) + f"({error_metric})<={watcher2d.threshold}]"
            if "algorithm" in config:
                output_prefix += f"_{algorithm}"
           
        load_log(
            conn=conn,
            cursor=cursor,
            log_filename=log_filename,
            output_prefix=output_prefix,
            n_tabs=len(tabs),
            sel_cols=sel_cols,
            pgtypes=pgtypes,
            tabs_for_model=tabs_for_model,
            watcher2d=watcher2d,
        )

        watcher2d.clean()
        close_connection(conn=conn, cursor=cursor)

