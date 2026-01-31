from tqdm import tqdm
import warnings
import json
import sys
import black_box
from connection import get_connection
from pg_hint_utility import get_real_latency

warnings.filterwarnings("ignore")

def load_trainingset(
    trainingset_filename, n_tabs, tabs_for_model, sel_cols, pgtypes
):
    assert len(tabs_for_model) == 1

    tab_to_info_id = {}
    for i in range(len(tabs_for_model)):
        tab_to_info_id[tabs_for_model[i]] = i + 1

    print("Executing Trainingset")
    assert "_old" in trainingset_filename
    new_trainingset_filename = trainingset_filename.replace("_old", "")

    with open(trainingset_filename, "r") as fin, open(new_trainingset_filename, "w") as fout:
        lines = list(fin.readlines())
        for i in tqdm(range(len(lines))):
            row = lines[i].strip().split(",")
            for j in range(n_tabs):
                if row[1 + j] in tabs_for_model:
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
                    
                    info_id = tab_to_info_id[tab]
                    cost, latency, planning_time = get_real_latency(
                        sql=sql.replace("{lb" + str(info_id) + "}", str(lb)).replace("{ub" + str(info_id) + "}", str(ub)),
                        hint=None,
                        enable_single_sel_injection=False,
                    )
                    fout.write(",".join(row) + f",{cost},{latency},{planning_time}\n")

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
        table_filenames = config["table_filenames"]
        trainingset_filename = config["trainingset_filename"]

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

        load_trainingset(
            trainingset_filename=trainingset_filename,
            n_tabs=len(tabs),
            tabs_for_model=tabs_for_model,
            sel_cols=sel_cols,
            pgtypes=pgtypes,
        )