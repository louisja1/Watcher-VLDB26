import os
import psycopg2
import subprocess
import numpy as np
import copy
from connection import get_connection

def get_single_table_pointer(sql):
    get_plan_cost(
        sql=sql,
        hint=None,
        plan_info=False,
        limit_worker=True,
        enable_single_sel_injection=False,
        enable_join_sel_injection=False,
    )

    with open("/winhomes/yl762/watcher_data/single_tbl_est_record.txt", "r") as fin:
        q_id = None
        single_tab_to_qid = {}
        for line in fin.readlines():
            if line.startswith("query:"):
                q_id = int(line.strip().split(" ")[-1])
            elif line.startswith("RELOPTINFO"):
                assert q_id is not None
                tab = line.split("(")[1].split(")")[0]
                single_tab_to_qid[tab] = q_id
        return single_tab_to_qid

def get_two_way_join_info(sql, tableset):
    get_plan_cost(
        sql=sql,
        hint=None,
        plan_info=False,
        limit_worker=True,
        enable_single_sel_injection=False,
        enable_join_sel_injection=False,
    )
    
    with open("/winhomes/yl762/watcher_data/join_est_record_job.txt", "r") as fin:
        q_id = None
        raw_rows = None
        relation_names = []
        min_join_qid = None
        max_join_qid = None

        two_way_join_pointer = None
        two_way_join_raw_rows = None

        for line in fin.readlines():
            if line.startswith("query:"):
                q_id = int(line.strip().split(" ")[-1])
                if min_join_qid is None or min_join_qid > q_id:
                    min_join_qid = q_id
                if max_join_qid is None or max_join_qid < q_id:
                    max_join_qid = q_id
            elif line.startswith("RELOPTINFO"):
                assert q_id is not None
                relation_names.extend(line.split("(")[1].split(")")[0].split(" "))
            elif line.startswith("Raw Rows"):
                raw_rows = int(float(line.split(": ")[-1]))
            elif line.startswith("Selectivity"):
                relation_names.sort()
                if tuple(relation_names) == tableset:
                    two_way_join_pointer, two_way_join_raw_rows = q_id, raw_rows
                q_id = None
                raw_rows = None
                relation_names = []
    return two_way_join_pointer, two_way_join_raw_rows, min_join_qid, max_join_qid

def inject_single_table_sels_and_get_cost(pointer, sel, n_tabs_in_from, q, hint, plan_info):
    sels_filename = "/winhomes/yl762/watcher_data/sels.txt"
    pointers_filename = "/winhomes/yl762/watcher_data/pointers.txt"
    assert pointer is not None and sel is not None

    with open(sels_filename, "w+") as sels_fout, \
        open(pointers_filename, "w+") as pointers_fout:
        sels = ["0.99"] * n_tabs_in_from
        sels[pointer] = str(sel)
        sels_fout.write("\n".join(sels))
        pointers_fout.write("\n".join([str(pointer)]))
    return get_plan_cost(sql=q, hint=hint, plan_info=plan_info, limit_worker=True, enable_single_sel_injection=True, enable_join_sel_injection=False)

def inject_multiple_single_table_sel_and_get_cost(pointers_and_sels, n_tabs_in_from, q, hint, plan_info):
    sels_filename = "/winhomes/yl762/watcher_data/sels.txt"
    pointers_filename = "/winhomes/yl762/watcher_data/pointers.txt"

    pointers_and_sels.sort(key=lambda x:x[0])

    with open(sels_filename, "w+") as sels_fout, \
        open(pointers_filename, "w+") as pointers_fout:
        sels = ["0.99"] * n_tabs_in_from
        for pointer, sel in pointers_and_sels:
            sels[pointer] = str(sel)
        sels_fout.write("\n".join(sels))
        pointers_fout.write("\n".join([str(pointer) for pointer, _ in pointers_and_sels]))
    
    _enable_single_sel_injection = len(pointers_and_sels) > 0
    return get_plan_cost(sql=q, hint=hint, plan_info=plan_info, limit_worker=True, enable_single_sel_injection=_enable_single_sel_injection, enable_join_sel_injection=False)

def inject_join_sel_and_get_cost(join_pointer, join_sel, min_join_qid, max_join_qid, q, hint, plan_info):
    single_sels_filename = "/winhomes/yl762/watcher_data/sels.txt"
    join_sels_filename = "/winhomes/yl762/watcher_data/join.txt"
    pointers_filename = "/winhomes/yl762/watcher_data/pointers.txt"

    with open(single_sels_filename, "w+") as single_sels_fout, \
        open(join_sels_filename, "w+") as join_sels_fout, \
        open(pointers_filename, "w+") as pointers_fout:
        single_sels = ["0.99"] * min_join_qid
        join_sels = ["0.99"] * (max_join_qid - min_join_qid + 1)
        join_sels[join_pointer - min_join_qid] = str(join_sel)

        single_sels_fout.write("\n".join(single_sels))
        join_sels_fout.write("\n".join(join_sels))
        pointers_fout.write("\n".join([str(join_pointer)]))
    return get_plan_cost(sql=q, hint=hint, plan_info=plan_info, limit_worker=True, enable_single_sel_injection=True, enable_join_sel_injection=True)

def inject_true_cards_and_get_cost(
    pointer, 
    sel, 
    join_pointer,
    join_true_cards,
    join_est_rows,
    join_raw_rows,
    join_outerrel,
    n_single,
    n_all, 
    q, 
    hint, 
    plan_info,
):
    sels_filename = "/winhomes/yl762/watcher_data/sels.txt"
    join_filename = "/winhomes/yl762/watcher_data/join.txt"
    pointers_filename = "/winhomes/yl762/watcher_data/pointers.txt"

    with open(sels_filename, "w+") as sels_fout, \
            open(pointers_filename, "w+") as pointers_fout, \
                open(join_filename, "w+") as join_fout:
        sels = ["0.99"] * n_single
        sels[pointer] = str(sel)
        sels_fout.write("\n".join(sels))
        pointers_fout.write("\n".join([str(pointer)]))

        tableset_in_a_list = list(join_pointer.keys())
        tableset_in_a_list.sort(key=lambda x:len(x))

        tableset_to_corr = {}
        join_sels = ["0.99"] * (n_all - n_single)
        for tableset in tableset_in_a_list:
            if tableset not in join_true_cards and tableset in join_est_rows and tableset in join_raw_rows and tableset in join_outerrel and join_outerrel[tableset] in tableset_to_corr:
                tableset_to_corr[tableset] = tableset_to_corr[join_outerrel[tableset]]
            if tableset in join_true_cards and tableset in join_est_rows and tableset in join_raw_rows and tableset in join_outerrel:
                true_card = join_true_cards[tableset]
                cur_corr = true_card / join_est_rows[tableset]
                cur_join_pointer = join_pointer[tableset]
                pointers_fout.write("\n" + str(cur_join_pointer))
                cur_join_sel = join_est_rows[tableset] / join_raw_rows[tableset] * cur_corr
                if join_outerrel[tableset] in tableset_to_corr:
                    cur_join_sel /= tableset_to_corr[join_outerrel[tableset]]
                cur_join_sel = min(1.0, max(0.0, cur_join_sel))
                join_sels[cur_join_pointer - n_single] = str(cur_join_sel)
                tableset_to_corr[tableset] = cur_corr
        join_fout.write("\n".join(join_sels))
    return get_plan_cost(sql=q, hint=hint, plan_info=plan_info, limit_worker=True, enable_single_sel_injection=True, enable_join_sel_injection=True)

def inject_single_and_join_sels_and_get_cost(pointers, single_sels, n_single, join_sels, n_all, q, hint, plan_info):
    single_filename = "/winhomes/yl762/watcher_data/sels.txt"
    join_filename = "/winhomes/yl762/watcher_data/join.txt"
    pointers_filename = "/winhomes/yl762/watcher_data/pointers.txt"

    with open(single_filename, "w+") as single_fout, \
            open(join_filename, "w+") as join_fout, \
                open(pointers_filename, "w+") as pointers_fout:
        pointers_fout.write("\n".join([str(x) for x in pointers]))
        j = 0
        for i in range(n_single):
            if i in pointers:
                single_fout.write(f"{single_sels[j]}")
                j += 1
            else:
                single_fout.write("0.99")
            if i < n_single - 1:
                single_fout.write("\n")
        j = 0
        for i in range(n_single, n_all):
            if i in pointers:
                join_fout.write(f"{join_sels[j]}")
                j += 1
            else:
                join_fout.write("0.99")
            if i < n_all - 1:
                join_fout.write("\n")
    
    _enable_single_sel_injection = len(single_sels) > 0
    _enable_join_sel_injection = len(join_sels) > 0
    return get_plan_cost(sql=q, hint=hint, plan_info=plan_info, limit_worker=True, enable_single_sel_injection=_enable_single_sel_injection, enable_join_sel_injection=_enable_join_sel_injection)

def get_plan_cost(
    sql, 
    hint=None, 
    plan_info=False, 
    limit_worker=True, 
    enable_single_sel_injection=False, 
    enable_join_sel_injection=False,
    debug_self_check=False,
):
    conn, cursor = get_connection()

    explain = "EXPLAIN (SUMMARY, COSTS, FORMAT JSON)"

    cursor.execute('DISCARD ALL;')
    cursor.execute("LOAD 'pg_hint_plan';")
    cursor.execute('SET enable_material = off;')
    cursor.execute("SET enable_bitmapscan = off;")

    if enable_single_sel_injection:
        cursor.execute("SET ml_cardest_enabled=true;")
        cursor.execute("SET query_no=0;")
        cursor.execute("SET ml_cardest_fname='sels.txt';")

    if enable_join_sel_injection:
        cursor.execute("SET ml_joinest_enabled=true;")
        cursor.execute("SET join_est_no=0;")
        cursor.execute("SET ml_joinest_fname='join.txt';")
    
    os.system("rm /winhomes/yl762/watcher_data/join_est_record_job.txt")
    os.system("rm /winhomes/yl762/watcher_data/single_tbl_est_record.txt")
    cursor.execute("SET print_single_tbl_queries=true;")
    cursor.execute("SET print_sub_queries=true;")

    if limit_worker:
        cursor.execute('SET max_parallel_workers_per_gather = 0;')

    try:
        if hint is not None:
            to_execute_ = explain + '\n' + hint + '\n' + sql
        else:
            to_execute_ = explain + '\n' + sql
        cursor.execute(to_execute_)
        query_plan = cursor.fetchall()
        cost = query_plan[0][0][0]['Plan']['Total Cost']
    except psycopg2.OperationalError as e:   
        print(to_execute_)
    except psycopg2.errors.SyntaxError as e:         
        print(to_execute_)

    if plan_info or debug_self_check:
        join_order, scan_mtd = decode(query_plan[0][0][0]['Plan'])
        if debug_self_check and hint is not None:
            assert hint == gen_final_hint(join_order, scan_mtd)
    
    cursor.close()
    conn.commit()
    conn.close()
    if plan_info:
        return cost, join_order, scan_mtd
    else:
        return cost

join_type_to_cond_field = {
    'Merge Join' : 'Merge Cond',
    'Hash Join' : 'Hash Cond',
    'Nested Loop' : 'Join Filter'
}
join_type = ['Merge Join', 'Hash Join', 'Nested Loop']
hint_type_dict = {
    'Merge Join': 'MergeJoin', 
    'Hash Join': 'HashJoin', 
    'Nested Loop': 'NestLoop'
}

def decode(cur):
    node_type = cur['Node Type']
    is_leaf = 'Plans' not in cur
    if is_leaf:
        assert node_type in ['Index Scan', 'Seq Scan', 'Bitmap Scan', 'Index Only Scan']
        return cur['Alias'], [node_type + '(' + cur['Alias'] + ')']
    else:
        if len(cur['Plans']) == 2:
            if cur['Plans'][0]['Parent Relationship'] == 'Inner' and cur['Plans'][1]['Parent Relationship'] == 'Outer':
                cur['Plans'][0], cur['Plans'][1] = cur['Plans'][1], cur['Plans'][0]
            if cur['Plans'][0]['Parent Relationship'] != 'Outer' and cur['Plans'][1]['Parent Relationship'] != 'Inner':
                raise ValueError('missing inner/outer relationship')

        if node_type in ['Aggregate', 'Gather', 'Sort', 'Materialize', 'Sort', 'Hash', 'Gather Merge']:
            assert len(cur['Plans']) == 1
            return decode(cur['Plans'][0])
        elif node_type in ['Merge Join', 'Hash Join', 'Nested Loop']:
            join_order = ''
            single_scans = []
            assert len(cur['Plans']) == 2
            join_order += node_type + '('
            for i in range(2):
                _join_order, _single_scans = decode(cur['Plans'][i])
                single_scans.extend(_single_scans)
                join_order += _join_order
                if i == 0:
                    join_order += ","
            join_order += ')'
            return join_order, single_scans
        else:
            raise NotImplementedError(node_type)

def card(a):
    if int(a) == 0:
        return 1
    else:
        return int(a)
    

def gen_final_hint(str, scan_mtd):
    result = '/*+'
    for i in gen_scan_hints(scan_mtd):
        result += '\n' + i
    join_hints, leading = gen_join_hints(str)
    for i in join_hints:
        result += '\n' + i
    result += '\n' + leading
    result += ' */'
    return result

def gen_scan_hints(scan_mtd):
    scan_hints = []
    for scan in scan_mtd:
        scan = scan.split(' ')
        tmp = ''
        for i in scan:
            tmp += i
        scan_hints.append(tmp)
    return scan_hints

def gen_join_hints(str):
    new_list = []
    tmp = str.split('(')
    for i in range(len(tmp)):
        new_list.append(tmp[i])
        if i < len(tmp) - 1:
            new_list.append('(')
    # print(new_list)
    final_list = parse_string(new_list, ')')
    final_list = parse_string(final_list, ',')
    # print(final_list)
    for i in final_list:
        if i == '':
            final_list.remove('')
    for i in final_list:    
        if i == ',':
            final_list.remove(',')
    for i in range(len(final_list)):
        final_list[i] = final_list[i].lstrip().rstrip()

    tabs = [x for x in final_list if x not in join_type]
    lead = 'Leading( '
    lead += ' '.join(tabs)
    # for i in final_list:
    #     if i not in join_type:
    #         lead += i + ' '
    lead = lead + ' )'

    # print(final_list)
    visited = [0] * len(final_list)
    join_hints = []

    for i in range(len(final_list)):
        if final_list[i] == ')' and visited[i] == 0:
            # print("i=", i , " ", final_list[i])
            tmp_rst = ')'
            visited[i] == 1
            for j in range(i-1, -1, -1):
                if final_list[j] not in ['(', ')'] and final_list[j] not in join_type:
                    tmp_rst = final_list[j] + ' ' + tmp_rst
                if final_list[j] == '(' and visited[j] == 0:
                    tmp_rst = hint_type_dict[final_list[j-1]] + ' ' + final_list[j] + ' ' + tmp_rst
                    visited[j] = 1
                    visited[j-1] = 1
                    break
            join_hints.append(tmp_rst)
            continue
    return join_hints, lead

def parse_string(new_list, d):
    final_list = []
    for i in range(len(new_list)):
        if d in new_list[i]:
            tmp_list = split_string(new_list[i], d)
            final_list += tmp_list
        else:
            final_list.append(new_list[i])
    return final_list

def split_string(tmp, d):
    tmp = tmp.split(d)
    tmp_list = []
    for k in range(len(tmp)):
        tmp_list.append(tmp[k])
        if k < len(tmp) - 1:
            tmp_list.append(d)
    return tmp_list

def drop_buffer_cache(cursor):
    # WARNING: no effect if PG is running on another machine
    subprocess.check_output(['free'])
    subprocess.check_output(['sync'])
    subprocess.check_output(
        ['sudo', 'sh', '-c', 'echo 3 > /proc/sys/vm/drop_caches'])
    subprocess.check_output(['free'])
    cursor.execute('DISCARD ALL;')

def extract_plan(plan_node, focus_on, tableset_to_truecard):
    operation = plan_node["Node Type"]
    relation_names = []
    actual_rows = int(float(plan_node["Actual Rows"]))
    if operation in ["Index Scan", "Seq Scan"]:
        relation_names = [plan_node["Alias"]]
    
    if "Plans" in plan_node:
        for subplan in plan_node["Plans"]:
            sub_relation_names = extract_plan(subplan, focus_on, tableset_to_truecard)
            if len(sub_relation_names) > 0:
                relation_names += copy.deepcopy(sub_relation_names)
    if len(relation_names) > 0:
        relation_names.sort()

    # print(operation, focus_on, relation_names)
    if ("Loop" in operation or "Join" in operation) and (focus_on is None or any(item in relation_names for item in focus_on)):
        # number of relations, relations list, actual rows
        tableset_to_truecard[tuple(relation_names)] = actual_rows
    return relation_names


def check_if_have(plan_node, if_have, model_name_to_have):
    operation = plan_node["Node Type"]
    relation_names = []
    if operation in ["Index Scan", "Seq Scan"]:
        relation_names = [plan_node["Alias"]]
    
    if "Plans" in plan_node:
        for subplan in plan_node["Plans"]:
            sub_relation_names = check_if_have(subplan, if_have, model_name_to_have)
            if len(sub_relation_names) > 0:
                relation_names += copy.deepcopy(sub_relation_names)
    if len(relation_names) > 0:
        relation_names.sort()

    # print(operation, focus_on, relation_names)
    if "Scan" in operation or "Loop" in operation or "Join" in operation:
        for model_name in if_have:
            if model_name in model_name_to_have:
                continue

            query_tableset = model_name.split("-")
            query_tableset.sort()
            if tuple(query_tableset) == tuple(relation_names):
                if "Loop" in operation or "Join" in operation:
                    model_name_to_have[model_name] = True
                else:
                    n_conds = 0
                    if "Filter" in plan_node:
                        n_conds += len(plan_node["Filter"].split("AND"))
                    if "Index Cond" in plan_node:
                        n_conds += len(plan_node["Index Cond"].split("AND"))
                    if n_conds == 2:
                        model_name_to_have[model_name] = True
                    else:
                        model_name_to_have[model_name] = False

    return relation_names

def debug_print_the_monitored_node(plan_node, focus_on):
    operation = plan_node["Node Type"]
    if operation in ["Index Scan", "Seq Scan", "Index Only Scan"] and plan_node["Alias"] in focus_on:
        with open("../result1d/dsb_sf2/Q013a/debug_scan_methods.txt", "a+") as debug_fout:
            _scan_node = {k: v for k, v in plan_node.items() if k != 'Plans'}
            debug_fout.write(f"{_scan_node}\n")
    
    if "Plans" in plan_node:
        for subplan in plan_node["Plans"]:
            debug_print_the_monitored_node(subplan, focus_on)

def get_real_latency(
    sql, 
    hint=None, 
    limit_time=50000, 
    limit_worker=True, 
    n_repetitions=3, #TODO
    extract_plan_info=False, 
    focus_on=None, 
    enable_single_sel_injection=True, 
    enable_join_sel_injection=False,
    plan_info=False,
    debug_self_check=False,
    debug_print_the_monitored_part=False,
    if_have=None,
):
    conn, cursor = get_connection()
    explain = "EXPLAIN (ANALYZE, SUMMARY, COSTS, FORMAT JSON)"

    if hint:
        hint = hint
    else:
        hint = ""
    to_execute_ = explain + '\n' + hint + sql
    
    all_latencies = []
    all_planning_time = []
    for T in range(n_repetitions):
        drop_buffer_cache(cursor)
        cursor.execute('DISCARD ALL;')
        cursor.execute("LOAD 'pg_hint_plan';")
        cursor.execute('SET enable_material = off;')
        cursor.execute("SET enable_bitmapscan = off;")

        if enable_single_sel_injection:
            cursor.execute("SET ml_cardest_enabled=true;")
            cursor.execute("SET query_no=0;")
            cursor.execute("SET ml_cardest_fname='sels.txt';")

        if enable_join_sel_injection:
            cursor.execute("SET ml_joinest_enabled=true;")
            cursor.execute("SET join_est_no=0;")
            cursor.execute("SET ml_joinest_fname='join.txt';")

        os.system("rm -f /winhomes/yl762/watcher_data/join_est_record_job.txt")
        os.system("rm -f /winhomes/yl762/watcher_data/single_tbl_est_record.txt")
        cursor.execute("SET print_single_tbl_queries=true;")
        cursor.execute("SET print_sub_queries=true;")

        if limit_time:
            cursor.execute(f'SET statement_timeout = {limit_time};')
        if limit_worker:
            cursor.execute('SET max_parallel_workers_per_gather = 0;')

        try:
            cursor.execute(to_execute_)
            query_plan = cursor.fetchall()
        except psycopg2.errors.QueryCanceled as e:
            return float('inf')

        cost = float(query_plan[0][0][0]['Plan']['Total Cost'])
        all_latencies.append(float(query_plan[0][0][0]['Plan']['Actual Total Time']))
        all_planning_time.append(float(query_plan[0][0][0]['Planning Time']))

        if T == 0 and (plan_info or debug_self_check):
            join_order, scan_mtd = decode(query_plan[0][0][0]['Plan'])
            if debug_self_check and hint is not None:
                assert hint == gen_final_hint(join_order, scan_mtd)

        if T == 0 and extract_plan_info and focus_on is not None:
            tableset_to_truecard = {}
            extract_plan(query_plan[0][0][0]["Plan"], focus_on, tableset_to_truecard)
        
        if T == 0 and debug_print_the_monitored_part:
            debug_print_the_monitored_node(query_plan[0][0][0]["Plan"], focus_on)

        if T == 0 and if_have is not None:
            model_name_to_have = {}
            check_if_have(query_plan[0][0][0]["Plan"], if_have, model_name_to_have)

    latency = np.median(all_latencies)
    planning_time = np.median(all_planning_time)

    cursor.close()
    conn.commit()
    conn.close()

    if extract_plan_info and focus_on is not None and if_have is not None:
        return cost, latency, planning_time, tableset_to_truecard, model_name_to_have
    elif extract_plan_info and focus_on is not None:
        return cost, latency, planning_time, tableset_to_truecard
    elif if_have is not None:
        return cost, latency, planning_time, model_name_to_have
    elif plan_info:
        return cost, latency, planning_time, join_order, scan_mtd
    else:
        return cost, latency, planning_time

def load_pg_join_subquery_info():
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
                elif len(line) == 0 and query_id is not None and selectivity is not None:
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
