from db_wrapper import LevelDBWrapper
import copy
from tqdm import tqdm
import time
import logger


# id(INT) -> list of attributes(comma separted string)
class Table:
    def __init__(
        self,
        name,
        jk_idx=0,
        sel_idx=1,
        jk_field_type="int32",
        sel_field_type="int32",
        max_level=5,
        fan_out=5,
        jk_theta=10,
        db=LevelDBWrapper,
        build_index=True,
        jk_delta=0, # +jk_delta to avoid negative values
        sel_delta=0, # +sel_delta to avoid negative values
    ):
        if jk_field_type == "float":
            self.jk_was_float = True
            jk_field_type = "int32"
        else:
            self.jk_was_float = False

        if sel_field_type == "float":
            self.sel_was_float = True
            sel_field_type = "int32"
        else:
            self.sel_was_float = False

        assert jk_field_type == "int32"
        assert sel_field_type == "int32"

        self.name = name
        self.table = LevelDBWrapper(
            name + "_table", key_field_type="int32", val_field_type="string"
        )
        self.table.clean(close=False)
        self.metadata = Metadata(name, jk_idx, sel_idx, jk_field_type, sel_field_type)
        self.max_level = max_level
        self.fan_out = fan_out
        self.jk_theta = jk_theta
        self.build_index = build_index
        self.jk_delta = jk_delta
        self.sel_delta = sel_delta

        self.size = 0
        self.next_id = 1 # to match the nextval(sequence) in postgres, which starts from 1

        if self.build_index:
            self.pointer = Pointer(name, key_field_type="int64", val_field_type="int32")
            self.index = []
            for i in range(self.max_level):
                self.index.append(
                    Index(name, i, key_field_type="int64", val_field_type="int32")
                )

    def clean(self, close=True):
        self.table.clean(close)
        self.metadata.clean(close)
        if self.build_index:
            self.pointer.clean(close)
            for i in range(self.max_level):
                self.index[i].clean(close)

    def _next_id(self):
        self.next_id += 1
        return self.next_id - 1

    def _encode(self, list_of_attr):
        return ",".join(list_of_attr)

    def _decode(self, attr_in_str):
        return attr_in_str.split(",")

    def _build_index(self, method="equi-depth"):
        assert method == "equi-depth"
        assert self.build_index

        # level 0 in Index
        bin_size = self.fan_out

        with self.metadata.jk_iterator() as it:
            lb = (self.metadata.jk_min << 32) + self.metadata.sel_min
            acc_cnt = 0
            for b_k, b_v in it:
                k, v = int.from_bytes(b_k, "big"), int.from_bytes(b_v, "big")
                if v >= self.jk_theta:
                    self.metadata.build_index(k)
                    with self.pointer.iterator(
                        start=(k << 32), stop=((k + 1) << 32), include_stop=False
                    ) as p_it:
                        for b_jk_sel, b_cnt in p_it:
                            if lb is None:
                                lb = int.from_bytes(b_jk_sel, "big")
                            acc_cnt += int.from_bytes(b_cnt, "big")
                            if acc_cnt >= bin_size:
                                self.index[0].init(lb, acc_cnt)
                                lb = None
                                acc_cnt = 0
            if acc_cnt > 0 and lb is not None:
                self.index[0].init(lb, acc_cnt)
                lb = None
                acc_cnt = 0

        # level 1 to level (max_level - 1)
        for level in range(1, self.max_level):
            n_pairs = 0
            with self.index[level - 1].iterator() as it:
                lb = None
                acc_cnt = 0
                ii = 0
                for b_jk_lb, b_cnt in it:
                    jk_lb = int.from_bytes(b_jk_lb, "big")
                    acc_cnt += int.from_bytes(b_cnt, "big")
                    if lb is None:
                        lb = jk_lb
                    if ii == self.fan_out - 1:
                        self.index[level].init(lb, acc_cnt)
                        n_pairs += 1
                        lb = None
                        acc_cnt = 0
                        ii = 0
                    ii += 1
                if acc_cnt > 0 and lb is not None:
                    self.index[level].init(lb, acc_cnt)
                    n_pairs += 1
                    lb = None
                    acc_cnt = 0
            if n_pairs == 1:
                self.max_level = level + 1
                break

        # add shared upper bound
        for level in range(self.max_level):
            self.index[level].init(self.shared_ub, 0)

        # debug only
        # with open(f"{self.name}_index_boundary.txt", "w") as debug_fout:
        #     for level in range(self.max_level):
        #         self.index[level].debug_only_print_boundary(
        #             debug_fout, self.name, level
        #         )

    def clean(self, close=True):
        if self.build_index:
            for level in range(self.max_level):
                self.index[level].clean(close)
            self.pointer.clean(close)
        self.metadata.clean(close)
        self.table.clean(close)

    def insert(self, attr_in_str):
        id = self._next_id()
        self.table.put(id, attr_in_str)
        self.size += 1

        if attr_in_str.split(",")[self.metadata.jk_idx] in ["", "None"]:
            jk = None
        elif self.jk_was_float:
            # to preserve 2 decimal places
            jk = int(float(attr_in_str.split(",")[self.metadata.jk_idx]) * 100)
        else:
            jk = int(attr_in_str.split(",")[self.metadata.jk_idx])
        if jk is not None:
            jk += self.jk_delta

        if attr_in_str.split(",")[self.metadata.sel_idx] in ["", "None"]:
            sel = None
        elif self.sel_was_float:
            # to preserve 2 decimal places
            sel = int(float(attr_in_str.split(",")[self.metadata.sel_idx]) * 100)
        else:
            sel = int(attr_in_str.split(",")[self.metadata.sel_idx])
        if sel is not None:
            sel += self.sel_delta

        if jk is not None:
            self.metadata.insert(jk)

        if self.build_index and jk is not None and sel is not None:
            jk_sel = (jk << 32) + sel
            self.pointer.insert(jk_sel)
            if self.metadata.has_index(jk):
                for level in range(self.max_level):
                    self.index[level].insert(jk, sel)
        return id

    def delete(self, id):
        jk = self.get_jk_by_id(id)
        sel = self.get_sel_by_id(id)

        if self.build_index and jk is not None and sel is not None:
            if self.metadata.has_index(jk):
                for level in range(self.max_level):
                    self.index[level].delete(jk, sel)

            jk_sel = (jk << 32) + sel
            self.pointer.delete(jk_sel)
        
        if jk is not None:
            self.metadata.delete(jk)

        self.size -= 1
        self.table.delete(id)

    def query(self, jk, lb, ub):
        assert self.build_index
        if jk is not None and self.jk_was_float:
            assert isinstance(jk, float)
            jk = int(jk * 100)
        if jk is not None:
            jk += self.jk_delta
        if self.sel_was_float:
            assert isinstance(lb, float)
            lb = int(lb * 100)
            assert isinstance(ub, float)
            ub = int(ub * 100)
        lb += self.sel_delta
        ub += self.sel_delta

        if jk is None or self.metadata.get_count(jk) == 0:
            return 0
        if lb <= self.metadata.sel_min and ub >= self.metadata.sel_max:
            return self.metadata.get_count(jk)

        ans = 0
        intervals = [(lb, ub)]
        index_search_ts = time.time()
        for level in range(self.max_level - 1, -1, -1):
            old_intervals = copy.deepcopy(intervals)
            intervals = []
            for _lb, _ub in old_intervals:
                cnt, q1, q2 = self.index[level].query(jk, _lb, _ub)
                ans += cnt
                if q1 is not None:
                    intervals.append(q1)
                if q2 is not None:
                    intervals.append(q2)
        logger.index_search_time += time.time() - index_search_ts

        for _lb, _ub in intervals:
            for _, v in self.pointer.iterator(
                start=(jk << 32) + _lb, stop=(jk << 32) + _ub, include_stop=True
            ):
                logger.n_kv_pairs += 1
                ans += int.from_bytes(v, byteorder="big")

        # ans1 = 0
        # for _, v in self.pointer.iterator(
        #     start=(jk << 32) + lb, stop=(jk << 32) + ub, include_stop=True
        # ):
        #     ans1 += int.from_bytes(v, byteorder="big")
        # assert ans == ans1

        return ans

    # return a mapping: query_id -> delta
    def query_bounds(self, jk, bounds):
        if jk is not None and self.jk_was_float:
            assert isinstance(jk, float)
            jk = int(jk * 100)
        if jk is not None:
            jk += self.jk_delta

        if self.sel_was_float:
            for i in range(len(bounds)):
                assert isinstance(bounds[i][1][0], float) and isinstance(bounds[i][1][1], float)
                bounds[i] = (bounds[i][0], (int(bounds[i][1][0] * 100), int(bounds[i][1][1] * 100)))
        
        for i in range(len(bounds)):
            assert isinstance(bounds[i][1][0], int) and isinstance(bounds[i][1][1], int)
            bounds[i] = (bounds[i][0], (bounds[i][1][0] + self.sel_delta, bounds[i][1][1] + self.sel_delta))

        m = {}
        for qid, _ in bounds:
            m[qid] = 0
        if jk is None:
            return m

        for level in range(self.max_level - 1, -1, -1):
            # print("level=====", level)
            # print("bounds", bounds)
            start_ts = time.time()
            if len(bounds) == 0:
                break
            logger.total_bounds += len(bounds)
            cover = []
            for i in range(len(bounds)):
                cover.append([self.metadata.sel_max + 1, self.metadata.sel_min - 1])

            sep_bounds = []
            for i in range(len(bounds)):
                sep_bounds.append((bounds[i][1][0], 1, i))  # insert
                sep_bounds.append((bounds[i][1][1], -1, i))  # delete

            sep_bounds.sort(key=lambda x: x[0] * 2 - x[1])

            mm = {}
            for _bound_id in range(len(bounds)):
                mm[_bound_id] = 0
            logger.query_bounds_other_time[0] += time.time() - start_ts

            start_ts_1 = time.time()
            i = 0
            while i < len(sep_bounds):
                j = i
                step = 1
                while j + 1 < len(sep_bounds) and step > 0:
                    if sep_bounds[j + 1][1]:
                        step += 1
                    else:
                        step -= 1
                    j += 1

                pref_sum = 0
                p = i
                # print("-----")
                lst_lb, lst_v = None, None
                with self.index[level].iterator(
                    start=(jk << 32) + sep_bounds[i][0],
                    stop=(jk + 1) << 32,
                    include_stop=False,
                ) as it:
                    for b_k, b_v in it:
                        logger.n_kv_pairs += 1
                        cur_jk = int.from_bytes(b_k[:4], "big")
                        lb = int.from_bytes(b_k[-4:], "big")
                        v = int.from_bytes(b_v, "big")
                        if cur_jk != jk:
                            lb = self.metadata.sel_max + 1
                        # print("what we have", cur_jk, lb, v)

                        while p <= j and sep_bounds[p][0] < lb:
                            _, _type, _bound_id = sep_bounds[p]
                            # print("consider", p, sep_bounds[p])
                            _qid = bounds[_bound_id][0]
                            if _type == 1:
                                # print(_qid, lb, -pref_sum)
                                mm[_bound_id] -= pref_sum
                                cover[_bound_id][0] = lb
                            else:
                                # print(_qid, lb, pref_sum - lst_v)
                                if lst_lb is not None and lst_v is not None:
                                    mm[_bound_id] += pref_sum - lst_v
                                    cover[_bound_id][1] = lst_lb - 1
                            p += 1
                        if p > j:
                            break
                        pref_sum += v
                        lst_lb, lst_v = lb, v
                i = j + 1
            logger.query_bounds_other_time[1] += time.time() - start_ts_1

            start_ts_2 = time.time()
            for i in range(len(bounds)):
                if (
                    cover[i][0] <= cover[i][1]
                    and cover[i][0] >= bounds[i][1][0]
                    and cover[i][1] <= bounds[i][1][1]
                ):
                    _qid = bounds[i][0]
                    m[_qid] += mm[i]
            logger.query_bounds_other_time[2] += time.time() - start_ts_2

            # print("m", m)
            # print("cover", cover)
            # print("Correct answer", self.query(jk, cover[0][0], cover[0][1]))

            start_ts_3 = time.time()
            new_bounds = []
            for i in range(len(bounds)):
                if (
                    cover[i][0] <= cover[i][1]
                    and cover[i][0] >= bounds[i][1][0]
                    and cover[i][1] <= bounds[i][1][1]
                ):
                    if cover[i][0] > bounds[i][1][0]:
                        new_bounds.append(
                            (bounds[i][0], (bounds[i][1][0], cover[i][0] - 1))
                        )
                    if cover[i][1] < bounds[i][1][1]:
                        new_bounds.append(
                            (bounds[i][0], (cover[i][1] + 1, bounds[i][1][1]))
                        )
                else:
                    new_bounds.append(
                        (bounds[i][0], (bounds[i][1][0], bounds[i][1][1]))
                    )
            bounds = new_bounds
            logger.query_bounds_other_time[3] += time.time() - start_ts_3

        logger.total_bounds += len(bounds)
        start_ts_4 = time.time()
        all_bounds = []
        for i in range(len(bounds)):
            # bound, qid, insert/delete
            all_bounds.append((bounds[i][1][0], bounds[i][0], 0))  # insert left bound
            all_bounds.append((bounds[i][1][1], bounds[i][0], 1))  # delete right bound
        all_bounds.sort(key=lambda x: x[0] * 2 + x[2])
        i = 0

        logger.query_bounds_other_time[4] += time.time() - start_ts_4
        while i < len(all_bounds):
            start_ts_5 = time.time()
            j = i
            step = 1
            while j + 1 < len(all_bounds) and step > 0:
                if all_bounds[j + 1][2] == 0:
                    step += 1
                else:
                    step -= 1
                j += 1

            logger.query_bounds_other_time[5] += time.time() - start_ts_5
            start_ts_6 = time.time()
            p = i
            pref_sum = 0
            with self.pointer.iterator(start=(jk << 32) + all_bounds[i][0]) as it:
                for b_k, b_v in it:
                    logger.n_kv_pairs += 1
                    cur_jk = int.from_bytes(b_k[:4], "big")
                    sel = int.from_bytes(b_k[-4:], "big")
                    v = int.from_bytes(b_v, "big")
                    if cur_jk != jk:
                        sel = self.metadata.sel_max + 1

                    while p <= j and (
                        all_bounds[p][0] < sel
                        or (all_bounds[p][0] == sel and all_bounds[p][2] == 0)
                    ):
                        _, _qid, _type = all_bounds[p]
                        if _type == 0:
                            m[_qid] -= pref_sum
                        else:
                            m[_qid] += pref_sum
                        p += 1
                    pref_sum += v
                    if p > j:
                        break
                assert p > j
            logger.query_bounds_other_time[6] += time.time() - start_ts_6
            i = j + 1
        return m

    def get_tuple_by_id(self, id):
        return self._decode(self.table.get(id))

    def get_jk_by_id(self, id):
        tmp = self.get_tuple_by_id(id)[self.metadata.jk_idx]
        if tmp in ["", "None"]:
            return None
        elif self.jk_was_float:
            # to preserve 2 decimal places
            jk = int(float(tmp) * 100)
        else:
            jk = int(tmp)
        jk += self.jk_delta
        return jk

    def get_sel_by_id(self, id):
        tmp = self.get_tuple_by_id(id)[self.metadata.sel_idx]
        if tmp  in ["", "None"]:
            return None
        elif self.sel_was_float:
            # to preserve 2 decimal places
            sel = int(float(tmp) * 100)
        else:
            sel = int(tmp)
        sel += self.sel_delta
        return sel

    def load_table(self, filename=None):
        print(f"Load Table {self.name} to leveldb")
        if filename is not None:
            # None means the initial table is already loaded
            with open(filename, "r") as fin:
                # skip the header
                for line in tqdm(list(fin.readlines())[1:]):
                    self.insert(line[:-1])

        assert self.metadata.jk_min is not None
        assert self.metadata.jk_max is not None
        assert self.metadata.sel_min is not None
        assert self.metadata.sel_max is not None

        assert self.metadata.jk_min - 1 >= 0
        assert self.metadata.jk_max + 1 < (1 << 32)
        assert self.metadata.sel_min >= 0
        assert self.metadata.sel_max < (1 << 32)

        self.shared_ub = (self.metadata.jk_max + 1) << 32
        if self.build_index:
            self.pointer.db.put(self.shared_ub, 0)
            self._build_index(method="equi-depth")

    def set_jk_bound(self, jk_min, jk_max):
        self.metadata.set_jk_bound(jk_min, jk_max)

    def set_sel_bound(self, sel_min, sel_max):
        self.metadata.set_sel_bound(sel_min, sel_max)


class Metadata:
    def __init__(self, name, jk_idx, sel_idx, jk_field_type, sel_field_type):
        self.count = LevelDBWrapper(name + "_count", "int32", "int32")
        self.is_index = LevelDBWrapper(name + "_has", "int32", "int1")
        self.count.clean(close=False)
        self.is_index.clean(close=False)

        self.jk_idx = jk_idx
        self.sel_idx = sel_idx
        self.jk_field_type = jk_field_type
        self.sel_field_type = sel_field_type

        self.jk_min = None
        self.jk_max = None
        self.sel_min = None
        self.sel_max = None

    def clean(self, close=True):
        self.count.clean(close)
        self.is_index.clean(close)

    def set_jk_bound(self, jk_min, jk_max):
        self.jk_min = jk_min
        self.jk_max = jk_max

    def set_sel_bound(self, sel_min, sel_max):
        self.sel_min = sel_min
        self.sel_max = sel_max

    def build_index(self, jk):
        self.is_index.put(jk, 1)

    def has_index(self, jk):
        flag = self.is_index.get(jk)
        return flag is not None and flag == 1

    def jk_iterator(self):
        return self.count.iterator()

    def get_count(self, jk):
        _cnt = self.count.get(jk)
        return 0 if _cnt is None else _cnt

    def insert(self, jk):
        _cnt = self.get_count(jk)
        self.count.put(jk, _cnt + 1)

    def delete(self, jk):
        _cnt = self.get_count(jk)
        self.count.put(jk, _cnt - 1)


# jk,sel -> count
class Pointer:
    def __init__(self, name, key_field_type, val_field_type):
        self.db = LevelDBWrapper(name + "_pointer", key_field_type, val_field_type)
        self.db.clean(close=False)

    def clean(self, close):
        self.db.clean(close)

    def insert(self, jk_sel):
        _cnt = self.db.get(jk_sel)
        self.db.put(jk_sel, 1 if _cnt is None else _cnt + 1)

    def delete(self, jk_sel):
        _cnt = self.db.get(jk_sel)
        self.db.put(jk_sel, _cnt - 1)

    def iterator(self, start=None, stop=None, include_stop=False):
        return self.db.iterator(start=start, stop=stop, include_stop=include_stop)


# jk,lb -> count
class Index:
    def __init__(self, name, id, key_field_type, val_field_type):
        self.db = LevelDBWrapper(name + f"_index{id}", key_field_type, val_field_type)
        self.db.clean(close=False)

    def clean(self, close=True):
        self.db.clean(close)

    def init(self, jk_lb, cnt):
        self.db.put(jk_lb, cnt)

    def insert(self, jk, sel):
        jk_sel = (jk << 32) + sel
        with self.db.iterator(stop=jk_sel, include_stop=True, reverse=True) as it:
            k, v = None, None
            for _k, _v in it:
                k, v = _k, _v
                break
            self.db.put(int.from_bytes(k, "big"), int.from_bytes(v, "big") + 1)

    def delete(self, jk, sel):
        jk_sel = (jk << 32) + sel
        with self.db.iterator(stop=jk_sel, include_stop=True, reverse=True) as it:
            k, v = None, None
            for _k, _v in it:
                k, v = _k, _v
                break
            self.db.put(int.from_bytes(k, "big"), int.from_bytes(v, "big") - 1)

    def iterator(self, start=None, stop=None, include_stop=False, reverse=False):
        return self.db.iterator(
            start=start, stop=stop, include_stop=include_stop, reverse=reverse
        )

    # count of jk, [lb, ub]
    # count, None/(lb, ub'), None/(lb',ub)
    def query(self, jk, lb, ub):
        jk_lb = (jk << 32) + lb
        jk_ub = (jk << 32) + ub

        ub_prime = None
        lb_prime = None
        count = 0

        l = []
        with self.db.iterator(start=jk_lb, stop=jk_ub + 1, include_stop=True) as it:
            for b_k, b_v in it:
                logger.n_kv_pairs += 1
                sel = int.from_bytes(b_k[-4:], "big")
                v = int.from_bytes(b_v, "big")
                l.append((sel, v))
            if len(l) <= 1:
                return 0, (lb, ub), None
            for i in range(len(l) - 1):
                _lb, _ub = l[i][0], l[i + 1][0] - 1
                if ub_prime is None:
                    ub_prime = l[i][0] - 1
                if _lb >= lb and _ub <= ub:
                    count += l[i][1]
            lb_prime = l[-1][0]

        if ub_prime is None and lb_prime is None:
            return 0, (lb, ub), None

        le_bound = None if ub_prime is None or ub_prime < lb else (lb, ub_prime)
        ri_bound = None if lb_prime is None or lb_prime > ub else (lb_prime, ub)
        return count, le_bound, ri_bound

    def debug_only_print_boundary(self, fout, name, level):
        with self.db.iterator() as it:
            fout.write("=====" + name + "," + str(level) + "=====" + "\n")
            for b_k, b_v in it:
                fout.write(
                    f"({int.from_bytes(b_k[:4], 'big')},{int.from_bytes(b_k[4:], 'big')},{int.from_bytes(b_v, 'big')})"
                )
            fout.write("\n")
