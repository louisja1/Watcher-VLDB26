import plyvel
import time

level_db_path = "/winhomes/yl762/leveldb_data/"

# cnts = {"put": 0, "get": 0, "delete": 0}
# all_cnts = {"put": 0, "get": 0, "delete": 0}
# sum_time = {"put": 0.0, "get": 0.0, "delete": 0.0}


# def init_cnts():
#     global cnts
#     cnts["put"] = 0
#     cnts["get"] = 0
#     cnts["delete"] = 0


class LevelDBWrapper:
    def __init__(
        self,
        db_name,
        key_field_type="string",
        val_field_type="int32",
        path=level_db_path,
    ):
        self.db = plyvel.DB(path + db_name + "/", create_if_missing=True)
        assert key_field_type == "string" or key_field_type.startswith("int")
        assert val_field_type == "string" or val_field_type.startswith("int")

        self.key_field_type = key_field_type
        self.key_len_bytes = (
            max(int(key_field_type[3:]) // 8, 1)
            if key_field_type.startswith("int")
            else None
        )
        self.val_field_type = val_field_type
        self.val_len_bytes = (
            max(int(val_field_type[3:]) // 8, 1)
            if val_field_type.startswith("int")
            else None
        )

    def clean(self, close=True):
        write_batch = self.db.write_batch()
        for key, _ in self.db:
            write_batch.delete(key)
        write_batch.write()
        if close:
            self.db.close()
            assert self.db.closed

    def key_to_bytes(self, x):
        if x is None:
            return None
        if self.key_field_type == "string":
            return bytes(x, "utf-8")
        else:
            return x.to_bytes(self.key_len_bytes, byteorder="big")

    def key_from_bytes(self, x):
        if x is None:
            return None
        if self.key_field_type == "string":
            return str(x, "utf-8")
        else:
            return int.from_bytes(x, "big")

    def val_to_bytes(self, x):
        if x is None:
            return None
        if self.val_field_type == "string":
            return bytes(x, "utf-8")
        else:
            return x.to_bytes(self.val_len_bytes, byteorder="big")

    def val_from_bytes(self, x):
        if x is None:
            return None
        if self.val_field_type == "string":
            return str(x, "utf-8")
        else:
            return int.from_bytes(x, "big")

    def put(self, key, value):
        # cnts["put"] += 1
        # all_cnts["put"] += 1
        start_ts = time.time()
        b_key = self.key_to_bytes(key)
        b_value = self.val_to_bytes(value)
        self.db.put(b_key, b_value)
        # sum_time["put"] += time.time() - start_ts

    def get(self, key):
        # cnts["get"] += 1
        # all_cnts["get"] += 1
        start_ts = time.time()
        b_key = self.key_to_bytes(key)
        b_value = self.db.get(b_key)
        value = self.val_from_bytes(b_value)
        # sum_time["get"] += time.time() - start_ts
        return value

    def delete(self, key):
        # cnts["delete"] += 1
        # all_cnts["delete"] += 1
        start_ts = time.time()
        b_key = self.key_to_bytes(key)
        self.db.delete(b_key)
        # sum_time["delete"] += time.time() - start_ts

    def iterator(self, start=None, stop=None, include_stop=False, reverse=False):
        b_start = self.key_to_bytes(start)
        b_stop = self.key_to_bytes(stop)
        return self.db.iterator(
            start=b_start, stop=b_stop, include_stop=include_stop, reverse=reverse
        )


def leveldb_unit_test():
    db = LevelDBWrapper(
        "test_leveldb", key_field_type="string", val_field_type="string"
    )
    db.put("hahaha", "hehehe")
    db.put("heihei", "hiehie")
    assert db.get("hahaha") == "hehehe"
    assert db.get("heihei") == "hiehie"
    db.delete("heihei")
    assert db.get("heihei") is None
    assert db.get("hh") is None
    db.clean()


def leveldb_unit_test_1():
    db = LevelDBWrapper("test_leveldb", key_field_type="int32", val_field_type="int32")
    db.put(1, 2)
    db.put(3, 4)
    db.put(5, 6)

    assert db.get(1) == 2
    assert db.get(3) == 4
    db.delete(1)
    assert db.get(1) is None
    assert db.get(5) == 6

    db.clean()


if __name__ == "__main__":
    leveldb_unit_test()
    leveldb_unit_test_1()
