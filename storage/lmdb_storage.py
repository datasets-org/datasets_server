import ujson
import lmdb
from .storage import Storage


class LmdbStorage(Storage):
    def __init__(self, cfg):
        self.cfg = cfg
        self.data = {}
        # todo set size reasonably
        self.env = lmdb.open(self.cfg.database_path, map_size=50 * 1024 ** 3)
        super().__init__()

    def load(self):
        print("loading storage from {}".format(self.cfg.database_path))
        self.data = {}
        with self.env.begin() as txn:
            cursor = txn.cursor()
            for key, value in cursor:
                key = key.decode("utf-8")
                data = ujson.loads(value.decode("utf-8"))
                self.data[key] = data

    def put(self, key, data):
        with self.env.begin(write=True) as txn:
            txn.put(key.encode("utf-8"), ujson.dumps(data).encode('utf-8'),
                    overwrite=True)

    def update(self, key, data):
        with self.env.begin(write=True) as txn:
            d = self.get(key)
            if d:
                d.update(data)
            else:
                d = data
            txn.put(key.encode("utf-8"), ujson.dumps(d).encode('utf-8'),
                    overwrite=True)

    def delete(self, key):
        with self.env.begin(write=True) as txn:
            txn.delete(key.encode("utf-8"))

    def delete_key(self, key, dict_key):
        with self.env.begin(write=True) as txn:
            data = self.get(key)
            del data[dict_key]
            txn.put(key.encode("utf-8"), ujson.dumps(data).encode('utf-8'),
                    overwrite=True)

    def get(self, key):
        with self.env.begin() as txn:
            data = txn._get(key.encode("utf-8"))
            if data:
                return ujson.loads(data.decode("utf-8"))
            else:
                return None
