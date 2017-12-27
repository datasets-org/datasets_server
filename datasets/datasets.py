import uuid
import time

from storage.storage import Storage


class Datasets(object):
    def __init__(self, storage: Storage) -> None:
        assert isinstance(storage, Storage)
        self._storage: Storage = storage
        self.datsets_filename = "dataset.yaml"

    def generate(self) -> str:
        while True:
            uid = str(uuid.uuid4())
            if not self._storage.get(uid):
                return uid

    def use(self, key: str, data: dict = {}) -> None:
        ds = self._storage.get(key)
        if "usages" not in ds:
            ds["usages"] = []
        data.update({
            "timestamp": time.time(),
        })
        ds["usages"].append(data)
        self._storage.update(key, ds)

    def get(self, key: str) -> dict:
        return self._storage.get(key)

    def create(self, key: str, data: dict) -> None:
        self._storage.put(key, data)
