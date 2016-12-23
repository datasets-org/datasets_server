import uuid
import os
import yaml
import time

skeleton = {

}


class Datasets(object):
    def __init__(self, cfg, storage):
        self.cfg = cfg
        self.storage = storage
        pass

    def scan(self, folders):
        for d in self.find_data_sets(folders):
            # todo notice update in changelog ... monitoring
            # todo paths are not cleaned
            path = os.path.dirname(os.path.abspath(d))
            ds = yaml.load(open(d))
            if ds:
                uid = ds["id"]
                stored_ds = self.storage.get(uid)
                field = "paths_new"
                print(os.path.islink(path))
                if os.path.islink(path):
                    print("LINK")
                    field = "links_new"
                if stored_ds:
                    if field not in stored_ds:
                        stored_ds[field] = []
                    if path not in stored_ds[field]:
                        stored_ds[field].append(path)
                    ds[field] = stored_ds[field]
                else:
                    if field not in ds:
                        ds.update({
                            field: [path],
                            "type:": "fs"
                        })
                del ds["id"]
                self.storage.update(uid, ds)
        self._merge_paths()

    def _merge_paths(self):
        # todo override old paths with new ones
        # todo may trigger alerts (when null, ...)

    def find_data_sets(self, folders):
        for folder in folders:
            for root, folders, files in os.walk(folder, followlinks=True):
                if len(files) > int(self.cfg["iter_file_limit"]):
                    continue
                for filename in files:
                    if filename == "dataset.yaml":
                        yield os.path.join(root, filename)

    def generate(self):
        while True:
            uid = str(uuid.uuid4())
            if not self.storage.get(uid):
                return uid

    def use(self, key, data):
        ds = self.storage.get(key)
        if "usages" not in ds:
            ds["usages"] = []
        data.update({
            "timestamp": time.time(),
        })
        ds["usages"].append(data)
        self.storage.update(key, ds)
