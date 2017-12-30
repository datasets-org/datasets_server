import copy
import os
import subprocess
import time

import yaml

from .changelog_entry import ChangelogEntry
from .dataset import Dataset
from .datasets import Datasets


class DatasetsLocal(Datasets):
    def __init__(self, cfg, storage):
        self.cfg = cfg
        self._storage = storage
        self.generated_fields = {"paths",
                                 "_paths",
                                 "links",
                                 "_links",
                                 "paths_new",
                                 "links_new",
                                 "changelog",
                                 "type",
                                 "usages",
                                 "characteristics",
                                 "_markdowns",
                                 "markdowns"}

    def scan(self, folders):
        for ds_path in self.find_files(folders):
            actions = []
            removed = []
            path = os.path.dirname(os.path.abspath(ds_path))
            ds = yaml.load(open(ds_path))
            if ds:
                uid = ds["id"]
                stored_ds = self._storage.get(uid)
                field = "paths_new"
                if os.path.islink(path):
                    field = "links_new"
                if stored_ds:
                    for d in ds:
                        if d == "id":
                            continue
                        if d in stored_ds:
                            if stored_ds[d] != ds[d]:
                                actions.append(
                                    (d, stored_ds[d], ds[d], time.time()))
                        else:
                            actions.append((d, None, ds[d], time.time()))
                    for d in stored_ds:
                        if d in self.generated_fields:
                            continue
                        if d not in ds:
                            actions.append((d, stored_ds[d], None, time.time()))
                            removed.append(d)
                    if field not in stored_ds:
                        stored_ds[field] = []
                    if path not in stored_ds[field]:
                        stored_ds[field].append(path)
                    ds[field] = stored_ds[field]
                else:
                    if field not in ds:
                        ds.update({
                            field: [path],
                            "type": "fs"
                        })
                        new_ds = copy.deepcopy(ds)
                        for i in ["paths_new", "links_new"]:
                            if i in new_ds:
                                del new_ds[i]
                        actions.append((None, None, new_ds, time.time()))
                if actions:
                    self.log_change(uid, actions)
                del ds["id"]
                for i in removed:
                    self._storage.delete_key(uid, i)
                self._storage.update(uid, ds)
        self._merge_paths()
        for k, v in self._storage.data.items():
            # todo load ds
            self._find_md(ds)
        self._find_characteristics()

    def log_change(self, dataset: Dataset, changes: ChangelogEntry):
        dataset.log_change(changes)
        self._storage.update(dataset.id, dataset)

    def _merge_paths(self):
        # todo may trigger alerts (when null, ...)
        self._storage.load()
        key_pairs = [
            ("paths", "paths_new"),
            ("links", "links_new"),
        ]
        for k, v in self._storage.data.items():
            changes = []
            for i in key_pairs:
                if i[1] in v:
                    if "_" + i[0] not in v:
                        changes.append([i[0], None, v[i[1]], time.time()])
                    elif v["_" + i[0]] != v[i[1]]:
                        changes.append(
                            [i[0], v["_" + i[0]], v[i[1]], time.time()])
                    v["_" + i[0]] = v[i[1]]
                    # not logging normalized path changes
                    v[i[0]] = self.normalize_path(v[i[1]])
                    del v[i[1]]
                else:
                    # delete no longer valid entry
                    if i[0] in v:
                        changes.append([i[0], v[i[0]], None, time.time()])
                        del v[i[0]]
            if changes:
                self.log_change(k, changes)
            self._storage.put(k, v)

    def normalize_path(self, path):
        if "storage_replace" in self.cfg:
            for i in self.cfg.storage_replace:
                return [pth.replace(i[0], i[1]) for pth in path]
        else:
            return list(path)

    def _find_md(self, ds: Dataset):
        # todo move to datasets object
        for p in ds.data:
            # todo get paths - not data
            markdowns = self.normalize_path(
                self.find_files(p, searched_filename="*.md"))
            # todo check if no markdowns found return None
            if ds.markdowns != markdowns:
                change = ChangelogEntry("markdowns", markdowns,
                                        old_value=ds.markdowns)
                self.log_change(ds, change)
        self._storage.update(ds.id, ds.struct())

    def _find_characteristics(self, ds: Dataset):
        # todo move to datasets object
        path = ds.path
        for p in ds.data:
            # todo change p to valid path
            characteristics = {}
            file = "characteristics_" + d + ".txt"
            pth = os.path.join(p, file)
            data_pth = os.path.join(p, d)
            if not os.path.exists(pth):
                with open(pth, "wb") as f:
                    proc = subprocess.Popen(
                        ["./get_characteristics.sh",
                         data_pth],
                        stdout=f)
                    proc.wait()
            characteristics[d] = self._parse_characteristics(pth)
            if characteristics != ds.characteristics:
                self.log_change(ds, ChangelogEntry("characteristics",
                                                   characteristics,
                                                   old_value=ds.characteristics))
            self._storage.update(ds.id, ds.struct())

    def _parse_characteristics(self, pth):
        with open(pth) as f:
            data = {}
            phases = ["count", "size", "counts", "csv"]
            phase = 0
            for line in f:
                try:
                    if phases[phase] == "count":
                        cnt = int(line.split()[1])
                        data["files_cnt"] = cnt
                        phase += 1
                        continue
                    if phases[phase] == "size":
                        if line == "\n":
                            phase += 1
                            continue
                        size = line.split()[0]
                        data["files_size"] = size
                        continue
                    if phases[phase] == "counts":
                        if line != "\n":
                            cnt, extension = line.split()
                            cnt = int(cnt)
                            if "extensions" not in data:
                                data["extensions"] = {}
                            data["extensions"][extension] = cnt
                        else:
                            phase += 1
                        continue
                    if phases[phase] == "csv":
                        if line == "0\n":
                            continue
                        else:
                            cnt, file = line.split()
                            cnt = int(cnt)
                            if "csv" not in data:
                                data["csv"] = {}
                            data["csv"][file] = cnt
                except Exception as e:
                    print(e)
            return data

    def find_files(self, folders, searched_filename="dataset.yaml"):
        # todo override
        for folder in folders:
            for root, folders, files in os.walk(folder,
                                                followlinks=True):
                if len(files) > int(self.cfg.iter_file_limit):
                    continue
                for filename in files:
                    if "*" in searched_filename:
                        if filename.endswith(
                                searched_filename.replace('*', '')):
                            yield os.path.join(root, filename)
                    else:
                        if filename == searched_filename:
                            yield os.path.join(root, filename)
