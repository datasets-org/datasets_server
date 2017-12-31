import os
import subprocess
from typing import Optional

from .dataset import Dataset
from .datasets import Datasets


class DatasetsLocal(Datasets):
    def __init__(self, cfg, storage):
        self._cfg = cfg
        self._storage = storage
        # todo set basepath based on config
        self.basepath = "/"
        self.generated_fields = {
            "paths",  # done
            "links",  # done
            "changelog",  # handled separately
            "type",  # todo
            "usages",  # provided directly
            "characteristics",  # done
            "markdowns"  # done
        }
        super().__init__(storage, cfg)

    def ds_path(self, dataset_file_path: str) -> str:
        return os.path.dirname(os.path.realpath(dataset_file_path))

    def link_path(self, dataset_file_path: str) -> Optional[str]:
        if not os.path.islink(os.path.dirname(dataset_file_path)):
            return None
        return os.path.dirname(os.path.abspath(dataset_file_path))

    def process_ds(self, f):
        for d in self.scan(f):
            ds = Dataset(open(d).read())
            # is stored?
            stored_ds = None  # todo load and parse
            # todo if new set type to fs

            # todo static field names
            # todo propagate data to object
            path = self.ds_path(f)
            ds.process_change("path", path)
            link = self.link_path(f)
            ds.process_change("links", link)
            md = self.find_md(ds)
            ds.process_change("markdowns", md)
            characteristics = self.get_characteristics(ds)
            ds.process_change("characteristics", characteristics)

            ds.flush_changes()
            self._storage.update(ds.id, ds.struct())

            removed = []

            # if actions:
            #     self.log_change(ds.id, actions)

            # todo something like if None
            for i in removed:
                self._storage.delete_key(ds.id, i)

            self._storage.update(ds.id, ds)

    def get_path(self, *args) -> str:
        return os.path.join(self.basepath, *args)

    def generate_characteristics(self, data_pth: str, out_file: str,
                                 force: bool = False) -> dict:
        present = os.path.exists(out_file)
        if present and not force:
            return self.parse_characteristics(open(out_file))
        # todo binary ?
        with open(out_file, "wb") as f:
            proc = subprocess.Popen(["./get_characteristics.sh", data_pth],
                                    stdout=f)
            proc.wait()
            return self.parse_characteristics(f)

    def find_files(self, folders, searched_filename="dataset.yaml"):
        for folder in folders:
            for root, folders, files in os.walk(folder, followlinks=True):
                if len(files) > int(self._cfg.iter_file_limit):
                    continue
                for filename in files:
                    if "*" in searched_filename:
                        # todo file match
                        if filename.endswith(
                                searched_filename.replace('*', '')):
                            yield os.path.join(root, filename)
                    else:
                        if filename == searched_filename:
                            yield os.path.join(root, filename)
