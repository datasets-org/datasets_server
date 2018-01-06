import os
import subprocess
from typing import Optional

from .dataset_type import DatasetType
from .dataset import Dataset, YamlDataset
from .datasets import Datasets


class DatasetsLocal(Datasets):
    def __init__(self, cfg, storage):
        self._cfg = cfg
        self._storage = storage
        # todo set basepath based on config
        self.basepath = "/"
        super().__init__(storage, cfg)

    def ds_path(self, dataset_file_path: str) -> str:
        return os.path.dirname(os.path.realpath(dataset_file_path))

    def link_path(self, dataset_file_path: str) -> Optional[str]:
        if not os.path.islink(os.path.dirname(dataset_file_path)):
            return None
        return os.path.dirname(os.path.abspath(dataset_file_path))

    def process_ds(self, f):
        for d in self.scan(f):
            ds = YamlDataset(open(d).read())
            stored_ds = self.get_ds_by_id(ds.id)
            stored_ds = stored_ds if stored_ds else ds
            if not stored_ds.type:
                stored_ds.type = DatasetType.FS

            # todo diff stored and loaded

            # todo dataset server will be needed
            path = self.ds_path(f)
            stored_ds.process_change(stored_ds.path_name, path)
            link = self.link_path(f)
            stored_ds.process_change(stored_ds.links_name, link)
            md = self.find_md(stored_ds)
            stored_ds.process_change(stored_ds.markdowns_name, md)
            characteristics = self.get_characteristics(ds)
            stored_ds.process_change(stored_ds.characteristics_name,
                                     characteristics)

            stored_ds.flush_changes()

            self.store(stored_ds)

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
