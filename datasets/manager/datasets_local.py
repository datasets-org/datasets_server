import os
import subprocess
from typing import Optional

from datasets.conf.datasets_conf import DatasetsConf
from datasets.struct.storage_server import StorageServer
from storage.storage import Storage
from datasets.struct.dataset_type import DatasetType
from datasets.struct.dataset import YamlDataset
from .datasets import Datasets


class DatasetsLocal(Datasets):
    def __init__(self,
                 cfg: DatasetsConf,
                 storage: Storage,
                 storage_server: StorageServer) -> None:
        self._cfg = cfg
        self._storage = storage
        self.storage_server = storage_server
        super().__init__(storage, cfg)

    def ds_path(self, dataset_file_path: str) -> str:
        return os.path.dirname(os.path.realpath(dataset_file_path))

    def link_path(self, dataset_file_path: str) -> Optional[str]:
        if not os.path.islink(os.path.dirname(dataset_file_path)):
            return None
        return os.path.dirname(os.path.abspath(dataset_file_path))

    def process_ds(self, f):
        for d in self.scan(f):
            new_ds = YamlDataset(open(d).read())
            stored_ds = self.get_ds_by_id(new_ds.id)
            # handle new dataset
            ds = stored_ds if stored_ds else new_ds
            ds.diff(new_ds)

            if not ds.type:
                ds.type = DatasetType.FS
            srv = self.storage_server.name
            if srv not in ds.servers:
                ds.servers.append(srv)
            ds.path = self.ds_path(f)
            link = self.link_path(f)
            if link not in ds.links:
                ds.links.append(link)
            md = self.find_md(ds)
            if md not in ds.markdowns:
                ds.markdowns.append(md)
            ds.characteristics = self.get_characteristics(ds)

            ds.flush_changes()
            self.store(ds)

    def get_path(self, *args) -> str:
        return os.path.join(self.storage_server.path, *args)

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
