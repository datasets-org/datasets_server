import time
import uuid
from typing import List
from typing import Tuple
from typing import Optional
from typing import Dict

from datasets.storage.storage import Storage
from datasets.characteristics import parse_characteristics
from datasets.struct.dataset import Dataset
from datasets.conf.datasets_conf import DatasetsConf

from datasets_server.datasets.struct.storage_server import StorageServer


class Datasets(object):
    def __init__(self, storage: Storage, conf: DatasetsConf) -> None:
        assert isinstance(storage, Storage)
        self._storage: Storage = storage
        self.datasets_filename = "dataset.yaml"
        self._conf = conf
        self.storage_servers = {}  # type: Dict[StorageServer]:
        self._parse_storage()

    def _parse_storage(self):
        for i in self._conf.storage:
            ss = StorageServer.parse_from_list(i)
            self.storage_servers[ss.name](ss)

    def generate(self) -> str:
        """ Get id for new dataset

        Returns:
            (str): id
        """
        while True:
            uid = str(uuid.uuid4())
            if not self._storage.get(uid):
                return uid

    def scan(self, folders: List[str]) -> List[str]:
        """ scan for dataset.yaml in given folders

        Args:
            folders (List[str]): list of folders to scan

        Returns:
            List[str]
        """
        for ds_path in self.find_files(folders):
            yield ds_path

    def use(self, key: str, data: dict = {}) -> None:
        # todo change to dataset
        """ log dataset use

        Args:
            key: dataset id
            data: usage specs
        """
        ds = self._storage.get(key)  # type: Dataset

        data.update({
            "timestamp": time.time(),
        })
        ds.usages.append(data)
        self._storage.update(key, ds)

    def _get(self, key: str) -> Optional[dict]:
        return self._storage.get(key)

    def get_all(self, fields: List[str] = None, sort_by: str = None) \
            -> List[Dataset]:
        data = {}
        if not fields:
            fields = ["name", "tags", "paths"]
        for k, v in self._storage.items():
            data[k] = {key: v[key] for key in fields}
        if sort_by:
            data = sorted(data.items(), key=lambda x: x[1][sort_by])
        return data

    def create(self, key: str, data: dict) -> None:
        self._storage.put(key, data)

    def find_md(self, ds: Dataset):
        md = []
        for p in ds.data:
            # todo get paths - not data
            md += self.find_files(p, searched_filename="*.md")
            # todo check if no markdowns found return None
        return md

    def get_characteristics(self, ds: Dataset, force_generate=False):
        characteristics = []
        for d in ds.data:
            file = "characteristics_{}.txt".format(d)
            characteristics_pth = self.get_path(ds.path, file)
            data_pth = self.get_path(ds.path, d)
            characteristics += self.generate_characteristics(
                data_pth, characteristics_pth, force=force_generate)
        return characteristics

    @staticmethod
    def parse_characteristics(content):
        return parse_characteristics(content)

    def remove_dataset(self):
        # todo
        pass

    def store(self, ds: Dataset) -> None:
        # todo this is not deling keys
        # todo solve by put (problem changelog, usages)
        self._storage.update(ds.id, ds.struct())

    def get_ds_by_id(self, uid: str) -> Optional[Dataset]:
        return self._get(uid)

    def get_path(self, *args) -> str:
        raise NotImplementedError("You have to use specific implementation")

    def find_files(self, folders: List[str],
                   searched_filename: str = "dataset.yaml"):
        raise NotImplementedError("You have to use specific implementation")

    def generate_characteristics(self, data_pth: str, out_file: str,
                                 force: bool = False) -> dict:
        raise NotImplementedError("You have to use specific implementation")
