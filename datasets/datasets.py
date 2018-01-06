import time
import uuid
from typing import List
from typing import Tuple
from typing import Optional

from storage.storage import Storage
from .characteristics import parse_characteristics
from .dataset import Dataset
from .datasets_conf import DatasetsConf


class Datasets(object):
    def __init__(self, storage: Storage, conf: DatasetsConf) -> None:
        assert isinstance(storage, Storage)
        self._storage: Storage = storage
        self.datasets_filename = "dataset.yaml"
        self._conf = conf

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

    def get(self, key: str) -> Optional[dict]:
        return self._storage.get(key)

    def create(self, key: str, data: dict) -> None:
        self._storage.put(key, data)

    def find_md(self, ds: Dataset):
        md = []
        for p in ds.data:
            # todo get paths - not data
            md += self.find_files(p, searched_filename="*.md")
            # todo check if no markdowns found return None
        return md

    # def log_change(self, dataset: Dataset, changes: ChangelogEntry):
    #     dataset.log_changes(changes)
    #     self._storage.update(dataset.id, dataset)

    def storages(self) -> List[Tuple[str, str, str]]:
        """ Return configured servers

        Returns:
            List: configured servers
        """
        return self._conf.storages

    def get_characteristics(self, ds: Dataset, force_generate=False):
        characteristics = []
        for d in ds.data:
            file = "characteristics_{}.txt".format(d)
            characteristics_pth = self.get_path(ds.path, file)
            data_pth = self.get_path(ds.path, d)
            characteristics += self.generate_characteristics(
                data_pth, characteristics_pth, force=force_generate)
        return characteristics

    def parse_characteristics(self, content):
        return parse_characteristics(content)

    def remove_dataset(self):
        # todo
        pass

    def store(self, ds: Dataset) -> None:
        # todo this is not deling keys
        self._storage.update(ds.id, ds.struct())

    def get_ds_by_id(self, id: str) -> Optional[Dataset]:
        return self.get(id)

    def get_path(self, *args) -> str:
        raise NotImplementedError("You have to use specific implementation")

    def find_files(self, folders: List[str],
                   searched_filename: str = "dataset.yaml"):
        raise NotImplementedError("You have to use specific implementation")

    def generate_characteristics(self, data_pth: str, out_file: str,
                                 force: bool = False) -> dict:
        raise NotImplementedError("You have to use specific implementation")
