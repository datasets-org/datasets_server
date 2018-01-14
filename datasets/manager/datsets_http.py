from typing import List

import requests
from urljoin import url_path_join

from storage.storage import Storage
from datasets.struct.dataset import Dataset
from .datasets import Datasets
from datasets.conf.http_conf import HttpConf


class DatasetsHttpJson(Datasets):
    def __init__(self, conf: HttpConf, storage: Storage) -> None:
        # todo handle different confs, or merge them
        self._conf = conf
        super().__init__(storage, conf)

    def scan_dir(self, path: str, recursive: bool = True, results=None) -> \
            List[Dataset]:
        r = requests.get(url_path_join(self._conf.get_server_address(), path,
                                       trailing_slash=True))
        if results is None:
            results = []
        if r.status_code != 200:
            # todo log fail but graceful
            return results  # todo some err

        for i in r.json():
            if i.get("type") == "directory":
                name = i.get("name")
                if recursive:
                    self.scan_dir(url_path_join(path, name), results=results)
            if i.get("type") == "file":
                name = i.get("name")
                if name == self.datasets_filename:
                    ds = self.get_dataset(url_path_join(path, name))
                    results.append(ds)
        # todo store
        return results

    def get_dataset(self, path: str) -> Dataset:
        dataset_request = requests.get(url_path_join(
            self._conf.get_server_address(), path))
        if dataset_request.status_code != 200:
            return None  # todo some err
        dataset_content = dataset_request.text
        dataset = Dataset(dataset_content)
        self._storage.put(dataset.id, dataset.struct())
        return dataset

    def get_path(self):
        # todo get args and pass them to urlpathjoin
        pass

    def analyze_dataset(self):
        # todo
        # todo find makrdowns
        # todo characteristics
        # todo files
        # todo paths
        # todo follow changelogs
        pass
