from typing import Tuple

from confobj import Config
from confobj import ConfigBase
from confobj import ConfigEnv


# import yaml
#
# class Cfg(object):
#     def __init__(self, path="conf/config.yaml"):
#         self.cfg = yaml.load(open(path))
#
#     def __getitem__(self, item):
#         return self.cfg[item]
#
#     def __contains__(self, item):
#         return item in self.cfg


class DatasetsConf(Config):
    def __init__(self, order: Tuple[ConfigBase] = (ConfigEnv(),)) -> None:
        self.database_path = "database"
        self.iter_file_limit = 200
        self.datasets = ["data"]
        self.storage_replace = [
            '/app/data/',
            '/data/',
        ]
        self.storages = [
            ("default", "fs", "/"),
            """ name, type, path """
        ]
        super().__init__(order=order)
        self.configure()
