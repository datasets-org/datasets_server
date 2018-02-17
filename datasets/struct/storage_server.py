from .dataset_type import DatasetType


class StorageServer(object):
    def __init__(self, name: str, storage_type: DatasetType, path: str) -> None:
        self.name = name  # type: str
        self.storage_type = storage_type  # type: DatasetType
        self.path = path  # type: str

    def struct(self):
        return [self.name, self.storage_type, self.path]

    @staticmethod
    def parse_from_list(d) -> 'StorageServer':
        return StorageServer(d[0], d[1], d[2])
