from datasets.struct.dataset_type import DatasetType


class StorageServer(object):
    def __init__(self, name: str, storage_type: DatasetType, path: str) -> None:
        self.name = name
        self.storage_type = storage_type
        self.path = path
