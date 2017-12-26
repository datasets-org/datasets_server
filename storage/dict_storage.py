from .storage import Storage


class DictStorage(Storage):
    def __init__(self):
        self.data: dict = {}

    def put(self, key, data):
        self.data[key] = data

    def update(self, key, data):
        self.data[key] = data

    def delete(self, key):
        self.data.pop(key)

    def delete_key(self, key, dict_key):
        self.data.get(key, {}).pop(dict_key)

    def get(self, key):
        return self.data.get(key)

    def load(self):
        pass
