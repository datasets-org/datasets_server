from storage.storage import Storage


class Tasks(object):
    def __init__(self, storage: Storage):
        self.storage = storage

    def list(self):
        return [v for k, v in self.storage.items()]

    def get(self, task_id):
        return self.storage.get(task_id)

    def set(self, task_id, data):
        self.storage.put(task_id, data)
