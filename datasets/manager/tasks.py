from storage.storage import Storage
from datasets.struct.task import Task
from datasets.struct.task import DictTask
from typing import List


class Tasks(object):
    def __init__(self, storage: Storage):
        self._storage = storage

    def list(self) -> List[Task]:
        """Return all tasks"""
        return [DictTask(v) for k, v in self._storage.items()]

    def get(self, task_id: str) -> Task:
        """Get task based on ID"""
        task_dict = self._storage.get(task_id)
        if not task_dict:
            return
        return DictTask(task_dict)

    def store(self, task: Task):
        """Store task"""
        self._storage.update(task.id, task.struct())

    def complete(self, task_id: str, success: bool, message: str = ""):
        # todo will it be done like this?
        """Mark task as completed"""
        t = self.get(task_id)
        if not t:
            return
        t.complete(success, message)

    def list_active(self) -> List[Task]:
        """List only not completed tasks sorted by date added"""
        return sorted([i for i in self.list() if not i.completed],
                      key=lambda x: x.created)

    def list_done(self) -> List[Task]:
        """List only completed tasks sorted by date added"""
        return sorted([i for i in self.list() if i.completed],
                      key=lambda x: x.created)
