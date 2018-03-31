from datetime import datetime
import uuid


class Task(object):
    def __init__(self, task):
        self.id = str(uuid.uuid4())
        self.created = datetime.now()
        self.completed = False
        # todo task definition
        # (operation, storage, path)
        self.task = task
        self.finished = None
        self.message = None
        self.success = None

    def __repr__(self):
        return str(self.struct())

    def complete(self, success, message=""):
        self.finished = datetime.now()
        self.completed = True
        self.message = message
        self.success = success

    def struct(self):
        finished = self.finished.isoformat() if self.finished else self.finished
        return {
            "id": self.id,
            "created": self.created.isoformat(),
            "completed": self.completed,
            "task": self.task,
            "finished": finished,
            "message": self.message,
            "success": self.success,
        }


class DictTask(Task):
    def __init__(self, d: dict):
        super().__init__(d.get("task", None))
        self.id = d.get("id", None)
        assert self.id is not None
        self.task = d.get("task", None)
        assert self.task is not None
        created = d.get("created", None)
        assert created is not None
        self.created = datetime.strptime(created, "%Y-%m-%dT%H:%M:%S.%f")
        self.completed = d.get("completed", False)
        self.finished = d.get("finished", None)
        if self.finished:
            self.finished = datetime.strptime(created, "%Y-%m-%dT%H:%M:%S.%f")
        self.message = d.get("message", None)
        self.success = d.get("success", None)
