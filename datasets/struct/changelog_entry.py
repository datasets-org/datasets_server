import time
from typing import Any
from typing import List


class ChangelogEntry(object):
    def __init__(self, prop: str, new_value: Any, old_value: Any = None,
                 timestamp: Any = None) -> None:
        if not timestamp:
            timestamp = time.time()
        self.property = prop  # type: str
        self.timestamp = timestamp  # type: time.time
        self.new_value = new_value  # type: Any
        self.old_value = old_value  # type: Any

    def struct(self) -> List[List[Any]]:
        return [[self.property,
                 self.old_value,
                 self.new_value,
                 self.timestamp]]
