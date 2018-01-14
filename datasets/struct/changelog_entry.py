import time


class ChangelogEntry(object):
    def __init__(self, prop, new_value, old_value=None, timestamp=None):
        if not timestamp:
            timestamp = time.time()
        self.property = prop
        self.timestamp = timestamp
        self.new_value = new_value
        self.old_value = old_value

    def struct(self):
        return [[self.property,
                 self.old_value,
                 self.new_value,
                 self.timestamp]]
