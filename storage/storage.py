class Storage(object):
    def put(self, key, data):
        raise NotImplementedError()

    def update(self, key, data):
        raise NotImplementedError()

    def delete(self, key):
        raise NotImplementedError()

    def get(self, key):
        raise NotImplementedError()

    def load(self):
        raise NotImplementedError()
