class Storage(object):
    def put(self, key, data):
        raise NotImplementedError()

    def update(self, key, data):
        raise NotImplementedError()

    def delete(self, key):
        raise NotImplementedError()

    def delete_key(self, key, dict_key):
        # todo this should not be needed
        raise NotImplementedError()

    def get(self, key):
        raise NotImplementedError()

    def items(self):
        raise NotImplementedError()
