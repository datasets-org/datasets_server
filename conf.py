import yaml


# todo add argparse for overload

class Cfg(object):
    def __init__(self, path="config.yaml"):
        self.cfg = yaml.load(open(path))

    def __getitem__(self, item):
        return self.cfg[item]
