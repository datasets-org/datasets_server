from storage.dict_storage import DictStorage
from datasets.manager.datasets import Datasets

# TODO old tests
# from conf import Cfg
# from datasets import DatasetsLocal

# cfg = Cfg()
# s = LmdbStorage(cfg)
# d = DatasetsLocal(cfg, s)
#
# # print(d.generate())
# # print("*" * 50)
#
# d.scan(["data"])
#
# s.load()
# print("*" * 50)
#
# # print(s.data)
# # s.delete("8b88a424-dbd8-4032-8be7-a930a415b9a5")
# # d.use("8b88a424-dbd8-4032-8be7-a930a415b9a5", {"user": "aaa", "op": "a"})

d = Datasets(DictStorage())


def test_uuid():
    assert isinstance(d.generate(), str)
    a = d.generate()
    b = d.generate()
    assert a != b


def test_use():
    d.create("k0", {})
    d.use("k0", {"a": 1})
    assert "usages" in d.get("k0")
    u = d.get("k0").get("usages")
    assert len(u) == 1
    assert u[0]._get("a") == 1
    assert "timestamp" in u[0]
    d.use("k0", {"a": 2})
    u = d.get("k0").get("usages")
    assert len(u) == 2
    assert u[0]._get("a") == 1
    assert "timestamp" in u[0]
    assert u[1]._get("a") == 2
    assert "timestamp" in u[1]
