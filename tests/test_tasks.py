from confobj import ConfigDict
from datasets.manager.datasets import Datasets
from storage.dict_storage import DictStorage

from datasets.conf.datasets_conf import DatasetsConf


def test_tasks():
    conf = DatasetsConf(order=(ConfigDict({
    }),))
    ds = Datasets(DictStorage(), conf)
    assert ds.tasks.list() == []
    assert ds.tasks.get(0) is None
    ds.tasks.set(0, {"task": 0})
    assert ds.tasks.list() == [{"task": 0}]
    assert ds.tasks.get(0) == {"task": 0}
    assert ds.tasks.get(1) is None
