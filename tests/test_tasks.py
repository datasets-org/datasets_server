from confobj import ConfigDict
from datasets.manager.datasets import Datasets
from datasets.struct.task import DictTask
from datasets.struct.task import Task
from storage.dict_storage import DictStorage

from datasets.conf.datasets_conf import DatasetsConf
import datetime


def test_tasks():
    conf = DatasetsConf(order=(ConfigDict({
    }),))
    ds = Datasets(DictStorage(), conf)
    assert ds.tasks.list() == []
    assert ds.tasks.get("0") is None
    t = Task(0)
    ds.tasks.store(t)
    assert len(ds.tasks.list()) == 1
    assert ds.tasks.list()[0].completed is False
    assert isinstance(ds.tasks.get(t.id), Task)
    assert ds.tasks.get(t.id).completed is False
    assert isinstance(ds.tasks.get(t.id).created, datetime.datetime)
    assert ds.tasks.list_done() == []
    assert len(ds.tasks.list_active()) == 1
    t.complete(True)
    ds.tasks.store(t)
    assert len(ds.tasks.list_done()) == 1
    assert len(ds.tasks.list_active()) == 0
    t1 = Task(1)
    t2 = Task(2)
    ds.tasks.store(t1)
    ds.tasks.store(t2)
    assert len(ds.tasks.list_active()) == 2
    assert ds.tasks.list_active()[0].id == t1.id
    assert ds.tasks.list_active()[1].id == t2.id
    t1.complete(True)
    t2.complete(False, message="fail")
    ds.tasks.store(t1)
    ds.tasks.store(t2)
    assert len(ds.tasks.list_active()) == 0
    assert len(ds.tasks.list_done()) == 3
    assert ds.tasks.list_done()[0].id == t.id
    assert ds.tasks.list_done()[1].id == t1.id
    assert ds.tasks.list_done()[2].id == t2.id
    assert ds.tasks.list_done()[1].success is True
    assert ds.tasks.list_done()[2].success is False
    assert ds.tasks.list_done()[2].message == "fail"

    assert ds.tasks.get("0") is None
