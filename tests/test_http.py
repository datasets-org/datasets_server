from datasets.datsets_http import DatasetsHttpJson
from storage.dict_storage import DictStorage
from datasets.http_conf import HttpConf
from confobj import ConfigDict


def test_scan():
    conf = HttpConf(order=(ConfigDict({
        "host": "http",
    }),))
    d = DatasetsHttpJson(conf, DictStorage())
    datasets = d.scan_dir("data")
    assert len(datasets) == 6
