from datasets.struct.dataset import Dataset


def test_dataset_diff():
    d = Dataset({"id": 1})
    d.path = "/a"
    assert d.changelog == []
    d.flush_changes()
    assert d.changelog[0][0][0][0] == 'path'
    assert d.changelog[0][0][0][1] is None
    assert d.changelog[0][0][0][2] == '/a'
    d.flush_changes()
    assert len(d.changelog) == 1
    s = d.struct()
    assert s["id"] == 1
    assert s["path"] == "/a"
    assert s["usages"] == []
    assert s["servers"] == []
    assert len(s["changelog"]) == 1
