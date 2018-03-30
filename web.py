import ujson
from flask import Flask, request
import time

from .datasets.conf.datasets_conf import DatasetsConf
from .datasets.manager.datasets_local import DatasetsLocal
from .datasets.manager.datasets import Datasets
from .datasets.storage.lmdbStorage import LmdbStorage

app = Flask(__name__)

cfg = DatasetsConf()
db = LmdbStorage(cfg)
# d = DatasetsLocal(cfg, db)
ds = Datasets(db, cfg)


@app.route("/")
def main():
    sort = request.form['sort'] if "sort" in request.form else None
    fields = request.form['fields'].split(",") if 'fields' in request.form \
        else None
    return ujson.dumps(ds.get_all(fields=fields, sort_by=sort))


@app.route("/detail/<ds_id>")
def detail(ds_id):
    data = ds.get_ds_by_id(ds_id)
    if "usages" in data:
        # todo usages should be sorted by default
        data["usages"] = sorted(data["usages"], key=lambda x: x["timestamp"],
                                reverse=True)
    if "changelog" in data:
        # todo changelog should be sorted by default
        data["changelog"] = sorted(data["changelog"], key=lambda x: x[0][3],
                                   reverse=True)
    if "characteristics" in data:
        data["characteristics"] = sorted(data["characteristics"].items(),
                                         key=lambda x: x[0])
        for i, (k, v) in enumerate(data["characteristics"]):
            data["characteristics"][i] = (
                k, sorted(v.items(), key=lambda x: x[0]))

    return ujson.dumps(data)


@app.route("/use/<ds_id>", methods=['POST'])
def use(ds_id):
    data = ds.get_ds_by_id(ds_id)
    usage = request.json
    if "action" not in usage:
        usage["action"] = "read"
    ds.use(ds_id, usage)
    return ujson.dumps(data)


@app.route("/update/<ds_id>", methods=['POST'])
def update(ds_id):
    # todo log only which things changed
    data = request.json
    stored = ds.get_ds_by_id(ds_id)
    data.log_change(id, [[None, stored, data, time.time()]])
    ds.store(data)
    return '', 200


@app.route("/storage")
def storage():
    return ujson.dumps([i.struct() for i in ds.storage_servers.values()])


# todo load data directly from storage
# @app.route("/reload")
# def reload():
#     # todo way to update periodically
#     db.load()
#     return '', 200


# @app.route("/scan")
# def scan():
#     p = Process(target=_scan_proc)
#     p.start()
#     return '', 200


# # todo this has to be killed
# def _scan_proc():
#     # this func is called from another process (passing objects may be bad idea)
#     cfg = Cfg()
#     db = LmdbStorage(cfg)
#     db.load()
#     d = Datasets(cfg, db)
#     d.scan(cfg["datasets"])
#     db.load()


@app.route("/new")
def new():
    return ujson.dumps(ds.generate())


@app.route("/task")
def tasks():
    return ujson.dumps(ds.tasks.list())


@app.route("/task/<task_id>")
def task(task_id):
    return ujson.dumps(ds.tasks.get(task_id))


if __name__ == "__main__":
    app.run(host="0.0.0.0")
