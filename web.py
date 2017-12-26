import os
import ujson
from flask import Flask, request
import time
from multiprocessing import Process

from datasets.datasets import generate
from .conf import DatasetsConf
from datasets import DatasetsLocal
from storage.lmdbStorage import LmdbStorage

app = Flask(__name__)

cfg = DatasetsConf()
db = LmdbStorage(cfg)
db.load()
d = DatasetsLocal(cfg, db)


@app.route("/")
def main():
    # todo sort by what?
    sort = False
    if "sort" in request.form:
        sort = bool(request.form['sort'])
    data = {}
    keys = ["name", "tags", "paths"]
    for k, v in db.data.items():
        data[k] = {key: v[key] for key in keys}
    if sort:
        sort_by = "name"
        data = sorted(data.items(), key=lambda x: x[1][sort_by])
    return ujson.dumps(data)


@app.route("/detail/<id>")
def detail(id):
    data = db.get(id)
    if "usages" in data:
        data["usages"] = sorted(data["usages"], key=lambda x: x["timestamp"],
                                reverse=True)
    if "changelog" in data:
        data["changelog"] = sorted(data["changelog"], key=lambda x: x[0][3],
                                   reverse=True)
    if "characteristics" in data:
        data["characteristics"] = sorted(data["characteristics"].items(),
                                         key=lambda x: x[0])
        for i, (k, v) in enumerate(data["characteristics"]):
            data["characteristics"][i] = (
                k, sorted(v.items(), key=lambda x: x[0]))

    return ujson.dumps(data)


@app.route("/use/<id>", methods=['POST'])
def use(id):
    data = db.get(id)
    usage = request.json
    if "action" not in usage:
        usage["action"] = "read"
    d.use(id, usage)
    return ujson.dumps(data)


@app.route("/update/<id>", methods=['POST'])
def update(id):
    # todo log only which things changed
    data = request.json
    stored = db.get(id)
    d.log_change(id, [[None, stored, data, time.time()]])
    db.update(id, data)
    return '', 200


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
    return ujson.dumps(generate(d.storage))


if __name__ == "__main__":
    app.run(host="0.0.0.0")
