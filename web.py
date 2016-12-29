from flask import Flask, request

app = Flask(__name__)

from conf import Cfg
from storage.lmdbStorage import LmdbStorage
from datasets import Datasets

import os
import ujson

cfg = Cfg()
db = LmdbStorage(cfg)
db.load()
d = Datasets(cfg, db)


@app.route("/")
def main():
    return ujson.dumps(db.data)


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
    # todo - this destroys changelog
    data = request.json
    db.update(id, data)
    return '', 200


@app.route("/reload")
def reload():
    # todo way to update periodically
    db.load()
    return '', 200


@app.route("/scan")
def scan():
    d.scan(cfg["datasets"])
    db.load()
    return '', 200


@app.route("/new")
def new():
    return ujson.dumps(d.generate())


if __name__ == "__main__":
    app.run(host="0.0.0.0")
