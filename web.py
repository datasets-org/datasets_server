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
    return ujson.dumps(data)

@app.route("/use/<id>", methods=['POST'])
def use(id):
    data = db.get(id)
    usage = request.json
    if "action" not in usage:
        usage["action"] = "read"
    d.use(id, usage)
    return ujson.dumps(data)

@app.route("/reload")
def reload():
    # todo way to update periodically
    db.load()
    return '', 200

@app.route("/scan")
def scan():
    d.scan(["data"])
    db.load()
    return '', 200

@app.route("/new")
def new():
    return ujson.dumps(d.generate())

if __name__ == "__main__":
    app.run(host="0.0.0.0")
