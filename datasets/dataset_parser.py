import yaml
from typing import Any
from typing import List


class Dataset(object):
    def __init__(self, yaml_content: str) -> None:
        self._data = yaml.load(yaml_content)
        if "id" not in self._data:
            raise Exception("Dataset is missing an id")
        self.id = self.get("id")

    def get(self, key: str) -> Any:
        return self._data.get(key)

    @property
    def data(self) -> List[str]:
        # todo may be also dict?
        return self.get("data")

    @property
    def name(self) -> str:
        return self.get("name")

    @property
    def internal(self) -> bool:
        return self.get("internal")

    @property
    def from_ds(self) -> str:
        return self.get("from")

    @property
    def url(self) -> str:
        return self.get("url")

    @property
    def maintainer(self) -> str:
        return self.get("maintainer")

    @property
    def tags(self) -> List[str]:
        return self.get("tags")

    def dict(self) -> dict:
        d = {
            "id": self.id
        }
        if self.data:
            d.update({"data": self.data})
        if self.name:
            d.update({"name": self.name})
        if self.internal:
            d.update({"internal": self.internal})
        if self.from_ds:
            d.update({"from": self.from_ds})
        if self.url:
            d.update({"url": self.url})
        if self.maintainer:
            d.update({"maintainer": self.maintainer})
        if self.tags:
            d.update({"tags": self.tags})
        return d
