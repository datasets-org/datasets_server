import yaml
from typing import Any
from typing import List
from .changelog_entry import ChangelogEntry


class Dataset(object):
    def __init__(self, yaml_content: str) -> None:
        self._data = yaml.load(yaml_content)
        if "id" not in self._data:
            raise Exception("Dataset is missing an id")
        self.id = self.get("id")
        self._tmp_changes = []
        # todo separate structure from parse (for DB load)

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

    @property
    def usages(self) -> List[dict]:
        usages = self.get("usages")
        return usages if usages else []

    @property
    def changelog(self) -> List[List]:
        changelog = self.get("changelog")
        return changelog if changelog else []

    @property
    def markdowns(self) -> List[str]:
        return self.get("markdowns")

    @property
    def characteristics(self) -> dict:
        return self.get("characteristics")

    @property
    def path(self) -> str:
        """ dataset basepath (path to dataset.yaml)

        Returns:
            (str): basepath
        """
        return self.get("path")

    @property
    def links(self) -> List[str]:
        return self.get("links")

    def log_change(self, change: ChangelogEntry) -> None:
        self._tmp_changes.append(change.struct())

    def process_change(self, property, value) -> None:
        if getattr(self, property) != value:
            change = ChangelogEntry(property, value,
                                    old_value=getattr(self, property))
            self.log_change(change)

    def flush_changes(self) -> None:
        self.changelog.append(self._tmp_changes)
        self._tmp_changes = []

    def struct(self) -> dict:
        d = {
            "id": self.id,
            "usages": self.usages,
            "changelog": self.changelog,
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
        if self.markdowns:
            d.update({"markdowns": self.markdowns})
        if self.characteristics:
            d.update({"characteristics": self.characteristics})
        if self.path:
            d.update({"path": self.path})
        if self.links:
            d.update({"links": self.links})
        return d
