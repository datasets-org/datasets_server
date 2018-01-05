import yaml
from typing import Any
from typing import List
from .changelog_entry import ChangelogEntry


class Dataset(object):
    # todo move this to lib and let people work with it
    def __init__(self, yaml_content: str) -> None:
        self._data = yaml.load(yaml_content)
        if "id" not in self._data:
            raise Exception("Dataset is missing an id")
        self.id_name = "id"
        self.data_name = "data"
        self.name_name = "name"
        self.internal_name = "internal"
        self.from_name = "from"
        self.url_name = "url"
        self.maintainer_name = "maintainer"
        self.tags_name = "tags"
        self.usages_name = "usages"
        self.changelog_name = "changelog"
        self.markdowns_name = "markdowns"
        self.characteristics_name = "characteristics_name"
        self.links_name = "links"
        self.path_name = "path"
        self.type_name = "type"

        self.id = self.get("id")
        self._tmp_changes = []

        # todo load here from data and store to properties
        self.type = self.get_type()
        # todo separate structure from parse (for DB load)

    def get(self, key: str) -> Any:
        return self._data.get(key)

    @property
    def data(self) -> List[str]:
        # todo may be also dict?
        return self.get(self.data_name)

    @property
    def name(self) -> str:
        return self.get(self.name_name)

    @property
    def internal(self) -> bool:
        return self.get(self.internal_name)

    @property
    def from_ds(self) -> str:
        return self.get(self.from_name)

    @property
    def url(self) -> str:
        return self.get(self.from_name)

    @property
    def maintainer(self) -> str:
        return self.get(self.maintainer_name)

    @property
    def tags(self) -> List[str]:
        return self.get(self.tags_name)

    def get_type(self) -> str:
        return self.get(self.type_name)

    @property
    def usages(self) -> List[dict]:
        usages = self.get(self.usages_name)
        return usages if usages else []

    @property
    def changelog(self) -> List[List]:
        changelog = self.get(self.changelog_name)
        return changelog if changelog else []

    @property
    def markdowns(self) -> List[str]:
        return self.get(self.markdowns_name)

    @property
    def characteristics(self) -> dict:
        return self.get(self.characteristics_name)

    @property
    def path(self) -> str:
        """ dataset basepath (path to dataset.yaml)

        Returns:
            (str): basepath
        """
        return self.get(self.path_name)

    @property
    def links(self) -> List[str]:
        return self.get(self.links_name)

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
            self.id_name: self.id,
            self.usages_name: self.usages,
            self.changelog_name: self.changelog,
        }
        if self.data:
            d.update({self.data_name: self.data})
        if self.name:
            d.update({self.name_name: self.name})
        if self.internal:
            d.update({self.internal_name: self.internal})
        if self.from_ds:
            d.update({self.from_name: self.from_ds})
        if self.url:
            d.update({self.url_name: self.url})
        if self.maintainer:
            d.update({self.maintainer_name: self.maintainer})
        if self.tags:
            d.update({self.tags_name: self.tags})
        if self.markdowns:
            d.update({self.markdowns_name: self.markdowns})
        if self.characteristics:
            d.update({self.characteristics_name: self.characteristics})
        if self.path:
            d.update({self.path_name: self.path})
        if self.links:
            d.update({self.links_name: self.links})
        return d
