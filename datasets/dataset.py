import yaml
from typing import Any
from typing import List
from .changelog_entry import ChangelogEntry


class Dataset(object):
    # todo move this to lib and let people work with it
    def __init__(self, data: dict) -> None:
        self._data = data
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
        self.characteristics_name = "characteristics"
        self.links_name = "links"
        self.path_name = "path"
        self.type_name = "type"
        self.servers_name = "servers"

        self.id = self._get(self.id_name)
        self._tmp_changes = []

        self.data = self._get_data()
        self.name = self._get_name()
        self.internal = self._get_internal()
        self.from_ds = self._get_from_ds()
        self.url = self._get_url()
        self.maintainer = self._get_maintainer()
        self.tags = self._get_tags()
        self.type = self._get_type()
        self.usages = self._get_usages()
        self.changelog = self._get_changelog()
        self.markdowns = self._get_markdowns()
        self.characteristics = self._get_characteristics()
        self.path = self._get_path()
        self.links = self._get_links()
        self.servers = self._get(self.servers_name, [])

    def _get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default=default)

    def _get_data(self) -> List[str]:
        # todo may be also dict?
        return self._get(self.data_name)

    # todo methods are unreasonable for the simple cases
    def _get_name(self) -> str:
        return self._get(self.name_name)

    def _get_internal(self) -> bool:
        return self._get(self.internal_name)

    def _get_from_ds(self) -> str:
        return self._get(self.from_name)

    def _get_url(self) -> str:
        return self._get(self.from_name)

    def _get_maintainer(self) -> str:
        return self._get(self.maintainer_name)

    def _get_tags(self) -> List[str]:
        return self._get(self.tags_name)

    def _get_type(self) -> str:
        return self._get(self.type_name)

    def _get_usages(self) -> List[dict]:
        usages = self._get(self.usages_name)
        return usages if usages else []

    def _get_changelog(self) -> List[List]:
        changelog = self._get(self.changelog_name)
        return changelog if changelog else []

    def _get_markdowns(self) -> List[str]:
        return self._get(self.markdowns_name)

    def _get_characteristics(self) -> dict:
        return self._get(self.characteristics_name)

    def _get_path(self) -> str:
        """ dataset basepath (path to dataset.yaml)

        Returns:
            (str): basepath
        """
        return self._get(self.path_name)

    def _get_links(self) -> List[str]:
        return self._get(self.links_name)

    def __contains__(self, item):
        return item in vars(self)

    def __getitem__(self, item):
        return self._get(item)

    def __setitem__(self, key, value):
        return setattr(self, key, value)

    def log_change(self, change: ChangelogEntry) -> None:
        self._tmp_changes.append(change.struct())

    def process_change(self, prop, value) -> None:
        if prop == "from":
            prop = "from_ds"
        if getattr(self, prop) != value:
            change = ChangelogEntry(prop, value,
                                    old_value=getattr(self, prop))
            self.log_change(change)
            setattr(self, prop, value)

    def flush_changes(self) -> None:
        self.changelog.append(self._tmp_changes)
        self._tmp_changes = []

    def struct(self) -> dict:
        d = {
            self.id_name: self.id,
            self.usages_name: self.usages,
            self.changelog_name: self.changelog,
            self.servers: self.servers,
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
        if self.type:
            d.update({self.type_name: self.type})
        if self.markdowns:
            d.update({self.markdowns_name: self.markdowns})
        if self.characteristics:
            d.update({self.characteristics_name: self.characteristics})
        if self.path:
            d.update({self.path_name: self.path})
        if self.links:
            d.update({self.links_name: self.links})
        return d


class YamlDataset(Dataset):
    def __init__(self, yaml_content: str) -> None:
        super(YamlDataset, self).__init__(yaml.load(yaml_content))
