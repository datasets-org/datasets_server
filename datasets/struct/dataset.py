import yaml
from typing import Any, Set
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
        self.from_name = "from_ds"
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
        self._props = [getattr(self, i) for i in vars(self) if
                       i.endswith("_name")]

        self.id = self._get(self.id_name)
        self._tmp_changes = []

        # todo difference data and path
        self.data = self._get_data()  # type: List[str]
        self.name = self._get_name()  # type: str
        self.internal = self._get_internal()  # type: bool
        self.from_ds = self._get_from_ds()  # type: str
        self.url = self._get_url()  # type: str
        self.maintainer = self._get_maintainer()  # type: str
        self.tags = self._get_tags()  # type: List[str]
        # todo type is given by the server
        self.type = self._get_type()  # type: str
        self.usages = self._get_usages()  # type: List[dict]
        self.changelog = self._get_changelog()  # type: List[List[List[Any]]]
        self.markdowns = self._get_markdowns()  # type: List[str]
        self.characteristics = self._get_characteristics()  # type: dict
        self.path = self._get_path()  # type: List[str]
        self.links = self._get_links()  # type: List[str]
        # todo use objects
        self.servers = self._get(self.servers_name, [])  # type: List[str]
        self._inited = True

    def _get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

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
        # todo sort
        return self._get(self.usages_name, [])

    def _get_changelog(self) -> List[List]:
        # todo parse to struct
        # todo sort
        # sorted(data["changelog"], key=lambda x: x[0][3], reverse=True)
        return self._get(self.changelog_name, [])

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
        return getattr(self, item)

    def __setitem__(self, key, value):
        return setattr(self, key, value)

    def __setattr__(self, key, value):
        if hasattr(self, "_inited") and self._inited:
            if key in self._props:
                self.process_change(key, value)
        super().__setattr__(key, value)

    def log_change(self, change: ChangelogEntry) -> None:
        self._tmp_changes.append(change.struct())

    def process_change(self, prop: str, value: Any) -> None:
        if getattr(self, prop) != value:
            change = ChangelogEntry(prop, value,
                                    old_value=getattr(self, prop))
            self.log_change(change)

    def flush_changes(self) -> None:
        if self._tmp_changes:
            self.changelog.append(self._tmp_changes)
        self._tmp_changes = []

    def diff(self, ds: 'Dataset'):
        if ds.id != self.id:
            raise Exception("Comparing different datasets")
        s = ds.struct()
        for k, _ in self.struct():
            setattr(self, k, s[k])

    def struct(self) -> dict:
        d = self.__dict__
        res = {}
        for i in self._props:
            if i in d and d[i] is not None:
                res[i] = d[i]
        return res


class YamlDataset(Dataset):
    def __init__(self, yaml_content: str) -> None:
        super(YamlDataset, self).__init__(yaml.load(yaml_content))
