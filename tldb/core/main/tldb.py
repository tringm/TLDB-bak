from .utils import SKIP_IN_PATH, INDEX_STRUCTURE_MAPPER
from tldb.core.main.structure.entry import Entry


class TLDB:
    """
    The TLDB client
    """
    def __init__(self, host=None):
        self.host = host
        # TODO: REPLACE map by some hashmap?
        self._indices = {}

    def get_index(self, name):
        if name not in self._indices:
            raise ValueError(f"{name} not existed")
        return self._indices[name]

    # TODO: think about flexible change file path to an url
    def index_csv_file(self, index_name, file_path, index_type='rtree', delimiter=',', id=None):
        for par in (index_name, file_path):
            if par not in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")
        if index_type not in INDEX_STRUCTURE_MAPPER:
            raise ValueError("Unsupported index_type. The index structure should be: ",
                             '|'.join(list(INDEX_STRUCTURE_MAPPER.keys())))
        with file_path.open() as f:
            contents = f.readlines()

        entries = [Entry(row.split(delimiter)) for row in contents]
        index_structure = INDEX_STRUCTURE_MAPPER[index_type]()
        index_structure.load(entries)
        self._indices[index_name] = index_structure
