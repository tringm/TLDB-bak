from pathlib import Path
from typing import List, Dict

from tldb.server.query.query import Query


class XMLQuery(Query):
    def __init__(self, obj_name: str, attributes: List[str] = None, relationships: Dict[str, List[str]] = None):
        super().__init__('xml_query')
        self.object_name = obj_name
        self.attributes_name = attributes
        self.relationships = relationships

    @property
    def traverse_order(self):
        return self.attributes_name

    def __str__(self):
        rep_string = self.object_name + '\n'
        rep_string += f"Traverse order: {'|'.join(self.attributes_name)}\n"
        rep_string += f"Relationships: {self.relationships}"
        return rep_string

    def load_from_matrix_file(self, file_path: Path):
        """
        Load XML query from a matrix file in the following format:
        - First line: Elements name in query of level order format
        - Following lines: Containing the relationship matrix telling the relationship between each element
            + 1 = parent children
            + 2 = ancestor descendant
        :param file_path:
        :return:
        """
        with file_path.open() as f:
            contents = f.readlines()
        contents = [c.strip() for c in contents]
        self.attributes_name = contents[0].split(" ")
        self.relationships = {attr: [] for attr in self.attributes_name}
        for c in contents[1:]:
            rel = c.split(' ')
            self.relationships[rel[0]].append((rel[1], rel[2]))
