### Is this necessary to split between SQLRTree Node and XML RTree Node


class RTreeNode:
    """RTreeNode

    Attributes:
        filtered (bool): True if this node if filtered
        value_filtering_visited(bool): True if this node has been full filtered before
        value_validation_visited (bool): True if this node has been validated before
        link_XML (dict): a dict contains a list of nodes for each linked element (children)
        link_SQL (dict): a dict contains a list of nodes for each linked tables (tables that has this element as highest element in XML query)
        max_n_children (int): maximum number of child Node
        parent (Node): parent Node
        boundary [[int, int], [int, int]]: MBR
                                  Each tuple contains list of 2 ints [lower_bound, upper_bound] of a dimension
        children [Node]: list of children nodes
        entries [Entry]: list of entries (if this node is a leaf node)
        validated_entries [Entry]: list of entries contained in this node (including children) to be used in validation
        name (String): name of this node (element name for RTree_XML node and table_name for R_Tree_SQL node)
    """

    def __init__(self, max_n_children):
        self.max_n_children = max_n_children
        self.parent = None
        self.children = []
        self.boundary = []
        self.entries = []
        self.name = ""

    def to_string(self):
        boundary_string = ""
        for bound in self.boundary:
            boundary_string = boundary_string + '[' + ','.join(str(num) for num in bound) + ']'
        return self.name + ':' + boundary_string


class XMLRTreeNode(RTreeNode):
    def __init__(self, max_n_children):
        XMLRTreeNode.__init__(self, max_n_children)

        self.filtered = False
        self.value_filtering_visited = False
        self.value_validation_visited = False
        self.link_xml = {}
        self.link_sql = {}

        # Necessary ???
        self.validated_entries = []
        self.link_SQL_range = {}


class SQLRTreeNode(RTreeNode):
    def __init(self, max_n_children):
        SQLRTreeNode.__init(self, max_n_children)