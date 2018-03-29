import RTree_XML
import RTree_SQL
from Node import Node
from Entry import Entry


def build_RTree_XML(element_name, max_n_children, dimension = 1):
	"""Summary
	Build a RTree for an XML element
	Args:
	    element_name (string): 
	    max_n_children (int): maximum number of children in RTree
	
	Returns:
	    Node: root node of built RTree
	"""
	entries = RTree_XML.load_element(element_name)
	root = RTree_XML.bulk_loading(entries, max_n_children, dimension)
	return root


def build_RTree_SQL(table_name, max_n_children, dimension):
    """Summary
    Build a RTree for a SQL table with each dimension is a column
    Args:
        table_name (string): 
        max_n_children (int): maximum number of children in RTree
    """
    entries = RTree_SQL.load(table_name)
    root = RTree_SQL.bulk_loading(entries, max_n_children, dimension)
    return root

root_A = build_RTree_XML('A', 2)
root_A.print_node()

root_A_B_C = build_RTree_SQL('A_B_C', 2, 1)
root_A_B_C.print_node()




def pairwise_filtering_ancestor_descendent(A_XML, D_XML, A_SQL, D_SQL, A_D_SQL):
    """Summary
    Structure filtering (D is descendant of A) and value filtering based on SQL tables

    Args:
        A_XML (Node): Root node of RTree_XML of A 
        D_XML (Node): Root node of RTree_XML of D
        A_SQL ([Node]): list of root node of RTree_SQL of tables of A 
        D_SQL ([Node]): list of root node of RTree_SQL of tables of D
        A_D_SQL ([Node]): list of root node of RTree_SQL of tables of A_D
    """
    


