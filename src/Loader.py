import timeit
import RTree_XML
import RTree_SQL
import os
import Node
import numpy as np

def get_index_highest_element(all_elements_name, table_name):
    """Summary
    This function return the index of highest level element (in XML query) of a table name
    e.g: If query is A->B then get_index_highest_element(['A, B'], 'B_A') = 1
    Args:
        all_elements_name (list[string]): list of all XML query elements' name
        table_name (String): Name of table
    
    Returns:
        int: index
    """
    table_elements = table_name.split('_')
    index = []
    for element_name in table_elements:
        index.append(all_elements_name.index(element_name))
    return np.argmin(np.asarray(index))

def load_XML_query(folder_name):
    file_path = "../data/" + folder_name + "/" + "XML_query.dat" 
    with open(file_path) as f:
        content = f.readlines()
    content = [x.strip() for x in content]
    
    all_elements_name = content[0].split(" ")

    relationship_matrix = np.zeros((len(all_elements_name), len(all_elements_name)))
    for i in range(1, len(content)):
        relationship = content[i].split(" ")
        relationship_matrix[all_elements_name.index(relationship[0]), all_elements_name.index(relationship[1])] = int(relationship[2])
    return (all_elements_name, relationship_matrix)

def load_elements(folder_name, all_elements_name, max_n_children):
    """Summary
    Load multiple elements and return list of root node of each element's R_Tree
    Args:
        folder_name (string)
        all_elements_name (list of string): list of name of elements in XML query
        max_n_children (int): maximum number of children in R_Tree
    
    Returns:
        dict[string, Node]: dict with key is name of element and value is corresponding root node of RTree_XML
    """
    all_elements_root = {}
    for element_name in all_elements_name:
        print('Loading element: ', element_name)
        all_elements_root[element_name] = build_RTree_XML(folder_name, element_name, max_n_children)
    return(all_elements_root)

def build_RTree_XML(folder_name, element_name, max_n_children, dimension = 1):
    """Summary
    Build a RTree for an XML element
    
    Args:
        folder_name (string): 
        element_name (string)
        max_n_children (int): maximum number of children in RTree
        dimension (int, optional): dimenstion to be sorted for RTree, currently 1 (value)
    
    Returns:
        Node: root node of built RTree
    """
    entries = RTree_XML.load(folder_name, element_name)
    print('Loaded ', len(entries), 'entries')
    # print(entries[0].coordinates)
    root = RTree_XML.bulk_loading(entries, element_name, max_n_children, dimension)

    return root

def load_tables(folder_name, all_elements_name, max_n_children):
    """Summary
    Load all tables inside a folder (files that end with _table.dat)
    Args:
        folder_name (string):
        all_elements_name (list of string): list of elements in XML query
        max_n_children (int): maximum number of children in R_Tree
    
    Returns:
        all_tables_root (dict[string, Node]): dict with key is name of table and value is corresponding root node of RTree_SQL
    """
    all_tables_root = {}
    for file_name in os.listdir("../data/" + folder_name):
        if 'table' in file_name:
            table_name = file_name[:-10]
            print('Loading table: ', table_name)
            all_tables_root[table_name]= build_RTree_SQL(folder_name, file_name, all_elements_name, max_n_children)
    return all_tables_root


def build_RTree_SQL(folder_name, file_name, all_elements_name, max_n_children):
    """Summary
    Build a RTree for a SQL table with each element is a column. This RTree is sorted by element that is at highest level (smallest index in elements list) in XML query tree
    
    Args:
        folder_name (string):
        file_name (string)
        all_elements_name (list of string): list of elements_name in XML query
        max_n_children (int): maximum number of children in RTree
    
    Returns:
        root (Node): root node of built RTree
        dimension (int): index of highest level element in XML query 
    """
    entries = RTree_SQL.load(folder_name, file_name)
    
    # Find which element in table is higest level and sort RTree based on this element 
    table_name = file_name[:-10]
    dimension = get_index_highest_element(all_elements_name, table_name)
    # table_elements = file_name[:-10].split('_')
    # index = []
    # for element_name in table_elements:
    #     index.append(all_elements_name.index(element_name))
    # dimension = np.argmin(np.asarray(index))

    root = RTree_SQL.bulk_loading(entries, table_name, max_n_children, dimension)
    return root



class Loader:

    """Summary
    This loader takes a folder path read the XML query by looking for a "XML_query.data" file
    It will then load all elements root given that each element will have a element_v.dat for elemnt value and element_id.dat for element Dewey index
    It will load all tables (files that contains "tables" in its name)
    Attributes:
        all_elements_root (TYPE): Description
        all_tables_root (TYPE): Description
    """
    
    def __init__(self, folder_name, max_n_children):
        start_loading = timeit.default_timer()
        self.max_n_children = max_n_children
        self.all_elements_name, self.relationship_matrix = load_XML_query(folder_name)
        self.all_elements_root = load_elements(folder_name, self.all_elements_name, self.max_n_children)
        self.all_tables_root = load_tables(folder_name, self.all_elements_name, self.max_n_children)
        end_loading = timeit.default_timer()
        self.time = end_loading - start_loading

