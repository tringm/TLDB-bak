import RTree_XML
import RTree_SQL
import queue
import os
import numpy as np
from Node import Node
from Entry import Entry
from Dewey_Index import *
import sys

import timeit


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


def ancestor_descendant_filter(node1, node2):
    """Summary
    This function check if a node D_XML is descendant of node A_XML
    Args:
        node1 (Node): R_Tree_XML Node to be checked if is ancestor
        node2 (Node): R_Tree_XML Node to be checked if is descendant
    
    Returns:
        Bool: True if node1 is ancestor of node2
    """
    node1_low = node1.boundary[0][0]
    node1_high = node1.boundary[0][1]
    node2_low = node2.boundary[0][0]
    node2_high = node2.boundary[0][1]

    # 2 cases for false:
    #   - node1 is on the left of node2, no intersection
    #   - node1 is on the right of node2, no intersection
    # Edge case 1.2.3 < 1.2.3.4 but is ancestor still
    if ((compare_DeweyId(node1_high, node2_low) and not is_ancestor(node1_high, node2_low)) or
        (compare_DeweyId(node2_high, node1_low))):
        return False
    return True

    # Range Index
    # if ((node1_low <= node2_low) and (node1_high >= node2_high)):
    #     return True
    # return False

def intersection_filter(node1, dimension1, node2, dimension2):
    """Summary
    This function check if node1 dimension1 range ande node2 dimension2 range intersect
    Args:
        node1 (Node): RTree_XML Node
        dimension1 (int): dimension of node1 (= 1 for value)
        node2 (Node): RTree_SQL node 
        dimension2 (int): dimension of node2
    
    Returns:
        Bool: True if intersect
    """
    # print('intersect_filter')
    # print('dimension1', dimension1)
    # print('dimension2', dimension2)
    node1_low = node1.boundary[dimension1][0]
    node1_high = node1.boundary[dimension1][1]
    node2_low = node2.boundary[dimension2][0]
    node2_high = node2.boundary[dimension2][1]
    if (node1_high < node2_low) or (node1_low > node2_high):
        return False
    return True


def value_filtering(filtering_node):
    """Summary
    This function do value filtering by updating all connected tables in filtering_node.link_SQL
    Filter this node if exist on table that has no match for this filtering_node
    Update filtering_node.link_SQL for table nodes that is not a match
    Args:
        filtering_node (RTree_XML Node): node to be checked for value filtering 
    """
    # print('value_filtering', filtering_node.name, filtering_node.boundary)
    # Go through each connected table
    for table_name in filtering_node.link_SQL.keys():
        # print('\t', 'table_name', table_name)
        table_nodes = filtering_node.link_SQL[table_name]                   # list of nodes in a connected table to be chedk
        has_one_satisfied_node = False
        link_nodes_removed = np.zeros(len(table_nodes))                     # array to store if a node in table_nodes should be removed
        table_dimension = table_name.split('_').index(filtering_node.name)
        # for condition_node in table_nodes:
        # print('len(table_nodes) ', len(table_nodes))

        for i in range(len(table_nodes)):
            if intersection_filter(filtering_node, 1, table_nodes[i], table_dimension):
                has_one_satisfied_node = True
                # print('\t' * 2, 'table_node', table_nodes[i].boundary, ' satisfied')
            else:
                # print('\t' * 2, 'table_node', table_nodes[i].boundary, ' unsatisfied')
                link_nodes_removed[i] = 1
        
        # If found no satisfied node -> filter current_node
        if not has_one_satisfied_node:
            # print('not has_one_satisfied_node')
            # print('link_nodes_removed', link_nodes_removed)
            filtering_node.filtered = True
            return

        # Update link
        new_link_nodes = []
        for i in range(len(table_nodes)):
            if link_nodes_removed[i] == 0:
                new_link_nodes.append(table_nodes[i])
        filtering_node.link_SQL[table_name] = new_link_nodes

def pair_value_filter(filtering_node, connected_element_node):
    """Summary
    This function do pair value filtering for filtering_node and a node connected to this filtering node
    by going through all tables in filtering_node.link_SQL that contains both filtering element and connected element
    and check if this pair value is in all those table
    Args:
        filtering_node (RTree_XML Node):
        connected_element_node (RTree_XML Node)
    """
    for table_name in filtering_node.link_SQL.keys():
        table_elements = table_name.split('_')
        # if this table contain both filtering element and connected element
        if ((filtering_node.name in table_elements) and (connected_element_node.name in table_elements)):
            # Check this table
            table_nodes = filtering_node.link_SQL[table_name]
            has_one_satisfied_node = False

            for i in range(len(table_nodes)):
                if intersection_filter(connected_element_node, 1, table_nodes[i], table_elements.index(connected_element_node.name)):
                    has_one_satisfied_node = True

            if not has_one_satisfied_node:
                return False
    return True

def connected_element_filtering(filtering_node):
    """Summary
    This function filter this filtering_node by checking all of its connected element and do pair structure filtering and pair value filtering
    Filter this filtering_node if there exists a connected element that has no suitable pair
    Update this filtering_node.link_XML for each connected element if not filtered
    Args:
        filtering_node (RTree_XML Node)
    """
    # print('connected_element_filtering of', filtering_node.name, filtering_node.boundary)
    for connected_element_name in filtering_node.link_XML.keys():
        connected_element_nodes = filtering_node.link_XML[connected_element_name]              # list of nodes in the connected element to be checked
        link_nodes_removed = np.zeros(len(connected_element_nodes))                            # array to store if a node in connected_element_nodes should be removed
        has_one_satisfied_node = False
        # Checking each node of this connected element
        for i in range(len(connected_element_nodes)):
            # print('\t' * 2, filtering_node.name, connected_element_name, connected_element_nodes[i].boundary)
            # Do full_filtering if has not been full filtered before
            if not connected_element_nodes[i].filter_visited:
                full_filtering(connected_element_nodes[i])
            # If after full_filtering this connected node is still not filtered
            if not connected_element_nodes[i].filtered:
                # if this filtering node can be ancestor of connected node (Structure filtering of this pair)
                if not ancestor_descendant_filter(filtering_node, connected_element_nodes[i]):
                    link_nodes_removed[i] = 1
                    # print('\t' * 2, filtering_node.name, filtering_node.boundary, connected_element_name, connected_element_nodes[i].boundary, 'ancestor_descendant_filter unsatisfied')
                else:
                    # Value filtering of this pair
                    if not pair_value_filter(filtering_node, connected_element_nodes[i]):
                        link_nodes_removed[i] = 1
                        # print('\t' * 2, filtering_node.name, filtering_node.boundary, connected_element_name, connected_element_nodes[i].boundary, 'pair_value_filter unsatisfied')
                    else:
                        # This pair is good
                        has_one_satisfied_node = True
        if not has_one_satisfied_node:
            filtering_node.filtered = True
            break

        # Update link
        new_link_nodes = []
        for i in range(len(connected_element_nodes)):
            if link_nodes_removed[i] == 0:
                new_link_nodes.append(connected_element_nodes[i])
        filtering_node.link_XML[connected_element_name] = new_link_nodes


def initialize_children_link(filtering_node):
    """Summary
    This function intialize children link_XML and link_SQL of a filtered node
    Args:
        filtering_node (RTree_XML Node)
    """

    # print('^^^^^^^^^^^^^^^^^^^^')
    # print('initialize_children_link')
    link_XML = {}
    link_SQL = {}
    for connected_element_name in filtering_node.link_XML.keys():
        link_XML[connected_element_name] = []
        for connected_element_node in filtering_node.link_XML[connected_element_name]:
            if len(connected_element_node.children) == 0:
                link_XML[connected_element_name].append(connected_element_node)
            else:
                for connected_element_node_child in connected_element_node.children:
                    link_XML[connected_element_name].append(connected_element_node_child)
    
    for table_name in filtering_node.link_SQL.keys():
        link_SQL[table_name] = []
        for table_node in filtering_node.link_SQL[table_name]:
            if len(table_node.children) == 0:
                link_SQL[table_name].append(table_node)
            else:
                for table_node_child in table_node.children:
                    link_SQL[table_name].append(table_node_child)

    for filtering_node_child in filtering_node.children:
        filtering_node_child.link_XML = link_XML.copy()
        filtering_node_child.link_SQL = link_SQL.copy()

    # print('Children link updated')
    # for filtering_node_child in filtering_node.children:
    #     print('\t', 'child', filtering_node_child.boundary)
    #     for connected_element_name in filtering_node_child.link_XML.keys():
    #         print('\t' * 2, 'connected_element_name', connected_element_name, end = ": ")
    #         for connected_element_node in filtering_node_child.link_XML[connected_element_name]:
    #             print(connected_element_node.boundary, end = ", ")
    #         print()
    #     print()
    
    #     for table_name in filtering_node_child.link_SQL.keys():
    #         print('\t' * 2, 'table_name', table_name, end = ": ")
    #         for table_node in filtering_node_child.link_SQL[table_name]:
    #             print(table_node.boundary, end = ", ")
    #     print()
    # print('^^^^^^^^^^^^^^^^^^^^')

def full_filtering(filtering_node):
    """Summary
    Perform full filtering in a node 
        1. Value filtering itself
        2. Filtering itself and its connected element
    Args:
        filtering_node (RTree_XML Node)
    """
    # print('##########################')
    # print('Full Filtering ', filtering_node.name, filtering_node.boundary)
    # Mark this node has been visited
    filtering_node.filter_visited = True
    if not filtering_node.filtered:
        value_filtering(filtering_node)
    # if filtering_node.filtered:
        # print(filtering_node.name, filtering_node.boundary, ' Filtered by value_filtering')
    if not filtering_node.filtered:
        # Continue to check all connected node
        connected_element_filtering(filtering_node)
        # if filtering_node.filtered:
            # print(filtering_node.name, filtering_node.boundary, ' Filtered by connected_element_filtering')

    if not filtering_node.filtered:
        # Update children link
        initialize_children_link(filtering_node)



def entries_value_validation(validating_node):
    """Summary
    This function checks entries in this validating node and match with entries in linked table node. Matched entries of validating_node entries are stored in link SQL
    If no match found for any validating_node_entry -> filter this validating_node
    Args:
        validating_node (RTree_XML Node)
    
    """

    # print("##################################")
    # print("entries_value_validation ", validating_node.name, validating_node.boundary)

    validating_node_entries = validating_node.validated_entries
    validating_node_entries_removed = np.zeros(len(validating_node_entries))

    # print("Entries: ")
    # for entry in validating_node_entries:
        # print(entry.coordinates)

    for table_name in validating_node.link_SQL.keys():
        # get all entries contained in linked table nodes

        # print("Checking table ", table_name)

        table_entries = []
        for table_node in validating_node.link_SQL[table_name]:
            for entry in table_node.get_entries():
                table_entries.append(entry)

        table_dimension = table_name.split('_').index(validating_node.name)

        has_one_matched_entry = False
        for i in range(len(validating_node_entries)):
            # if this entry hasnt been removed
            if (validating_node_entries_removed[i] == 0):

                validating_node_entry = validating_node_entries[i]
                validating_node_entry.link_SQL[table_name] = []

                # print("checking entry: ", validating_node_entries[i].coordinates)

                for table_entry in table_entries:
                    if (validating_node_entry.coordinates[1] == table_entry.coordinates[table_dimension]):
                        # print("Found a good match: ", validating_node_entry.coordinates, table_entry.coordinates)
                        validating_node_entry.link_SQL[table_name].append(table_entry)

                # if this validating_node_entry has no match in table_entries
                if not validating_node_entry.link_SQL[table_name]:
                    # print("entry removed")
                    validating_node_entries_removed[i] = 1

                else:
                    has_one_matched_entry = True

        if not has_one_matched_entry:
            validating_node.filtered = True
            return

    # Update entries
    validating_node.validated_entries = []
    for i in range(len(validating_node_entries)):
        if validating_node_entries_removed[i] == 0:
            validating_node.validated_entries.append(validating_node_entries[i])

def pair_value_validation(validating_node_entry, connected_element_entry, validating_node_name, connected_element_name):
    """Summary
    This function validate if entry of connected element can be match with validating_node_entry by going through entries in linked table entries
    Args:
        validating_node_entry (Entry)
        connected_element_entry (Entry)
        validating_node_name (String)
        connected_element_name (String)
    
    Returns:
        True if can find a match
    """
    for table_name in validating_node_entry.link_SQL.keys():
        table_elements = table_name.split('_')
        # if this table contain both filtering element and connected element
        if ((validating_node_name in table_elements) and (connected_element_name in table_elements)):
            # Check this table
            table_entries = validating_node_entry.link_SQL[table_name]
            has_one_satisfied_node = False

            for i in range(len(table_entries)):
                if connected_element_entry.coordinates[1] == table_entries[i].coordinates[table_elements.index(connected_element_name)]:
                    has_one_satisfied_node = True

            if not has_one_satisfied_node:
                return False
    return True


def entries_connected_element_validation(validating_node, all_elements_name, relationship_matrix):
    """Summary
    This function check entries in connected element of validating node to find matches and store it in link XML
    If no match found for any validating_node entries -> filter this validating_node
    Args:
        validating_node (RTree_XML Node):
        all_elements_name (list of String): list of all elements' name in XML_query ordered by level order
        relationship_matrix ([][]): 2 dimension array store relationship between element. Relation[parent, children] = 1, relation[ancestor, descendant] = 2, other = 0
    
    Returns:
        TYPE: Description
    """

    # print("##################################")
    # print("## entries_connected_element_validation ", validating_node.name, validating_node.boundary)

    validating_node_entries = validating_node.validated_entries
    validating_node_entries_removed = np.zeros(len(validating_node_entries))

    for connected_element_name in validating_node.link_XML.keys():
        # print("## Checking connected element ", connected_element_name, "for ", validating_node.name)

        # relationship between validating_node and connected_element_node
        relationship = relationship_matrix[all_elements_name.index(validating_node.name), all_elements_name.index(connected_element_name)]
        # print("relationship ", relationship)


        connected_element_nodes = validating_node.link_XML[connected_element_name]
        for connected_element_node in connected_element_nodes:
            # Do validation if not been validated
            if (not connected_element_node.filtered) and (not connected_element_node.validation_visited):
                node_validation(connected_element_node, all_elements_name, relationship_matrix)

        # get all entries in unfiltered connected_element_nodes
        connected_element_entries = []
        for connected_element_node in connected_element_nodes:
            if not connected_element_node.filtered:
                for entry in connected_element_node.validated_entries:
                    connected_element_entries.append(entry)


        # print("## remaining entries: ")
        # for entry in connected_element_entries:
            # print(entry.coordinates)

        has_one_matched_entry = False

        # print("## Structure check")


        for i in range(len(validating_node_entries)):
            # if this entry hasn't been removed
            if validating_node_entries_removed[i] == 0:
                validating_node_entry = validating_node_entries[i]
                validating_node_entry.link_XML[connected_element_name] = []

                # print("validating entry", validating_node_entry.coordinates)

                for connected_element_entry in connected_element_entries:
                    # print("connected_element_entry", connected_element_entry.coordinates)
                    #######################
                    # structure validation
                    structure_validation_satisfied = False
                    
                    # if parent_children relationship
                    if relationship == 1:
                        if is_parent(validating_node_entry.coordinates[0], connected_element_entry.coordinates[0]):
                            structure_validation_satisfied = True
                    # if ancestor_descendant relationship 
                    if relationship == 2:
                        if is_ancestor(validating_node_entry.coordinates[0], connected_element_entry.coordinates[0]):
                            structure_validation_satisfied = True

                    if structure_validation_satisfied:
                        ####################
                        # value validation
                        if pair_value_validation(validating_node_entry, connected_element_entry, validating_node.name, connected_element_name):
                            # print("Structure and value satisfied")
                            validating_node_entry.link_XML[connected_element_name].append(connected_element_entry)

                # if this validating_node_entry has no match in connected_element_entries
                if not validating_node_entry.link_XML[connected_element_name]:
                    validating_node_entries_removed[i] = 1
                    # print("Validating entry removed")
                else:
                    has_one_matched_entry = True

        if not has_one_matched_entry:
            validating_node.filtered = True
            return
                    
    # Update entries
    validating_node.validated_entries = []
    for i in range(len(validating_node_entries)):
        if validating_node_entries_removed[i] == 0:
            validating_node.validated_entries.append(validating_node_entries[i])


def node_validation(validating_node, all_elements_name, relationship_matrix):
    """Summary
    Performs validation for a node
    Args:
        validating_node (RTree XML Node)
        all_elements_name (list of String): list of all elements' name in XML_query ordered by level order
        relationship_matrix ([][]): 2 dimension array store relationship between element. Relation[parent, children] = 1, relation[ancestor, descendant] = 2, other = 0
    """
    # Intialization
    validating_node.validation_visited = True
    validating_node.validated_entries = validating_node.get_entries()

    if not validating_node.filtered:
        entries_value_validation(validating_node)

    if not validating_node.filtered:
        entries_connected_element_validation(validating_node, all_elements_name, relationship_matrix)


def validation_XML_SQL(all_elements_name, relationship_matrix, XML_query_root_node):
    """Summary
    Perform validation after filtering
    Args:
        all_elements_name (list of String): list of all elements' name in XML_query ordered by level order
        relationship_matrix ([][]): 2 dimension array store relationship between element. Relation[parent, children] = 1, relation[ancestor, descendant] = 2, other = 0
        all_elements_root (Node): RTree_XML root Node of XML_query root
    """

    XML_query_root_leaf_nodes = XML_query_root_node.get_leaf_node_not_filtered()
    for node in XML_query_root_leaf_nodes:
        # print(node.boundary)
        node_validation(node, all_elements_name, relationship_matrix)

    # print('###############################')
    # for node in XML_query_root_leaf_nodes:
    #     print('Node ', node.boundary)
    #     if not node.filtered:
    #         for entry in node.validated_entries:
    #             print('\t', entry.coordinates)
    #             print('\t', 'Entry link_XML')
    #             for connected_element in entry.link_XML.keys():
    #                 print('\t', connected_element)
    #                 for entry_XML in entry.link_XML[connected_element]:
    #                     print('\t' * 2, entry_XML.coordinates)
    #             print('\t', 'Entry link_SQL')
    #             for table_name in entry.link_SQL.keys():
    #                 print('\t', table_name)
    #                 for entry_SQL in entry.link_SQL[table_name]:
    #                     print('\t' * 2, entry_SQL.coordinates)



# def get_final_results(all_elements_name, relationship_matrix, XML_query_root_node):

#     results = []
    
#     XML_query_root_leaf_nodes = XML_query_root_node.get_leaf_node_not_filtered()

#     for root_leaf_node in XML_query_root_leaf_nodes:
#         root_leaf_node_entries = root_leaf_node.validated_entries

#         for root_leaf_node_entry in root_leaf_node_entries:
#             result = {}
#             result[all_elements_name[0]] = root_leaf_node_entry

#             for i in range(len(relationship_matrix)):
#                 for j in range(i + 1, len(relationship_matrix)):
#                     if relationship_matrix[i, j] != 0:
#                         entries = 



def join_XML_SQL(folder_name, all_elements_name, relationship_matrix, max_n_children):
    """Summary
    This function join XML DB and SQL DB given a XML query
    Args:
        folder_name (String): folder contain data files
        all_elements_name (list of String): list of all elements' name in XML_query ordered by level order
        relationship_matrix ([][]): 2 dimension array store relationship between element. Relation[parent, children] = 1, relation[ancestor, descendant] = 2, other = 0
        max_n_children (int): maximum of number of children in RTree Node
    """
    # Loading XML and SQL database into R_Tree
    start_loading = timeit.default_timer()
    all_elements_root = load_elements(folder_name, all_elements_name, max_n_children)
    all_tables_root = load_tables(folder_name, all_elements_name, max_n_children)
    end_loading = timeit.default_timer()
    print('loading time', end_loading - start_loading)

    ######################################
    # Print Tree
    # print('XML')
    # for element in all_elements_name:
    #     print(element)
    #     all_elements_root[element].print_node()

    # print('SQL')
    # for table_name in all_tables_root.keys():
    #     print(table_name)
    #     all_tables_root[table_name].print_node()

    ################################################################
    # Intialization
    # Start from root, link XML root of an element with root of its connected element in XML query

    start_filtering = timeit.default_timer()

    for i in range(len(all_elements_name)):
        element_name = all_elements_name[i]
        element_root = all_elements_root[element_name]
        for j in range(i + 1, len(all_elements_name)):
            if (relationship_matrix[i, j] != 0):
                connected_element = all_elements_name[j]
                element_root.link_XML[connected_element] = []
                element_root.link_XML[connected_element].append(all_elements_root[connected_element])

    ###################################################################3
    # Link tables root with XML root of highest element in XML query
    for table_name in all_tables_root.keys():
        table_root = all_tables_root[table_name]

        # find highest element
        table_elements = table_name.split('_') 
        highest_element_name = table_elements[get_index_highest_element(all_elements_name, table_name)]
        # link
        all_elements_root[highest_element_name].link_SQL[table_name] = []
        all_elements_root[highest_element_name].link_SQL[table_name].append(table_root)

    ################################################################
    # PRINT OUT LINK
    # for i in range(len(all_elements_name)):
    #     element = all_elements_name[i]
    #     element_root = all_elements_root[element]
    #     print(element)
    #     print('LINK_XML')
    #     for connected_element in element_root.link_XML.keys():
    #         print(connected_element)
    #         for connected_element_root in element_root.link_XML[connected_element]:
    #             print(connected_element_root.boundary)
    #     print('link_SQL')
    #     for connected_table_name in element_root.link_SQL.keys():
    #         print(connected_table_name)
    #         for connected_table_root in element_root.link_SQL[connected_table_name]:
    #             print(connected_table_root.boundary)

        
    ##################################################################
    # Push root of XML query RTree root node to queue
    XML_query_root_element = all_elements_name[0]
    queue_XML_query_root = queue.Queue()
    queue_XML_query_root.put(all_elements_root[XML_query_root_element])

    while not queue_XML_query_root.empty():
        XML_query_root_node = queue_XML_query_root.get()
        full_filtering(XML_query_root_node)
        if not XML_query_root_node.filtered:
            for XML_query_root_node_child in XML_query_root_node.children:
                queue_XML_query_root.put(XML_query_root_node_child)

    end_filtering = timeit.default_timer()
    print('filtering time', end_filtering - start_filtering)

    ##################################################################
    # Print filtered tree    
    # all_elements_root[XML_query_root_element].print_node_not_filtered_with_link()

    ##################################################################
    # Perform validation
    start_validation = timeit.default_timer()
    validation_XML_SQL(all_elements_name, relationship_matrix, all_elements_root[XML_query_root_element])
    end_validation = timeit.default_timer()

    print('validation time', end_validation - start_validation)

    ##################################################################
    # Return final result
    # get_final_results(all_elements_name, relationship_matrix, all_elements_root[XML_query_root_element])






def test_2():
    folder_name = 'test_2'
    all_elements_name = ['A', 'B', 'C', 'D']
    relationship_matrix = np.zeros((4, 4))
    relationship_matrix[0, 1] = 2
    relationship_matrix[0, 2] = 2
    relationship_matrix[2, 3] = 1
    max_n_children = 2
    join_XML_SQL(folder_name, all_elements_name, relationship_matrix, max_n_children)


def xiye_test_1():
    folder_name = 'xiye_test_1'
    all_elements_name = ['Orderline', 'asin', 'price']
    relationship_matrix = np.zeros((3, 3))
    relationship_matrix[0, 1] = 1
    relationship_matrix[0, 2] = 1
    max_n_children = 5
    join_XML_SQL(folder_name, all_elements_name, relationship_matrix, max_n_children)



test_2()
# xiye_test_1()







# A_XML = build_RTree_XML('test_2', 'A', 2, dimension = 1)
# A_XML.print_node()

# root_L_XML = build_RTree_XML('test_1', 'L', 2)
# print('L_XML')
# root_L_XML.print_node()

# root_L_SQL = build_RTree_SQL('L', 2, 0)
# print('L_SQL')
# root_L_SQL.print_node()

# root_K_XML = build_RTree_XML('K', 2)
# print('K_XML')
# root_K_XML.print_node()

# root_K_SQL = build_RTree_SQL('K', 2, 0)
# print('K_SQL')
# root_K_SQL.print_node()

# root_L_K_SQL = build_RTree_SQL('L_K', 2, 1)
# print('L_K_SQL')
# root_L_K_SQL.print_node()

# pairwise_filtering_ancestor_descendant(root_L_XML, root_K_XML, root_L_SQL, root_K_SQL, root_L_K_SQL, 1)
# root_L_XML.print_node_not_filtered()
# root_K_XML.print_node_not_filtered()
