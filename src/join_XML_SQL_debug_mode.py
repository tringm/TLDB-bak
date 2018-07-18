import RTree_XML
import RTree_SQL
from Loader import Loader
import queue
import numpy as np
from Node import Node
from Entry import Entry
from Dewey_Index import *
import sys
import copy

import timeit


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


def ancestor_descendant_filtering(node1, node2):
    """Summary
    This function check if a node D_XML is descendant of node A_XML
    Args:
        node1 (Node): R_Tree_XML Node to be checked if is ancestor
        node2 (Node): R_Tree_XML Node to be checked if is descendant
    
    Returns:
        Bool: True if node1 cannot be ancestor of node2
    """
    node1_low = node1.boundary[0][0]
    node1_high = node1.boundary[0][1]
    node2_low = node2.boundary[0][0]
    node2_high = node2.boundary[0][1]

    # 2 cases for false:
    #   - node1 is on the left of node2, no intersection
    #   - node1 is on the right of node2, no intersection
    # Edge case 1.2.3 < 1.2.3.4 but is ancestor still
    if (compare_DeweyId(node2_high, node1_low)):
        return True
    if (compare_DeweyId(node1_high, node2_low)):
        if is_ancestor(node1_high, node2_low) == False:
            return True
    return False

    # Range Index
    # if ((node1_low <= node2_low) and (node1_high >= node2_high)):
    #     return True
    # return False

def intersection_filter(node1, dimension1, node2, dimension2):
    """Summary
    This function check if node1 dimension1 range and node2 dimension2 range intersect
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


def value_filtering(filtering_node, all_elements_name):
    """Summary
    This function do value filtering by updating all connected tables in filtering_node.link_SQL
    Filter this node if exist on table that has no match for this filtering_node
    Update filtering_node.link_SQL for table nodes that is not a match
    Update the range of linking nodes
    Each node should be value filtered once
    Args:
        filtering_node (RTree_XML Node): node to be checked for value filtering 
    """
    print("#######################")
    print('value_filtering', filtering_node.name, filtering_node.boundary)

    filtering_node.value_filtering_visited = True

    link_SQL_range = {}
    for element in all_elements_name:
        link_SQL_range[element] = []


    # Go through each connected table
    print("Value filtering: Going through tables")
    for table_name in filtering_node.link_SQL.keys():
        print('\t' + 'table_name', table_name)
        table_nodes = filtering_node.link_SQL[table_name]                   # list of nodes in a connected table to be chedk

        has_one_satisfied_node = False
        link_nodes_removed = np.zeros(len(table_nodes))                     # array to store if a node in table_nodes should be removed
        table_dimension = table_name.split('_').index(filtering_node.name)
        table_elements = table_name.split('_')

        combined_range = {}
        for element in all_elements_name:
            combined_range[element] = []

        print('\t' + 'len(table_nodes) ', len(table_nodes))

        for i in range(len(table_nodes)):
            if intersection_filter(filtering_node, 1, table_nodes[i], table_dimension):
                has_one_satisfied_node = True

                boundary = table_nodes[i].boundary

                for element in table_elements:
                    if not combined_range[element]:
                        combined_range[element] = list(boundary[table_elements.index(element)])
                    else: 
                        if (boundary[table_elements.index(element)][0] < combined_range[element][0]):
                            combined_range[element][0] = boundary[table_elements.index(element)][0]
                        if (boundary[table_elements.index(element)][1] > combined_range[element][1]):
                            combined_range[element][1] = boundary[table_elements.index(element)][1]

                print('\t' * 2, 'table_node', table_nodes[i].boundary, ' satisfied')
            else:
                print('\t' * 2, 'table_node', table_nodes[i].boundary, ' unsatisfied')
                link_nodes_removed[i] = 1
        
        # If found no satisfied node -> filter current_node
        if not has_one_satisfied_node:
            print('\t'+'not has_one_satisfied_node')
            print('\t'+'link_nodes_removed', link_nodes_removed)
            filtering_node.filtered = True
            return

        # Update link_SQL
        new_link_nodes = []
        for i in range(len(table_nodes)):
            if link_nodes_removed[i] == 0:
                new_link_nodes.append(table_nodes[i])
        print('\t', 'new_link_nodes')
        for node in new_link_nodes:
            print('\t' * 2, node.boundary)
        filtering_node.link_SQL[table_name] = new_link_nodes

        # Update link_SQL_range
        for element in table_elements:
            if not link_SQL_range[element]:
                link_SQL_range[element] = combined_range[element]
            else:
                if (link_SQL_range[element][0] < combined_range[element][0]):
                    link_SQL_range[element][0] = combined_range[element][0]
                if (link_SQL_range[element][1] > combined_range[element][1]):
                    link_SQL_range[element][1] = combined_range[element][1]

    filtering_node.link_SQL_range = link_SQL_range



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

def connected_element_filtering(filtering_node, limit_range, all_elements_name):
    """Summary
    This function filter this filtering_node by checking all of its connected element and do pair structure filtering and pair value filtering
    Filter this filtering_node if there exists a connected element that has no suitable pair
    Update this filtering_node.link_XML for each connected element if not filtered
    
    Args:
        filtering_node (RTree_XML Node): Description
        limit_range (dict): passed from full_filtering
        all_elements_name (list): list of all element names in XML query
    """
    print("Connected element filtering")
    for connected_element_name in filtering_node.link_XML.keys():
        print("Checking: ", connected_element_name)

        connected_element_nodes = filtering_node.link_XML[connected_element_name]              # list of nodes in the connected element to be checked
        link_nodes_removed = np.zeros(len(connected_element_nodes))                            # array to store if a node in connected_element_nodes should be removed
        has_one_satisfied_node = False
        # Checking each node of this connected element
        connected_element_nodes_limit_range = []
        for i in range(len(connected_element_nodes)):
            if connected_element_nodes[i].filtered:
                link_nodes_removed[i] = 1
                continue

            # If this filtering node cannot be ancestor of connected node
            if ancestor_descendant_filtering(filtering_node, connected_element_nodes[i]):
                link_nodes_removed[i] = 1
                continue

            # Check limit range
            if ((connected_element_nodes[i].boundary[1][0] > limit_range[connected_element_name][1]) or
                (connected_element_nodes[i].boundary[1][1] < limit_range[connected_element_name][0])):
                link_nodes_removed[i] = 1
                continue

            # This pair is good
            has_one_satisfied_node = True


        if not has_one_satisfied_node:
            print("No satisfied node")
            filtering_node.filtered = True
            return

        # Update link_XML

        new_link_nodes = []
        for i in range(len(connected_element_nodes)):
            if link_nodes_removed[i] == 0:
                new_link_nodes.append(connected_element_nodes[i])
                # # update limit_range
                # for j in range(len(updated_limit_range)):
                #     if connected_element_nodes_limit_range[i][j][0] < updated_limit_range[j][0]:
                #         updated_limit_range[j][0] = connected_element_nodes_limit_range[i][j][0] 
                #     if connected_element_nodes_limit_range[i][j][1] > updated_limit_range[j][1]:
                #         updated_limit_range[j][1] = connected_element_nodes_limit_range[i][j][1] 

        filtering_node.link_XML[connected_element_name] = new_link_nodes

def check_lower_level(filtering_node, limit_range, all_elements_name):
    # print("Checking lower level")
    for connected_element_name in filtering_node.link_XML.keys():
        # print("Value filtering on: ", connected_element_name)
        connected_element_nodes = filtering_node.link_XML[connected_element_name]              # list of nodes in the connected element to be checked

        link_nodes_removed = np.zeros(len(connected_element_nodes))                            # array to store if a node in connected_element_nodes should be removed

        has_one_satisfied_node = False

        combined_range = {}
        for element in all_elements_name:
            combined_range[element] = []

        for i in range(len(connected_element_nodes)):
            if not connected_element_nodes[i].value_filtering_visited:
                value_filtering(connected_element_nodes[i], all_elements_name)

            if filtering_node.filtered:
                link_nodes_removed[i] = 1
                continue

            link_SQL_range = connected_element_nodes[i].link_SQL_range
            for element in all_elements_name:
                if link_SQL_range[element]:
                    if not combined_range[element]:
                        combined_range[element] = link_SQL_range[element]
                    else:
                        if (link_SQL_range[element][0] < combined_range[element][0]):
                            combined_range[element][0] = link_SQL_range[element][0]
                        if (link_SQL_range[element][1] < combined_range[element][1]):
                            combined_range[element][1] = link_SQL_range[element][1]

            has_one_satisfied_node = True

        if not has_one_satisfied_node: 
            filtering_node.filtered = True
            return

        # Update link_XML
        new_link_nodes = []
        for i in range(len(connected_element_nodes)):
            if link_nodes_removed[i] == 0:
                new_link_nodes.append(connected_element_nodes[i])
        filtering_node.link_XML[connected_element_name] = new_link_nodes

        # Update limit range
        for element in all_elements_name:
            if combined_range[element]:
                if (limit_range[element][0] < combined_range[element][0]):
                    limit_range[element][0] = combined_range[element][0]
                if (limit_range[element][1] > combined_range[element][1]):
                    limit_range[element][1] = combined_range[element][1]


    for connected_element_name in filtering_node.link_XML.keys():
        # print("Actually going deeper: ", connected_element_name)
        connected_element_nodes = filtering_node.link_XML[connected_element_name]              # list of nodes in the connected element to be checked

        link_nodes_removed = np.zeros(len(connected_element_nodes))                            # array to store if a node in connected_element_nodes should be removed

        has_one_satisfied_node = False

        combined_range = {}
        for element in all_elements_name:
            combined_range[element] = []

        for i in range(len(connected_element_nodes)):
            children_limit_range = full_filtering(connected_element_nodes[i], all_elements_name, limit_range)
            # print("Children limit range")
            # print(children_limit_range)

            if (connected_element_nodes[i].filtered) or (not children_limit_range):
                link_nodes_removed[i] = 1

            else: 
                has_one_satisfied_node = True

                for element in all_elements_name:
                    if not combined_range[element]:
                        combined_range[element] = children_limit_range[element]
                    else: 
                        if (children_limit_range[element][0] < combined_range[element][0]):
                            combined_range[element][0] = children_limit_range[element][0]
                        if (children_limit_range[element][1] < combined_range[element][1]):
                            combined_range[element][1] = children_limit_range[element][1]

        if not has_one_satisfied_node:
            filtering_node.filtered = True
            return

        # Update link_XML
        new_link_nodes = []
        for i in range(len(connected_element_nodes)):
            if link_nodes_removed[i] == 0:
                new_link_nodes.append(connected_element_nodes[i])
        filtering_node.link_XML[connected_element_name] = new_link_nodes

        # Update limit range
        for element in all_elements_name:
            if combined_range[element]:
                if (limit_range[element][0] < combined_range[element][0]):
                    limit_range[element][0] = combined_range[element][0]
                if (limit_range[element][1] > combined_range[element][1]):
                    limit_range[element][1] = combined_range[element][1]

    return limit_range

    

def initialize_children_link(filtering_node, all_elements_name, limit_range):
    """Summary
    This function intialize children link_XML and link_SQL of a filtered node
    Args:
        filtering_node (RTree_XML Node)
    """

    # print('^^^^^^^^^^^^^^^^^^^^')
    # print('initialize_children_link', filtering_node.name, filtering_node.boundary)
    link_XML = {}
    link_SQL = {}
    for connected_element_name in filtering_node.link_XML.keys():
        # print("connected_element_name ", connected_element_name)
        link_XML[connected_element_name] = []
        for connected_element_node in filtering_node.link_XML[connected_element_name]:
            # print("connected_element_node ", connected_element_node.boundary)
            if len(connected_element_node.children) == 0:
                link_XML[connected_element_name].append(connected_element_node)
            else:
                for connected_element_node_child in connected_element_node.children:
                    if ((connected_element_node_child.boundary[1][1] < limit_range[connected_element_name][0]) 
                    or (connected_element_node_child.boundary[1][0] > limit_range[connected_element_name][1])):
                        # print("ANOTHER VAMPIRE")
                        continue
                    else:
                        link_XML[connected_element_name].append(connected_element_node_child)

    
    for table_name in filtering_node.link_SQL.keys():
        link_SQL[table_name] = []
        for table_node in filtering_node.link_SQL[table_name]:
            if len(table_node.children) == 0:
                link_SQL[table_name].append(table_node)
            else:
                for table_node_child in table_node.children:
                    link_SQL[table_name].append(table_node_child)

    # for connected_element_name in filtering_node.link_XML.keys():
    #     for node in link_XML[connected_element_name]:
    #         print(node.boundary)

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

def full_filtering(filtering_node, all_elements_name, limit_range):
    """Summary
    Perform filtering of XML query branch starting from a root node:
        1. Perform value filtering starting from root node, update the range of connected element in linked tables


    For each node:
        1. Perform value filtering, update range for nodes 
        2. Filtering itself and its connected element
    Args:
        filtering_node (RTree_XML Node)
    """
    
    print('##########################')
    print('Full Filtering ', filtering_node.name, filtering_node.boundary)
    try:
        parent_boundary = filtering_node.parent.boundary
    except AttributeError as e:
        parent_boundary = "Root"
    print("Parent:", parent_boundary)
    print('limit_range', limit_range)

    # Go through limit range and check if this node satisfy the limit range
    print("############")
    print("Going through limit range")
    filtering_node_limit_range = limit_range[filtering_node.name]
    if (filtering_node.boundary[1][1] < filtering_node_limit_range[0]) or (filtering_node.boundary[1][0] > filtering_node_limit_range[1]):
        print('Filtered by limit range')
        # filtering_node.filtered = True
        return []
    print("Limit range requirement success")
    print("############")
    # If not value filtered before => Perform value filtering by checking all connected table and return limit ranges based on these tables
    if not filtering_node.value_filtering_visited:
        value_filtering(filtering_node, all_elements_name)
    print("After value filtering ", filtering_node.name, filtering_node.boundary, filtering_node.filtered)
    print("############")

    if filtering_node.filtered:
        return []
    
    # If not filtered after value filtering => update limit range
    link_SQL_range = filtering_node.link_SQL_range
    for element in all_elements_name:
        if link_SQL_range[element]:
            if (limit_range[element][0] < link_SQL_range[element][0]):
                limit_range[element][0] = link_SQL_range[element][0]
            if (limit_range[element][1] > link_SQL_range[element][1]):
                limit_range[element][1] = link_SQL_range[element][1]

    print("Limit range updated based on value filtering")
    print(limit_range)
    


    print("############")
    print("Checking connected elements")
    connected_element_filtering(filtering_node, limit_range, all_elements_name)
    print("After connected element filtering ", filtering_node.name, filtering_node.boundary, filtering_node.filtered)
    print("############")

    if filtering_node.filtered:
        return[]

    print("############")
    print("Check lower level")
    limit_range =  check_lower_level(filtering_node, limit_range, all_elements_name)
    print("After checking lower level ", filtering_node.name, filtering_node.boundary, filtering_node.filtered)
    print("############")

    if filtering_node.filtered:
        return[]


    if not filtering_node.filtered:
        # Update children link
        initialize_children_link(filtering_node, all_elements_name, limit_range)

    return limit_range


def entries_value_validation(validating_node, all_elements_name):
    print('---------')
    print("entries_value_validation ", validating_node.name, validating_node.boundary)
    validating_node.value_validation = True

    validating_node_entries = validating_node.get_entries()
    validating_node_entries_removed = np.zeros(len(validating_node_entries))

    
    print("Number of entries in this node: ", len(validating_node_entries))
    for entry in validating_node_entries:
        print(entry.coordinates)

    table_names = list(validating_node.link_SQL.keys())

    table_entries = []
    table_elements = []
    table_indexes = []

    for i in range(len(table_names)):
        print('\t', "Table:", table_names[i])
        start_get_table_entries = timeit.default_timer()

        table_name = table_names[i]
        entries = []

        for table_node in validating_node.link_SQL[table_name]:
            print('\t' * 2, 'Table Node:', table_node.boundary)
            for entry in table_node.get_entries():
                entries.append(entry)
        table_entries.append(entries)
        end_get_table_entries = timeit.default_timer()

        print("\t", "Got:", len(entries), "entries in: ", end_get_table_entries - start_get_table_entries)

        for entry in table_entries[i]:
            print("\t", entry.coordinates)

        table_elements.append(table_name.split('_'))
        table_indexes.append(0)

    print('Going through each entry of validating_entries')

    possible_combinations = []

    for i in range(len(validating_node_entries)):
        validating_entry = validating_node_entries[i]

        print('\t', "Entry: ", validating_entry.coordinates)

        this_entry_combinations = []

        for j in range(len(table_names)):

            print('\t' * 2, 'Going thru table:', table_names[j])
            print('\t' * 2, 'Initial this_entry_combinations:')
            for combination in this_entry_combinations:
                print('\t' * 3, combination)

            this_table_combinations = []

            dimension = table_elements[j].index(validating_node.name)

            # Increasing the index of this table until it is = or > current checking entry coordinate of validating_node.name (since entries is sorted)
            while ((table_indexes[j] < len(table_entries[j]) - 1) and (table_entries[j][table_indexes[j]].coordinates[dimension] < validating_entry.coordinates[1])):
                table_indexes[j] += 1
            if table_entries[j][table_indexes[j]].coordinates[dimension] != validating_entry.coordinates[1]:
                # Filter if no matching entry between table and validating entries
                validating_node_entries_removed[i] = 1
                break
            else: 
                # Create combinations as long as there are matching entry
                while (table_indexes[j] < len(table_entries[j]) - 1) and (table_entries[j][table_indexes[j]].coordinates[dimension] == validating_entry.coordinates[1]):
                    combination = [None] * len(all_elements_name)

                    for element in table_elements[j]:
                        combination[all_elements_name.index(element)] = table_entries[j][table_indexes[j]].coordinates[table_elements[j].index(element)]
                    this_table_combinations.append(combination)

                    table_indexes[j] += 1

            print('\t' * 2, 'this_table_combinations')
            for combination in this_table_combinations:
                print('\t' * 3, combination)

            # Either appends to possible combinations (if empty) or check match and upate
            if not this_entry_combinations:
                for combination in this_table_combinations:
                    this_entry_combinations.append(combination)
            else:
                this_entry_combinations_removed = np.zeros(len(this_entry_combinations))

                for k in range(len(this_entry_combinations)):
                    has_one_match_combination = False
                    merge_combination = this_entry_combinations[k][:]

                    for this_table_combination in this_table_combinations:
                        this_pair_of_combination_match = True
                        for element_index in range(len(all_elements_name)):
                            if ((this_table_combination[element_index] is not None) and 
                                (merge_combination[element_index] is not None) and 
                                (this_table_combination[element_index] != merge_combination[element_index])):
                                this_pair_of_combination_match = False
                                break
                            if (merge_combination[element_index] is None):
                                merge_combination[element_index] = this_table_combination[element_index]

                        if this_pair_of_combination_match:
                            this_entry_combinations[k] = merge_combination
                            has_one_match_combination = True
                    if not has_one_match_combination:
                        this_entry_combinations_removed[k] = 1

                # if all combinations in this_entry_combinations is removed => no match between 2 tables => remove this entry
                new_entry_combinations = []
                for k in range(len(this_entry_combinations)):
                    if this_entry_combinations_removed[k] == 0:
                        new_entry_combinations.append(this_entry_combinations[k])

                if not new_entry_combinations:
                    validating_node_entries_removed[i] = 1
                    break

                this_entry_combinations = new_entry_combinations

            print('\t' * 2, 'Updated this_entry_combinations')
            for combination in this_entry_combinations:
                print('\t' * 3, combination)
            print('\t' * 2 + '#####')

        for combination in this_entry_combinations:
            possible_combinations.append(combination)

    for combination in possible_combinations:
        print(combination)





def entries_value_validation_2(validating_node):
    """Summary
    This function checks entries in this validating node and match with entries in linked table node. Matched entries of validating_node entries are stored in link SQL
    If no match found for any validating_node_entry -> filter this validating_node
    Args:
        validating_node (RTree_XML Node)
    
    """


    # print("##################################")
    print('---------')
    print("entries_value_validation ", validating_node.name, validating_node.boundary)

    validating_node_entries = validating_node.validated_entries
    validating_node_entries_removed = np.zeros(len(validating_node_entries))

    print("Number of entries in this node: ", len(validating_node_entries))

    # print("Entries: ")
    # for entry in validating_node_entries:
        # print(entry.coordinates)

    for table_name in validating_node.link_SQL.keys():
        # get all entries contained in linked table nodes

        # print("Checking table ", table_name)

        start_get_table_entries = timeit.default_timer()

        table_entries = []
        for table_node in validating_node.link_SQL[table_name]:
            for entry in table_node.get_entries():
                table_entries.append(entry)

        end_get_table_entries = timeit.default_timer()
        print("\t", "Got:", len(table_entries), "table", table_name ,"entries in: ", end_get_table_entries - start_get_table_entries)

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
        print("\t", "Remaining table entries:", len(validating_node_entries_removed) - sum(validating_node_entries_removed))

        if not has_one_matched_entry:
            validating_node.filtered = True
            print("Filtered")
            return

    # Update entries
    validating_node.validated_entries = []
    for i in range(len(validating_node_entries)):
        if validating_node_entries_removed[i] == 0:
            validating_node.validated_entries.append(validating_node_entries[i])
    print("Remaining entries in this node: ", len(validating_node.validated_entries))
    print('---------')

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
    print("entries_connected_element_validation ", validating_node.name, validating_node.boundary)
    print("Number of entries", len(validating_node.validated_entries))

    validating_node_entries = validating_node.validated_entries
    validating_node_entries_removed = np.zeros(len(validating_node_entries))

    for connected_element_name in validating_node.link_XML.keys():
        print("        ####    ")
        print("        Checking connected element ", connected_element_name, "for ", validating_node.name)

        # relationship between validating_node and connected_element_node
        relationship = relationship_matrix[all_elements_name.index(validating_node.name), all_elements_name.index(connected_element_name)]
        # print("relationship ", relationship)

        start_connected_element_validation = timeit.default_timer()
        connected_element_nodes = validating_node.link_XML[connected_element_name]

        print("        Number of connected_element_node:", len(connected_element_nodes))
        # for connected_element_node in connected_element_nodes:
            # print(connected_element_node.boundary, sep='\t')

        for connected_element_node in connected_element_nodes:
            # Do validation if not been validated
            if (not connected_element_node.filtered) and (not connected_element_node.validation_visited):
                node_validation(connected_element_node, all_elements_name, relationship_matrix)
        end_connected_element_validation = timeit.default_timer()
        print("        Do validation on children" , connected_element_name,"took: ", end_connected_element_validation - start_connected_element_validation)

        # get all entries in unfiltered connected_element_nodes
        connected_element_entries = []
        for connected_element_node in connected_element_nodes:
            if not connected_element_node.filtered:
                for entry in connected_element_node.validated_entries:
                    connected_element_entries.append(entry)

        print("        Parent entries: ", len(validating_node_entries))
        print("        Children entries: ", len(connected_element_entries))

        # print("## remaining entries: ")
        # for entry in connected_element_entries:
            # print(entry.coordinates)

        has_one_matched_entry = False

        # print("## Structure check")

        start_actual_validation = timeit.default_timer()

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

        end_actual_validation = timeit.default_timer()
        print("        Actual validation took: ", end_actual_validation - start_actual_validation)

        print("        Parent remaining entries: ", len(validating_node_entries))
        print("        Children remaining entries: ", len(connected_element_entries))

        if not has_one_matched_entry:
            validating_node.filtered = True
            print("Filtered")
            return
                    
    # Update entries
    validating_node.validated_entries = []
    for i in range(len(validating_node_entries)):
        if validating_node_entries_removed[i] == 0:
            validating_node.validated_entries.append(validating_node_entries[i])
    print("Remaining entries", validating_node.name, validating_node.boundary, len(validating_node.validated_entries))


def node_validation(validating_node, all_elements_name, relationship_matrix):
    """Summary
    Performs validation for a node
    Args:
        validating_node (RTree XML Node)
        all_elements_name (list of String): list of all elements' name in XML_query ordered by level order
        relationship_matrix ([][]): 2 dimension array store relationship between element. Relation[parent, children] = 1, relation[ancestor, descendant] = 2, other = 0
    """
    # print("$$$$$$$$$$$$$$$$")
    # print("node_validation of ", validating_node.name, validating_node.boundary)
    # for connected_element_name in validating_node.link_XML.keys():
    #     print("link_XML:", connected_element_name)
    #     for node in validating_node.link_XML[connected_element_name]:
    #         print(node.boundary)
    # print("$$$$$$$$$$$$$$$$")


    # Intialization
    validating_node.validation_visited = True
    validating_node.validated_entries = validating_node.get_entries()

    start_node_value_validation = timeit.default_timer()
    
    if not validating_node.filtered:
        entries_value_validation(validating_node)

    end_node_value_validation = timeit.default_timer()
    print("Node value validation of ", validating_node.name, validating_node.boundary, "took: ", end_node_value_validation - start_node_value_validation)
    print("After node value validation", validating_node.name, validating_node.boundary, validating_node.filtered)
    print("---------")
    start_node_structure_validation = timeit.default_timer()

    if not validating_node.filtered:
        entries_connected_element_validation(validating_node, all_elements_name, relationship_matrix)

    end_node_structure_validation = timeit.default_timer()
    print("Node structure validation of ", validating_node.name, validating_node.boundary, "took: ", end_node_structure_validation - start_node_structure_validation)
    print("After node structure validation", validating_node.name, validating_node.boundary, validating_node.filtered)




def validation_XML_SQL(all_elements_name, relationship_matrix, XML_query_root_node):
    """Summary
    Perform validation after filtering
    Args
        all_elements_name (list of String): list of all elements' name in XML_query ordered by level order
        relationship_matrix ([][]): 2 dimension array store relationship between element. Relation[parent, children] = 1, relation[ancestor, descendant] = 2, other = 0
        all_elements_root (Node): RTree_XML root Node of XML_query root
    """

    XML_query_root_leaf_nodes = XML_query_root_node.get_leaf_node_not_filtered()
    print("#######################")
    print("Number of remaining leaf nodes of root after Filtering:", len(XML_query_root_leaf_nodes))
    print("#######################")
    for node in XML_query_root_leaf_nodes:
        # print(node.boundary)
        print("#######################")
        # node_validation(node, all_elements_name, relationship_matrix)
        print(node.boundary)
        # entries_value_validation(node, all_elements_name)
        print("#######################")

    print("#######################")
    print("Number of remaining leaf nodes of root after validation:", len(XML_query_root_node.get_leaf_node_not_filtered()))
    print("#######################")

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



def join_XML_SQL(all_elements_name, all_elements_root, all_tables_root, relationship_matrix, max_n_children):
    """Summary
    This function join XML DB and SQL DB given a XML query
    Args:
        folder_name (String): folder contain data files
        all_elements_name (list of String): list of all elements' name in XML_query ordered by level order
        relationship_matrix ([][]): 2 dimension array store relationship between element. Relation[parent, children] = 1, relation[ancestor, descendant] = 2, other = 0
        max_n_children (int): maximum of number of children in RTree Node
    """
    # Loading XML and SQL database into R_Tree
    # start_loading = timeit.default_timer()
    # all_elements_root = load_elements(folder_name, all_elements_name, max_n_children)
    # all_tables_root = load_tables(folder_name, all_elements_name, max_n_children)
    # end_loading = timeit.default_timer()
    # print('loading time', end_loading - start_loading)
    # print("######################################")

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
    start_initializing_link = timeit.default_timer()

    limit_range = {}

    for i in range(len(all_elements_name)):
        element_name = all_elements_name[i]
        element_root = all_elements_root[element_name]
        limit_range[element_name] = element_root.boundary[1]
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

    end_initializing_link = timeit.default_timer()
    print("Initialize link took: ",end_initializing_link - start_initializing_link)
    print("######################################")

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
    start_filtering = timeit.default_timer()

    XML_query_root_element = all_elements_name[0]
    queue_XML_query_root = queue.Queue()
    queue_XML_query_root.put(all_elements_root[XML_query_root_element])
    queue_limit_range = queue.Queue()
    queue_limit_range.put(limit_range)

    n_root_node_full_filtered = 0

    while not queue_XML_query_root.empty():
        XML_query_root_node = queue_XML_query_root.get()
        limit_range = queue_limit_range.get()
        print('Limit range from queue', limit_range)

        start_one_node_filtering = timeit.default_timer()

        updated_limit_range = full_filtering(XML_query_root_node, all_elements_name, limit_range)

        end_one_node_filtering = timeit.default_timer()

        n_root_node_full_filtered += 1
        # print("Updated limit range", updated_limit_range)
        print("One root node filter time:", end_one_node_filtering - start_one_node_filtering)
        print("######")
        print("link_SQL of ", XML_query_root_node.boundary)
        for connected_table_name in XML_query_root_node.link_SQL.keys():
            print('\t', connected_table_name)
            for connected_table_node in XML_query_root_node.link_SQL[connected_table_name]:
                print('\t' * 2, connected_table_node.boundary)


        print('\n' * 3)

        if (not XML_query_root_node.filtered) and (updated_limit_range):
            for XML_query_root_node_child in XML_query_root_node.children:
                queue_XML_query_root.put(XML_query_root_node_child)
                dict_copy = copy.deepcopy(updated_limit_range)
                queue_limit_range.put(dict_copy)

    end_filtering = timeit.default_timer()
    print('Filtering time', end_filtering - start_filtering)
    print('Average filtering time for one node:', (end_filtering - start_filtering)/n_root_node_full_filtered)
    print("######################################")
    
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



def main():
    if len(sys.argv) < 3:
        raise ValueError('Missing arguments. Requires 2 arguments in the following order: folder_name, max_n_children')
    folder_name = sys.argv[1]
    max_n_children = int(sys.argv[2])
    sys.stdout = open("../io/" + folder_name + "_max_children_" + str(max_n_children) + ".txt", 'w')
    loader = Loader(folder_name, max_n_children)
    print("Loading time: ", loader.time)
    join_XML_SQL(loader.all_elements_name, loader.all_elements_root, loader.all_tables_root, loader.relationship_matrix, loader.max_n_children)




def simple_small(max_n_children):
    folder_name = 'simple_small'
    sys.stdout = open("../io/" + folder_name + "_max_children_" + str(max_n_children) + ".txt", 'w')
    all_elements_name = ['A', 'B', 'C', 'D']
    relationship_matrix = np.zeros((4, 4))
    relationship_matrix[0, 1] = 2
    relationship_matrix[0, 2] = 2
    relationship_matrix[2, 3] = 1
    
    join_XML_SQL(folder_name, all_elements_name, relationship_matrix, max_n_children)


def invoice_simple(max_n_children):
    folder_name = 'invoice_simple'
    sys.stdout = open("../io/" + folder_name + "_max_children_" + str(max_n_children) + ".txt", 'w')

    all_elements_name = ['Orderline', 'price', 'asin']
    relationship_matrix = np.zeros((3, 3))
    relationship_matrix[0, 1] = 1
    relationship_matrix[0, 2] = 1
    max_n_children = 50
    join_XML_SQL(folder_name, all_elements_name, relationship_matrix, max_n_children)

def invoice_complex(max_n_children):
    folder_name = 'invoice_complex'
    sys.stdout = open("../io/" + folder_name + "_max_children_" + str(max_n_children) + ".txt", 'w')

    all_elements_name = ['Invoice', 'OrderId','Orderline', 'price', 'asin']
    relationship_matrix = np.zeros((5, 5))
    relationship_matrix[0, 1] = 1
    relationship_matrix[0, 2] = 1
    relationship_matrix[2, 3] = 1
    relationship_matrix[2, 4] = 1
    max_n_children = 200
    join_XML_SQL(folder_name, all_elements_name, relationship_matrix, max_n_children)




# output_file_path = 'output_testcase1_max_children_200'
# sys.stdout = open(output_file_path, 'w')

# simple_small(2)
# invoice_complex(500)

# sys.stdout = open("../io/" + "simple_small" + "_max_children_2" + "_try_2" + ".txt", 'w')
main()








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
