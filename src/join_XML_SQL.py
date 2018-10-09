import sys
import copy

from .Node import Node
from .Result import Result
from .Loader import Loader, get_index_highest_element
from .DeweyID import *
from .Filterer import full_filtering

import timeit
import numpy as np
import logging
import queue

logger = logging.getLogger("Main")
logger.disabled = False


# def ancestor_descendant_filtering(node1, node2):
#     """Summary
#     This function check if a node D_XML is descendant of node A_XML
#     Args:
#         node1 (Node): R_Tree_XML Node to be checked if is ancestor
#         node2 (Node): R_Tree_XML Node to be checked if is descendant
#
#     Returns:
#         Bool: True if node1 cannot be ancestor of node2
#     """
#     node1_low = node1.boundary[0][0]
#     node1_high = node1.boundary[0][1]
#     node2_low = node2.boundary[0][0]
#     node2_high = node2.boundary[0][1]
#
#     # 2 cases for false:
#     #   - node1 is on the left of node2, no intersection
#     #   - node1 is on the right of node2, no intersection
#     # Edge case 1.2.3 < 1.2.3.4 but is ancestor still
#     if node2_high <= node1_low:
#         return True
#
#     if node2_low > node1_high:
#         if not is_ancestor(node1_high, node2_low):
#             return True
#
#     return False
#
#     # if node2_high < node1_low:
#     #     return True
#     # if node1_high < node2_low:
#     #     if not is_ancestor(node1_high, node2_low):
#     #         return True
#     # return False
#
#     # Range Index
#     # if ((node1_low <= node2_low) and (node1_high >= node2_high)):
#     #     return True
#     # return False


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


# def value_filtering(filtering_node: Node, all_elements_name: [str]):
#     """Summary
#     This function do value filtering by checking all connected tables' nodes in filtering_node.link_sql
#     Filter this node if exist one table that has no match for this filtering_node
#     Update filtering_node.link_sql for table nodes that is not a match
#     Update the range of linking nodes
#     Each node should be value filtered once
#
#     :param filtering_node: node to be value filtered
#     :param all_elements_name: query elements
#     :return:
#     """
#     # filtering_node_index = all_elements_name.index(filtering_node.name)
#     # print('\t' * filtering_node_index,"###")
#     # print('\t' * filtering_node_index,"Value Filtering", filtering_node.name, filtering_node.boundary)
#
#     filtering_node.value_filtering_visited = True
#
#     link_sql_range = {}
#     for element in all_elements_name:
#         if element == filtering_node.name:
#             link_sql_range[element] = filtering_node.boundary[1]
#         else:
#             link_sql_range[element] = []
#
#     # Go through each connected table
#     # print("Value filtering: Going through tables")
#     for table_name in filtering_node.link_sql.keys():
#         # print('\t' + 'table_name', table_name)
#         table_nodes = filtering_node.link_sql[table_name]  # list of nodes in a connected table to be check
#         table_dimension = table_name.split('_').index(filtering_node.name)
#         table_elements = table_name.split('_')
#
#         combined_range = {}
#         for element in all_elements_name:
#             combined_range[element] = []
#
#         # print('\t' + 'len(table_nodes) ', len(table_nodes))
#         i = 0
#
#         remaining_link_nodes = []
#
#         while ((i < len(table_nodes) - 1) and (
#                 table_nodes[i].boundary[table_dimension][1] < filtering_node.boundary[1][0])):
#             i += 1
#
#         while table_nodes[i].boundary[table_dimension][0] <= filtering_node.boundary[1][1]:
#             remaining_link_nodes.append(table_nodes[i])
#             boundary = table_nodes[i].boundary
#
#             for element in table_elements:
#                 if not combined_range[element]:
#                     combined_range[element] = list(boundary[table_elements.index(element)])
#                 else:
#                     if (boundary[table_elements.index(element)][0] < combined_range[element][0]):
#                         combined_range[element][0] = boundary[table_elements.index(element)][0]
#                     if (boundary[table_elements.index(element)][1] > combined_range[element][1]):
#                         combined_range[element][1] = boundary[table_elements.index(element)][1]
#
#             if i == len(table_nodes) - 1:
#                 break
#
#             i += 1
#
#         # If found no satisfied node -> filter current_node
#         if not remaining_link_nodes:
#             # print('\t' * filtering_node_index, 'No match in', table_name, '---> FILTERED')
#             filtering_node.filtered = True
#             return
#
#         # Update link_sql
#         filtering_node.link_sql[table_name] = remaining_link_nodes
#
#         # Update link_sql_range
#         for element in table_elements:
#             if not link_sql_range[element]:
#                 link_sql_range[element] = list(combined_range[element])
#             else:
#                 if (link_sql_range[element][0] < combined_range[element][0]):
#                     link_sql_range[element][0] = combined_range[element][0]
#                 if (link_sql_range[element][1] > combined_range[element][1]):
#                     link_sql_range[element][1] = combined_range[element][1]
#
#     filtering_node.link_sql_range = link_sql_range
#     # print('\t' * filtering_node_index,"###")


# def connected_element_filtering(filtering_node, limit_range, all_elements_name):
#     """Summary
#     This function filter this filtering_node by checking all of its connected element and do pair structure filtering and pair value filtering
#     Filter this filtering_node if there exists a connected element that has no suitable pair
#     Update this filtering_node.link_xml for each connected element if not filtered
#
#     Args:
#         filtering_node (RTree_XML Node): Description
#         limit_range (dict): passed from full_filtering
#         all_elements_name (list): list of all element names in XML query
#     """
#     # filtering_node_index = all_elements_name.index(filtering_node.name)
#     # print('\t' * filtering_node_index,"Connected element filtering")
#     # print('\t' * filtering_node_index,"limit_range", limit_range)
#     for connected_element_name in filtering_node.link_xml.keys():
#         # print('\t' * (filtering_node_index + 1), "Checking: ", connected_element_name)
#
#         connected_element_nodes = filtering_node.link_xml[
#             connected_element_name]  # list of nodes in the connected element to be checked
#         link_nodes_removed = np.zeros(
#             len(connected_element_nodes))  # array to store if a node in connected_element_nodes should be removed
#         has_one_satisfied_node = False
#         # Checking each node of this connected element
#         for i in range(len(connected_element_nodes)):
#             # print('\t' * (filtering_node_index + 2), connected_element_nodes[i].boundary)
#             if connected_element_nodes[i].filtered:
#                 # print('\t' * (filtering_node_index + 3), '=> Already filtered')
#                 link_nodes_removed[i] = 1
#                 continue
#
#             # If this filtering node cannot be ancestor of connected node
#             if ancestor_descendant_filtering(filtering_node, connected_element_nodes[i]):
#                 # print('\t' * (filtering_node_index + 3), '=> not descendant')
#                 link_nodes_removed[i] = 1
#                 continue
#
#             # Check limit range
#             if ((connected_element_nodes[i].boundary[1][0] > limit_range[connected_element_name][1]) or
#                     (connected_element_nodes[i].boundary[1][1] < limit_range[connected_element_name][0])):
#                 # print('\t' * (filtering_node_index + 3), '=> Filtered by limit_range')
#                 link_nodes_removed[i] = 1
#                 continue
#
#             # This pair is good
#             has_one_satisfied_node = True
#             # print('\t' * (filtering_node_index + 3), '=> satisfied')
#
#         if not has_one_satisfied_node:
#             # print('\t' * (filtering_node_index + 1), "No satisfied node")
#             filtering_node.filtered = True
#             return
#
#         # Update link_xml
#
#         new_link_nodes = []
#         for i in range(len(connected_element_nodes)):
#             if link_nodes_removed[i] == 0:
#                 new_link_nodes.append(connected_element_nodes[i])
#
#         filtering_node.link_xml[connected_element_name] = new_link_nodes


# def check_lower_level(filtering_node, limit_range, all_elements_name):
#     # filtering_node_index = all_elements_name.index(filtering_node.name)
#     # print('\t' * filtering_node_index,"check_lower_level", filtering_node.name, filtering_node.boundary)
#     for connected_element_name in filtering_node.link_xml.keys():
#         # print('\t' * (filtering_node_index + 1), "Check: ", connected_element_name)
#         connected_element_nodes = filtering_node.link_xml[
#             connected_element_name]  # list of nodes in the connected element to be checked
#
#         link_nodes_removed = np.zeros(
#             len(connected_element_nodes))  # array to store if a node in connected_element_nodes should be removed
#
#         has_one_satisfied_node = False
#
#         combined_range = {}
#         for element in all_elements_name:
#             combined_range[element] = []
#
#         for i in range(len(connected_element_nodes)):
#             # print('\t' * (filtering_node_index + 2), connected_element_nodes[i].boundary)
#             if not connected_element_nodes[i].value_filtering_visited:
#                 value_filtering(connected_element_nodes[i], all_elements_name)
#
#             if connected_element_nodes[i].filtered:
#                 link_nodes_removed[i] = 1
#                 # print('\t' * (filtering_node_index + 3), '=> Filtered after value filtering')
#                 continue
#
#             link_sql_range = connected_element_nodes[i].link_sql_range
#             # print('\t' * (filtering_node_index + 2),'link_sql_range', link_sql_range)
#             for element in all_elements_name:
#                 if link_sql_range[element]:
#                     if not combined_range[element]:
#                         combined_range[element] = list(link_sql_range[element])
#                     else:
#                         if (link_sql_range[element][0] < combined_range[element][0]):
#                             combined_range[element][0] = link_sql_range[element][0]
#                         if (link_sql_range[element][1] > combined_range[element][1]):
#                             combined_range[element][1] = link_sql_range[element][1]
#
#             has_one_satisfied_node = True
#
#             # print('\t' * (filtering_node_index + 2), 'Updated combined_range', combined_range)
#
#         if not has_one_satisfied_node:
#             filtering_node.filtered = True
#             return
#
#         # Update link_xml
#         new_link_nodes = []
#         for i in range(len(connected_element_nodes)):
#             if link_nodes_removed[i] == 0:
#                 new_link_nodes.append(connected_element_nodes[i])
#         filtering_node.link_xml[connected_element_name] = new_link_nodes
#
#         # Update limit range of this connected element
#         # print('\t' * (filtering_node_index + 1), 'Limit range', limit_range)
#
#         for element in all_elements_name:
#             if combined_range[element]:
#                 if (limit_range[element][0] < combined_range[element][0]):
#                     limit_range[element][0] = combined_range[element][0]
#                 if (limit_range[element][1] > combined_range[element][1]):
#                     limit_range[element][1] = combined_range[element][1]
#         # print('\t' * (filtering_node_index + 1), 'Updated limit_range', limit_range)
#
#     # print('\t' * (filtering_node_index), '----------')
#     # print('\t'* (filtering_node_index), 'Go further')
#
#     for connected_element_name in filtering_node.link_xml.keys():
#         # print('\t' * (filtering_node_index + 1), connected_element_name)
#         connected_element_nodes = filtering_node.link_xml[
#             connected_element_name]  # list of nodes in the connected element to be checked
#
#         link_nodes_removed = np.zeros(
#             len(connected_element_nodes))  # array to store if a node in connected_element_nodes should be removed
#
#         has_one_satisfied_node = False
#
#         combined_range = {}
#         for element in all_elements_name:
#             combined_range[element] = []
#
#         for i in range(len(connected_element_nodes)):
#             # print('\t' * (filtering_node_index + 2), connected_element_nodes[i].boundary)
#             children_limit_range = copy.deepcopy(limit_range)
#             children_limit_range = full_filtering(connected_element_nodes[i], all_elements_name, children_limit_range)
#             # print('\t' * (filtering_node_index + 2), 'Got limit_range', children_limit_range)
#             # print(children_limit_range)
#
#             if connected_element_nodes[i].filtered:
#                 # print('\t' * (filtering_node_index + 3), '->> Filtered')
#                 link_nodes_removed[i] = 1
#
#             else:
#                 has_one_satisfied_node = True
#
#                 for element in all_elements_name:
#                     if not combined_range[element]:
#                         combined_range[element] = list(children_limit_range[element])
#                     else:
#                         if (children_limit_range[element][0] < combined_range[element][0]):
#                             combined_range[element][0] = children_limit_range[element][0]
#                         if (children_limit_range[element][1] > combined_range[element][1]):
#                             combined_range[element][1] = children_limit_range[element][1]
#
#         if not has_one_satisfied_node:
#             # print('\t' * (filtering_node_index), 'No children node in ', connected_element_name, '->>> Filtered')
#             filtering_node.filtered = True
#             return
#
#         # Update link_xml
#         new_link_nodes = []
#         for i in range(len(connected_element_nodes)):
#             if link_nodes_removed[i] == 0:
#                 new_link_nodes.append(connected_element_nodes[i])
#         filtering_node.link_xml[connected_element_name] = new_link_nodes
#
#         # Update limit range
#         for element in all_elements_name:
#             if combined_range[element]:
#                 if (limit_range[element][0] < combined_range[element][0]):
#                     limit_range[element][0] = combined_range[element][0]
#                 if (limit_range[element][1] > combined_range[element][1]):
#                     limit_range[element][1] = combined_range[element][1]
#
#     return limit_range


# def initialize_children_link(filtering_node, all_elements_name, limit_range):
#     """Summary
#     This function intialize children link_xml and link_sql of a filtered node
#     Args:
#         filtering_node (RTree_XML Node)
#     """
#     # filtering_node_index = all_elements_name.index(filtering_node.name)
#     # print('\t' * filtering_node_index, '^^^^^^^^^^^^^^^^^^^^')
#     # print('\t' * filtering_node_index, 'initialize_children_link', filtering_node.name, filtering_node.boundary)
#     # print('\t' * filtering_node_index, 'limit_range', limit_range)
#     link_xml = {}
#     link_sql = {}
#     for connected_element_name in filtering_node.link_xml.keys():
#         # print("connected_element_name ", connected_element_name)
#         link_xml[connected_element_name] = []
#         for connected_element_node in filtering_node.link_xml[connected_element_name]:
#             # print("connected_element_node ", connected_element_node.boundary)
#             if len(connected_element_node.children) == 0:
#                 link_xml[connected_element_name].append(connected_element_node)
#             else:
#                 for connected_element_node_child in connected_element_node.children:
#                     if ((connected_element_node_child.boundary[1][1] < limit_range[connected_element_name][0])
#                             or (connected_element_node_child.boundary[1][0] > limit_range[connected_element_name][1])):
#                         # print("ANOTHER VAMPIRE")
#                         continue
#                     else:
#                         link_xml[connected_element_name].append(connected_element_node_child)
#
#     for table_name in filtering_node.link_sql.keys():
#         link_sql[table_name] = []
#         for table_node in filtering_node.link_sql[table_name]:
#             if len(table_node.children) == 0:
#                 link_sql[table_name].append(table_node)
#             else:
#                 for table_node_child in table_node.children:
#                     link_sql[table_name].append(table_node_child)
#
#     # print('\t' * filtering_node_index, 'link_xml')
#     # for key in link_xml.keys():
#     # print('\t' * filtering_node_index, key)
#     # for node in link_xml[key]:
#     # print('\t' * (filtering_node_index + 1), node.boundary, end = ", ")
#     # print()
#     # print('\t' * filtering_node_index, 'link_sql')
#     # for key in link_sql.keys():
#     # print('\t' * filtering_node_index, key)
#     # for node in link_sql[key]:
#     # print('\t' * (filtering_node_index + 1), node.boundary, end = ", ")
#     # print()
#
#     for filtering_node_child in filtering_node.children:
#         filtering_node_child.link_xml = link_xml.copy()
#         filtering_node_child.link_sql = link_sql.copy()
#
#     # print('Children link updated')
#     # for filtering_node_child in filtering_node.children:
#     #     print('\t', 'child', filtering_node_child.boundary)
#     #     for connected_element_name in filtering_node_child.link_xml.keys():
#     #         print('\t' * 2, 'connected_element_name', connected_element_name, end = ": ")
#     #         for connected_element_node in filtering_node_child.link_xml[connected_element_name]:
#     #             print(connected_element_node.boundary, end = ", ")
#     #         print()
#     #     print()
#
#     #     for table_name in filtering_node_child.link_sql.keys():
#     #         print('\t' * 2, 'table_name', table_name, end = ": ")
#     #         for table_node in filtering_node_child.link_sql[table_name]:
#     #             print(table_node.boundary, end = ", ")
#     #     print()
#     # print('\t' * filtering_node_index, '^^^^^^^^^^^^^^^^^^^^')


# def full_filtering(filtering_node, all_elements_name, limit_range):
#     """Summary
#     Perform filtering of XML query branch starting from a root node:
#         1. Perform value filtering starting from root node, update the range of connected element in linked tables
#
#     For each node:
#         1. Perform value filtering, update range for nodes
#         2. Filtering itself and its connected element
#     Args:
#         filtering_node (RTree_XML Node)
#     """
#     # filtering_node_index = all_elements_name.index(filtering_node.name)
#     # print('\t' * filtering_node_index,'--- Begin Full Filtering ---')
#     # print('\t' * filtering_node_index, 'Full Filtering ', filtering_node.name, filtering_node.boundary)
#     # if filtering_node.parent is None:
#     #   parent = 'Root'
#     # else:
#     # parent = filtering_node.parent.boundary
#     # print('\t' * (filtering_node_index + 1), 'Parent', parent)
#
#     # print('\t' * filtering_node_index, "Limit range")
#     # print('\t' * (filtering_node_index + 1), limit_range)
#
#     # Go through limit range and check if this node satisfy the limit range
#     # print("############")
#     # print("Going through limit range")
#     filtering_node_limit_range = limit_range[filtering_node.name]
#     if (filtering_node.boundary[1][1] < filtering_node_limit_range[0]) or (
#             filtering_node.boundary[1][0] > filtering_node_limit_range[1]):
#         # print('\t' * (filtering_node_index + 1), '=>>>', filtering_node.boundary,'Filtered by limit range')
#         filtering_node.filtered = True
#         return []
#
#     # Change limit range to intersection of this node range
#     if limit_range[filtering_node.name][0] < filtering_node.boundary[1][0]:
#         limit_range[filtering_node.name][0] = filtering_node.boundary[1][0]
#         # print('\t' * filtering_node_index, "Limit range updated based on filtering_node")
#         # print('\t' * (filtering_node_index + 1), limit_range)
#     if limit_range[filtering_node.name][1] > filtering_node.boundary[1][1]:
#         limit_range[filtering_node.name][1] = filtering_node.boundary[1][1]
#         # print('\t' * filtering_node_index, "Limit range updated based on filtering_node")
#         # print('\t' * (filtering_node_index + 1), limit_range)
#
#     # If not value filtered before => Perform value filtering by checking all connected table and return limit ranges based on these tables
#     if not filtering_node.value_filtering_visited:
#         value_filtering(filtering_node, all_elements_name)
#
#     if filtering_node.filtered:
#         # print('\t' * (filtering_node_index + 1), '=>>>', filtering_node.boundary, 'Filtered by value filtering')
#         return []
#
#     # If not filtered after value filtering => update limit range
#     link_sql_range = filtering_node.link_sql_range
#     for element in all_elements_name:
#         if link_sql_range[element]:
#             if (limit_range[element][0] < link_sql_range[element][0]):
#                 limit_range[element][0] = link_sql_range[element][0]
#             if (limit_range[element][1] > link_sql_range[element][1]):
#                 limit_range[element][1] = link_sql_range[element][1]
#
#     # print('\t' * filtering_node_index, "Limit range updated based on value filtering")
#     # print('\t' * (filtering_node_index + 1), limit_range)
#
#     # print("############")
#     # print("Checking connected elements")
#     connected_element_filtering(filtering_node, limit_range, all_elements_name)
#     # print("After connected element filtering ", filtering_node.name, filtering_node.boundary, filtering_node.filtered)
#     # print("############")
#
#     if filtering_node.filtered:
#         # print('\t' * (filtering_node_index + 1), '=>>>', filtering_node.boundary, 'Filtered by connected_element_filtering')
#         return []
#
#     # print("############")
#     # print("Check lower level")
#     limit_range = check_lower_level(filtering_node, limit_range, all_elements_name)
#     # print("After checking lower level ", filtering_node.name, filtering_node.boundary, filtering_node.filtered)
#     # print("############")
#
#     if filtering_node.filtered:
#         # print('\t' * (filtering_node_index + 1), '=>>>', filtering_node.boundary, 'Filtered by connected_element_filtering')
#         return []
#
#     if not filtering_node.filtered:
#         # Update children link
#         initialize_children_link(filtering_node, all_elements_name, limit_range)
#
#     # print('\t' * filtering_node_index,'--- End Full Filtering ---')
#     return limit_range


def entries_value_validation(validating_node, all_elements_name):
    # print('---------')
    # print("entries_value_validation ", validating_node.name, validating_node.boundary)
    validating_node.value_validation_visited = True

    validating_node_entries = validating_node.get_entries()
    validating_node_entries_removed = np.zeros(len(validating_node_entries))

    # print("Number of entries in this node: ", len(validating_node_entries))
    # for entry in validating_node_entries:
    #   print(entry.coordinates)

    table_names = list(validating_node.link_sql.keys())

    table_entries = []
    table_elements = []
    table_indexes = []

    for i in range(len(table_names)):
        # print('\t', "Table:", table_names[i])
        # start_get_table_entries = timeit.default_timer()

        table_name = table_names[i]
        entries = []

        for table_node in validating_node.link_sql[table_name]:
            # print('\t' * 2, 'Table Node:', table_node.boundary)
            for entry in table_node.get_entries():
                entries.append(entry)
        table_entries.append(entries)
        # end_get_table_entries = timeit.default_timer()

        # print("\t", "Got:", len(entries), "entries in: ", end_get_table_entries - start_get_table_entries)

        # for entry in table_entries[i]:
        #   print("\t", entry.coordinates)

        table_elements.append(table_name.split('_'))
        table_indexes.append(0)

    # print('Going through each entry of validating_entries')

    for i in range(len(validating_node_entries)):
        validating_entry = validating_node_entries[i]

        # print('\t', "Entry: ", validating_entry.coordinates)

        this_entry_combinations = []

        for j in range(len(table_names)):

            # print('\t' * 2, 'Going thru table:', table_names[j])
            # print('\t' * 2, 'Initial this_entry_combinations:')
            # for combination in this_entry_combinations:
            # print('\t' * 3, combination)

            # print('\t' * 2, 'Initial index:', table_indexes[j])

            this_table_combinations = []

            dimension = table_elements[j].index(validating_node.name)

            # Increasing the index of this table until it is = or > current checking entry coordinate of validating_node.name (since entries is sorted)
            while ((table_indexes[j] < len(table_entries[j]) - 1) and (
                    table_entries[j][table_indexes[j]].coordinates[dimension] < validating_entry.coordinates[1])):
                table_indexes[j] += 1
            if table_entries[j][table_indexes[j]].coordinates[dimension] != validating_entry.coordinates[1]:
                # Filter if no matching entry between table and validating entries
                validating_node_entries_removed[i] = 1
                # print('\t' * 2, 'No Match')
                break
            else:
                # Create combinations as long as there are matching entry
                # print('\t' * 2, 'Match with', table_entries[j][table_indexes[j]].coordinates)
                while (table_entries[j][table_indexes[j]].coordinates[dimension] == validating_entry.coordinates[1]):
                    combination = [None] * len(all_elements_name)
                    for element in table_elements[j]:
                        combination[all_elements_name.index(element)] = table_entries[j][table_indexes[j]].coordinates[
                            table_elements[j].index(element)]
                    # print('\t' * 2, 'Found a combination:', combination)
                    this_table_combinations.append(combination)

                    if table_indexes[j] == len(table_entries[j]) - 1:
                        break
                    table_indexes[j] += 1

            # print('\t' * 2, 'this_table_combinations:')
            # for combination in this_table_combinations:
            # print('\t' * 3, combination)

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

            # print('\t' * 2, 'Updated this_entry_combinations:')
            # for combination in this_entry_combinations:
            # print('\t' * 3, combination)
            # print('\t' * 2 + '#####')

        if not this_entry_combinations:
            validating_node_entries_removed[i] = 1
        else:
            validating_entry.possible_combinations = this_entry_combinations

    if np.sum(validating_node_entries_removed) == len(validating_node_entries):
        validating_node.filtered = True
        return

    remaining_entries = []

    for i in range(len(validating_node_entries)):
        if validating_node_entries_removed[i] == 0:
            remaining_entries.append(validating_node_entries[i])

    validating_node.entries = remaining_entries

    # print('Remaining Entries')
    # for entry in validating_node.entries:
    # print('\t', entry.coordinates)
    # print('\t Combinations:')
    # for combination in entry.possible_combinations:
    # print('\t' * 2, combination)

    # print('---------')


def initialize_root_leaf_node_results(root_leaf_node, all_elements_name):
    # print('------------')
    # print('initialize_root_leaf_node_results')
    entries_value_validation(root_leaf_node, all_elements_name)

    if root_leaf_node.filtered:
        return []

    remaining_entries = root_leaf_node.get_entries()
    root_leaf_node_index = all_elements_name.index(root_leaf_node.name)

    results = []

    for entry in remaining_entries:
        for combination in entry.possible_combinations:
            result = Result(len(all_elements_name))
            result.index[root_leaf_node_index] = entry.coordinates[0]
            for i in range(len(combination)):
                result.value[i] = combination[i]

            results.append(result)

    return results


def check_XML_children_validation(validating_node, possible_results, all_elements_name, relationship_matrix):
    # print('---------')
    # print('check_XML_children_validation:', validating_node.boundary)
    validating_node_index = all_elements_name.index(validating_node.name)

    for connected_element_name in validating_node.link_xml.keys():
        # print('\t', 'Checking: ', connected_element_name)

        connected_element_index = all_elements_name.index(connected_element_name)

        relationship = relationship_matrix[validating_node_index, connected_element_index]

        connected_element_nodes = validating_node.link_xml[connected_element_name]
        connected_element_nodes_removed = np.zeros(len(connected_element_nodes))

        for i in range(len(connected_element_nodes)):
            connected_element_node = connected_element_nodes[i]
            # print('\t' * 2, 'Checking: ', connected_element_name, connected_element_node.boundary)

            if not connected_element_node.value_validation_visited:
                entries_value_validation(connected_element_node, all_elements_name)

            connected_node_remaining_entries = connected_element_node.get_entries()
            # print('\t' * 2, 'Remaining entries: ')

            this_node_has_a_match_entry = False

            for entry in connected_node_remaining_entries:
                # print('\t' * 3, entry.coordinates)
                for result in possible_results:
                    # Check for value
                    if ((result.value[connected_element_index] == entry.coordinates[1]) or
                            (result.value[connected_element_index] is None)):

                        # Check for structure
                        structure_validation_satisfied = False
                        if relationship == 1:
                            if is_parent(result.index[validating_node_index], entry.coordinates[0]):
                                structure_validation_satisfied = True
                        if relationship == 2:
                            if is_ancestor(result.index[validating_node_index], entry.coordinates[0]):
                                structure_validation_satisfied = True

                        if structure_validation_satisfied:
                            # This entry is good
                            result.index[connected_element_index] = entry.coordinates[0]
                            result.value[connected_element_index] = entry.coordinates[1]
                            this_node_has_a_match_entry = True
                            # print('\t' * 4, '=>>>> MATCH')
                            # print('\t' * 4, connected_element_name, entry.coordinates)

                # print('\t' * 4, '=>>>> NO MATCH')

            if not this_node_has_a_match_entry:
                connected_element_nodes_removed[i] = 1

        if (np.sum(connected_element_nodes_removed) == len(connected_element_nodes)):
            validating_node.filtered = True
            # print('Validating node', validating_node.name, validating_node.boundary, 'FILTERED')
            return

        updated_connected_elements_nodes = []
        for i in range(len(connected_element_nodes)):
            if connected_element_nodes_removed[i] == 0:
                updated_connected_elements_nodes.append(connected_element_nodes[i])

        validating_node.link_xml[connected_element_name] = updated_connected_elements_nodes

    # print('Updated Results:')
    # for result in possible_results:
    # print(result)

    for connected_element_name in validating_node.link_xml.keys():
        connected_element_nodes = validating_node.link_xml[connected_element_name]
        for node in connected_element_nodes:
            check_XML_children_validation(node, possible_results, all_elements_name, relationship_matrix)


def root_leaf_node_validation(root_leaf_node, all_elements_name, relationship_matrix):
    # print('root_leaf_node_validation', root_leaf_node.boundary)

    possible_results = initialize_root_leaf_node_results(root_leaf_node, all_elements_name)

    # print("possible_results after initializing")
    # for result in possible_results:
    # print(result)

    # if root_leaf_node.filtered or not possible_results:
    # print("This root leaf node is validated => filtered")

    check_XML_children_validation(root_leaf_node, possible_results, all_elements_name, relationship_matrix)

    final_results = []
    for i in range(len(possible_results)):
        if possible_results[i].is_final_result():
            final_results.append(possible_results[i])

    return final_results


def validation_xml_sql(all_elements_name, relationship_matrix, XML_query_root_node):
    """Summary
    Perform validation after filtering
    Args
        all_elements_name (list of String): list of all elements' name in XML_query ordered by level order
        relationship_matrix ([][]): 2 dimension array store relationship between element. Relation[parent, children] = 1, relation[ancestor, descendant] = 2, other = 0
        all_elements_root (Node): RTree_XML root Node of XML_query root
    """

    XML_query_root_leaf_nodes = XML_query_root_node.get_leaf_node_not_filtered()
    # print("#######################")
    logger.info('%s %d', "Number of remaining leaf nodes of root after Filtering:", len(XML_query_root_leaf_nodes))
    logger.info(str([node.to_string() for node in XML_query_root_leaf_nodes]))
    all_results = []
    for node in XML_query_root_leaf_nodes:
        # print(node.boundary)
        # node_validation(node, all_elements_name, relationship_matrix)
        # print(node.boundary)
        node_results = root_leaf_node_validation(node, all_elements_name, relationship_matrix)
        for result in node_results:
            # print(result)
            all_results.append(result)

    # print("#######################")
    logger.info('%s %d', "Number of remaining leaf nodes of root after validation:",
          len(XML_query_root_node.get_leaf_node_not_filtered()))
    logger.info("All Possible results:")
    for result in all_results:
        logger.info(result)
    # print("#######################")

    # print('###############################')
    # for node in XML_query_root_leaf_nodes:
    #     print('Node ', node.boundary)
    #     if not node.filtered:
    #         for entry in node.validated_entries:
    #             print('\t', entry.coordinates)
    #             print('\t', 'Entry link_xml')
    #             for connected_element in entry.link_xml.keys():
    #                 print('\t', connected_element)
    #                 for entry_XML in entry.link_xml[connected_element]:
    #                     print('\t' * 2, entry_XML.coordinates)
    #             print('\t', 'Entry link_sql')
    #             for table_name in entry.link_sql.keys():
    #                 print('\t', table_name)
    #                 for entry_SQL in entry.link_sql[table_name]:
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


def join_xml_sql(all_elements_name: [str], all_elements_root: [Node], all_tables_root: [Node], relationship_matrix: []):
    """Summary
    This function join XML DB and SQL DB given a XML query

    :param all_elements_name    : list of all elements' name in XML_query ordered by level order
    :param all_elements_root    : list of all XML element root nodes (based on the order of the XML query)
    :param all_tables_root      : list of all SQL table root nodes (based on the highest order in XML query level orer)
    :param relationship_matrix  : 2 dimension array store relationship between element.
                                Relation[parent, children] = 1, relation[ancestor, descendant] = 2, other = 0
    :return:
    """

    ######################################
    # # Print Tree
    print('XML')
    for element in all_elements_name:
        print(element)
        all_elements_root[element].print_node()

    print('SQL')
    for table_name in all_tables_root.keys():
        print(table_name)
        all_tables_root[table_name].print_node()

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
            if relationship_matrix[i, j] != 0:
                connected_element = all_elements_name[j]
                element_root.link_xml[connected_element] = []
                element_root.link_xml[connected_element].append(all_elements_root[connected_element])

    ###################################################################3
    # Link tables root with XML root of highest element in XML query
    for table_name in all_tables_root.keys():
        table_root = all_tables_root[table_name]

        # find highest element
        table_elements = table_name.split('_')
        highest_element_name = table_elements[get_index_highest_element(all_elements_name, table_name)]
        # link
        all_elements_root[highest_element_name].link_sql[table_name] = []
        all_elements_root[highest_element_name].link_sql[table_name].append(table_root)

    end_initializing_link = timeit.default_timer()
    logger.info('%s %d', "Initialize link took: ", end_initializing_link - start_initializing_link)

    ################################################################
    # PRINT OUT LINK
    # for i in range(len(all_elements_name)):
    #     element = all_elements_name[i]
    #     element_root = all_elements_root[element]
    #     print(element)
    #     print('link_xml')
    #     for connected_element in element_root.link_xml.keys():
    #         print(connected_element)
    #         for connected_element_root in element_root.link_xml[connected_element]:
    #             print(connected_element_root.boundary)
    #     print('link_sql')
    #     for connected_table_name in element_root.link_sql.keys():
    #         print(connected_table_name)
    #         for connected_table_root in element_root.link_sql[connected_table_name]:
    #             print(connected_table_root.boundary)

    ##################################################################
    # Push root of XML query RTree root node to queue
    start_filtering = timeit.default_timer()

    query_root_name = all_elements_name[0]
    queue_query_root_rtreexml_node = queue.Queue()
    queue_query_root_rtreexml_node.put(all_elements_root[query_root_name])
    queue_limit_range = queue.Queue()
    queue_limit_range.put(limit_range)

    n_root_node_full_filtered = 0

    while not queue_query_root_rtreexml_node.empty():
        query_root_rtreexml_node = queue_query_root_rtreexml_node.get()  # type: Node
        limit_range = queue_limit_range.get()

        start_one_node_filtering = timeit.default_timer()

        updated_limit_range = full_filtering(query_root_rtreexml_node, all_elements_name, limit_range)

        end_one_node_filtering = timeit.default_timer()

        n_root_node_full_filtered += 1
        logger.info('%s %s %s %d %s %s %s', 'query root node', query_root_rtreexml_node.to_string(), 'filter time:',
                    end_one_node_filtering - start_one_node_filtering, 'filtered:', query_root_rtreexml_node.filtered, query_root_rtreexml_node.reason_of_filtered)

        if not query_root_rtreexml_node.filtered:
            for XML_query_root_node_child in query_root_rtreexml_node.children:
                queue_query_root_rtreexml_node.put(XML_query_root_node_child)
                queue_limit_range.put(copy.deepcopy(updated_limit_range))
        # print("###############")

    end_filtering = timeit.default_timer()
    logger.info('%s %d','Filtering time', end_filtering - start_filtering)
    logger.info('%s %d', 'Average filtering time for one node:',
                (end_filtering - start_filtering) / n_root_node_full_filtered)
    # print("######################################")

    ##################################################################
    # Print filtered tree
    # all_elements_root[XML_query_root_element].print_node_not_filtered_with_link()

    ##################################################################
    # Perform validation
    start_validation = timeit.default_timer()
    validation_xml_sql(all_elements_name, relationship_matrix, all_elements_root[query_root_name])
    end_validation = timeit.default_timer()

    logger.info('%s %d', 'validation time', end_validation - start_validation)

    ##################################################################
    # Return final result
    # get_final_results(all_elements_name, relationship_matrix, all_elements_root[XML_query_root_element])


def main():
    if len(sys.argv) < 3:
        raise ValueError('Missing arguments. Requires 2 arguments in the following order: folder_name, max_n_children')
    folder_name = sys.argv[1]
    max_n_children = int(sys.argv[2])
    # sys.stdout = open("io/" + folder_name + "/max_children_" + str(max_n_children) + ".txt", 'w')
    loader = Loader(folder_name, max_n_children)
    join_xml_sql(loader.all_elements_name, loader.all_elements_root, loader.all_tables_root, loader.relationship_matrix)

