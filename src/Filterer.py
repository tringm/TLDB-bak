import copy
import logging
from typing import Dict, List, Any

from .Boundary import *
from .Node import Node


def value_filtering(filtering_node: Node, all_elements_name: [str]):
    """
    This function do value filtering by checking all connected tables' nodes in filtering_node.link_SQL
    Filter this node if exist one table that has no match for this filtering_node
    Update filtering_node.link_SQL for table nodes that is not a match
    Update the range of linking nodes
    Each node should be value filtered once
    :param filtering_node:
    :param all_elements_name:
    :return:
    """

    logger = logging.getLogger("Value Filterer")

    filtering_node_index = all_elements_name.index(filtering_node.name)
    logger.debug('\t' * filtering_node_index + 'Begin Value Filtering ' + filtering_node.to_string())

    if not filtering_node.link_sql:
        logger.debug('\t' * filtering_node_index + 'Empty link_sql')
        return

    filtering_node.value_filtering_visited = True

    # Pre-filter the link_sql nodes with filtering_node.boundary
    for table_name in filtering_node.link_sql:
        table_nodes = filtering_node.link_sql[table_name]  # type: [Node]
        if not table_nodes:
            filtering_node.filtered = True
            filtering_node.reason_of_filtered = 'Value Filter: ' + table_name + ' is empty'
            return
        dimension = table_name.split('_').index(filtering_node.name)
        remaining_nodes = []
        i = 0
        while ((i < len(table_nodes) - 1) and (
                table_nodes[i].boundary[dimension][1] < filtering_node.boundary[1][0])):
            i += 1
        while table_nodes[i].boundary[dimension][0] <= filtering_node.boundary[1][1]:
            remaining_nodes.append(table_nodes[i])
            if i == len(table_nodes) - 1:
                break
            i += 1

        if not remaining_nodes:
            filtering_node.filtered = True
            filtering_node.reason_of_filtered = "Value Filter: No linked_sql table has any node " \
                                                "that intersects with value range"
            logger.debug('\t' * filtering_node_index + filtering_node.to_string() + "FILTERED by prefilter")
            return
        filtering_node.link_sql[table_name] = remaining_nodes

    # init intersected_value_boundary
    intersected_value_boundary = []  # type: List[Dict[str, List[int]]]
    first_table_name = list(filtering_node.link_sql.keys())[0]
    first_table_nodes = filtering_node.link_sql[first_table_name]  # type: List[Node]
    first_table_elements = first_table_name.split('_')  # type: [str]
    for node in first_table_nodes:
        intersected_value_boundary.append(dict(zip(first_table_elements, node.boundary)))

    logger.debug('\t' * filtering_node_index + 'Init intersected_value_boundary' + str(intersected_value_boundary))

    logger.debug('\t' * filtering_node_index + 'Find intersected value boundary')
    # Checking nodes of different table, if found no intersection -> Filter
    for table_name in filtering_node.link_sql:
        table_elements = table_name.split('_')
        table_nodes = filtering_node.link_sql[table_name]  # type: List[Node]
        logger.debug('\t' * filtering_node_index + 'Checking ' + table_name + ' Nodes: ' + str([node.boundary for node in table_nodes]))
        remaining_nodes = []
        value_boundary_cursor = 0
        table_cursor = 0
        updated_intersected_value_boundary = []

        while table_cursor < len(table_nodes) and value_boundary_cursor < len(intersected_value_boundary):
            current_table_node = table_nodes[table_cursor]  # type: Node
            current_value_boundary = intersected_value_boundary[value_boundary_cursor]  # type: {str, List[int]}

            if current_table_node.boundary[current_table_node.dimension][0] > \
                    current_value_boundary[filtering_node.name][1]:
                if value_boundary_cursor < len(intersected_value_boundary):
                    value_boundary_cursor += 1
                else:
                    table_cursor = len(table_nodes)
            elif current_table_node.boundary[current_table_node.dimension][1] < \
                    current_value_boundary[filtering_node.name][0]:
                if table_cursor < len(table_nodes):
                    table_cursor += 1
                else:
                    value_boundary_cursor = len(intersected_value_boundary)
            else:
                # if has intersection -> check other element
                all_elements_checked_ok = True

                current_value_boundary_intersected = {}
                # check element of the current node, if it intersect with the value boundary
                for i in range(len(table_elements)):
                    element = table_elements[i]
                    if element not in current_value_boundary:
                        current_value_boundary_intersected[element] = current_table_node.boundary[i]
                    else:
                        if value_boundary_has_intersection(current_table_node.boundary[i],
                                                           current_value_boundary[element]):
                            current_value_boundary_intersected[element] = value_boundary_intersection(
                                current_table_node.boundary[i], current_value_boundary[element])
                        else:
                            all_elements_checked_ok = False
                            break

                # If all element check are satisfied -> add the intersected boundary, add current_table_node
                if all_elements_checked_ok:
                    # add element in current_value_boundary that not in current_node
                    for element in current_value_boundary:
                        if element not in table_elements:
                            current_value_boundary_intersected[element] = current_value_boundary[element]
                    updated_intersected_value_boundary.append(current_value_boundary_intersected)
                    remaining_nodes.append(current_table_node)

                # if current node already on the right side of value boundary
                #     1. Move right value boundary if can
                #     2. Can't move right value boundary cursor -> all nodes on the right side of current_table_node is
                #     eliminated
                if current_table_node.boundary[current_table_node.dimension][1] > \
                        current_value_boundary[filtering_node.name][1]:
                    if value_boundary_cursor < len(intersected_value_boundary):
                        value_boundary_cursor += 1
                    else:
                        table_cursor = len(table_nodes)
                else:
                    if table_cursor < len(table_nodes):
                        table_cursor += 1

        logger.debug('\t' * filtering_node_index + 'remaining_nodes: ' + str([node.boundary for node in remaining_nodes]))
        logger.debug('\t' * filtering_node_index + 'Updated intersected value boundary: ' + str(updated_intersected_value_boundary))

        if not remaining_nodes:
            if updated_intersected_value_boundary:
                raise ValueError('Something wrong: has some remaining nodes but no intersected value')
            filtering_node.filtered = True
            filtering_node.reason_of_filtered = 'Value Filter: ' + table_name + \
                                                str([node.to_string() for node in table_nodes]) + \
                                                'has no intersection with other tables'
            logger.debug('\t' * filtering_node_index + filtering_node.to_string() + "FILTERED by intersected boundary")
            return

        intersected_value_boundary = updated_intersected_value_boundary
        filtering_node.link_sql[table_name] = remaining_nodes

    link_sql_elements_intersection_range = {}  # type: Dict[str, List[List[int]]]
    for element in intersected_value_boundary[0]:
        link_sql_elements_intersection_range[element] = [value_boundary[element]
                                                         for value_boundary in intersected_value_boundary]
    filtering_node.link_sql_elements_intersection_range = link_sql_elements_intersection_range

    logger.debug('\t' * filtering_node_index + 'Value Filter result')
    logger.debug('\t' * filtering_node_index + 'link sql: ')
    for table_name in filtering_node.link_sql:
        logger.debug('\t' * (filtering_node_index + 1) + table_name + str(
            [node.boundary for node in filtering_node.link_sql[table_name]]))
    logger.debug('\t' * filtering_node_index + 'link sql intersection range: ')
    for element in filtering_node.link_sql_elements_intersection_range:
        logger.debug('\t' * (filtering_node_index + 1) + element + str(filtering_node.link_sql_elements_intersection_range[element]))


def connected_element_filtering(filtering_node: Node, all_elements_name: [str], limit_range: Dict[str, List[int]]):
    """
    This function filter this filtering_node by checking all of its connected element if they can
    satisfy the limit range and the structure range requirement (ancestor, descendant)
    If any of the connected_element does not have any satisfying node -> Filter this filtering_node
    Update this filtering_node.link_XML if found unsatisfied nodes

    :param filtering_node:
    :param all_elements_name: list of all elements based on xml query level order traversal
    :param limit_range: constraint limit range of each element based on the order of all_elements_name
    :return:
    """
    logger = logging.getLogger("Connected Element")
    filtering_node_index = all_elements_name.index(filtering_node.name)
    logger.debug('\t' * filtering_node_index + 'Connected element filtering')
    logger.debug('\t' * filtering_node_index + 'Limit range ' + str(limit_range))

    for connected_element_name in filtering_node.link_xml:
        logger.debug('\t' * (filtering_node_index + 1) + 'Checking: ' + connected_element_name)

        connected_element_nodes = filtering_node.link_xml[connected_element_name]
        if not connected_element_nodes:
            filtering_node.filtered = True
            filtering_node.reason_of_filtered = 'Connected Element Filter: ' + connected_element_name + ' is empty'
            return

        remaining_nodes = []

        for connected_element_node in connected_element_nodes:
            logger.debug('\t' * (filtering_node_index + 2) + connected_element_node.to_string())
            if connected_element_node.filtered:
                continue

            if not index_boundary_can_be_ancestor(filtering_node.boundary[0], connected_element_node.boundary[0]):
                continue

            if not value_boundary_has_intersection(connected_element_node.boundary[1],
                                                   limit_range[connected_element_name]):
                continue
            remaining_nodes.append(connected_element_node)

        logger.debug('\t' * (filtering_node_index + 1) + 'Remaining nodes: ' +
                     str([node.to_string() for node in remaining_nodes]))

        filtering_node.link_xml[connected_element_name] = remaining_nodes

        if not remaining_nodes:
            filtering_node.filtered = True
            filtering_node.reason_of_filtered = 'Connected Element Filter: ' + connected_element_name + \
                                                'has no satisfying node'
            return


def check_lower_level(filtering_node: Node, all_elements_name: [str], limit_range: Dict[str, List[int]]) -> [] or None:
    """
    This function perform full filtering of the query children of this filtering_node
    :param filtering_node:
    :param all_elements_name:
    :param limit_range:
    :return: updated limit range based on combined range of full_filtered children,
             or None if filtering node is filtered
    """

    filtering_node_index = all_elements_name.index(filtering_node.name)
    logger = logging.getLogger("Traversing Query")
    logger.debug('\t' * filtering_node_index + 'Start check lower lever ' + filtering_node.to_string())

    # Value Filtering connected_element_nodes to get the updated limit range
    # TODO: Is this efficient, even though each node only has to value filtered once?

    for connected_element_name in filtering_node.link_xml:
        logger.debug('\t' * (filtering_node_index + 1) + 'Traverse down ' + connected_element_name)
        connected_element_nodes = filtering_node.link_xml[connected_element_name]
        remaining_nodes = []
        children_combined_limit_range = {}

        for connected_element_node in connected_element_nodes:
            child_limit_range = copy.deepcopy(limit_range)
            updated_child_limit_range = full_filtering(connected_element_node, all_elements_name, child_limit_range)

            if connected_element_node.filtered:
                continue

            remaining_nodes.append(connected_element_node)

            for element in all_elements_name:
                if element not in children_combined_limit_range:
                    children_combined_limit_range[element] = updated_child_limit_range[element]
                else:
                    children_combined_limit_range[element] = value_boundary_union(
                        children_combined_limit_range[element], updated_child_limit_range[element])

        if not remaining_nodes:
            filtering_node.filtered = True
            filtering_node.reason_of_filtered = 'Traversing down: None of ' + connected_element_name + \
                                                ' nodes pass full filtering'
            return

        filtering_node.link_xml[connected_element_name] = remaining_nodes
        # Update limit range by doing intersection with combined limit range of all children
        for element in all_elements_name:
            limit_range[element] = value_boundary_intersection(limit_range[element],
                                                               children_combined_limit_range[element])

    return limit_range


def initialize_children_link(filtering_node, all_elements_name, limit_range):
    """
    This function intialize children link_xml and link_sql of a filtered node
    :param filtering_node:
    :param all_elements_name:
    :param limit_range:
    :return:
    """
    filtering_node_index = all_elements_name.index(filtering_node.name)
    logger = logging.getLogger("Init Children")

    if not filtering_node.children:
        logger.debug('\t' + ' This is a leaf node ')
        return

    logger.debug('\t' * filtering_node_index + 'Start initialize children link ' + filtering_node.to_string())
    logger.debug('\t' * filtering_node_index + 'limit_range ' + str(limit_range))

    # Init link_xml checking if the children can match the structure and value requirement
    children_link_xml = {}
    for connected_element_name in filtering_node.link_xml:
        children_link_xml[connected_element_name] = []
        for connected_element_node in filtering_node.link_xml[connected_element_name]:
            logger.debug('\t' * (filtering_node_index + 1) + connected_element_node.to_string() + ' - Children: ' +
                         str([child.boundary for child in connected_element_node.children]))
            if len(connected_element_node.children) == 0:
                children_link_xml[connected_element_name].append(connected_element_node)
            else:
                for connected_element_node_child in connected_element_node.children:
                    if not index_boundary_can_be_ancestor(filtering_node.boundary[0],
                                                          connected_element_node_child.boundary[0]):
                        continue
                    if not value_boundary_has_intersection(connected_element_node_child.boundary[1],
                                                           limit_range[connected_element_name]):
                        logger.debug('VAMPIRE')
                        continue
                    children_link_xml[connected_element_name].append(connected_element_node_child)
        # if this connected element does not have any satisfy children node -> filter this node
        if not children_link_xml[connected_element_name]:
            filtering_node.filtered = True
            filtering_node.reason_of_filtered = "Init Children: No child node of " + connected_element_name + 'satisfy'
            return

        logger.debug('\t' * (filtering_node_index + 1) + 'link_xml[' + connected_element_name + '] ' +
                     str([node.boundary for node in children_link_xml[connected_element_name]]))

    # Init link_sql and check if they satisfy the limit range
    children_link_sql = {}  # type: Dict[str, List[Node]]
    for table_name in filtering_node.link_sql:
        children_link_sql[table_name] = []
        table_elements = table_name.split('_')
        for table_node in filtering_node.link_sql[table_name]:
            logger.debug('\t' * (filtering_node_index + 1) + table_node.to_string() + ' - Children: ' +
                         str([child.boundary for child in table_node.children]))
            if len(table_node.children) == 0:
                children_link_sql[table_name].append(table_node)
            else:
                for table_node_child in table_node.children:
                    table_node_child_satisfy_limit_range = True
                    for i in range(len(table_elements)):
                        if not value_boundary_has_intersection(
                                table_node_child.boundary[i], limit_range[table_elements[i]]):
                            table_node_child_satisfy_limit_range = False
                            break
                    if table_node_child_satisfy_limit_range:
                        children_link_sql[table_name].append(table_node_child)
        # if this table does not have any satisfy children node -> filter this node
        if not children_link_sql[table_name]:
            filtering_node.filtered = True
            filtering_node.reason_of_filtered = 'Init Children: No child node of ' + table_name + ' satisfy'
            return

        logger.debug('\t' * (filtering_node_index + 1) + 'link_sql[' + table_name + '] ' +
                     str([node.boundary for node in children_link_sql[table_name]]))

    # if child does not fall into any part range of the link_sql_elements_intersection_range[filtering_node.name]
    # => filtered and not init
    # Similar execution to value filtering
    # TODO: Should be a more elegant way to handle case which element does not connected to any than using if
    if filtering_node.link_sql_elements_intersection_range:
        allowed_value_boundary = filtering_node.link_sql_elements_intersection_range[filtering_node.name]
        children_cursor = 0
        value_boundary_cursor = 0
        remaining_children = []
        logger.debug('\t' + 'filtering_node children: ' + str([node.to_string() for node in filtering_node.children]))
        logger.debug('\t' + 'allowed_value_boundary: ' + str(allowed_value_boundary))

        while children_cursor < len(filtering_node.children) and value_boundary_cursor < len(allowed_value_boundary):
            current_children_node = filtering_node.children[children_cursor]  # type: Node
            current_value_boundary = allowed_value_boundary[value_boundary_cursor]  # type: List[int]

            logger.debug('\t' + 'current_children: ' + current_children_node.to_string())
            logger.debug('\t' + 'current_allowed_value_boundary: ' + str(current_value_boundary))

            if current_children_node.boundary[1][0] > current_value_boundary[1]:
                if value_boundary_cursor < len(allowed_value_boundary):
                    # Check next boundary
                    value_boundary_cursor += 1
                    logger.debug('\t' * 2 + 'case 1.1')
                else:
                    # skip
                    children_cursor = len(filtering_node.children)
                    logger.debug('\t' * 2 + 'case 1.2')
            elif current_children_node.boundary[1][1] < current_value_boundary[0]:
                if children_cursor < len(filtering_node.children):
                    # Check next children
                    children_cursor += 1
                    logger.debug('\t' * 2 + 'case 2.1')
                else:
                    # skip
                    value_boundary_cursor = len(allowed_value_boundary)
                    logger.debug('\t' * 2 + 'case 2.2')
            else:
                remaining_children.append(current_children_node)
                logger.debug('\t' * 2 + 'SATISFY')
                if current_children_node.boundary[1][1] > current_value_boundary[1]:
                    if value_boundary_cursor < len(allowed_value_boundary):
                        value_boundary_cursor += 1
                    else:
                        children_cursor = len(filtering_node.children)
                else:
                    if children_cursor < len(filtering_node.children):
                        children_cursor += 1

        logger.debug('\t' + 'remaining children nodes: ' + str([node.to_string() for node in remaining_children]))

        filtering_node.children = remaining_children
        if not remaining_children:
            filtering_node.filtered = True
            filtering_node.reason_of_filtered = "Init Children: No children satisfy"
            return

        cursor = {}
        for table_name in filtering_node.link_sql:
            cursor[table_name] = 0

        for filtering_node_child in filtering_node.children:
            filtering_node_child.link_xml = children_link_xml.copy()

            this_child_link_sql = {}

            for table_name in filtering_node.link_sql:
                this_child_link_sql[table_name] = []

                while cursor[table_name] < len(children_link_sql[table_name]):
                    node = children_link_sql[table_name][cursor[table_name]]  # type: Node
                    if node.boundary[node.dimension][1] < filtering_node_child.boundary[1][0]:
                        cursor[table_name] += 1
                    elif node.boundary[node.dimension][0] > filtering_node_child.boundary[1][1]:
                        break
                    else:
                        this_child_link_sql[table_name].append(node)
                        cursor[table_name] += 1

            filtering_node_child.link_sql = this_child_link_sql

    else:
        for filtering_node_child in filtering_node.children:
            filtering_node_child.link_xml = children_link_xml.copy()
            filtering_node_child.link_sql = children_link_sql.copy()

    for filtering_node_child in filtering_node.children:
        logger.debug('\t' + 'Child ' + filtering_node_child.to_string())
        logger.debug('\t' * 2 + 'link xml: ')
        for connected_element_name in filtering_node_child.link_xml:
            logger.debug('\t' * 3 + connected_element_name +
                         str([node.to_string() for node in filtering_node_child.link_xml[connected_element_name]]))
        logger.debug('\t' * 2 + 'link sql: ')
        for table_name in filtering_node_child.link_sql:
            logger.debug('\t' * 3 + table_name + str(
                [node.to_string() for node in filtering_node_child.link_sql[table_name]]))


def full_filtering(filtering_node: Node, all_elements_name: [str], limit_range: Dict[str, List[int]]) \
        -> Dict[str, List[int]] or None:
    """
    1. Perform value filtering by going through tables
    => update limit range for this node and other nodes in the checked tables
    2. Filter its connected element

    :param filtering_node: 
    :param all_elements_name: list of all elements based on xml query level order traversal
    :param limit_range: constraint limit range of each element based on the order of all_elements_name
    :return: updated limit range or None if this node is filtered
    """

    logger = logging.getLogger("Main Filterer")

    filtering_node_index = all_elements_name.index(filtering_node.name)
    logger.debug('\t' * filtering_node_index + '--- Begin Filtering --- ' + filtering_node.to_string())
    if filtering_node.parent is None:
        logger.debug('\t' * (filtering_node_index + 1) + 'is Root')
    else:
        logger.debug('\t' * (filtering_node_index + 1) + 'Parent: ' + filtering_node.parent.to_string())
    logger.debug('\t' * (filtering_node_index + 1) + 'Limit range: ' + str(limit_range))

    filtering_node_value_boundary = filtering_node.boundary[1]

    # Go through limit range and check if this node satisfy the limit range
    filtering_node_limit_range = limit_range[filtering_node.name]
    if value_boundary_has_intersection(filtering_node_limit_range, filtering_node_value_boundary):
        limit_range[filtering_node.name] = value_boundary_intersection(filtering_node_limit_range,
                                                                       filtering_node_value_boundary)
        logger.debug('\t' * (filtering_node_index + 1) + 'Limit range UPDATED based on node value' + str(limit_range))
    else:
        filtering_node.filtered = True
        filtering_node.reason_of_filtered = 'Main Filter: Filtered by limit range ' + str(limit_range)
        logger.debug('\t' * (filtering_node_index + 1) + filtering_node.to_string() + 'FILTERED by limit range')
        return

    # If not value filtered before =>
    # Perform value filtering by checking all connected table and return limit ranges based on these tables
    if not filtering_node.value_filtering_visited:
        value_filtering(filtering_node, all_elements_name)

    if filtering_node.filtered:
        logger.debug('\t' * (filtering_node_index + 1) + filtering_node.to_string() + 'FILTERED by value filtering')
        return

    # If not filtered after value filtering => update limit range
    link_sql_elements_intersection_range = filtering_node.link_sql_elements_intersection_range

    for element in link_sql_elements_intersection_range:
        limit_range[element] = value_boundary_intersection(
            limit_range[element], value_boundaries_union(link_sql_elements_intersection_range[element]))

    logger.debug('\t' * filtering_node_index + 'Limit range UPDATED based on value filtering' + str(limit_range))

    logger.debug('\t' * filtering_node_index + 'Begin Connected Element Filtering' + filtering_node.to_string())

    connected_element_filtering(filtering_node, all_elements_name, limit_range)

    if filtering_node.filtered:
        logger.debug('\t' * (filtering_node_index + 1) + filtering_node.to_string() +
                     'FILTERED by connected_element_filtering')
        return

    limit_range = check_lower_level(filtering_node, all_elements_name, limit_range)

    if filtering_node.filtered:
        logger.debug('\t' * (filtering_node_index + 1) + filtering_node.to_string() +
                     'FILTERED by when check_lower_level')
        return

    # Update children's links
    initialize_children_link(filtering_node, all_elements_name, limit_range)

    # print('\t' * filtering_node_index,'--- End Full Filtering ---')
    return limit_range
