import copy
import logging
import timeit
from typing import Dict, List

from src.lib.Boundary import *
from src.structure.Node import Node
from src.io_support.LoggerSupport import *

# TODO:
#     * Improve Prefilter
#     * Implement logging.verbose: DONE
#     * Fix the while loop (at least in the init children so that next node would take previous_node index - 1 , in
#     case where previous node range overlap with the next node)


def value_filtering(filtering_node: XMLNode, all_elements_name: [str]):
    """
    This function do value filtering by checking all connected tables' nodes in filtering_node.link_SQL
    Filter this node if exist one table that has no match for this filtering_node or tables doesn't match (matching
    nodes of A_B does not match with any nodes of A_B_C)
    Each node should be value filtered once
    Result:
        1. Updated link sql | Filtered
        2. Init intersection_range | Filtered
    """
    start_value_filtering = timeit.default_timer()
    logger = logging.getLogger("Value Filterer")
    logger.setLevel(logging.getLogger("Main Filterer").level)

    filtering_node_index = all_elements_name.index(filtering_node.name)
    logger.debug('\t' * filtering_node_index + 'Begin Value Filtering ' + str(filtering_node))

    if not filtering_node.link_sql:
        logger.debug('\t' * (filtering_node_index + 1) + 'EMPTY LINK_SQL')
        end_value_filtering = timeit.default_timer()
        filtering_node.value_filtering_time = end_value_filtering - start_value_filtering
        return

    filtering_node.value_filtering_visited = True

    # Always look for table with the most number of elements first
    table_names = list(filtering_node.link_sql.keys())
    table_names.sort(key=lambda name: len(name), reverse=True)

    # Pre-filter the link_sql nodes with filtering_node.boundary
    # TODO: This is usually not needed since it's already done when init
    logger.verbose('\t' * filtering_node_index + 'Begin Pre-Filter')
    for table_name in table_names:
        table_nodes = filtering_node.link_sql[table_name]  # type: [Node]
        if not table_nodes:
            filtering_node.filtered = True
            filtering_node.reason_of_filtered = 'Value Filter: ' + table_name + ' is empty'
            logger.debug('\t' * (filtering_node_index + 1) + '###')
            logger.debug('\t' * (filtering_node_index + 1) + 'FILTERED: ' + filtering_node.reason_of_filtered)
            end_value_filtering = timeit.default_timer()
            filtering_node.value_filtering_time = end_value_filtering - start_value_filtering
            return
        dimension = table_name.split('_').index(filtering_node.name)
        remaining_nodes = []
        i = 0
        while (table_nodes[i].boundary[dimension][1] < filtering_node.boundary[1][0]) and (i < len(table_nodes) - 1):
            i += 1
        while table_nodes[i].boundary[dimension][0] <= filtering_node.boundary[1][1]:
            remaining_nodes.append(table_nodes[i])
            if i == len(table_nodes) - 1:
                break
            i += 1

        if not remaining_nodes:
            filtering_node.filtered = True
            filtering_node.reason_of_filtered = 'Value Filter: linked_sql ' + table_name + \
                                                ' has no node that intersects with value range'
            logger.debug('\t' * (filtering_node_index + 1))
            logger.debug('\t' * (filtering_node_index + 1) + 'FILTERED: ' + filtering_node.reason_of_filtered)
            end_value_filtering = timeit.default_timer
            filtering_node.value_filtering_time = end_value_filtering - start_value_filtering
            return
        filtering_node.link_sql[table_name] = remaining_nodes

    # init intersected_value_boundary
    intersected_value_boundary = []  # type: List[Dict[str, List[int]]]
    first_table_name = table_names[0]
    first_table_nodes = filtering_node.link_sql[first_table_name]  # type: List[Node]
    first_table_elements = first_table_name.split('_')  # type: [str]
    for node in first_table_nodes:
        intersected_value_boundary.append(dict(zip(first_table_elements, node.boundary)))

    logger.verbose('\t' * filtering_node_index + 'Find intersected value boundary')
    # Checking nodes of different table, if found no intersection -> Filter
    for table_name in table_names:
        if table_name != first_table_name:
            table_elements = table_name.split('_')
            table_nodes = filtering_node.link_sql[table_name]  # type: List[Node]
            logger.verbose('\t' * filtering_node_index + 'Checking ' + table_name + ' Nodes: '
                         + str([node.boundary for node in table_nodes]))
            remaining_nodes = []
            boundary_cursor = 0
            table_cursor = 0
            updated_intersected_value_boundary = []

            while table_cursor < len(table_nodes) and boundary_cursor < len(intersected_value_boundary):
                current_table_node = table_nodes[table_cursor]  # type: Node
                current_boundary = intersected_value_boundary[boundary_cursor]  # type: {str, List[int]}
                logger.verbose('\t' * (filtering_node_index + 1) + 'current_table_node: ' + str(current_table_node))
                logger.verbose('\t' * (filtering_node_index + 1) + 'current_value_boundary: ' + str(current_boundary))

                if current_table_node.boundary[current_table_node.dimension][0] > \
                        current_boundary[filtering_node.name][1]:
                    if boundary_cursor < len(intersected_value_boundary):
                        logger.verbose('\t' * (filtering_node_index + 2) + 'NO INTERSECT, Move to next boundary')
                        boundary_cursor += 1
                    else:
                        logger.verbose('\t' * (filtering_node_index + 2) + 'NO INTERSECT, Skip remaining nodes')
                        table_cursor = len(table_nodes)
                elif current_table_node.boundary[current_table_node.dimension][1] < \
                        current_boundary[filtering_node.name][0]:
                    if table_cursor < len(table_nodes):
                        logger.verbose('\t' * (filtering_node_index + 2) + 'NO INTERSECT, Move to next table node')
                        table_cursor += 1
                    else:
                        logger.verbose('\t' * (filtering_node_index + 2) + 'NO INTERSECT, Skip remaining nodes')
                        boundary_cursor = len(intersected_value_boundary)
                else:
                    # if has intersection -> check other element
                    logger.verbose('\t' * (filtering_node_index + 2) + 'INTERSECT, Find element-wise intersection')
                    all_elements_checked_ok = True
                    current_boundary_intersected = {}
                    # check element of the current node, if it intersect with the value boundary
                    for i in range(len(table_elements)):
                        element = table_elements[i]
                        if element not in current_boundary:
                            current_boundary_intersected[element] = current_table_node.boundary[i]
                        else:
                            if value_boundary_has_intersection(current_table_node.boundary[i],
                                                               current_boundary[element]):
                                current_boundary_intersected[element] = value_boundary_intersection(
                                    current_table_node.boundary[i], current_boundary[element])
                            else:
                                all_elements_checked_ok = False
                                break

                    # If all element check are satisfied -> add the intersected boundary, add current_table_node
                    if all_elements_checked_ok:
                        # add element in current_value_boundary that not in current_node
                        for element in current_boundary:
                            if element not in table_elements:
                                current_boundary_intersected[element] = current_boundary[element]
                        updated_intersected_value_boundary.append(current_boundary_intersected)
                        logger.verbose('\t' * (filtering_node_index + 3) + 'element check OK ' +
                                       str(current_boundary_intersected))
                        if current_table_node not in remaining_nodes:
                            remaining_nodes.append(current_table_node)
                    else:
                        logger.verbose('\t' * (filtering_node_index + 3) + 'element check failed')

                    # if current node already on the right side of value boundary
                    #     1. Move right value boundary if can
                    #     2. Else: all nodes on the right side of current_table_node is eliminated
                    if current_table_node.boundary[current_table_node.dimension][1] > \
                            current_boundary[filtering_node.name][1]:
                        if boundary_cursor < len(intersected_value_boundary):
                            logger.verbose('\t' * (filtering_node_index + 2) + 'Move to next boundary')
                            boundary_cursor += 1
                        else:
                            logger.verbose('\t' * (filtering_node_index + 2) + 'SKip remaining table nodes')
                            table_cursor = len(table_nodes)
                    else:
                        if table_cursor < len(table_nodes):
                            logger.verbose('\t' * (filtering_node_index + 2) + 'Move to next table node')
                            table_cursor += 1

            logger.verbose('\t' * filtering_node_index + 'remaining nodes: '
                         + str([node.boundary for node in remaining_nodes]))
            logger.verbose('\t' * filtering_node_index + 'Updated intersected boundary: '
                         + str(updated_intersected_value_boundary))

            if not remaining_nodes:
                if updated_intersected_value_boundary:
                    raise ValueError('Something wrong: has some remaining nodes but no intersected value')
                filtering_node.filtered = True
                filtering_node.reason_of_filtered = 'Value Filter: ' + table_name + \
                                                    'has no intersection with other tables'
                logger.debug('\t' * (filtering_node_index + 1) + '###')
                logger.debug('\t' * (filtering_node_index + 1) + 'FILTERED: ' + filtering_node.reason_of_filtered)
                end_value_filtering = timeit.default_timer
                filtering_node.value_filtering_time = end_value_filtering - start_value_filtering
                return

            intersected_value_boundary = updated_intersected_value_boundary
            filtering_node.link_sql[table_name] = remaining_nodes

    filtering_node.intersection_range = {}  # type: Dict[str, List[List[int]]]
    for element in intersected_value_boundary[0]:
        filtering_node.intersection_range[element] = [value_boundary[element]
                                                      for value_boundary in intersected_value_boundary]

    logger.debug('\t' * filtering_node_index + 'Value Filter result')
    log_node_link_sql(filtering_node, logger.debug, filtering_node_index + 1)
    log_node_intersection_range(filtering_node, logger.debug, filtering_node_index + 1)
    end_value_filtering = timeit.default_timer()
    filtering_node.value_filtering_time = end_value_filtering - start_value_filtering


def connected_element_filtering(filtering_node: Node, all_elements_name: [str], limit_range: Dict[str, List[int]]):
    """
    This function check filtering_node connected element nodes if they can statisfy the limit range and structure
    requirement (ancestor, descendant)

    Result:
        1. Updated link xml | Filtered
    """
    start_ce_filtering = timeit.default_timer()
    logger = logging.getLogger("Connected Element")
    logger.setLevel(logging.getLogger("Main Filterer").level)
    filtering_node_index = all_elements_name.index(filtering_node.name)
    logger.debug('\t' * filtering_node_index + 'Connected element filtering')
    logger.debug('\t' * filtering_node_index + 'Limit range ' + str(limit_range))

    if not filtering_node.link_xml:
        logger.debug('\t' * (filtering_node_index + 1) + 'EMPTY LINK_XML')
        end_ce_filtering = timeit.default_timer()
        filtering_node.connected_element_filtering_time = end_ce_filtering - start_ce_filtering
        return

    # Always go by the highest element order
    connected_elements = list(filtering_node.link_xml.keys())
    connected_elements.sort(key=lambda element: all_elements_name.index(element))

    for connected_element in connected_elements:
        logger.verbose('\t' * (filtering_node_index + 1) + 'Checking: ' + connected_element)

        connected_element_nodes = filtering_node.link_xml[connected_element]
        if not connected_element_nodes:
            filtering_node.filtered = True
            filtering_node.reason_of_filtered = 'Connected Element Filter: ' + connected_element + ' is empty'
            logger.debug('\t' * (filtering_node_index + 1) + '###')
            logger.debug('\t' * (filtering_node_index + 1) + 'FILTERED: ' + filtering_node.reason_of_filtered)
            end_ce_filtering = timeit.default_timer()
            filtering_node.connected_element_filtering_time = end_ce_filtering - start_ce_filtering
            return

        remaining_nodes = []

        for connected_element_node in connected_element_nodes:
            if connected_element_node.filtered:
                logger.verbose('\t' * (filtering_node_index + 2) + str(connected_element_node) + ' Already FILTERED')
                continue

            if not index_boundary_can_be_ancestor(filtering_node.boundary[0], connected_element_node.boundary[0]):
                logger.verbose('\t' * (filtering_node_index + 2) + str(connected_element_node) +
                               ' REMOVED by structure filtering')
                continue

            if not value_boundary_has_intersection(connected_element_node.boundary[1],
                                                   limit_range[connected_element]):
                logger.verbose('\t' * (filtering_node_index + 2) + str(connected_element_node) +
                               ' REMOVED by value filtering')
                continue
            logger.verbose('\t' * (filtering_node_index + 2) + str(connected_element_node) + ' OK')
            remaining_nodes.append(connected_element_node)

        filtering_node.link_xml[connected_element] = remaining_nodes

        if not remaining_nodes:
            filtering_node.filtered = True
            filtering_node.reason_of_filtered = 'Connected Element Filter: ' + connected_element + \
                                                ' has no satisfying node'
            logger.debug('\t' * (filtering_node_index + 1) + '###')
            logger.debug('\t' * (filtering_node_index + 1) + 'FILTERED: ' + filtering_node.reason_of_filtered)
            end_ce_filtering = timeit.default_timer()
            filtering_node.connected_element_filtering_time = end_ce_filtering - start_ce_filtering
            return

    logger.debug('\t' * filtering_node_index + 'Connected Element Filter result')
    log_node_link_xml(filtering_node, logger.debug, filtering_node_index + 1)
    end_ce_filtering = timeit.default_timer()
    filtering_node.connected_element_filtering_time = end_ce_filtering - start_ce_filtering


def check_lower_level(filtering_node: Node, all_elements_name: [str], limit_range: Dict[str, List[int]]) -> [] or None:
    """
    This function traverse to lower level of the XML Query to filter descendants
    Result:
        1. Filtered Descendant
        2. Updated limit range based on combined range of full_filtered children
        3. Updated link xml | Filtered
    """
    start_check_lower_level = timeit.default_timer()
    filtering_node_index = all_elements_name.index(filtering_node.name)
    logger = logging.getLogger("Traversing Query")
    logger.setLevel(logging.getLogger("Main Filterer").level)
    logger.debug('\t' * filtering_node_index + 'Start check lower lever ' + str(filtering_node))

    # Value Filtering connected_element_nodes to get the updated limit range
    # TODO: Is this efficient, even though each node only has to value filtered once?
    connected_elements = list(filtering_node.link_xml.keys())
    connected_elements.sort(key=lambda e: all_elements_name.index(e))

    for connected_element in connected_elements:
        logger.verbose('\t' * (filtering_node_index + 1) + 'Traverse down ' + connected_element)
        connected_element_nodes = filtering_node.link_xml[connected_element]
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
            filtering_node.reason_of_filtered = 'Traversing down: None of ' + connected_element + \
                                                ' nodes pass full filtering'
            end_check_lower_level = timeit.default_timer()
            filtering_node.check_lower_level_time = end_check_lower_level - start_check_lower_level
            return

        filtering_node.link_xml[connected_element] = remaining_nodes
        # Update limit range by doing intersection with combined limit range of all children
        for element in all_elements_name:
            limit_range[element] = value_boundary_intersection(limit_range[element],
                                                               children_combined_limit_range[element])

    end_check_lower_level = timeit.default_timer()
    filtering_node.check_lower_level_time = end_check_lower_level - start_check_lower_level

    return limit_range


def initialize_children_link(filtering_node, all_elements_name, limit_range):
    """
    This function initialize children link_xml and link_sql of a filtered node
    Result:
        1. Filtered children link xml based on structure filtering
        2. Filtered children link sql based on limit range
        3. Or FILTERED
    """
    start_init_children = timeit.default_timer()
    filtering_node_index = all_elements_name.index(filtering_node.name)
    logger = logging.getLogger("Init Children")
    logger.setLevel(logging.getLogger("Main Filterer").level)

    if not filtering_node.children:
        logger.debug('\t' * (filtering_node_index + 1) + ' This is a leaf node ')
        end_init_children = timeit.default_timer()
        filtering_node.init_children_time = end_init_children - start_init_children
        return [], []

    logger.debug('\t' * filtering_node_index + 'Start initialize children link ' + str(filtering_node))

    # Init link_xml checking if the children can match the structure and value requirement
    logger.verbose('\t' * filtering_node_index +
                   'Init xml_link and check if connected_element_node_child can satisfy limit range and structure')
    if not filtering_node.link_xml:
        ('\t' * (filtering_node_index + 1) + 'Link xml is empty')
    connected_elements = list(filtering_node.link_xml.keys())
    connected_elements.sort(key=lambda element: all_elements_name.index(element))
    children_link_xml = {}
    for connected_element in connected_elements:
        children_link_xml[connected_element] = []
        for connected_element_node in filtering_node.link_xml[connected_element]:
            logger.verbose('\t' * (filtering_node_index + 1) + 'Checking connected node ' + str(connected_element_node))
            if len(connected_element_node.children) == 0:
                logger.verbose('\t' * (filtering_node_index + 2) + ' is Leaf')
                children_link_xml[connected_element].append(connected_element_node)
            else:
                for connected_element_node_child in connected_element_node.children:
                    if not index_boundary_can_be_ancestor(filtering_node.boundary[0],
                                                          connected_element_node_child.boundary[0]):
                        logger.verbose('\t' * (filtering_node_index + 2) + str(connected_element_node_child) +
                                       ' IGNORED: not satisfy structure requirement')
                        continue
                    if not value_boundary_has_intersection(connected_element_node_child.boundary[1],
                                                           limit_range[connected_element]):
                        logger.verbose('\t' * (filtering_node_index + 2) + str(connected_element_node_child) +
                                       ' IGNORED: not satisfy limit_range')
                        continue
                    logger.verbose('\t' * (filtering_node_index + 2) + str(connected_element_node_child) + ' OK')
                    children_link_xml[connected_element].append(connected_element_node_child)
        # if this connected element does not have any satisfy children node -> filter this node
        if not children_link_xml[connected_element]:
            filtering_node.filtered = True
            filtering_node.reason_of_filtered = \
                "No child node of " + connected_element + 'satisfy limit range or structure'
            logger.debug('\t' * (filtering_node_index + 1) + '###')
            logger.debug('\t' * (filtering_node_index + 1) + 'FILTERED: ' + filtering_node.reason_of_filtered)
            end_init_children = timeit.default_timer()
            filtering_node.init_children_time = end_init_children - start_init_children
            return

    # Init link_sql and check if they satisfy the limit range
    logger.verbose('\t' * filtering_node_index +
                   'Init sql_link and check if connected_element_node_child can satisfy limit range')
    logger.verbose('\t' * filtering_node_index + 'limit_range ' + str(limit_range))
    children_link_sql = {}  # type: Dict[str, List[Node]]
    for table_name in filtering_node.link_sql:
        children_link_sql[table_name] = []
        table_elements = table_name.split('_')
        for table_node in filtering_node.link_sql[table_name]:
            logger.verbose('\t' * (filtering_node_index + 1) + 'Checking table_node ' + str(table_node))
            if len(table_node.children) == 0:
                logger.verbose('\t' * (filtering_node_index + 2) + ' is Leaf')
                children_link_sql[table_name].append(table_node)
            else:
                for table_node_child in table_node.children:
                    table_node_child_satisfy_limit_range = True
                    for i in range(len(table_elements)):
                        if not value_boundary_has_intersection(
                                table_node_child.boundary[i], limit_range[table_elements[i]]):
                            table_node_child_satisfy_limit_range = False
                            logger.debug('\t' * (filtering_node_index + 2) + str(table_node_child) +
                                         ' IGNORED: not satisfy limit range')
                            break
                    if table_node_child_satisfy_limit_range:
                        logger.debug('\t' * (filtering_node_index + 2) + str(table_node_child) + ' OK')
                        children_link_sql[table_name].append(table_node_child)
        # if this table does not have any satisfy children node -> filter this node
        if not children_link_sql[table_name]:
            filtering_node.filtered = True
            filtering_node.reason_of_filtered = 'Init Children: No child node of ' + table_name + ' satisfy limit range'
            logger.debug('\t' * (filtering_node_index + 1) + '###')
            logger.debug('\t' * (filtering_node_index + 1) + 'FILTERED: ' + filtering_node.reason_of_filtered)
            end_init_children = timeit.default_timer()
            filtering_node.init_children_time = end_init_children - start_init_children
            return

    end_init_children = timeit.default_timer()
    filtering_node.init_children_time = end_init_children - start_init_children

    return children_link_xml, children_link_sql


def filter_children(filtering_node, all_elements_name, limit_range, link_xml, link_sql):
    """
    This function filter the filtering_node children and its link _sql based on the intersection range
    and assign the link xml and link sql
    Result:
        1. Assign link xml to children
        2. Assign filtered link sql to children
    :param filtering_node:
    :return:
    """
    start_filter_children = timeit.default_timer()
    filtering_node_index = all_elements_name.index(filtering_node.name)
    logger = logging.getLogger("Filter Children")
    logger.setLevel(logging.getLogger("Main Filterer").level)

    if not filtering_node.children:
        logger.debug('\t' * (filtering_node_index + 1) + 'is a leaf node')
        end_filter_children = timeit.default_timer()
        filtering_node.filter_children_time = end_filter_children - start_filter_children
        return

    # if child does not fall into any part range of the intersection_range[filtering_node.name]
    # => filtered and not init
    # Similar execution to value filtering
    if not filtering_node.intersection_range:
        logger.debug('\t' * filtering_node_index + 'No intersection range')
        for filtering_node_child in filtering_node.children:
            filtering_node_child.link_xml = link_xml.copy()
            filtering_node_child.link_sql = link_sql.copy()
            logger.verbose('\t' * filtering_node_index + 'Result')
            log_node_all_link(filtering_node_child, logger.debug, filtering_node_index + 1)
        end_filter_children = timeit.default_timer()
        filtering_node.filter_children_time = end_filter_children - start_filter_children
        return

    logger.debug('\t' * filtering_node_index + 'Begin Filter Children')

    logger.debug('\t' * filtering_node_index + 'Begin Filter Children Node based on intersection range')
    allowed_value_boundary = filtering_node.intersection_range[filtering_node.name]
    children_cursor = 0
    value_boundary_cursor = 0
    remaining_children = []
    logger.verbose('\t' * filtering_node_index + 'filtering_node children: ' +
                   str([str(node) for node in filtering_node.children]))
    logger.verbose('\t' * filtering_node_index + 'allowed_value_boundary: ' + str(allowed_value_boundary))

    while children_cursor < len(filtering_node.children) and value_boundary_cursor < len(allowed_value_boundary):
        current_children_node = filtering_node.children[children_cursor]  # type: Node
        current_value_boundary = allowed_value_boundary[value_boundary_cursor]  # type: List[int]

        logger.verbose('\t' * (filtering_node_index + 1) + 'current_child: ' + str(current_children_node))
        logger.verbose('\t' * (filtering_node_index + 1) + 'current_allowed_value_boundary: ' +
                       str(current_value_boundary))

        if current_children_node.boundary[1][0] > current_value_boundary[1]:
            if value_boundary_cursor < len(allowed_value_boundary):
                value_boundary_cursor += 1
                logger.verbose('\t' * (filtering_node_index + 2) + 'NO INTERSECT, move value boundary')
            else:
                children_cursor = len(filtering_node.children)
                logger.verbose('\t' * (filtering_node_index + 2) + 'NO INTERSECT, skip remaining nodes')
        elif current_children_node.boundary[1][1] < current_value_boundary[0]:
            if children_cursor < len(filtering_node.children):
                children_cursor += 1
                logger.verbose('\t' * (filtering_node_index + 2) + 'NO INTERSECT, move to next child')
            else:
                value_boundary_cursor = len(allowed_value_boundary)
                logger.verbose('\t' * (filtering_node_index + 3) + 'NO INTERSECT, checked all children')
        else:
            if current_children_node not in remaining_children:
                remaining_children.append(current_children_node)
                current_children_node.allowed_intersection_range_index = []
            current_children_node.allowed_intersection_range_index.append(value_boundary_cursor)

            if current_value_boundary[1] < current_children_node.boundary[1][1]:
                if value_boundary_cursor < len(allowed_value_boundary):
                    value_boundary_cursor += 1
                    logger.verbose('\t' * (filtering_node_index + 2) + 'INTERSECT, move value boundary')
                else:
                    children_cursor = len(filtering_node.children)
                    logger.verbose('\t' * (filtering_node_index + 2) + 'INTERSECT, skip remaining nodes')
            else:
                if children_cursor < len(filtering_node.children):
                    children_cursor += 1
                    logger.verbose('\t' * (filtering_node_index + 2) + 'INTERSECT, move to next child')
                else:
                    value_boundary_cursor = len(allowed_value_boundary)
                    logger.verbose('\t' * (filtering_node_index + 2) + 'INTERSECT, checked all children')

    filtering_node.children = remaining_children
    if not remaining_children:
        filtering_node.filtered = True
        filtering_node.reason_of_filtered = "Filter Children: No children satisfy intersect boundary"
        logger.debug('\t' * (filtering_node_index + 1) + '###')
        logger.debug('\t' * (filtering_node_index + 1) + 'FILTERED: ' + filtering_node.reason_of_filtered)
        end_filter_children = timeit.default_timer()
        filtering_node.filter_children_time = end_filter_children - start_filter_children
        return

    logger.debug('\t' * filtering_node_index + 'Begin Filter Children Node link sql based on intersection range')
    cursor = {}
    for table_name in link_sql:
        cursor[table_name] = 0
        logger.verbose('\t' * (filtering_node_index + 1) + 'current link_sql[' + table_name + ']:' +
                       str([str(node) for node in link_sql[table_name]]))

    for filtering_node_child in filtering_node.children:
        # Assign link xml
        filtering_node_child.link_xml = link_xml.copy()

        # Safe measurement: Make sure that all child node can check the previous node just to be safe
        # TODO: bad implementation fix this
        for table_name in link_sql:
            if cursor[table_name] > 0:
                cursor[table_name] -= 1

        logger.verbose('\t' * (filtering_node_index + 1) + 'Checking children ' + str(filtering_node_child))

        allowed_intersection_range = {}
        for element in filtering_node.intersection_range:
            allowed_intersection_range[element] = [filtering_node.intersection_range[element][index] for index
                                                   in filtering_node_child.allowed_intersection_range_index]

        logger.verbose('\t' * (filtering_node_index + 1) + 'allowed intersection range: ')
        for element in filtering_node.intersection_range:
            logger.verbose('\t' * (filtering_node_index + 2) + element + str(allowed_intersection_range))

        this_child_link_sql = {}

        for table_name in filtering_node.link_sql:
            table_elements = table_name.split('_')

            logger.verbose('\t' * (filtering_node_index + 2) + 'Checking table: ' + table_name)
            this_child_link_sql[table_name] = []
            # TODO: Does this even make sense
            # TODO: Intergrate with the intersection range using allow_boundary_index
            # When moving to new node, if previous node already reach the end of table -> always check last node
            while cursor[table_name] < len(link_sql[table_name]):
                node = link_sql[table_name][cursor[table_name]]  # type: Node
                logger.verbose('\t' * (filtering_node_index + 3) + 'cursor: ' + str(cursor[table_name]) +
                               ' current table node: ' + str(node))
                if node.boundary[node.dimension][1] < filtering_node_child.boundary[1][0]:
                    logger.verbose('\t' * (filtering_node_index + 3) + 'NO INTERSECT, Move to next table node')
                    cursor[table_name] += 1
                elif node.boundary[node.dimension][0] > filtering_node_child.boundary[1][1]:
                    logger.verbose('\t' * (filtering_node_index + 3) + 'NO INTERSECT, Move to next table or end')
                    break
                else:
                    logger.verbose('\t' * (filtering_node_index + 3) + 'INTERSECT, proceed to check all elements')
                    satisfy_intersection_range = False
                    for allowed_index in filtering_node_child.allowed_intersection_range_index:
                        logger.verbose('\t' * (filtering_node_index + 4) + 'Checking allowed index: '
                                       + str(allowed_index))
                        all_elements_satisfy = True
                        for i in range(len(table_elements)):
                            element = table_elements[i]
                            element_intersect_boundary = filtering_node.intersection_range[element][allowed_index]
                            if not value_boundary_has_intersection(node.boundary[i], element_intersect_boundary):
                                logger.verbose('\t' * (filtering_node_index + 5) + element + ' NOT INTERSECT')
                                all_elements_satisfy = False
                                break
                        if all_elements_satisfy:
                            logger.verbose('\t' * (filtering_node_index + 5) + 'OK')
                            satisfy_intersection_range = True
                            break
                    if satisfy_intersection_range:
                        this_child_link_sql[table_name].append(node)
                        logger.verbose('\t' * (filtering_node_index + 3) + 'Add and move to next table node')
                        cursor[table_name] += 1

        filtering_node_child.link_sql = this_child_link_sql

    logger.debug('\t' * filtering_node_index + 'Result')
    for filtering_node_child in filtering_node.children:
        logger.debug('\t' * filtering_node_index + 'Children: ' + str(filtering_node_child))
        log_node_all_link(filtering_node_child, logger.debug, filtering_node_index + 1)

    end_filter_children = timeit.default_timer()
    filtering_node.filter_children_time = end_filter_children - start_filter_children


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

    def check_if_filtered(_filtering_node: Node, _logger, _start_time):
        if filtering_node.filtered:
            _logger.debug('\t' * filtering_node_index + '###')
            _logger.debug('\t' * filtering_node_index + 'FILTERED: ' + filtering_node.reason_of_filtered)
            _end_full_filtering = timeit.default_timer()
            filtering_node.full_filtering_time = _end_full_filtering - _start_time
            return True
        return False

    start_full_filtering = timeit.default_timer()
    logger = logging.getLogger("Main Filterer")

    filtering_node_index = all_elements_name.index(filtering_node.name)
    logger.debug('\t' * filtering_node_index + '--- Begin Filtering --- ' + str(filtering_node))
    if filtering_node.parent is None:
        logger.debug('\t' * (filtering_node_index + 1) + 'is Root')
    else:
        logger.debug('\t' * (filtering_node_index + 1) + 'Parent: ' + str(filtering_node.parent))
    logger.verbose('\t' * (filtering_node_index + 1) + 'Limit range: ' + str(limit_range))

    filtering_node_value_boundary = filtering_node.boundary[1]

    # Go through limit range and check if this node satisfy the limit range
    filtering_node_limit_range = limit_range[filtering_node.name]
    if value_boundary_has_intersection(filtering_node_limit_range, filtering_node_value_boundary):
        limit_range[filtering_node.name] = value_boundary_intersection(filtering_node_limit_range,
                                                                       filtering_node_value_boundary)
        logger.verbose('\t' * (filtering_node_index + 1) + 'Limit range updated based on node value' + str(limit_range))
    else:
        filtering_node.filtered = True
        filtering_node.reason_of_filtered = 'Main Filter: Filtered by limit range ' + str(limit_range)
        check_if_filtered(filtering_node, logger, start_full_filtering)
        return

    # If not value filtered before =>
    # Perform value filtering by checking all connected table and return limit ranges based on these tables
    if not filtering_node.value_filtering_visited:
        value_filtering(filtering_node, all_elements_name)

    if check_if_filtered(filtering_node, logger, start_full_filtering):
        return

    # If not filtered after value filtering => update limit range
    intersection_range = filtering_node.intersection_range
    for element in intersection_range:
        limit_range[element] = value_boundary_intersection(
            limit_range[element], value_boundaries_union(intersection_range[element]))
    logger.verbose('\t' * filtering_node_index + 'Limit range updated based on value filtering ' + str(limit_range))

    logger.debug('\t' * filtering_node_index + 'Begin Connected Element Filtering ' + str(filtering_node))
    connected_element_filtering(filtering_node, all_elements_name, limit_range)

    if check_if_filtered(filtering_node, logger, start_full_filtering):
        return

    limit_range = check_lower_level(filtering_node, all_elements_name, limit_range)

    if check_if_filtered(filtering_node, logger, start_full_filtering):
        return

    # Update children's links
    link_xml, link_sql = initialize_children_link(filtering_node, all_elements_name, limit_range)
    filter_children(filtering_node, all_elements_name, limit_range, link_xml, link_sql)

    end_full_filtering = timeit.default_timer()
    filtering_node.full_filtering_time = end_full_filtering - start_full_filtering

    return limit_range
