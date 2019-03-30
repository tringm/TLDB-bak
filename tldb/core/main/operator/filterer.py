import logging
import timeit
import queue
import copy
import numpy as np
from typing import Dict, List


# TODO: log last layer of node
# TODO: Implement RTree Range Search => find node in range, instead of going level by level (with depth for optimization)
# TODO: Implement caching for faster test iteration
# TODO: query given range load from loader

class Filterer:
    def __init__(self, loader, query_given_range):
        self.total_time = -1
        self.loader = loader
        self.query_given_range = query_given_range
        self.query_given_range = query_given_range
        self.elements = loader.all_elements_name
        self.roots = loader.all_elements_root

        self.main_trackers = ['n_processed', 'n_filtered', 'n_skipped']
        self.detail_trackers = ['n_flt_lr', 'n_flt_v', 'n_flt_c_e', 'n_flt_filter_desc', 
                                'n_flt_init_children_link', 'n_flt_filter_children', 'n_flt_filter_children_sql']

        self.status = dict(zip(self.main_trackers + self.detail_trackers,
                               np.zeros(len(self.main_trackers + self.detail_trackers))))

        self.status['n_processed'] = 0
        self.status['n_filtered'] = 0
        self.status['n_skipped'] = 0
        self.status['n_flt_lr'] = 0
        self.status['n_flt_v'] = 0
        self.status['n_flt_c_e'] = 0
        self.status['n_flt_filter_desc'] = 0
        self.status['n_flt_init_children_link'] = 0
        self.status['n_flt_filter_children'] = 0
        self.status['n_flt_filter_children_sql'] = 0

    def perform(self):
        logger = logging.getLogger("Filterer")
        # Push root of XML query RTree root node to queue
        start_filtering = timeit.default_timer()

        root = self.elements[0]
        queue_root_node = queue.Queue()
        queue_limit_range = queue.Queue()
        queue_root_node.put(self.roots[root])
        queue_limit_range.put(self.query_given_range)

        while not queue_root_node.empty():
            root_node = queue_root_node.get()  # type: XMLNode
            limit_range = queue_limit_range.get()
            updated_lr = self.node_full_filtering(root_node, limit_range)
            logger.debug('Filtering root node: ' + str(root_node))
            log_node_filter_status(root_node, logger.debug, 1)
            logger.debug('%s %3f', '\t' + 'Root node filter time: ', root_node.full_filtering_time)
            log_node_time_details(root_node, logger.verbose, 1)
            logger.debug('')

            if not root_node.filtered:
                for child in root_node.children:
                    queue_root_node.put(child)
                    queue_limit_range.put(copy.deepcopy(updated_lr))

        end_filtering = timeit.default_timer()
        self.total_time = end_filtering - start_filtering
        self.log_all_status(logger.info)

    def mark_node_as_filtered(self, node: XMLNode, filter_type: str, reason_of_filtered: str):
        possible_filter_types = ['limit_range', 'value_filter', 'connected_element', 'check_lower_level',
                                 'init_children_link', 'filter_children', 'filter_link_sql']
        if filter_type not in possible_filter_types:
            raise ValueError('Filter type: ' + filter_type + ' is wrong')

        if filter_type == 'limit_range':
            self.status['n_flt_lr'] += 1
        if filter_type == 'value_filter':
            self.status['n_flt_v'] += 1
            node.end_value_filtering = timeit.default_timer()
        if filter_type == 'connected_element':
            node.end_ce_filtering = timeit.default_timer()
            self.status['n_flt_c_e'] +=1
        if filter_type == 'check_lower_level':
            node.end_check_lower_level = timeit.default_timer()
            self.status['n_flt_filter_desc'] += 1
        if filter_type == 'init_children_link':
            node.end_init_children_link = timeit.default_timer()
            self.status['n_flt_init_children_link'] += 1
        if filter_type == 'filter_children':
            node.end_filter_children = timeit.default_timer()
            self.status['n_flt_filter_children'] += 1
        if filter_type == 'filter_children_link_sql':
            node.end_filter_children_link_sql = timeit.default_timer()
            self.status['n_flt_filter_children_sql'] += 1

        node.filtered = True
        node.reason_of_filtered = filter_type + ': ' + reason_of_filtered
        node.end_full_filtering = timeit.default_timer()
        self.status['n_filtered'] += 1

    def node_full_filtering(self, filtering_node: XMLNode, limit_range):
        """
        1. Perform value filtering by going through tables
        => update limit range for this node and other nodes in the checked tables
        2. Filter its connected element

        :param filtering_node:
        :param limit_range: Dict[str, List[int]]
                            constraint limit range of each element based on the order of all_elements_name
        :return: Dict[str, List[int]] or None
                 updated limit range or None if this node is filtered
        """
        self.status['n_processed'] += 1
        filtering_node.start_full_filtering = timeit.default_timer()

        logger = logging.getLogger("Full Filterer")

        f_n_idx = self.elements.index(filtering_node.name)
        logger.debug('\t' * f_n_idx + '--- Begin Filtering --- ' + str(filtering_node))
        if filtering_node.parent is None:
            logger.debug('\t' * (f_n_idx + 1) + 'is Root')
        else:
            logger.debug('\t' * (f_n_idx + 1) + 'Parent: ' + str(filtering_node.parent))
        logger.verbose('\t' * (f_n_idx + 1) + 'Limit range: ' + str(limit_range))

        if filtering_node.filtered:
            self.status['n_skipped'] += 1
            return

        # Go through limit range and check if this node satisfy the limit range
        limit_range[filtering_node.name] = value_boundary_intersection(limit_range[filtering_node.name],
                                                                       filtering_node.boundary[1])
        if limit_range[filtering_node.name] is None:
            if filtering_node.name == self.elements[0]:
                self.mark_node_as_filtered(filtering_node, 'limit_range', 'Node v_boundary not satisfy')
                log_node_filter_status(filtering_node, logger.verbose, f_n_idx + 1)
            else:
                self.status['n_skipped'] += 1
            return

        logger.verbose('\t' * (f_n_idx + 1) + 'Limit range updated based on node value' + str(limit_range))

        # If not value filtered before =>
        # Perform value filtering by checking all connected table and return limit ranges based on these tables
        if not filtering_node.value_filtering_visited:
            if self.loader.method == 'stripe':
                self.node_value_filter_stripe(filtering_node)
            else:
                self.node_value_filter_str(filtering_node)

        if filtering_node.filtered:
            return

        # Update limit range
        for e in filtering_node.intersection_range:
            limit_range[e] = value_boundary_intersection(limit_range[e],
                                                         value_boundaries_union(filtering_node.intersection_range[e]))
        logger.verbose('\t' * f_n_idx + 'Limit range updated based on value filtering ' + str(limit_range))
        for e in limit_range:
            if limit_range[e] is None:
                if filtering_node.name == self.elements[0]:
                    self.mark_node_as_filtered(filtering_node, 'limit_range',
                                               'node value interX range does not intersect')
                else:
                    self.status['n_skipped'] += 1
                return

        self.connected_element_filtering(filtering_node, limit_range)
        if filtering_node.filtered:
            return
        limit_range = self.check_lower_level(filtering_node, limit_range)
        logger.verbose('\t' * (f_n_idx + 1) + 'Limit range updated after check lower level:' + str(limit_range))
        if filtering_node.filtered:
            return

        # Update children's links
        link_xml, link_sql = self.init_children_link(filtering_node, limit_range)
        if filtering_node.filtered:
            return

        self.filter_children(filtering_node)
        children_link_sql = []
        if link_sql:
            children_link_sql = self.filter_children_link_sql(filtering_node, link_sql)

        # Assign link xml and link sql to children
        for idx, child in enumerate(filtering_node.children):
            child.link_xml = copy.deepcopy(link_xml)
            if children_link_sql:
                child.link_sql = children_link_sql[idx]
            else:
                child.link_sql = copy.deepcopy(link_sql)

        logger.verbose('\t' * (f_n_idx + 1) + 'Limit range:' + str(limit_range))
        filtering_node.end_full_filtering = timeit.default_timer()

        return limit_range

    def node_value_filter_stripe(self, filtering_node):
        """
            This function do value filtering by checking all connected tables' nodes in filtering_node.link_SQL
            Filter this node if exist one table that has no match for this filtering_node or tables doesn't match (matching
            nodes of A_B does not match with any nodes of A_B_C)
            Each node should be value filtered once
            Result:
                1. Updated link sql | Filtered
                2. Init intersection_range | Filtered
            """
        filtering_node.start_value_filtering = timeit.default_timer()
        logger = logging.getLogger("Value Filterer")
        logger.setLevel(logging.getLogger("MFull Filterer").level)

        f_n_idx = self.elements.index(filtering_node.name)
        logger.debug('\t' * f_n_idx + 'Begin Value Filtering ' + str(filtering_node))

        if not filtering_node.link_sql:
            logger.debug('\t' * (f_n_idx + 1) + 'EMPTY LINK_SQL')
            filtering_node.end_value_filtering = timeit.default_timer()
            return None

        filtering_node.value_filtering_visited = True

        # Always look for table with the most number of elements first
        table_names = list(filtering_node.link_sql.keys())
        table_names.sort(key=lambda name: len(name), reverse=True)

        # Pre-filter the link_sql nodes with filtering_node.boundaries
        # TODO: This is usually not needed since it's already done when init
        logger.verbose('\t' * f_n_idx + 'Begin Pre-Filter')
        for table_name in table_names:
            table_nodes = filtering_node.link_sql[table_name]  # type: [Node]
            if not table_nodes:
                self.mark_node_as_filtered(filtering_node, 'value_filter', table_name + ' is empty')
                log_node_filter_status(filtering_node, logger.debug, f_n_idx)
                return
            dimension = table_name.split('_').index(filtering_node.name)
            remaining_nodes = []
            i = 0
            while (table_nodes[i].boundary[dimension][1] < filtering_node.boundary[1][0]) and (
                    i < len(table_nodes) - 1):
                i += 1
            while table_nodes[i].boundary[dimension][0] <= filtering_node.boundary[1][1]:
                remaining_nodes.append(table_nodes[i])
                if i == len(table_nodes) - 1:
                    break
                i += 1

            if not remaining_nodes:
                self.mark_node_as_filtered(filtering_node, 'value_filter', 'linked_sql ' + table_name +
                                           ' has no node that intersects with value range')
                log_node_filter_status(filtering_node, logger.debug, f_n_idx)
                return
            filtering_node.link_sql[table_name] = remaining_nodes

        # init intersected_value_boundary
        intersected_value_boundary = []  # type: List[Dict[str, List[int]]]
        first_table_name = table_names[0]
        first_table_nodes = filtering_node.link_sql[first_table_name]  # type: List[Node]
        first_table_elements = first_table_name.split('_')  # type: [str]
        for node in first_table_nodes:
            intersected_value_boundary.append(dict(zip(first_table_elements, node.boundary)))

        logger.verbose('\t' * f_n_idx + 'Find intersected value boundaries')
        # Checking nodes of different table, if found no intersection -> Filter
        for table_name in table_names:
            if table_name != first_table_name:
                table_elements = table_name.split('_')
                table_nodes = filtering_node.link_sql[table_name]  # type: List[Node]
                logger.verbose('\t' * f_n_idx + 'Checking ' + table_name + ' Nodes: '
                               + str([node.boundary for node in table_nodes]))
                remaining_nodes = []
                boundary_cursor = 0
                table_cursor = 0
                updated_intersected_value_boundary = []

                while table_cursor < len(table_nodes) and boundary_cursor < len(intersected_value_boundary):
                    current_table_node = table_nodes[table_cursor]  # type: Node
                    current_boundary = intersected_value_boundary[boundary_cursor]  # type: {str, List[int]}
                    logger.verbose('\t' * (f_n_idx + 1) + 'current_table_node: ' + str(current_table_node))
                    logger.verbose(
                        '\t' * (f_n_idx + 1) + 'current_value_boundary: ' + str(current_boundary))

                    if current_table_node.boundary[current_table_node.dimension][0] > \
                            current_boundary[filtering_node.name][1]:
                        if boundary_cursor < len(intersected_value_boundary):
                            logger.verbose('\t' * (f_n_idx + 2) + 'NO INTERSECT, Move to next boundaries')
                            boundary_cursor += 1
                        else:
                            logger.verbose('\t' * (f_n_idx + 2) + 'NO INTERSECT, Skip remaining nodes')
                            table_cursor = len(table_nodes)
                    elif current_table_node.boundary[current_table_node.dimension][1] < \
                            current_boundary[filtering_node.name][0]:
                        if table_cursor < len(table_nodes):
                            logger.verbose('\t' * (f_n_idx + 2) + 'NO INTERSECT, Move to next table node')
                            table_cursor += 1
                        else:
                            logger.verbose('\t' * (f_n_idx + 2) + 'NO INTERSECT, Skip remaining nodes')
                            boundary_cursor = len(intersected_value_boundary)
                    else:
                        # if has intersection -> check other element
                        logger.verbose('\t' * (f_n_idx + 2) + 'INTERSECT, Find element-wise intersection')
                        all_elements_checked_ok = True
                        current_boundary_intersected = {}
                        # check element of the current node, if it intersect with the value boundaries
                        for idx, e in enumerate(table_elements):
                            if e not in current_boundary:
                                current_boundary_intersected[e] = current_table_node.boundary[idx]
                            else:
                                intersection = value_boundary_intersection(current_table_node.boundary[idx],
                                                                           current_boundary[e])
                                if intersection is not None:
                                    current_boundary_intersected[e] = intersection
                                else:
                                    all_elements_checked_ok = False
                                    break

                        # If all element check are satisfied -> add the intersected boundaries, add current_table_node
                        if all_elements_checked_ok:
                            # add element in current_value_boundary that not in current_node
                            for element in current_boundary:
                                if element not in table_elements:
                                    current_boundary_intersected[element] = current_boundary[element]
                            updated_intersected_value_boundary.append(current_boundary_intersected)
                            logger.verbose('\t' * (f_n_idx + 3) + 'element check OK ' +
                                           str(current_boundary_intersected))
                            if current_table_node not in remaining_nodes:
                                remaining_nodes.append(current_table_node)
                        else:
                            logger.verbose('\t' * (f_n_idx + 3) + 'element check failed')

                        # if current node already on the right side of value boundaries
                        #     1. Move right value boundaries if can
                        #     2. Else: all nodes on the right side of current_table_node is eliminated
                        if current_table_node.boundary[current_table_node.dimension][1] > \
                                current_boundary[filtering_node.name][1]:
                            if boundary_cursor < len(intersected_value_boundary):
                                logger.verbose('\t' * (f_n_idx + 2) + 'Move to next boundaries')
                                boundary_cursor += 1
                            else:
                                logger.verbose('\t' * (f_n_idx + 2) + 'SKip remaining table nodes')
                                table_cursor = len(table_nodes)
                        else:
                            if table_cursor < len(table_nodes):
                                logger.verbose('\t' * (f_n_idx + 2) + 'Move to next table node')
                                table_cursor += 1

                logger.verbose('\t' * f_n_idx + 'remaining nodes: '
                               + str([node.boundary for node in remaining_nodes]))
                logger.verbose('\t' * f_n_idx + 'Updated intersected boundaries: '
                               + str(updated_intersected_value_boundary))

                if not remaining_nodes:
                    if updated_intersected_value_boundary:
                        raise ValueError('Something wrong: has some remaining nodes but no intersected value')
                    self.mark_node_as_filtered(filtering_node, 'value_filter', table_name +
                                               'has no intersection with other tables')
                    log_node_filter_status(filtering_node, logger.debug, f_n_idx)
                    return

                intersected_value_boundary = updated_intersected_value_boundary
                filtering_node.link_sql[table_name] = remaining_nodes

        filtering_node.intersection_range = {}  # type: Dict[str, List[List[int]]]
        for element in intersected_value_boundary[0]:
            filtering_node.intersection_range[element] = [value_boundary[element]
                                                          for value_boundary in intersected_value_boundary]

        logger.debug('\t' * f_n_idx + 'Value Filter result')
        log_node_link_sql(filtering_node, logger.debug, f_n_idx + 1)
        log_node_intersection_range(filtering_node, logger.debug, f_n_idx + 1)
        filtering_node.end_value_filtering = timeit.default_timer()
        return

    def node_value_filter_str(self, filtering_node:XMLNode):
        """
        This function do value filtering by checking all connected tables' nodes in filtering_node.link_SQL
        Filter this node if exist one table that has no match for this filtering_node or tables doesn't match (matching
        nodes of A_B does not match with any nodes of A_B_C)
        Each node should be value filtered once
        Result:
            1. Updated link sql | Filtered
            2. Init intersection_range | Filtered
        """
        filtering_node.start_value_filtering = timeit.default_timer()
        logger = logging.getLogger("Value Filterer")
        logger.setLevel(logging.getLogger("Full Filterer").level)

        f_n_idx = self.elements.index(filtering_node.name)
        logger.debug('\t' * f_n_idx + 'Begin Value Filtering ' + str(filtering_node))

        filtering_node.value_filtering_visited = True

        if not filtering_node.link_sql:
            filtering_node.end_value_filtering = timeit.default_timer()
            logger.debug('\t' * (f_n_idx + 1) + 'EMPTY LINK_SQL')
            return

        # Always look for table with the most number of elements first
        table_names = list(filtering_node.link_sql.keys())
        table_names.sort(key=lambda name: len(name), reverse=True)

        # init join_range for each element
        join_range = []  # type: List[Dict[str, List[int]]]
        join_range.append({filtering_node.name: filtering_node.boundary[1]})
        # first_table_nodes = filtering_node.link_sql[table_names[0]]  # type: List[Node]
        # first_table_elements = table_names[0].split('_')  # type: [str]
        # for node in first_table_nodes:
        #     join_range.append(dict(zip(first_table_elements, node.boundaries)))
        # for idx, node in enumerate(first_table_nodes):
        #     join_range[idx][filtering_node.name] = value_boundary_intersection(node.boundaries[node.dimension],
        #                                                                        filtering_node.boundaries[1])

        logger.verbose('\t' * f_n_idx + 'Find intersected value boundaries')
        # Checking nodes of different table, if found no intersection -> Filter
        for table_name in table_names:
            logger.verbose('\t' * f_n_idx + 'Checking ' + table_name)
            table_elements = table_name.split('_')
            table_nodes = filtering_node.link_sql[table_name]  # type: List[Node]
            remaining_nodes = []
            updated_join_range = []

            for node in table_nodes:
                logger.verbose('\t' * (f_n_idx + 1) + 'Checking ' + str(node))
                node_ok = False
                for boundary in join_range:
                    logger.verbose('\t' * (f_n_idx + 2) + 'Checking boundaries' + str(boundary))
                    intersected_boundary = {}
                    all_elements_checked_ok = True
                    for idx, e in enumerate(table_elements):
                        if e not in boundary:
                            intersected_boundary[e] = node.boundary[idx]
                        else:
                            intersected_boundary[e] = value_boundary_intersection(node.boundary[idx], boundary[e])
                            if intersected_boundary[e] is None:
                                logger.verbose('\t' * (f_n_idx + 3) + 'Doesn\'t intersect at ' + e)
                                all_elements_checked_ok = False
                                break
                    if all_elements_checked_ok:
                        logger.verbose('\t' * (f_n_idx + 3) + 'Intersect')
                        node_ok = True
                        for element in boundary:
                            if element not in table_elements:
                                intersected_boundary[element] = boundary[element]
                        updated_join_range.append(intersected_boundary)

                if node_ok:
                    remaining_nodes.append(node)

            filtering_node.link_sql[table_name] = remaining_nodes
            join_range = updated_join_range
            logger.verbose('\t' * f_n_idx + 'remaining nodes: '
                           + str([node.boundary for node in remaining_nodes]))
            logger.verbose('\t' * f_n_idx + 'Updated join range: '
                           + str(join_range))

            if not remaining_nodes:
                if join_range:
                    raise ValueError('Something wrong: has some remaining nodes but no intersected value')
                self.mark_node_as_filtered(filtering_node, 'value_filter', table_name +
                                           'has no intersection with other tables')
                log_node_filter_status(filtering_node, logger.debug, f_n_idx)
                return
        if not join_range:
            self.mark_node_as_filtered(filtering_node, 'value_filter', 'No join results between table')
            log_node_filter_status(filtering_node, logger.debug, f_n_idx)
            return

        logger.verbose('\t' * f_n_idx + 'Update link sql')
        for table in table_names:
            logger.verbose('\t' * (f_n_idx + 1) + 'Table: ' + table)
            remaining_nodes = []
            elements = table.split('_')
            for node in filtering_node.link_sql[table]:
                node_ok = False
                for boundary in join_range:
                    all_elements_checked_ok = True
                    for i in range(len(elements)):
                        element = elements[i]
                        if value_boundary_intersection(node.boundary[i], boundary[element]) is None:
                            all_elements_checked_ok = False
                            break
                    if all_elements_checked_ok:
                        node_ok = True
                        break
                if node_ok:
                    remaining_nodes.append(node)
                    logger.verbose('\t' * (f_n_idx + 1) + 'Nodes: ' + str(remaining_nodes))
            filtering_node.link_sql[table] = remaining_nodes

        filtering_node.intersection_range = {}  # type: Dict[str, List[List[int]]]
        for element in join_range[0]:
            filtering_node.intersection_range[element] = [value_boundary[element] for value_boundary in join_range]

        logger.debug('\t' * f_n_idx + 'Value Filter result')
        log_node_link_sql(filtering_node, logger.debug, f_n_idx + 1)
        log_node_intersection_range(filtering_node, logger.debug, f_n_idx + 1)
        logger.debug("")
        filtering_node.end_value_filtering = timeit.default_timer()
        return

    def connected_element_filtering(self, filtering_node: XMLNode, limit_range: Dict[str, List[int]]):
        """
        This function check filtering_node connected element nodes if they can satisfy the limit range and structure
        requirement (ancestor, descendant)

        Result:
            1. Updated link xml | Filtered
        """
        filtering_node.start_ce_filtering = timeit.default_timer()
        logger = logging.getLogger("Connected Element Filterer")
        logger.setLevel(logging.getLogger("Full Filterer").level)
        f_n_idx = self.elements.index(filtering_node.name)
        logger.debug('\t' * f_n_idx + 'Connected element filtering')
        logger.debug('\t' * f_n_idx + 'Limit range ' + str(limit_range))

        if not filtering_node.link_xml:
            logger.debug('\t' * (f_n_idx + 1) + 'EMPTY LINK_XML')
            filtering_node.end_ce_filtering = timeit.default_timer()
            return

        # Always go by the highest element order
        connected_elements = list(filtering_node.link_xml.keys())
        connected_elements.sort(key=lambda element: self.elements.index(element))

        for c_e in connected_elements:
            logger.verbose('\t' * (f_n_idx + 1) + 'Checking: ' + c_e)

            c_e_nodes = filtering_node.link_xml[c_e]
            if not c_e_nodes:
                self.mark_node_as_filtered(filtering_node, 'connected_element', c_e + ' is empty')
                log_node_filter_status(filtering_node, logger.verbose, f_n_idx + 1)
                return

            remaining_nodes = []

            for c_e_node in c_e_nodes:
                if c_e_node.filtered:
                    logger.verbose(
                        '\t' * (f_n_idx + 2) + str(c_e_node) + ' Already FILTERED')
                    continue

                if not index_boundary_can_be_ancestor(filtering_node.boundary[0], c_e_node.boundary[0]):
                    logger.verbose('\t'*(f_n_idx + 2) + str(c_e_node) + ' REMOVED by structure filtering')
                    continue

                if value_boundary_intersection(c_e_node.boundary[1], limit_range[c_e]) is None:
                    logger.verbose('\t'*(f_n_idx + 2) + str(c_e_node) + ' REMOVED by value filtering')
                    continue
                logger.verbose('\t' * (f_n_idx + 2) + str(c_e_node) + ' OK')
                remaining_nodes.append(c_e_node)

            filtering_node.link_xml[c_e] = remaining_nodes

            if not remaining_nodes:
                self.mark_node_as_filtered(filtering_node, 'connected_element', c_e + ' has no satisfying node')
                log_node_filter_status(filtering_node, logger.verbose, f_n_idx + 1)
                return

        logger.debug('\t' * f_n_idx + 'Connected Element Filter result')
        log_node_link_xml(filtering_node, logger.debug, f_n_idx + 1)
        logger.debug("")
        filtering_node.end_ce_filtering = timeit.default_timer()

    def check_lower_level(self, filtering_node: XMLNode, limit_range: Dict[str, List[int]]) -> [] or None:
        """
        This function traverse to lower level of the XML Query to filter descendants
        Result:
            1. Filtered Descendant
            2. Updated limit range based on combined range of full_filtered children
            3. Updated link xml | Filtered
        """
        filtering_node.start_check_lower_level = timeit.default_timer()
        f_n_idx = self.elements.index(filtering_node.name)
        logger = logging.getLogger("Check Lower Level")
        logger.setLevel(logging.getLogger("Full Filterer").level)
        logger.debug('\t' * f_n_idx + 'Start check lower lever ' + str(filtering_node))

        # Value Filtering connected_element_nodes to get the updated limit range
        # TODO: Is this efficient, even though each node only has to value filtered once?
        connected_elements = list(filtering_node.link_xml.keys())
        connected_elements.sort(key=lambda e: self.elements.index(e))

        for c_e in connected_elements:
            logger.verbose('\t' * (f_n_idx + 1) + 'Traverse down ' + c_e)
            c_e_nodes = filtering_node.link_xml[c_e]
            remaining_nodes = []
            children_combined_lr = {}

            for node in c_e_nodes:
                child_lr = copy.deepcopy(limit_range)
                update_child_lr = self.node_full_filtering(node, child_lr)
                if node.filtered or update_child_lr is None:
                    continue
                remaining_nodes.append(node)
                for e in self.elements:
                    if e not in children_combined_lr:
                        children_combined_lr[e] = update_child_lr[e]
                    else:
                        children_combined_lr[e] = value_boundary_union(children_combined_lr[e], update_child_lr[e])

            if not remaining_nodes:
                self.mark_node_as_filtered(filtering_node, 'check_lower_level',
                                           'None of ' + c_e + ' nodes pass full filtering')
                log_node_filter_status(filtering_node, logger.verbose, f_n_idx + 1)
                return

            filtering_node.link_xml[c_e] = remaining_nodes
            # Update limit range by doing intersection with combined limit range of all children
            for e in self.elements:
                limit_range[e] = value_boundary_intersection(limit_range[e], children_combined_lr[e])

        logger.debug("")
        filtering_node.end_check_lower_level = timeit.default_timer()
        return limit_range

    def init_children_link(self, filtering_node: XMLNode, limit_range):
        """
        This function initialize children link_xml and link_sql of a filtered node
        Result:
            1. Filtered children link xml based on structure filtering
            2. Filtered children link sql based on limit range
            3. Or FILTERED
        """
        filtering_node.start_init_children_link = timeit.default_timer()
        f_n_idx = self.elements.index(filtering_node.name)
        logger = logging.getLogger("Init Children Link")
        logger.setLevel(logging.getLogger("Full Filterer").level)
        logger.debug('\t' * f_n_idx + 'Start initialize children link ' + str(filtering_node))

        if not filtering_node.children:
            logger.debug('\t' * (f_n_idx + 1) + ' This is a leaf node ')
            filtering_node.end_init_children_link = timeit.default_timer()
            return [], []
        logger.verbose('\t'*f_n_idx + 'Limit range: ' + str(limit_range))

        def init_link_xml():
            logger.verbose('\t' * f_n_idx +
                           'Init link_xml: c_e_node children that satisfy value and structure requirement')
            if not filtering_node.link_xml:
                logger.verbose('\t' * (f_n_idx + 1) + 'Link xml is empty')
                return {}

            connected_elements = list(filtering_node.link_xml.keys())
            connected_elements.sort(key=lambda element: self.elements.index(element))
            children_link_xml = {}
            for c_e in connected_elements:
                children_link_xml[c_e] = []

                for node in filtering_node.link_xml[c_e]:
                    logger.verbose('\t' * (f_n_idx + 1) + 'Checking ' + str(node))
                    if node.isLeaf:
                        logger.verbose('\t' * (f_n_idx + 2) + ' is Leaf')
                        children_link_xml[c_e].append(node)
                    else:
                        for child in node.children:
                            if not index_boundary_can_be_ancestor(filtering_node.boundary[0], child.boundary[0]):
                                logger.verbose('\t' * (f_n_idx + 2) + str(child) +
                                               ' IGNORED: not satisfy structure requirement')
                                continue
                            if value_boundary_intersection(child.boundary[1], limit_range[c_e]) is None:
                                logger.verbose('\t' * (f_n_idx + 2) + str(child) +
                                               ' IGNORED: not satisfy limit_range')
                                continue
                            logger.verbose('\t' * (f_n_idx + 2) + str(child) + 'OK')
                            children_link_xml[c_e].append(child)

                if not children_link_xml[c_e]:
                    self.mark_node_as_filtered(filtering_node, 'init_children_link',
                                               'No child node of ' + c_e + ' satisfy limit range or structure')
                    log_node_filter_status(filtering_node, logger.verbose, f_n_idx + 1)
                    return {}
            return children_link_xml

        def init_link_sql():
            logger.verbose('\t' * f_n_idx +
                           'Init link_sql: link_sql children node that satisfy limit range')
            logger.verbose('\t' * f_n_idx + 'limit_range ' + str(limit_range))
            children_link_sql = {}
            for table in filtering_node.link_sql:
                children_link_sql[table] = []
                table_elements = table.split('_')
                for node in filtering_node.link_sql[table]:
                    logger.verbose('\t' * (f_n_idx + 1) + 'Checking ' + str(node))
                    if node.isLeaf:
                        logger.verbose('\t' * (f_n_idx + 2) + ' is Leaf')
                        children_link_sql[table].append(node)
                    else:
                        for child in node.children:
                            node_satisfy = True
                            for idx, e in enumerate(table_elements):
                                if value_boundary_intersection(child.boundary[idx], limit_range[e]) is None:
                                    node_satisfy = False
                                    logger.debug('\t' * (f_n_idx + 2) + 'Child ' + str(child) +
                                                 ' IGNORED: not satisfy limit range')
                                    break
                            if node_satisfy:
                                logger.debug('\t' * (f_n_idx + 2) + 'Child ' + str(child) + 'OK')
                                children_link_sql[table].append(child)
                if not children_link_sql[table]:
                    self.mark_node_as_filtered(filtering_node, 'init_children_link',
                                               'No child node of ' + table + ' satisfy limit range')
                    log_node_filter_status(filtering_node, logger.verbose, f_n_idx + 1)
                    return {}
            return children_link_sql

        link_xml = init_link_xml()
        if filtering_node.filtered:
            return {}, {}
        link_sql = init_link_sql()
        if filtering_node.filtered:
            return {}, {}
        logger.debug('\t'*f_n_idx + 'Result')
        logger.debug('\t'*(f_n_idx + 1) + 'Inited link_xml: ' + str(link_xml))
        logger.debug('\t' * (f_n_idx + 1) + 'Inited link_sql: ' + str(link_sql))
        logger.debug("")
        filtering_node.end_init_children_link = timeit.default_timer()
        return link_xml, link_sql

    def filter_children(self, filtering_node: XMLNode):
        """
        This function filter its children based on the intersection range
        :param filtering_node:
        :return:
        """
        filtering_node.start_filter_children = timeit.default_timer()
        f_n_idx = self.elements.index(filtering_node.name)
        logger = logging.getLogger("Filter Children")
        logger.setLevel(logging.getLogger("Full Filterer").level)
        logger.debug('\t' * f_n_idx + 'Begin Filter Children')

        if filtering_node.isLeaf:
            logger.debug('\t' * f_n_idx + 'is a leaf node')
            filtering_node.end_filter_children = timeit.default_timer()
            return

        if not filtering_node.intersection_range:
            logger.debug('\t' * f_n_idx + 'No intersection range')
            filtering_node.end_filter_children = timeit.default_timer()
            return

        # if child does not fall into any part range of the intersection_range[filtering_node.name] => filtered
        boundaries = filtering_node.intersection_range[filtering_node.name]
        logger.verbose('\t' * f_n_idx + 'Current children: ' + str(filtering_node.children))
        logger.verbose('\t' * f_n_idx + 'allowed_value_boundary: ' + str(boundaries))

        remaining_children = []
        for child in filtering_node.children:
            child._intersection_boundary_index = []
            for i in range(len(boundaries)):
                boundary = boundaries[i]
                if value_boundary_intersection(child.boundary[1], boundary) is not None:
                    child._intersection_boundary_index.append(i)
            if child._intersection_boundary_index:
                remaining_children.append(child)

        filtering_node.children = remaining_children
        if not remaining_children:
            self.mark_node_as_filtered(filtering_node, 'filter_children', 'No children satisfy intersect boundaries')
            log_node_filter_status(filtering_node, logger.verbose, f_n_idx + 1)
            return
        logger.debug('\t'*f_n_idx + 'Result: ' + str(filtering_node.children))
        logger.debug("")
        filtering_node.end_filter_children = timeit.default_timer()

    def filter_children_link_sql(self, filtering_node: XMLNode, link_sql):
        """
        This function filter the filtering_node children and its link _sql based on the intersection range
        and assign the link xml and link sql
        Result:
            1. Assign link xml to children
            2. Assign filtered link sql to children
        :param filtering_node:
        :param link_sql:
        :return:
        """
        filtering_node.start_filter_children_link_sql = timeit.default_timer()
        f_n_idx = self.elements.index(filtering_node.name)
        logger = logging.getLogger("Filter Children Link SQL")
        logger.setLevel(logging.getLogger("Full Filterer").level)

        if filtering_node.isLeaf:
            logger.debug('\t' * (f_n_idx + 1) + 'is a leaf node')
            filtering_node.end_filter_link_sql = timeit.default_timer()
            return []

        # if child does not fall into any part range of the intersection_range[filtering_node.name]
        # => filtered and not init
        # Similar execution to value filtering
        logger.debug('\t' * f_n_idx + 'Begin Filter Children Node link sql based on intersection range')
        if not filtering_node.intersection_range:
            logger.debug('\t' * f_n_idx + 'No intersection range')
            filtering_node.end_filter_link_sql = timeit.default_timer()
            children_link_sql = []
            for child in filtering_node.children:
                children_link_sql.append(link_sql)
            return children_link_sql

        for table in link_sql:
            logger.verbose('\t'*(f_n_idx + 1) + 'current link_sql[' + table + ']:' + str(link_sql[table]))
        remaining_children = []
        children_link_sql = []
        for child in filtering_node.children:
            logger.verbose('\t' * (f_n_idx + 1) + 'Checking children ' + str(child))
            logger.verbose('\t' * (f_n_idx + 1) + 'Getting allowed range ' + str(child))
            allowed_intersection_range = {}
            for e in filtering_node.intersection_range:
                allowed_intersection_range[e] = [filtering_node.intersection_range[e][idx]
                                                 for idx in child._intersection_boundary_index]
                logger.verbose('\t'*(f_n_idx + 2) + e + ': ' + str(allowed_intersection_range[e]))

            child_link_sql = {}
            child_ok = True

            for table in filtering_node.link_sql:
                table_elements = table.split('_')
                child_link_sql[table] = []
                for tbl_node in link_sql[table]:
                    node_ok = False
                    for idx in child._intersection_boundary_index:
                        intersect = True
                        for d, e in enumerate(table_elements):
                            if value_boundary_intersection(tbl_node.boundary[d],
                                                           filtering_node.intersection_range[e][idx]) is None:
                                intersect = False
                                break
                        if intersect:
                            logger.verbose('\t' * (f_n_idx + 2) + 'Table node: ' + str(tbl_node) + ' OK')
                            node_ok = True
                            break
                    if node_ok:
                        child_link_sql[table].append(tbl_node)
                if not child_link_sql[table]:
                    logger.verbose('\t' * (f_n_idx + 1) + 'This child does not have ' + table + ' nodes')
                    child_ok = False
                    break

            if child_ok:
                remaining_children.append(child)
                children_link_sql.append(child_link_sql)

        if not remaining_children:
            self.mark_node_as_filtered(filtering_node, 'filter_children_link_sql', 'No remaining children')
            log_node_filter_status(filtering_node, logger.verbose, f_n_idx + 1)

        filtering_node.children = remaining_children
        logger.debug('\t'*f_n_idx + 'Result')
        for idx, child in enumerate(filtering_node.children):
            logger.debug('\t'*(f_n_idx + 1) + 'Child ' + str(child) + str(children_link_sql[idx]))

        logger.debug("")
        filtering_node.end_filter_children_link_sql = timeit.default_timer()
        return children_link_sql

    def log_trackers(self, logger_function, tracker_type, n_prefix_tab=0):
        if tracker_type == 'main':
            for tracker in self.main_trackers:
                logger_function('%s %s %d', '\t' * n_prefix_tab, tracker, self.status[tracker])
        if tracker_type == 'details':
            for tracker in self.detail_trackers:
                logger_function('%s %s %d', '\t' * n_prefix_tab, tracker, self.status[tracker])

    def log_all_status(self, logger_function, n_prefix_tab=0):
        logger_function('%s %s %3f', '\t' * n_prefix_tab, 'Total time:', self.total_time)
        self.log_trackers(logger_function, 'main', n_prefix_tab)
        self.log_trackers(logger_function, 'details', n_prefix_tab)
        logger_function('%s %s %3f', '\t' * n_prefix_tab, 'Average filtering time for one node:',
                        self.total_time / self.status['n_processed'])

