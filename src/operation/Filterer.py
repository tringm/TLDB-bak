import logging
import timeit
import queue
import copy
from src.io_support.logger_support import *
from src.lib.boundary import value_boundary_intersection


# TODO: Implement RTree Range Search => find node in range, instead of going level by level (with depth for optimization)
# Implement caching for faster test iteration

class Filterer:
    def __init__(self, loader, initial_limit_range):
        self.total_time = -1
        self.loader = loader
        self.initial_limit_range = initial_limit_range
        self.elements = loader.all_elements_name
        self.roots = loader.all_elements_root
        self.n_node_processed = 0
        self.n_node_filtered = 0
        self.n_node_filtered_by_limit_range = 0
        self.n_node_filtered_by_value = 0
        self.n_node_filtered_by_connected_element = 0
        self.n_node_filtered_by_init_children = 0

    def perform(self):
        logger = logging.getLogger("Filterer")
        # Push root of XML query RTree root node to queue
        start_filtering = timeit.default_timer()

        root = self.elements[0]
        queue_root_node = queue.Queue()
        queue_limit_range = queue.Queue()
        queue_root_node.put(self.elements[root])
        queue_limit_range.put(self.initial_limit_range)

        while not queue_root_node.empty():
            root_node = queue_root_node.get()
            limit_range = queue_limit_range.get()
            updated_limit_range = self.node_full_filtering(root_node, limit_range)
            logger.debug('Filtering root node: ' + str(root_node))
            log_node_filter_status(root_node, logger.debug, 1)
            logger.debug('%s %3f', '\t' + 'Root node filter time: ', root_node.full_filtering_time)
            log_node_time_details(root_node, logger.verbose, 1)
            logger.debug('')

            if not root_node.filtered:
                for child in root_node.children:
                    queue_root_node.put(child)
                    queue_limit_range.put(copy.deepcopy(updated_limit_range))

        end_filtering = timeit.default_timer()
        self.total_time = end_filtering - start_filtering

    def mark_node_as_filtered(self, node: XMLNode, filter_type, reason_of_filtered):
        possible_filter_types = ['limit_range', 'value', 'connected_element', 'children']
        if filter_type not in possible_filter_types:
            raise ValueError('Filter type: ' + filter_type + ' is wrong')
        node.filtered = True
        node.reason_of_filtered = reason_of_filtered
        node.end_full_filtering = timeit.default_timer()
        self.n_node_filtered += 1

        if filter_type == 'limit_range':
            self.n_node_filtered_by_limit_range += 1
        if filter_type == 'value':
            self.n_node_filtered_by_value += 1
            node.end_value_filtering = timeit.default_timer
        if filter_type == 'connected_element':
            self.n_node_filtered_by_connected_element +=1
        if filter_type == 'children':
            self.n_node_filtered_by_init_children += 1


    def node_full_filtering(self, filtering_node, limit_range):
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
        self.n_node_processed += 1
        filtering_node.start_full_filtering = timeit.default_timer()

        logger = logging.getLogger("Node Full Filterer")

        filtering_node_index = self.elements.index(filtering_node.name)
        logger.debug('\t' * filtering_node_index + '--- Begin Filtering --- ' + str(filtering_node))
        if filtering_node.parent is None:
            logger.debug('\t' * (filtering_node_index + 1) + 'is Root')
        else:
            logger.debug('\t' * (filtering_node_index + 1) + 'Parent: ' + str(filtering_node.parent))
        logger.verbose('\t' * (filtering_node_index + 1) + 'Limit range: ' + str(limit_range))

        # Go through limit range and check if this node satisfy the limit range
        intersected_lr = value_boundary_intersection(limit_range[filtering_node.name], filtering_node.boundary[1])
        if intersected_lr is not None:
            limit_range[filtering_node.name] = intersected_lr
            logger.verbose('\t' * (filtering_node_index + 1) +
                           'Limit range updated based on node value' + str(limit_range))
        else:
            self.mark_node_as_filtered(filtering_node, 'limit_range',
                                       'Main Filter: Filtered by limit range ' + str(limit_range))
            log_node_filter_status(filtering_node, logger.debug, filtering_node_index)
            return None

        # If not value filtered before =>
        # Perform value filtering by checking all connected table and return limit ranges based on these tables
        if not filtering_node.value_filtering_visited:
            if self.loader.method == 'stripe':
                self.node_value_filter_stripe(filtering_node, limit_range)
            else:
                self.node_value_filter_str(filtering_node, limit_range)

        #
        # if check_if_filtered(filtering_node, logger, start_full_filtering):
        #     return
        #
        # # If not filtered after value filtering => update limit range
        # intersection_range = filtering_node.intersection_range
        # for element in intersection_range:
        #     limit_range[element] = value_boundary_intersection(
        #         limit_range[element], value_boundaries_union(intersection_range[element]))
        # logger.verbose(
        #     '\t' * filtering_node_index + 'Limit range updated based on value filtering ' + str(limit_range))
        #
        # logger.debug('\t' * filtering_node_index + 'Begin Connected Element Filtering ' + str(filtering_node))
        # connected_element_filtering(filtering_node, all_elements_name, limit_range)
        #
        # if check_if_filtered(filtering_node, logger, start_full_filtering):
        #     return
        #
        # limit_range = check_lower_level(filtering_node, all_elements_name, limit_range)
        #
        # if check_if_filtered(filtering_node, logger, start_full_filtering):
        #     return
        #
        # # Update children's links
        # link_xml, link_sql = initialize_children_link(filtering_node, all_elements_name, limit_range)
        # if check_if_filtered(filtering_node, logger, start_full_filtering):
        #     return
        #
        # filter_children(filtering_node, all_elements_name, limit_range, link_xml, link_sql)
        #
        # end_full_filtering = timeit.default_timer()
        # filtering_node.full_filtering_time = end_full_filtering - start_full_filtering
        #
        # return limit_range

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
        logger = logging.getLogger("Node Value Filterer")
        logger.setLevel(logging.getLogger("MNode Full Filterer").level)

        filtering_node_index = self.elements.index(filtering_node.name)
        logger.debug('\t' * filtering_node_index + 'Begin Value Filtering ' + str(filtering_node))

        if not filtering_node.link_sql:
            logger.debug('\t' * (filtering_node_index + 1) + 'EMPTY LINK_SQL')
            filtering_node.end_value_filtering = timeit.default_timer()
            return None

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
                self.mark_node_as_filtered(filtering_node, 'value', 'Value Filter: ' + table_name + ' is empty')
                log_node_filter_status(filtering_node, logger.debug, filtering_node_index)
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
                self.mark_node_as_filtered(filtering_node, 'value', 'Value Filter: linked_sql ' + table_name +
                                           ' has no node that intersects with value range')
                log_node_filter_status(filtering_node, logger.debug, filtering_node_index)
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
                    logger.verbose(
                        '\t' * (filtering_node_index + 1) + 'current_value_boundary: ' + str(current_boundary))

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
                                intersection = value_boundary_intersection(current_table_node.boundary[i],
                                                                           current_boundary[element])
                                if intersection is not None:
                                    current_boundary_intersected[element] = intersection
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
                    self.mark_node_as_filtered(filtering_node, 'value', 'Value Filter: ' + table_name +
                                               'has no intersection with other tables')
                    log_node_filter_status(filtering_node, logger.debug, filtering_node_index)
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
        filtering_node.end_value_filtering = timeit.default_timer()
        return

    def node_value_filter_str(self, filtering_node):
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
        logger = logging.getLogger("Node Value Filterer")
        logger.setLevel(logging.getLogger("Node Full Filterer").level)

        filtering_node_index = self.elements.index(filtering_node.name)
        logger.debug('\t' * filtering_node_index + 'Begin Value Filtering ' + str(filtering_node))

        if not filtering_node.link_sql:
            logger.debug('\t' * (filtering_node_index + 1) + 'EMPTY LINK_SQL')
            filtering_node.end_value_filtering = timeit.default_timer()
            return None

        filtering_node.value_filtering_visited = True

        # Always look for table with the most number of elements first
        table_names = list(filtering_node.link_sql.keys())
        table_names.sort(key=lambda name: len(name), reverse=True)

        # init join_range for each element
        join_range = []  # type: List[Dict[str, List[int]]]
        first_table_nodes = filtering_node.link_sql[table_names[0]]  # type: List[Node]
        first_table_elements = table_names[0].split('_')  # type: [str]
        for node in first_table_nodes:
            join_range.append(dict(zip(first_table_elements, node.boundary)))

        # # Pre-filter the link_sql nodes with filtering_node.boundary
        # # TODO: This is usually not needed since it's already done when init
        # logger.verbose('\t' * filtering_node_index + 'Begin Pre-Filter')
        # for table_name in table_names:
        #     table_nodes = filtering_node.link_sql[table_name]  # type: [Node]
        #     if not table_nodes:
        #         self.mark_node_as_filtered(filtering_node, 'value', 'Value Filter: ' + table_name + ' is empty')
        #         log_node_filter_status(filtering_node, logger.debug, filtering_node_index)
        #         return
        #     dimension = table_name.split('_').index(filtering_node.name)
        #     remaining_nodes = []
        #     for table_node in table_nodes:
        #         join_range
        #     i = 0
        #     while (table_nodes[i].boundary[dimension][1] < filtering_node.boundary[1][0]) and (
        #             i < len(table_nodes) - 1):
        #         i += 1
        #     while table_nodes[i].boundary[dimension][0] <= filtering_node.boundary[1][1]:
        #         remaining_nodes.append(table_nodes[i])
        #         if i == len(table_nodes) - 1:
        #             break
        #         i += 1
        #
        #     if not remaining_nodes:
        #         self.mark_node_as_filtered(filtering_node, 'value', 'Value Filter: linked_sql ' + table_name +
        #                                    ' has no node that intersects with value range')
        #         log_node_filter_status(filtering_node, logger.debug, filtering_node_index)
        #         return
        #     filtering_node.link_sql[table_name] = remaining_nodes
        logger.verbose('\t' * filtering_node_index + 'Find intersected value boundary')
        # Checking nodes of different table, if found no intersection -> Filter
        for table_name in table_names[1:]:
            logger.verbose('\t' * filtering_node_index + 'Checking ' + table_name)
            table_elements = table_name.split('_')
            table_nodes = filtering_node.link_sql[table_name]  # type: List[Node]
            remaining_nodes = []
            updated_join_range = []

            for node in table_nodes:
                logger.verbose('\t' * (filtering_node_index + 1) + 'Checking ' + str(node))
                node_ok = False
                for boundary in join_range:
                    logger.verbose('\t' * (filtering_node_index + 2) + 'Checking boundary' + str(boundary))
                    intersected_boundary = {}
                    all_elements_checked_ok = True
                    for i in range(len(table_elements)):
                        element = table_elements[i]
                        if element not in boundary:
                            intersected_boundary[element] = node.boundary[i]
                        else:
                            intersected_boundary[element] = value_boundary_intersection(node.boundary[i], boundary[element])
                            if intersected_boundary[element] is None:
                                logger.verbose('\t' * (filtering_node_index + 3) + 'Doesn\'t intersect at ' + element)
                                all_elements_checked_ok = False
                                break
                    if all_elements_checked_ok:
                        logger.verbose('\t' * (filtering_node_index + 3) + 'Intersect')
                        node_ok = True
                        for element in boundary:
                            if element not in table_elements:
                                intersected_boundary[element] = boundary[element]
                        updated_join_range.append(intersected_boundary)

                if node_ok:
                    remaining_nodes.append(node)

            filtering_node.link_sql[table_name] = remaining_nodes
            join_range = updated_join_range
            logger.verbose('\t' * filtering_node_index + 'remaining nodes: '
                           + str([node.boundary for node in remaining_nodes]))
            logger.verbose('\t' * filtering_node_index + 'Updated join range: '
                           + str(join_range))

            if not remaining_nodes:
                if join_range:
                    raise ValueError('Something wrong: has some remaining nodes but no intersected value')
                self.mark_node_as_filtered(filtering_node, 'value', 'Value Filter: ' + table_name +
                                           'has no intersection with other tables')
                log_node_filter_status(filtering_node, logger.debug, filtering_node_index)
                return

        for table in table_names:
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
            filtering_node.link_sql[table] = remaining_nodes

        filtering_node.intersection_range = {}  # type: Dict[str, List[List[int]]]
        for element in join_range[0]:
            filtering_node.intersection_range[element] = [value_boundary[element] for value_boundary in join_range]

        logger.debug('\t' * filtering_node_index + 'Value Filter result')
        log_node_link_sql(filtering_node, logger.debug, filtering_node_index + 1)
        log_node_intersection_range(filtering_node, logger.debug, filtering_node_index + 1)
        filtering_node.end_value_filtering = timeit.default_timer()
        return

    def log_status(self, logger_function, n_prefix_tab=0):
        logger_function('%s %s %3f', '\t' * n_prefix_tab, 'Total time:', self.total_time)
        logger_function('%s %s %d', '\t' * n_prefix_tab, 'Number of node processed:', self.n_node_processed)
        logger_function('%s %s %d', '\t' * n_prefix_tab, 'Number of node filtered:', self.n_node_filtered)
        logger_function('%s %s %3f', '\t' * n_prefix_tab, 'Average filtering time for one node:',
                        self.total_time / self.n_node_processed)

