import copy
import logging
import queue
import timeit
from typing import Dict, List

from src.io_support.logger_support import *
from src.lib.boundary import *
from src.structure.Context import Context
from src.structure.Node import SQLNode


# TODO: c_e should follow order of query, not link_xml (unordered dict)


class Filterer:
    def __init__(self, loader, query_given_range):
        self.total_time = -1
        self.loader = loader
        self.query_given_range = query_given_range
        self.elements = loader.all_elements_name
        self.roots = loader.all_elements_root
        self.logger = logging.getLogger('Filterer')

        self.status = {'n_processed': 0, 'n_filtered': 0}

    def mark_node_as_filtered(self, node: XMLNode, filter_type: str, reason: str, _l=logging.getLogger('Filter')):
        possible_filter_types = ['init_link']
        if filter_type not in possible_filter_types:
            raise ValueError('Filter type: ' + filter_type + ' is wrong')

        if filter_type == 'init_link':
            node.end_init_link = timeit.default_timer()

        node.filtered = True
        node.reason_of_filtered = filter_type + ': ' + reason

        self.status['n_filtered'] += 1

        log_node_filter_status(node, _l.debug, self.elements.index(node.name))

    def perform(self):
        _l = self.logger
        # Push root of XML query RTree root node to queue
        start_filtering = timeit.default_timer()

        initial_context = Context(self.elements)
        initial_context.boundaries = self.query_given_range

        root = self.elements[0]
        queue_root_node = queue.Queue()
        queue_contexts = queue.Queue()
        queue_root_node.put(self.roots[root])
        queue_contexts.put([initial_context])

        while not queue_root_node.empty():
            root_node = queue_root_node.get()  # type: XMLNode
            contexts = queue_contexts.get()
            _l.debug('Filtering ' + str(root_node) + 'with contexts:' + str(contexts))
            success, updated_contexts = self.filter_with_context(root_node,contexts)

            if not root_node.filtered and success:
                _l.info(str(root_node) + ' OK, returned contexts: ' + str(updated_contexts))

                _l.debug('\tAdd children ' + str(root_node.link_children))
                _l.debug('\tWith contexts:' + str(updated_contexts))
                for child in root_node.link_children:
                    queue_root_node.put(child)
                    queue_contexts.put(copy.deepcopy(updated_contexts))
            else:
                _l.info(str(root_node) + ' not OK')
                log_node_filter_status(root_node, _l.info)
                _l.info('Returned context: ' + str(updated_contexts))
            _l.info("")

        end_filtering = timeit.default_timer()
        self.total_time = end_filtering - start_filtering
        # self.log_all_status(_l.info)

    def init_link(self, flt_node: XMLNode):
        _l = logging.getLogger("Init Link: "+str(flt_node))
        _l.setLevel(logging.getLogger("Init Link").level)

        f_n_idx = self.elements.index(flt_node.name)

        flt_node.inited_link = True
        flt_node.start_init_link = timeit.default_timer()

        def init_link_sql():
            _l.debug('\t' * f_n_idx + 'Init Link SQL')
            ancestor_link_sql = flt_node.link_sql_first_ancestor()

            if not ancestor_link_sql:
                _l.debug('\t' * (f_n_idx + 1) + 'Ancestor Link SQL is empty')
                return True, {}, {}, {}

            tables = sorted(list(ancestor_link_sql.keys()), key=lambda tbl: len(tbl), reverse=True)
            link_sql = {}
            join_boundaries = []

            for table in tables:
                # Find nodes in table that match with v_range
                _l.verbose('\t' * f_n_idx + 'Checking table: ' + table)
                starters = ancestor_link_sql[table]  # type: List[SQLNode]
                table_nodes = []
                for starter in starters:
                    boundaries = [None] * len(starter.boundary)
                    boundaries[starter.dimension] = flt_node.v_boundary
                    nodes_in_range = starter.range_search(boundaries)
                    if nodes_in_range is not None:
                        for node in nodes_in_range:
                            table_nodes.append(node)
                _l.verbose('\t' * (f_n_idx + 1) + 'table nodes match v_range ' + str(table_nodes))
                if not table_nodes:
                    self.mark_node_as_filtered(flt_node, 'init_link',
                                               'No node in table: ' + table + ' : matches v_range', _l)
                    return False, None, None, None
                # Check join boundaries
                table_e = table.split('_')
                _l.verbose('\t' * (f_n_idx + 1) + 'Checking with join boundaries')
                if not join_boundaries:
                    link_sql[table] = []
                    for node in table_nodes:
                        join_boundaries.append(dict(zip(table_e, node.boundary)))
                        link_sql[table].append(node)
                else:
                    updated_join_b = []
                    remaining_nodes = []
                    for node in table_nodes:
                        node_ok = False
                        for join_b in join_boundaries:
                            interx_b = {}
                            all_e_ok = True
                            for idx, e in enumerate(table_e):
                                if e not in join_b:
                                    interx_b[e] = node.boundary[idx]
                                else:
                                    interx_b[e] = value_boundary_intersection(node.boundary[idx], join_b[e])
                                    if interx_b[e] is None:
                                        _l.verbose('\t' * (f_n_idx + 2) + str(node) + ': ' + str(join_b) +
                                                   ' not interX at ' + e)
                                        all_e_ok = False
                                        break
                            if all_e_ok:
                                _l.verbose('\t' * (f_n_idx + 2) + str(node) + ': ' + str(join_b) + ' interX')
                                node_ok = True
                                for e in join_b:
                                    if e not in table_e:
                                        interx_b[e] = join_b[e]
                                updated_join_b.append(interx_b)
                        if node_ok:
                            remaining_nodes.append(node)

                    link_sql[table] = remaining_nodes
                    join_boundaries = updated_join_b

                _l.verbose('\t' * (f_n_idx + 1) + 'remaining nodes: ' +
                           str([node.boundary for node in link_sql[table]]))
                _l.verbose('\t' * (f_n_idx + 1) + 'Updated join boundaries: ' + str(join_boundaries))
                if not link_sql[table]:
                    if join_boundaries:
                        raise ValueError('Something wrong: has no remaining nodes but some interx boundaries')
                    self.mark_node_as_filtered(flt_node, 'init_link', table + ' not join with others', _l)
                    return False, None, None, None

            if not join_boundaries:
                self.mark_node_as_filtered(flt_node, 'init_link', 'No join results between table', _l)
                return False, None, None, None

            _l.verbose('\t' * f_n_idx + 'Update link sql based on join boundaries')
            for table in tables:
                _l.verbose('\t' * (f_n_idx + 1) + 'Table ' + table)
                _l.verbose('\t' * (f_n_idx + 2) + str([node.boundary for node in link_sql[table]]))
                remaining_nodes = []
                table_e = table.split('_')
                for node in link_sql[table]:
                    node_ok = False
                    for join_b in join_boundaries:
                        all_e_ok = True
                        for d, e in enumerate(table_e):
                            if value_boundary_intersection(node.boundary[d], join_b[e]) is None:
                                all_e_ok = False
                                break
                        if all_e_ok:
                            node_ok = True
                            break
                    if node_ok:
                        remaining_nodes.append(node)
                _l.verbose('\t' * (f_n_idx + 1) + 'Remaining: ' + str([node.boundary for node in remaining_nodes]))
                link_sql[table] = remaining_nodes
                if not remaining_nodes:
                    self.mark_node_as_filtered(flt_node, 'init_link', table + ' not join with others', _l)

            join_boundaries_as_dict = {}
            join_boundaries_combined = {}

            for e in join_boundaries[0]:
                join_boundaries_as_dict[e] = [join_b[e] for join_b in join_boundaries]
                join_boundaries_combined[e] = value_boundaries_union(join_boundaries_as_dict[e])

            return True, link_sql, join_boundaries_as_dict, join_boundaries_combined

        def init_link_children():
            _l.debug('\t' * f_n_idx + 'Init Link Children')

            if flt_node.isLeaf:
                _l.debug('\t' * (f_n_idx + 1) + 'is leaf node')
                return True, []

            if not flt_node.join_boundaries:
                return True, flt_node.children

            link_children = set()
            for join_boundary in flt_node.join_boundaries[flt_node.name]:
                nodes_in_range = flt_node.desc_range_search(flt_node.idx_boundary, join_boundary)
                if nodes_in_range is not None:
                    for node in nodes_in_range:
                        link_children.add(node)

            if not link_children:
                self.mark_node_as_filtered(flt_node, 'init_link', 'No descendant node satisfy join boundaries', _l)
                return False, None

            return True, list(link_children)

        def init_link_xml():
            _l.debug('\t' * f_n_idx + 'Init Link XML')
            ancestor_link_xml = flt_node.link_xml_first_ancestor()
            if not ancestor_link_xml:
                _l.debug('\t' * (f_n_idx + 1) + 'Ancestor Link XML is empty')
                return True, {}

            link_xml = {}
            child_elements = sorted(list(ancestor_link_xml.keys()), key=lambda e: self.elements.index(e))

            for c_e in child_elements:
                _l.verbose('\t' * (f_n_idx + 1) + 'Checking: ' + c_e)
                starters = ancestor_link_xml[c_e]  # type: List[XMLNode]

                if c_e in flt_node.join_boundaries_combined:
                    c_e_boundary = flt_node.join_boundaries_combined[c_e]
                else:
                    c_e_boundary = None

                c_e_nodes = []
                for starter in starters:
                    nodes_in_range = starter.desc_range_search(flt_node.idx_boundary, c_e_boundary)

                    if nodes_in_range is not None:
                        for node in nodes_in_range:
                            c_e_nodes.append(node)

                c_e_nodes = [node for node in c_e_nodes if not node.filtered]

                _l.verbose('\t' * (f_n_idx + 2) + 'Linked nodes: ' + str(c_e_nodes))

                if not c_e_nodes:
                    self.mark_node_as_filtered(flt_node, 'init_link', str(c_e) + ' none satisfy join boundaries', _l)
                    return False, None

                link_xml[c_e] = c_e_nodes

            return True, link_xml

        success, flt_node.link_sql, flt_node.join_boundaries, flt_node.join_boundaries_combined = init_link_sql()
        _l.debug('\t' * f_n_idx + 'Init link sql Results: ' + str(success))
        log_node_link_sql(flt_node, _l.debug, f_n_idx + 1)
        log_node_join_boundaries(flt_node, _l.debug, f_n_idx + 1)
        _l.debug("")
        if not success:
            return False

        success, flt_node.link_children = init_link_children()
        _l.debug('\t' * f_n_idx + 'Init link children Results: ' + str(success))
        _l.debug('\t' * f_n_idx + 'Link Children: ' + str(flt_node.link_children))
        _l.debug("")
        if not success:
            return False

        success, flt_node.link_xml = init_link_xml()
        _l.debug('\t' * f_n_idx + 'Init link xml Results: ' + str(success))
        log_node_link_xml(flt_node, _l.debug, f_n_idx + 1)
        _l.debug("")
        if not success:
            return False

        flt_node.end_init_link = timeit.default_timer()
        _l.debug("")
        return True

    def filter_with_context(self, flt_node: XMLNode, contexts: List[Context]):
        self.status['n_processed'] += 1
        _l = logging.getLogger("Filter With Context: " + str(flt_node))
        _l.setLevel(logging.getLogger("Filter With Context").level)

        f_n_idx = self.elements.index(flt_node.name)
        _l.debug('\t' * f_n_idx + 'Contexts: ' + str(contexts))

        updated_contexts = []
        for context in contexts:
            context.boundaries[flt_node.name] = value_boundary_intersection(context.boundaries[flt_node.name],
                                                                            flt_node.v_boundary)
            if context.boundaries[flt_node.name] is not None:
                updated_contexts.append(context)
        contexts = updated_contexts

        _l.verbose('\t' * f_n_idx + 'Contexts updated base on node_v: ' + str(contexts))
        if not contexts:
            return False, None

        if not flt_node.inited_link:
            success = self.init_link(flt_node)
            if not success:
                return False, None

        # update context
        remaining_contexts = []
        for context in contexts:
            context_ok = True
            for e in flt_node.join_boundaries_combined:
                interx_b = value_boundary_intersection(context.boundaries[e], flt_node.join_boundaries_combined[e])
                if interx_b is None:
                    context_ok = False
                    break
                else:
                    context.boundaries[e] = interx_b
            if context_ok:
                context.nodes[flt_node.name] = flt_node
                remaining_contexts.append(context)
        _l.verbose('\t' * f_n_idx + 'Contexts updated base on join_boundaries: ' + str(contexts))
        if not remaining_contexts:
            return False, None

        for c_e in flt_node.link_xml:
            updated_contexts = []
            _l.verbose('\t' * (f_n_idx + 1) + 'Checking element ' + c_e)
            for node in flt_node.link_xml[c_e]:
                _l.verbose('\t' * (f_n_idx + 2) + 'Traverse to ' + str(node))
                success, node_contexts = self.filter_with_context(node, copy.deepcopy(contexts))
                _l.verbose('\t' * (f_n_idx + 2) + 'Returned contexts of ' + str(node) + ': ' + str(node_contexts))
                if success:
                    for context in node_contexts:
                        updated_contexts.append(context)
            _l.verbose('\t' * (f_n_idx + 1) + 'Updated context after ' + c_e + ': ' + str(updated_contexts))
            if not updated_contexts:
                return False, None
            contexts = updated_contexts
        return True, contexts

    def log_trackers(self, logger_function, tracker_type, n_prefix_tab=0):
        if tracker_type == 'main':
            for tracker in self.main_trackers:
                logger_function('%s %s %d', '\t' * n_prefix_tab, tracker, self.status[tracker])
        if tracker_type == 'details':
            for tracker in self.detail_trackers:
                logger_function('%s %s %d', '\t' * n_prefix_tab, tracker, self.status[tracker])

    def log_all_status(self, logger_function, n_prefix_tab=0):
        logger_function('%s %s %3f', '\t' * n_prefix_tab, 'Total time:', self.total_time)
        logger_function('%s %s %3f', '\t' * n_prefix_tab, 'N Nodes Processed:', self.status['n_processed'])
        logger_function('%s %s %3f', '\t' * n_prefix_tab, 'N Nodes Filtered :', self.status['n_filtered'])
        # self.log_trackers(logger_function, 'main', n_prefix_tab)
        # self.log_trackers(logger_function, 'details', n_prefix_tab)
        logger_function('%s %s %3f', '\t' * n_prefix_tab, 'Average filtering time for one node:',
                        self.total_time / self.status['n_processed'])

