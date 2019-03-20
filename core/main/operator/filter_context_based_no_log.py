import copy
import logging
import queue
import timeit
from typing import List

# from core.io_support.logger_support import *
from core.main.structure.context import Context
from core.main.structure import SQLNode, XMLNode


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

        # log_node_filter_status(node, _l.debug, self.elements.index(node.name))

    def perform(self):
        # _l = self.logger
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
            # _l.debug('Filtering ' + str(root_node) + 'with contexts:' + str(contexts))
            success, updated_contexts = self.filter_with_context(root_node, contexts)

            if not root_node.filtered and success:
                # _l.info('\t' + str(root_node) + ' OK, returned ' + str(len(updated_contexts)) + ' contexts')
                # _l.debug('\tContexts: ' + str(updated_contexts))
                # _l.debug('\tAdd children ' + str(root_node.link_children))
                # _l.debug('\tWith contexts:' + str(updated_contexts))
                for child in root_node.link_children:
                    queue_root_node.put(child)
                    queue_contexts.put([copy.copy(context) for context in updated_contexts])
            # else:
                # _l.info(str(root_node) + ' not OK')
                # log_node_filter_status(root_node, _l.info)
                # _l.info('Returned context: ' + str(updated_contexts))
            # _l.info('\n')

        end_filtering = timeit.default_timer()
        self.total_time = end_filtering - start_filtering
        print(self.total_time)
        # self.log_all_status(_l.info)

    def init_link(self, flt_node: XMLNode):
        # _l = logging.getLogger("Init Link: "+str(flt_node))
        # _l.setLevel(logging.getLogger("Init Link").level)

        f_n_idx = self.elements.index(flt_node.name)

        flt_node.inited_link = True
        flt_node.timer['init_link'][0] = timeit.default_timer()

        def init_link_sql():
            # _l.debug('\t' * f_n_idx + 'Init Link SQL')
            ancestor_link_sql = flt_node.link_sql_first_ancestor()

            if not ancestor_link_sql:
                # _l.debug('\t' * (f_n_idx + 1) + 'Ancestor Link SQL is empty')
                return True, {}, {}, {}

            tables = sorted(list(ancestor_link_sql.keys()), key=lambda tbl: len(tbl), reverse=True)
            link_sql = {}
            join_boundaries = []

            for table in tables:
                # Find nodes in table that match with v_range
                # _l.verbose('\t' * f_n_idx + 'Checking table: ' + table)
                starters = ancestor_link_sql[table]  # type: List[SQLNode]
                table_nodes = []
                for starter in starters:
                    boundaries = [None] * len(starter.boundary)
                    boundaries[starter.dimension] = flt_node.v_boundary
                    nodes_in_range = starter.range_search(boundaries)
                    if nodes_in_range is not None:
                        for node in nodes_in_range:
                            table_nodes.append(node)
                # _l.verbose('\t' * (f_n_idx + 1) + 'table nodes match v_range ' + str(table_nodes))
                if not table_nodes:
                    # self.mark_node_as_filtered(flt_node, 'init_link',
                    #                            'No node in table: ' + table + ' : matches v_range', _l)
                    self.mark_node_as_filtered(flt_node, 'init_link',
                                               'No node in table: ' + table + ' : matches v_range')
                    return False, None, None, None
                # Check join boundaries
                table_e = table.split('_')
                # _l.verbose('\t' * (f_n_idx + 1) + 'Checking with join boundaries')
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
                                        # _l.verbose('\t' * (f_n_idx + 2) + str(node) + ': ' + str(join_b) +
                                        #            ' not interX at ' + e)
                                        all_e_ok = False
                                        break
                            if all_e_ok:
                                # _l.verbose('\t' * (f_n_idx + 2) + str(node) + ': ' + str(join_b) + ' interX')
                                node_ok = True
                                for e in join_b:
                                    if e not in table_e:
                                        interx_b[e] = join_b[e]
                                updated_join_b.append(interx_b)
                        if node_ok:
                            remaining_nodes.append(node)

                    link_sql[table] = remaining_nodes
                    join_boundaries = updated_join_b

                # _l.verbose('\t' * (f_n_idx + 1) + 'remaining nodes: ' +
                #            str([node.boundary for node in link_sql[table]]))
                # _l.verbose('\t' * (f_n_idx + 1) + 'Updated join boundaries: ' + str(join_boundaries))
                if not link_sql[table]:
                    if join_boundaries:
                        raise ValueError('Something wrong: has no remaining nodes but some interx boundaries')
                    # self.mark_node_as_filtered(flt_node, 'init_link', table + ' not join with others', _l)
                    self.mark_node_as_filtered(flt_node, 'init_link', table + ' not join with others')
                    return False, None, None, None

            if not join_boundaries:
                self.mark_node_as_filtered(flt_node, 'init_link', 'No join results between table')
                # self.mark_node_as_filtered(flt_node, 'init_link', 'No join results between table', _l)
                return False, None, None, None

            # _l.verbose('\t' * f_n_idx + 'Update link sql based on join boundaries')
            for table in tables:
                # _l.verbose('\t' * (f_n_idx + 1) + 'Table ' + table)
                # _l.verbose('\t' * (f_n_idx + 2) + str([node.boundary for node in link_sql[table]]))
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
                # _l.verbose('\t' * (f_n_idx + 1) + 'Remaining: ' + str([node.boundary for node in remaining_nodes]))
                link_sql[table] = remaining_nodes
                if not remaining_nodes:
                    self.mark_node_as_filtered(flt_node, 'init_link', table + ' not join with others')
                    # self.mark_node_as_filtered(flt_node, 'init_link', table + ' not join with others', _l)

            join_boundaries_as_dict = {}
            join_boundaries_combined = {}

            for e in join_boundaries[0]:
                join_boundaries_as_dict[e] = [join_b[e] for join_b in join_boundaries]
                join_boundaries_combined[e] = value_boundaries_union(join_boundaries_as_dict[e])

            return True, link_sql, join_boundaries_as_dict, join_boundaries_combined

        def init_link_children():
            # _l.debug('\t' * f_n_idx + 'Init Link Children')

            if flt_node.isLeaf:
                # _l.debug('\t' * (f_n_idx + 1) + 'is leaf node')
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
                # self.mark_node_as_filtered(flt_node, 'init_link', 'No descendant node satisfy join boundaries', _l)
                self.mark_node_as_filtered(flt_node, 'init_link', 'No descendant node satisfy join boundaries')
                return False, None

            return True, list(link_children)

        def init_link_xml():
            # _l.debug('\t' * f_n_idx + 'Init Link XML')
            ancestor_link_xml = flt_node.link_xml_first_ancestor()
            if not ancestor_link_xml:
                # _l.debug('\t' * (f_n_idx + 1) + 'Ancestor Link XML is empty')
                return True, {}

            link_xml = {}
            child_elements = sorted(list(ancestor_link_xml.keys()), key=lambda e: self.elements.index(e))

            for c_e in child_elements:
                # _l.verbose('\t' * (f_n_idx + 1) + 'Checking: ' + c_e)
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

                # _l.verbose('\t' * (f_n_idx + 2) + 'Linked nodes: ' + str(c_e_nodes))

                if not c_e_nodes:
                    self.mark_node_as_filtered(flt_node, 'init_link', str(c_e) + ' none satisfy join boundaries')
                    # self.mark_node_as_filtered(flt_node, 'init_link', str(c_e) + ' none satisfy join boundaries', _l)
                    return False, None

                link_xml[c_e] = c_e_nodes

            return True, link_xml

        flt_node.timer['init_link_sql'][0] = timeit.default_timer()
        success, flt_node.link_sql, flt_node.join_boundaries, flt_node.join_boundaries_combined = init_link_sql()
        flt_node.timer['init_link_sql'][1] = timeit.default_timer()
        # _l.debug('\t' * f_n_idx + 'Init link sql Results: ' + str(success))
        # log_node_link_sql(flt_node, _l.debug, f_n_idx + 1)
        # log_node_join_boundaries(flt_node, _l.debug, f_n_idx + 1)
        if not success:
            flt_node.timer['init_link'][1] = timeit.default_timer()
            # log_node_timer(flt_node, _l.timer, f_n_idx)
            return False

        flt_node.timer['init_link_children'][0] = timeit.default_timer()
        success, flt_node.link_children = init_link_children()
        flt_node.timer['init_link_children'][1] = timeit.default_timer()
        # _l.debug('\t' * f_n_idx + 'Init link children Results: ' + str(success))
        # _l.debug('\t' * f_n_idx + 'Link Children: ' + str(flt_node.link_children))
        if not success:
            flt_node.timer['init_link'][1] = timeit.default_timer()
            # log_node_timer(flt_node, _l.timer, f_n_idx)
            return False

        flt_node.timer['init_link_xml'][0] = timeit.default_timer()
        success, flt_node.link_xml = init_link_xml()
        flt_node.timer['init_link_xml'][1] = timeit.default_timer()
        # _l.debug('\t' * f_n_idx + 'Init link xml Results: ' + str(success))
        # log_node_link_xml(flt_node, _l.debug, f_n_idx)
        if not success:
            flt_node.timer['init_link'][1] = timeit.default_timer()
            # log_node_timer(flt_node, _l.timer, f_n_idx)
            return False

        flt_node.timer['init_link'][1] = timeit.default_timer()
        # log_node_timer(flt_node, _l.timer, f_n_idx)
        return True

    def filter_with_context(self, flt_node: XMLNode, contexts: List[Context]):
        self.status['n_processed'] += 1
        _l = logging.getLogger("Filter With Context: " + str(flt_node))
        _l.setLevel(logging.getLogger("Filter With Context").level)
        start_flt_w_c = timeit.default_timer()
        f_n_idx = self.elements.index(flt_node.name)
        _l.debug('\t' * f_n_idx + str(len(contexts)) + ' contexts')
        _l.verbose('\t' * f_n_idx + 'Contexts: ' + str(contexts))

        if flt_node.filtered:
            _l.debug('\t' * f_n_idx + 'Node already filtered')
            _l.timer('%s %.3f', '\t' * f_n_idx + 'Filter with context took: ', timeit.default_timer() - start_flt_w_c)
            _l.debug('\n')
            return False, None

        updated_contexts = []
        for context in contexts:
            context.boundaries[flt_node.name] = value_boundary_intersection(context.boundaries[flt_node.name],
                                                                            flt_node.v_boundary)
            if context.boundaries[flt_node.name] is not None:
                updated_contexts.append(context)

        _l.timer('%s %.3f', '\t' * f_n_idx + 'Filter ' + str(len(contexts)) + '->' + str(len(updated_contexts)) +
                 ' contexts based on node v took: ', timeit.default_timer() - start_flt_w_c)
        contexts = updated_contexts

        if not contexts:
            _l.debug('\t' * f_n_idx + 'None of contexts interX with node v_boundary')
            _l.timer('%s %.3f', '\t' * f_n_idx + 'Filter with context took: ', timeit.default_timer() - start_flt_w_c)
            _l.debug('\n')
            return False, None

        if not flt_node.inited_link:
            success = self.init_link(flt_node)
            if not success:
                _l.debug('\t' * f_n_idx + 'Node filtered after init link')
                _l.timer('%s %.3f', '\t' * f_n_idx + 'Filter with context took: ', timeit.default_timer() - start_flt_w_c)
                _l.debug('\n')
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

        _l.timer('%s %.3f', '\t' * f_n_idx + 'Filter ' + str(len(contexts)) + '->' + str(len(remaining_contexts)) +
                 ' contexts based on node join boundaries took: ', timeit.default_timer() - start_flt_w_c)
        contexts = remaining_contexts

        _l.verbose('\t' * f_n_idx + 'Contexts updated base on join_boundaries: ' + str(contexts))
        if not remaining_contexts:
            _l.debug('\t' * f_n_idx + 'None of contexts interX with node join boundaries')
            _l.timer('%s %.3f', '\t' * f_n_idx + 'Filter with context took: ', timeit.default_timer() - start_flt_w_c)
            _l.debug('\n')
            return False, None

        child_elements = sorted(list(flt_node.link_xml.keys()), key=lambda e: self.elements.index(e))
        for c_e in child_elements:
            updated_contexts = []
            _l.verbose('\t' * (f_n_idx + 1) + 'Checking element ' + c_e)
            for node in flt_node.link_xml[c_e]:
                _l.verbose('\t' * (f_n_idx + 2) + 'Traverse to ' + str(node))
                if node.filtered:
                    _l.verbose('\t' * (f_n_idx + 3) + 'Already Filtered')
                    continue
                success, updated_n_c = self.filter_with_context(node, [copy.copy(context) for context in contexts])
                _l.verbose('\t' * (f_n_idx + 3) + 'Returned contexts of ' + str(node) + ': ' + str(updated_n_c))
                if success:
                    for context in updated_n_c:
                        updated_contexts.append(context)
            _l.verbose('\t' * (f_n_idx + 1) + 'Updated context after ' + c_e + ': ' + str(updated_contexts))
            _l.timer('%s %.3f', '\t' * f_n_idx + 'Filter ' + str(len(contexts)) + '->' + str(len(updated_contexts)) +
                     ' contexts based on ' + c_e + ' nodes took: ', timeit.default_timer() - start_flt_w_c)
            if not updated_contexts:
                _l.debug('\t' * f_n_idx + 'Cannot find any node of ' + c_e + ' that satisfy contexts')
                _l.timer('%s %.3f', '\t' * f_n_idx + 'Filter with context took: ',
                         timeit.default_timer() - start_flt_w_c)
                _l.debug('\n')
                return False, None
            contexts = updated_contexts

        _l.debug('\t' * f_n_idx + 'Results: ' + str(contexts))
        _l.timer('%s %.3f', '\t' * f_n_idx + 'Filter with context took: ', timeit.default_timer() - start_flt_w_c)
        _l.debug('\n')
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

