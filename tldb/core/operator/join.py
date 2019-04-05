import copy
import itertools
import logging
import queue
import timeit
from typing import List

from tldb.core.client import TLDB
from tldb.core.io_support.logger_support import log_node_filter_status, log_node_all_link
from tldb.core.lib.interval import union_multiple_intervals
from tldb.core.lib.nodes import nodes_range_search
from tldb.core.object import HierarchyObject
from tldb.core.structure.boundary import Boundary
from tldb.core.structure.context import RangeContext
from tldb.core.structure.node import XMLNode
from tldb.server.query.xml_query import XMLQuery
from .op import Operator


class ComplexXMLSQLJoin(Operator):
    def __init__(self, tldb: TLDB,
                 # xml_objects: List[str],
                 xml_query: XMLQuery,
                 tables: List[str],
                 # on: List[str],
                 initial_range_context: RangeContext):
        super().__init__(name='complex_xml_sql_join', tldb=tldb)
        self.timer = {'perform': -1, 'filter_with_context': []}
        self.status = {'n_filtered': 0}
        # self.xml_objects = xml_objects
        self.xml_query = xml_query
        self.tables = tables
        # self.on = on
        self.initial_range_context = initial_range_context
        self.logger = logging.getLogger('JOIN')
        self.init_root_links()

    def init_root_links(self):
        # Start from root, link XML root of an element with root of its connected element in XML query
        xml_object = self.tldb.get_object(self.xml_query.object_name)
        attributes = self.xml_query.attributes_name
        for attr in attributes:
            attr_root = xml_object.get_attribute(attr).index_structure.root
            for linked_attr in self.xml_query.relationships[attr]:
                linked_root = xml_object.get_attribute(linked_attr[0]).index_structure.root
                attr_root.link_xml[linked_attr[0]] = [linked_root]
        for table in self.tables:
            table_root = self.tldb.get_object(table).index_structure.root
            table_attributes = sorted(table.split('_'), key=lambda e: self.xml_query.traverse_order.index(e))
            xml_object.get_attribute(table_attributes[0]).index_structure.root.link_sql[table] = [table_root]

        self.logger.debug(f"LINKS INITIALIZED")
        for attr in attributes:
            log_node_all_link(xml_object.get_attribute(attr).index_structure.root, self.logger.debug)
        self.logger.debug(f"{'-' * 20}")

    def mark_node_as_filtered(self, node: XMLNode, reason: str):
        node.filtered = True
        node.reason_of_filtered = reason
        self.status['n_filtered'] += 1
        log_node_filter_status(node, self.logger.debug, self.xml_query.traverse_order.index(node.name))

    def perform(self):
        start_perform = timeit.default_timer()
        _l = self.logger
        traverse_order = self.xml_query.attributes_name
        xml_object: HierarchyObject = self.tldb.get_object(self.xml_query.object_name)
        root_attr_index_structure = xml_object.get_attribute(traverse_order[0]).index_structure
        traverse_queue = queue.Queue()
        traverse_queue.put((root_attr_index_structure.root, [self.initial_range_context]))

        while not traverse_queue.empty():
            attr_node, contexts = traverse_queue.get()
            try:
                reduced_contexts = self.filter_with_context(attr_node, contexts)
                _l.info(f"\tFilter {str(attr_node)} SUCCESS, returned {str(len(reduced_contexts))} reduced contexts")
                _l.debug(f"\tReduced contexts: {str(reduced_contexts)}")
                _l.debug(f"\tAssign this node children to queue with the reduced contexts: {str(attr_node.children)}")
                for child in attr_node.children:
                    traverse_queue.put((child, [copy.copy(c) for c in reduced_contexts]))
            except Exception:
                _l.info(f"\tFilter {str(attr_node)} FAILED")
                _l.info(f"\t{attr_node.reason_of_filtered}")
            _l.info('-----\n')
        self.timer['perform'] = timeit.default_timer() - start_perform

    def filter_with_context(self, flt_node: XMLNode, contexts: List[RangeContext]) -> List[RangeContext]:
        def filter_node_and_raise_exception(msg):
            _l.debug(f"\t {msg}")
            self.mark_node_as_filtered(flt_node, msg)
            self.timer['filter_with_context'].append((flt_node, timeit.default_timer() - start_filter_with_context))
            raise Exception(msg)

        _l = self.logger
        _l.debug(f"Filter {flt_node} with {len(contexts)} contexts")
        _l.debug(f"Contexts: {contexts}")
        start_filter_with_context = timeit.default_timer()

        if flt_node.filtered:
            _l.debug('Node already filtered')
            _l.debug(f"{'-' * 20}")
            return
        try:
            ori_len_contexts = len(contexts)
            contexts = [c for c in contexts
                        if c.check_intersection_and_update_boundaries(RangeContext([flt_node.name], [flt_node.v_interval]))]
            _l.debug(f"\tFilter {ori_len_contexts} -> {len(contexts)} contexts based on node v_boundary took "
                     f"{(timeit.default_timer() - start_filter_with_context):.3f}")
            if not contexts:
                filter_node_and_raise_exception('None of contexts interX with node v_boundary')

            if not flt_node.inited_link:
                self.init_link(flt_node)

            start_filter_ctx_join_b = timeit.default_timer()
            ori_len_contexts = len(contexts)
            compare_context = RangeContext(flt_node.join_boundaries_attributes, flt_node.join_intervals_combined)
            contexts = [c for c in contexts if c.check_intersection_and_update_boundaries(compare_context)]
            _l.debug(f"\tFilter {ori_len_contexts} -> {len(contexts)} contexts based on join boundaries combined took "
                     f"{(timeit.default_timer() - start_filter_ctx_join_b):.3f}")
            _l.verbose(f"Updated contexts based on join_b: {contexts}")
            if not contexts:
                filter_node_and_raise_exception('None of contexts interX with node join boundaries')

            children_attributes = self.xml_query.relationships[flt_node.name]
            for c_attr in children_attributes:
                updated_contexts = []
                start_filter_child_attr = timeit.default_timer()
                _l.verbose(f"\t Check linked attribute {c_attr}")
                for c_node in flt_node.link_xml[c_attr]:
                    _l.verbose(f"\t Check linked attribute node {c_node}")
                    try:
                        c_node_contexts = self.filter_with_context(c_node, [copy.copy(c) for c in contexts])
                    except Exception:
                        continue
                    updated_contexts.append(c_node_contexts)
                updated_contexts = itertools.chain(*updated_contexts)
                _l.debug(f"Filter {c_attr} children attribute {len(contexts)} -> {len(updated_contexts)} took "
                         f"{(timeit.default_timer() - start_filter_child_attr):.3f}")
                _l.verbose(f"Updated contexts based on {c_attr} children attribute: {updated_contexts}")
                if not updated_contexts:
                    filter_node_and_raise_exception(f"Cannot find any node of {c_attr} that satisfy contexts")
                contexts = updated_contexts

            _l.debug(f"\t Results context: {contexts}")
            _l.debug(f"Filter {flt_node} with contexts took {(timeit.default_timer() - start_filter_with_context):.3f}")
            _l.debug('=====\n')
        except Exception as e:
            self.mark_node_as_filtered(flt_node, e)
            _l.debug(f"node is FILTERED: {e}")
            raise Exception(e)
        _l.debug(f"node OK: {e}")
        return contexts

    def init_link(self, flt_node: XMLNode):
        _l = self.logger
        _l.debug(f"Init link {flt_node}")

        flt_node.inited_link = True

        def init_link_sql():
            """
            Filter this filtering node with its link_sql
            Compare between tables that share the filtering node name
            Assign the update link sql, join_boundaries (as dict), and join_boundaries_combined
            :return: true if success, false if error
            """

            _l.debug(f"{'-' * 20}")
            _l.debug(f"Init Link SQL {flt_node}")
            ancestor_link_sql = flt_node.link_sql_first_ancestor()

            if not ancestor_link_sql:
                _l.debug('\t' * 2 + 'Ancestor Link SQL is empty')
                return

            tables = sorted(list(ancestor_link_sql.keys()), key=lambda tbl: len(tbl), reverse=True)
            link_sql = {}

            _l.verbose('\t' + 'Init join boundary with table: ' + tables[0])

            join_intervals = set()
            join_boundaries_attributes = None

            for table in tables:
                # Find nodes in table that match with v_range
                _l.verbose(f"\tChecking table: {table}")
                table_attributes = table.split('_')
                starter_nodes = ancestor_link_sql[table]  # type: List[SQLNode]

                updated_table_nodes = set()
                updated_join_intervals = set()

                if not join_intervals:
                    search_intervals = [None] * len(table_attributes)
                    search_intervals[table_attributes.index(flt_node.name)] = flt_node.v_interval
                    search_boundary = Boundary(search_intervals)
                    join_boundaries_attributes = table_attributes
                    nodes_in_range = nodes_range_search(starter_nodes, search_boundary)
                    for node in nodes_in_range:
                        updated_join_intervals.add(node.boundary.intervals)
                        updated_table_nodes.add(node)
                else:
                    # Find the common attribute of this table and the join_boundaries_attributes
                    join_b_attr_as_set = set(join_boundaries_attributes)
                    common_attr = join_b_attr_as_set.intersection(set(table_attributes))
                    only_join_b_attr = join_b_attr_as_set.difference(common_attr)
                    comm_attr_to_join_b_idx = {a: join_boundaries_attributes.index(a) for a in common_attr}
                    comm_attr_to_tbl_idx = {a: table_attributes.index(a) for a in common_attr}
                    only_join_b_attr_to_idx = {a: join_boundaries_attributes.index(a) for a in only_join_b_attr}

                    for join_intv in join_intervals:
                        search_intervals = [None] * len(table_attributes)
                        for attr in common_attr:
                            search_intervals[comm_attr_to_tbl_idx[attr]] = join_intv[comm_attr_to_join_b_idx[attr]]
                        search_boundary = Boundary(search_intervals)
                        nodes_in_range = nodes_range_search(starter_nodes, search_boundary)
                        _l.verbose(f"\t Nodes in range join interval {join_intv}: "
                                   f"{[str(n.boundary) for n in nodes_in_range]}")

                        for node in nodes_in_range:
                            new_join_interval = node.boundary.intervals
                            only_join_b_boundaries = [join_intv[only_join_b_attr_to_idx[a]] for a in only_join_b_attr]
                            new_join_interval = new_join_interval + tuple(only_join_b_boundaries)
                            updated_join_intervals.add(new_join_interval)
                            updated_table_nodes.add(node)
                    join_boundaries_attributes = table_attributes
                    join_boundaries_attributes.extend(list(only_join_b_attr))

                link_sql[table] = updated_table_nodes
                join_intervals = updated_join_intervals

                _l.verbose(f"\t Updated join interval: {updated_join_intervals}")
                _l.verbose(f"\t Link to {table} nodes: {[str(n.boundary) for n in link_sql[table]]}")

                if not link_sql[table]:
                    if join_intervals:
                        raise ValueError('Something wrong: has no remaining nodes but some interx boundaries')
                    # self.mark_node_as_filtered(flt_node, )
                    raise Exception(f"link_sql: {table} does not intersect with other")
            if not join_intervals:
                # self.mark_node_as_filtered(flt_node, 'No join results between table')
                raise Exception(f"link_sql: No join results between table")

            # _l.verbose('\t'+ 'Update link sql based on join boundaries')
            # for table in tables:
            #     _l.verbose('\t' * 2 + 'Table ' + table)
            #
            #     remaining_nodes = []
            #     table_e = table.split('_')
            #     for node in link_sql[table]:
            #         node_ok = False
            #         for join_b in join_boundaries:
            #             all_e_ok = True
            #             for d, e in enumerate(table_e):
            #                 if value_boundary_intersection(node.boundary[d], join_b[e]) is None:
            #                     all_e_ok = False
            #                     break
            #             if all_e_ok:
            #                 node_ok = True
            #                 break
            #         if node_ok:
            #             remaining_nodes.append(node)
            #     _l.verbose('\t' * 2 + 'Remaining: ' + str([node.boundary for node in remaining_nodes]))
            #     link_sql[table] = remaining_nodes
            #     if not remaining_nodes:
            #         self.mark_node_as_filtered(flt_node, 'init_link', table + ' not join with others', _l)

            flt_node.join_boundaries_attributes = join_boundaries_attributes
            flt_node.join_intervals = join_intervals
            join_intervals_combined = []
            for i in range(len(join_boundaries_attributes)):
                join_intervals_combined.append(union_multiple_intervals([intv[i] for intv in join_intervals]))
            flt_node.join_intervals_combined = tuple(join_intervals_combined)
            _l.debug(f"{'-' * 20}")

        def init_link_children():
            """
            Link this flt_node with children that satisfy the join boundaries
            :return: True if success, false if faile
            """
            _l.debug(f"{'-' * 20}")
            _l.debug(f"Init Link Children {flt_node}")

            if flt_node.is_leaf:
                _l.debug('\t' * 2 + 'is leaf node')
                return

            if not flt_node.join_intervals:
                _l.debug('\t' * 2 + 'is leaf node')
                return

            link_children = set()
            join_b_attributes = flt_node.join_boundaries_attributes
            join_b_attr_to_index = {a: join_b_attributes.index(a) for a in join_b_attributes}
            for join_intv in flt_node.join_intervals:
                init_context = RangeContext(join_b_attributes, join_intv)
                nodes_in_range = flt_node.desc_range_search(flt_node.idx_interval,
                                                            join_intv[join_b_attr_to_index[flt_node.name]])
                for node in nodes_in_range:
                    link_children.add(node)
                    node.inited_contexts.add(init_context)

            if not link_children:
                # self.mark_node_as_filtered(flt_node, 'No descendant node satisfy join boundaries')
                raise Exception(f"link_children: No descendant node satisfy join intervals")
            flt_node.link_children = link_children
            _l.debug(f"{'-' * 20}")

        def init_link_xml():
            """
            Link this flt_node with descendant XML attribute nodes that satisfy the join_b
            :return:
            """
            _l.debug(f"{'-' * 20}")
            _l.debug(f"Init Link XML {flt_node}")
            ancestor_link_xml = flt_node.link_xml_first_ancestor()
            if not ancestor_link_xml:
                _l.debug(f"\t Ancestor Link XML is empty")
                return

            link_xml = {}
            child_elements = list(ancestor_link_xml.keys())
            child_elements.sort(key=lambda e: self.xml_query.traverse_order.index(e))

            for c_e in child_elements:
                _l.verbose(f"\t Checking: {c_e}")
                starter_nodes = ancestor_link_xml[c_e]# type: List[XMLNode]

                join_attributes = flt_node.join_boundaries_attributes
                join_intervals_combined = flt_node.join_intervals_combined
                join_attr_to_idx = {a: join_attributes.index(a) for a in join_attributes}
                if c_e in join_attributes:
                    c_e_boundary = join_intervals_combined[join_attr_to_idx[c_e]]
                else:
                    c_e_boundary = None

                c_e_nodes = []
                for node in starter_nodes:
                    nodes_in_range = node.desc_range_search(flt_node.idx_interval, c_e_boundary)

                    if not nodes_in_range:
                        c_e_nodes.extend(nodes_in_range)

                c_e_nodes = [node for node in c_e_nodes if not node.filtered]

                _l.verbose(f"\t Linked {c_e} nodes: {c_e_nodes}")

                if not c_e_nodes:
                    # self.mark_node_as_filtered(flt_node, f"None of {c_e} satisfy join boundaries")
                    raise Exception(f"None of {c_e} satisfy join boundaries")

                link_xml[c_e] = c_e_nodes
            flt_node.link_xml = link_xml
            _l.debug(f"{'-' * 20}")
            return

        # TODO: refactor this to raise error
        try:
            init_link_sql()
            _l.debug(f"Init link sql OK")
            _l.debug(f"Link sql results: {flt_node.link_sql}")
            _l.debug(f"Join Boundaries Attributes: {flt_node.join_boundaries_attributes}")
            _l.debug(f"Join intervals: {flt_node.join_intervals}")
            _l.debug(f"Join intervals combined: {flt_node.join_intervals_combined}")
            _l.debug(f"{'-' * 20}")
            init_link_children()
            _l.debug(f"Init link children OK")
            _l.debug(f"Link children results: {flt_node.children}")
            _l.debug(f"{'-' * 20}")
            init_link_xml()
            _l.debug(f"Init link xml OK")
            _l.debug(f"Link xml results: {flt_node.link_xml}")
            _l.debug(f"{'-' * 20}")
        except Exception as e:
            _l.debug(f"Init link FAILED {e}")
            raise Exception(e)

