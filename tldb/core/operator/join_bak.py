import copy
import itertools
import logging
import queue
import timeit
from typing import List

from tldb.core.client import TLDB
from tldb.core.io_support.logger_support import log_node_all_link
from tldb.core.lib.interval import union_multiple_intervals
from tldb.core.lib.nodes import nodes_range_search
from tldb.core.object import HierarchyObject
from tldb.core.structure.boundary import Boundary
from tldb.core.structure.context import RangeContext
from tldb.core.structure.node import XMLNode
from tldb.server.query.xml_query import XMLQuery
from .op import Operator


class NodeFilteredException(Exception):
    pass

# TODO: BUG (is leaf)
# TODO: saved children nodes path (context.nodes)


class ComplexXMLSQLJoin(Operator):
    def __init__(self, tldb: TLDB,
                 # xml_objects: List[str],
                 xml_query: XMLQuery,
                 tables: List[str],
                 # on: List[str],
                 initial_range_context: RangeContext):
        super().__init__(name='complex_xml_sql_join', tldb=tldb)
        self.timer = {'filter_with_context': []}
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
        raise NodeFilteredException(reason)

    def perform(self):
        start_perform = timeit.default_timer()
        _l = self.logger
        traverse_order = self.xml_query.attributes_name
        xml_object: HierarchyObject = self.tldb.get_object(self.xml_query.object_name)
        root_attr_index_structure = xml_object.get_attribute(traverse_order[0]).index_structure
        traverse_queue = queue.Queue()
        traverse_queue.put((root_attr_index_structure.root, [self.initial_range_context]))

        results = []

        while not traverse_queue.empty():
            attr_node, contexts = traverse_queue.get()
            try:
                reduced_contexts = self.filter_with_context(attr_node, contexts)
                _l.info(f"\tFilter {str(attr_node)} SUCCESS, returned {str(len(reduced_contexts))} reduced contexts")
                _l.verbose(f"\tReduced contexts: {str(reduced_contexts)}")
                _l.debug(f"\tAssign this node children to queue with the reduced contexts: {str(attr_node.children)}")
                if attr_node.is_leaf:
                    results.append((attr_node, reduced_contexts))
                for child in attr_node.children:
                    traverse_queue.put((child, [copy.copy(c) for c in reduced_contexts]))
            except NodeFilteredException:
                pass
                _l.info(f"\tFilter {str(attr_node)} FAILED")
                _l.info(f"\t{attr_node.reason_of_filtered}")
            _l.debug("END STEP")
            _l.debug(f"{'=' * 20}\n")

        _l.info(f"Results {results}")
        _l.info(f"Join operation took: {timeit.default_timer() - start_perform}")
        _l.debug(f"Filter {len(self.timer['filter_with_context'])} nodes with contexts")
        for node, time in self.timer['filter_with_context']:
            _l.verbose(f"Node:{node} - time:{time:.3f}")

    def filter_with_context(self, flt_node: XMLNode, contexts: List[RangeContext]):
        def filter_node(msg):
            self.timer['filter_with_context'].append((flt_node, timeit.default_timer() - start_filter_with_context))
            self.mark_node_as_filtered(flt_node, f"filter_w_context: {msg}")

        _l = self.logger
        _l.debug(f"{'-'*5} FILTER WITH CONTEXT {'-' * 5}")
        _l.debug(f"Filter {flt_node} with {len(contexts)} contexts")
        _l.verbose(f"Contexts: {contexts}")
        start_filter_with_context = timeit.default_timer()

        if flt_node.filtered:
            _l.debug('Node already filtered')
            _l.debug(f"{'-' * 20}")
            return
        try:
            ori_len_contexts = len(contexts)
            v_interval_range_context = RangeContext([flt_node.name], [flt_node.v_interval])

            contexts = [c for c in contexts if c.check_intersection_and_update_boundaries(v_interval_range_context)]
            _l.debug(f"\tFilter {ori_len_contexts} -> {len(contexts)} contexts based on node v_boundary took {(timeit.default_timer() - start_filter_with_context):.3f}")
            if not contexts:
                filter_node(f"None of contexts interX with v_interval {flt_node.v_interval}")

            _l.debug(f"{'-' * 20}")

            if not flt_node.inited_link:
                self.init_link(flt_node)

            _l.debug(f"{'-' * 20}")
            _l.debug("Filter by join intervals combined")
            if flt_node.join_intervals_combined:
                start_flt_join_intv = timeit.default_timer()
                ori_len_contexts = len(contexts)
                compare_context = RangeContext(flt_node.join_boundaries_attributes, flt_node.join_intervals_combined)
                contexts = [c for c in contexts if c.check_intersection_and_update_boundaries(compare_context)]
                _l.debug("Filter context based on join intervals combined")
                _l.debug(f"\tFilter {ori_len_contexts} -> {len(contexts)} took {(timeit.default_timer() - start_flt_join_intv):.3f}")
                _l.verbose(f"Updated contexts based on join_b: {contexts}")
                if not contexts:
                    filter_node('None of contexts interX with node join boundaries')
            else:
                _l.debug("No join interval combined")
                pass

            _l.debug(f"{'-' * 20}")

            _l.debug("Traverse to children attributes")
            children_relationships = self.xml_query.relationships[flt_node.name]
            for c_rel in children_relationships:
                c_attr = c_rel[0]
                updated_contexts = []
                start_filter_child_attr = timeit.default_timer()
                for c_node in flt_node.link_xml[c_attr]:
                    try:
                        c_node_contexts = self.filter_with_context(c_node, [copy.copy(c) for c in contexts])
                    except NodeFilteredException:
                        continue
                    updated_contexts.append(c_node_contexts)
                updated_contexts = list(itertools.chain(*updated_contexts))
                _l.debug(f"Filter {len(flt_node.link_xml[c_attr])} {c_attr} children attribute {len(contexts)} -> {len(updated_contexts)} contexts took {(timeit.default_timer() - start_filter_child_attr):.3f}")
                _l.verbose(f"Updated contexts based on {c_attr} children attribute: {updated_contexts}")
                if not updated_contexts:
                    filter_node(f"None of child attribute {c_attr} satisfy contexts")
                contexts = updated_contexts
                _l.debug(f"{'-' * 20}")
            _l.debug(f"{'-' * 20}")
        except NodeFilteredException as e:
            _l.info(f"Filter with context {flt_node} FAILED")
            _l.info(f"\tReason: {flt_node.reason_of_filtered}")
            raise e
        flt_time = timeit.default_timer() - start_filter_with_context
        self.timer['filter_with_context'].append((flt_node, flt_time))
        _l.debug(f"Filter with context OK return {len(contexts)}")
        _l.verbose(f"\t Results contexts: {contexts}")
        _l.debug(f"Filter {flt_node} with contexts took {flt_time:.3f}")
        _l.debug('=====\n')
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
                _l.debug(f"\tChecking table: {table}")
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

                        for node in nodes_in_range:
                            new_join_interval = node.boundary.intervals
                            only_join_b_boundaries = [join_intv[only_join_b_attr_to_idx[a]] for a in only_join_b_attr]
                            new_join_interval = new_join_interval + tuple(only_join_b_boundaries)
                            updated_join_intervals.add(new_join_interval)
                            updated_table_nodes.add(node)
                    join_boundaries_attributes = table_attributes
                    join_boundaries_attributes.extend(list(only_join_b_attr))

                _l.debug(f"Found {len(join_intervals)} -> {len(updated_join_intervals)} join intervals")
                link_sql[table] = updated_table_nodes
                join_intervals = updated_join_intervals

                _l.verbose(f"\t Updated join interval: {updated_join_intervals}")
                _l.verbose(f"\t Linked {table} nodes: {[str(n.boundary) for n in link_sql[table]]}")

                if not link_sql[table]:
                    if join_intervals:
                        raise ValueError('Something wrong: has no remaining nodes but some interx boundaries')
                    self.mark_node_as_filtered(flt_node, f"link_sql: {table} does not intersect with other")
            if not join_intervals:
                self.mark_node_as_filtered(flt_node, f"link_sql: No join results between table")

            flt_node.join_boundaries_attributes = join_boundaries_attributes
            flt_node.join_intervals = join_intervals
            flt_node.link_sql = link_sql
            join_intervals_combined = []
            # TODO: combine join interval can be done by just combine the initial linked nodes
            for i in range(len(join_boundaries_attributes)):
                join_intervals_combined.append(union_multiple_intervals([intv[i] for intv in join_intervals]))
            flt_node.join_intervals_combined = tuple(join_intervals_combined)
            _l.debug(f"Combined join interval {flt_node.join_boundaries_attributes} {flt_node.join_intervals_combined}")

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
                nodes_in_range = flt_node.range_search(join_intv[join_b_attr_to_index[flt_node.name]])
                for node in nodes_in_range:
                    link_children.add(node)
                    node.inited_contexts.add(init_context)

            if not link_children:
                self.mark_node_as_filtered(flt_node, f"link_children: No descendant node satisfy join boundaries")
            flt_node.link_children = link_children
            _l.debug(f"Found {len(flt_node.link_children)} descendant nodes")

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
                starter_nodes = ancestor_link_xml[c_e]  # type: List[XMLNode]

                join_attributes = flt_node.join_boundaries_attributes
                join_intervals_combined = flt_node.join_intervals_combined
                try:
                    c_e_boundary = join_intervals_combined[join_attributes.index(c_e)]
                except ValueError:
                    c_e_boundary = None

                c_e_nodes = []
                for node in starter_nodes:
                    nodes_in_range = node.desc_range_search(flt_node.idx_interval, c_e_boundary)
                    if nodes_in_range:
                        c_e_nodes.extend(nodes_in_range)
                c_e_nodes = [node for node in c_e_nodes if not node.filtered]

                _l.verbose(f"\t Linked {c_e} nodes: {c_e_nodes}")
                if not c_e_nodes:
                    self.mark_node_as_filtered(flt_node, f"link_xml: None descendant {c_e}")
                link_xml[c_e] = c_e_nodes
                _l.debug(f"Found {len(c_e_nodes)} for {c_e} attribute")
            flt_node.link_xml = link_xml
            return

        init_link_sql()
        _l.verbose(f"Link sql results: {flt_node.link_sql}")
        _l.verbose(f"Join Boundaries Attributes: {flt_node.join_boundaries_attributes}")
        _l.verbose(f"Join intervals: {flt_node.join_intervals}")
        _l.verbose(f"Join intervals combined: {flt_node.join_intervals_combined}")
        _l.debug(f"Init link sql OK")
        init_link_children()
        _l.verbose(f"Link children results: {flt_node.children}")
        _l.debug(f"Init link children OK")
        init_link_xml()
        _l.verbose(f"Link xml results: {flt_node.link_xml}")
        _l.debug(f"Init link xml OK")
        _l.debug(f"{'-' * 20}")

