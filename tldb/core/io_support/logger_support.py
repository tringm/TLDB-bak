# Helper library for logging
from tldb.core.operator.loader import Loader
from tldb.core.structure.node import XMLNode


def log_loader(loader: Loader, logger_function, n_prefix_tab=0, only_unfiltered=False):
    logger_function('\t' * n_prefix_tab + 'Method: ' + str(loader.method))
    logger_function('\t' * n_prefix_tab + 'Elements: ' + str(loader.all_elements_name))
    logger_function('\t' * n_prefix_tab + 'Relationship: ' + str(loader.relationship_matrix))
    logger_function('\t' * n_prefix_tab + 'n_children' + str(loader.max_n_children))
    logger_function('\t' * n_prefix_tab + 'XML Trees')
    for e in loader.all_elements_name:
        logger_function('\t' * (n_prefix_tab + 1) + e)
        log_tree_from_root(loader.all_elements_root[e], logger_function, n_prefix_tab+1, only_unfiltered)
        logger_function("")

    logger_function('\t' * n_prefix_tab + 'SQL Trees')
    for e in loader.all_tables_root:
        logger_function('\t' * (n_prefix_tab + 1) + e)
        log_tree_from_root(loader.all_tables_root[e], logger_function, n_prefix_tab + 1, only_unfiltered)
        logger_function("")
    logger_function("")


def log_tree_from_root(node, logger_function, n_prefix_tab=0, only_unfiltered=False):
    if only_unfiltered:
        if node.filtered:
            return
    if node.is_leaf:
        logger_function('\t'*n_prefix_tab + str(node) + ' is Leaf')
        for entry in node.entries:
            logger_function('\t'*(n_prefix_tab + 1) + str(entry))
    else:
        logger_function('\t' * n_prefix_tab + str(node))
        for child in node.children:
            log_tree_from_root(child, logger_function, n_prefix_tab+1, only_unfiltered)


def log_node_filter_status(node: XMLNode, logger_function, n_prefix_tab=0):
    if not isinstance(node, XMLNode):
        raise ValueError('Cannot log filter status of ' + str(node) + ' if is not XMLNode')
    logger_function('\t' * n_prefix_tab + str(node) + ' status: ')
    logger_function('\t' * (n_prefix_tab + 1) + 'Filtered: ' + str(node.filtered))
    logger_function('\t' * (n_prefix_tab + 1) + 'Reason of Filtered: ' + node.reason_of_filtered)


def log_node_link_xml(node: XMLNode, logger_function, n_prefix_tab=0):
    if not isinstance(node, XMLNode):
        raise ValueError('Cannot log link xml of  ' + str(node) + ' if is not XMLNode')
    logger_function('\t' * n_prefix_tab + 'link xml: ')
    if not node.link_xml:
        logger_function('\t'*(n_prefix_tab + 1) + 'None')
    else:
        for connected_element in node.link_xml:
            logger_function('\t' * (n_prefix_tab + 1) + str([str(node) for node in node.link_xml[connected_element]]))


def log_node_link_sql(node: XMLNode, logger_function, n_prefix_tab=0):
    if not isinstance(node, XMLNode):
        raise ValueError('Cannot log link sql of  ' + str(node) + ' if is not XMLNode')
    logger_function('\t' * n_prefix_tab + 'link sql: ')
    if not node.link_sql:
        logger_function('\t'*(n_prefix_tab + 1) + str(node.link_sql))
    else:
        for table_name in node.link_sql:
            logger_function('\t' * (n_prefix_tab + 1) + str([str(node) for node in node.link_sql[table_name]]))


def log_node_all_link(node: XMLNode, logger_function, n_prefix_tab=0):
    if not isinstance(node, XMLNode):
        raise ValueError('Cannot log link xml and sql of  ' + str(node) + ' if is not XMLNode')
    log_node_link_xml(node, logger_function, n_prefix_tab)
    log_node_link_sql(node, logger_function, n_prefix_tab)


# def log_node_intersection_range(node: XMLNode, logger_function, n_prefix_tab=0):
#     if not isinstance(node, XMLNode):
#         raise ValueError('Cannot log interesect range of  ' + str(node) + ' if is not XMLNode')
#     logger_function('\t' * n_prefix_tab + 'intersection_range: ')
#     for element in node.intersection_range:
#         logger_function('\t' * (n_prefix_tab + 1) + element + ': ' +
#                         str([str(boundary) for boundary in node.intersection_range[element]]))


def log_node_join_boundaries(node: XMLNode, logger_function, n_prefix_tab=0):
    if not isinstance(node, XMLNode):
        raise ValueError('Cannot log join boundaries of  ' + str(node) + ' if is not XMLNode')
    logger_function('\t' * n_prefix_tab + 'join boundaries: ')
    if not node.join_boundaries:
        logger_function('\t' * (n_prefix_tab + 1) + str(node.join_boundaries))
    else:
        for element in node.join_boundaries:
            logger_function('\t' * (n_prefix_tab + 1) + element + ': ' +
                            str([str(boundary) for boundary in node.join_boundaries[element]]))


# def log_node_filter_time_details(node: XMLNode, logger_function, n_prefix_tab=0):
#     if not isinstance(node, XMLNode):
#         raise ValueError('Cannot log filtering time of  ' + str(node) + ' if is not XMLNode')
#     logger_function('\t' * n_prefix_tab + 'Filtering time break down: ')
#     logger_function('%s %.3f', '\t' * (n_prefix_tab + 1) + 'Total Filtering Time: ', node.full_filtering_time)
#     logger_function('%s %.3f', '\t' * (n_prefix_tab + 1) + 'Value Filtering Time: ', node.value_filtering_time)
#     logger_function('%s %.3f', '\t' * (n_prefix_tab + 1) + 'Connected Element Filtering Time: ',
#                     node.connected_element_filtering_time)
#     logger_function('%s %.3f', '\t' * (n_prefix_tab + 1) + 'Check Descendants Time: ', node.check_lower_level_time)
#     logger_function('%s %.3f', '\t' * (n_prefix_tab + 1) + 'Init Children Link Time: ', node.init_children_link_time)
#     logger_function('%s %.3f', '\t' * (n_prefix_tab + 1) + 'Filter Children Time: ', node.filter_children_time)
#     logger_function('%s %.3f', '\t' * (n_prefix_tab + 1) + 'Filter Children Link Sql Time: ',
#                     node.filter_children_link_sql_time)


# def log_node_validation_time_details(node: XMLNode, logger_function, n_prefix_tab=0):
#     if not isinstance(node, XMLNode):
#         raise ValueError('Cannot log validation time of  ' + str(node) + ' if is not XMLNode')
#     logger_function('\t' * n_prefix_tab + 'Validation time break down: ')
#     logger_function('%s %.3f', '\t' * (n_prefix_tab + 1) + 'Value Validation Time:', node.value_validation_time)
#     logger_function('%s %.3f', '\t' * (n_prefix_tab + 1) + 'Structure validation Time:', node.structure_validation_time)
#
#
# def log_node_time_details(node, logger_function, n_prefix_tab=0):
#     log_node_filter_time_details(node, logger_function, n_prefix_tab)
#     log_node_validation_time_details(node, logger_function, n_prefix_tab)


def log_node_timer(node: XMLNode, logger_function, n_prefix_tab=0):
    def minus_tuple(a_tuple):
        return a_tuple[1] - a_tuple[0]

    logger_function('%s %.3f', '\t' * n_prefix_tab + 'Init Link took: ', minus_tuple(node.timer['init_link']))
    logger_function('%s %.3f', '\t' * (n_prefix_tab + 1) + 'Init Link SQL took: ',
                    minus_tuple(node.timer['init_link_sql']))
    logger_function('%s %.3f', '\t' * (n_prefix_tab + 1) + 'Init Link Children took: ',
                    minus_tuple(node.timer['init_link_children']))
    logger_function('%s %.3f', '\t' * (n_prefix_tab + 1) + 'Init Link XML took: ',
                    minus_tuple(node.timer['init_link_xml']))
    logger_function('\n')
