# Helper library for logging
from src.Node import XMLNode


def log_node_filter_status(node: XMLNode, logger_function, n_prefix_tab=1):
    if not isinstance(node, XMLNode):
        raise ValueError('Cannot log filter status of ' + str(node) + ' if is not XMLNode')
    logger_function('\t' * n_prefix_tab + 'status: ')
    logger_function('\t' * (n_prefix_tab + 1) + 'Filtered: ' + str(node.filtered))
    logger_function('\t' * (n_prefix_tab + 1) + 'Reason of Filtered: ' + node.reason_of_filtered)


def log_node_link_xml(node: XMLNode, logger_function, n_prefix_tab=1):
    if not isinstance(node, XMLNode):
        raise ValueError('Cannot log link xml of  ' + str(node) + ' if is not XMLNode')
    logger_function('\t' * n_prefix_tab + 'link xml: ')
    for connected_element in node.link_xml:
        logger_function('\t' * (n_prefix_tab + 1) + str([str(node) for node in node.link_xml[connected_element]]))


def log_node_link_sql(node: XMLNode, logger_function, n_prefix_tab=1):
    if not isinstance(node, XMLNode):
        raise ValueError('Cannot log link sql of  ' + str(node) + ' if is not XMLNode')
    logger_function('\t' * n_prefix_tab + 'link sql: ')
    for table_name in node.link_sql:
        logger_function('\t' * (n_prefix_tab + 1) + str([str(node) for node in node.link_sql[table_name]]))


def log_node_all_link(node: XMLNode, logger_function, n_prefix_tab=1):
    if not isinstance(node, XMLNode):
        raise ValueError('Cannot log link xml and sql of  ' + str(node) + ' if is not XMLNode')
    log_node_link_xml(node, logger_function, n_prefix_tab)
    log_node_link_sql(node, logger_function, n_prefix_tab)


def log_node_intersection_range(node: XMLNode, logger_function, n_prefix_tab=1):
    if not isinstance(node, XMLNode):
        raise ValueError('Cannot log interesect range of  ' + str(node) + ' if is not XMLNode')
    logger_function('\t' * n_prefix_tab + 'intersection_range: ')
    for element in node.intersection_range:
        logger_function('\t' * (n_prefix_tab + 1) + element + ': ' +
                     str([str(boundary) for boundary in node.intersection_range[element]]))


def log_node_filter_time_details(node: XMLNode, logger_function, n_prefix_tab = 1):
    if not isinstance(node, XMLNode):
        raise ValueError('Cannot log filtering time of  ' + str(node) + ' if is not XMLNode')
    logger_function('\t' * n_prefix_tab + 'Filtering time break down: ')
    logger_function('%s %.3f', '\t' * (n_prefix_tab + 1) + 'Value Filtering Time: ', node.value_filtering_time)
    logger_function('%s %.3f', '\t' * (n_prefix_tab + 1) + 'Connected Element Filtering Time: ',
                    node.connected_element_filtering_time)
    logger_function('%s %.3f', '\t' * (n_prefix_tab + 1) + 'Check Descendants Time: ', node.check_lower_level_time)
    logger_function('%s %.3f', '\t' * (n_prefix_tab + 1) + 'Init Children Time: ', node.init_children_time)
    logger_function('%s %.3f', '\t' * (n_prefix_tab + 1) + 'Filter Children Time: ', node.filter_children_time)


def log_node_validation_time_details(node: XMLNode, logger_function, n_prefix_tab=1):
    if not isinstance(node, XMLNode):
        raise ValueError('Cannot log validation time of  ' + str(node) + ' if is not XMLNode')
    logger_function('\t' * n_prefix_tab + 'Validation time break down: ')
    logger_function('%s %.3f', '\t' * (n_prefix_tab + 1) + 'Value Validation Time:', node.value_validation_time)
    logger_function('%s %.3f', '\t' * (n_prefix_tab + 1) + 'Structure validation Time:', node.structure_validation_time)


def log_node_time_details(node, logger_function, n_prefix_tab = 1):
    log_node_filter_time_details(node, logger_function, n_prefix_tab)
    log_node_validation_time_details(node, logger_function, n_prefix_tab)