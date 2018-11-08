# Helper library for logging


def log_node_link_xml(node, logger, n_prefix_tab=1):
    logger.debug('\t' * n_prefix_tab + 'link xml: ')
    for connected_element in node.link_xml:
        logger.debug('\t' * (n_prefix_tab + 1) + str([str(node) for node in node.link_xml[connected_element]]))


def log_node_link_sql(node, logger, n_prefix_tab=1):
    logger.debug('\t' * n_prefix_tab + 'link sql: ')
    for table_name in node.link_sql:
        logger.debug('\t' * (n_prefix_tab + 1) + str([str(node) for node in node.link_sql[table_name]]))


def log_node_all_link(node, logger, n_prefix_tab=1):
    log_node_link_xml(node, logger, n_prefix_tab)
    log_node_link_sql(node, logger, n_prefix_tab)


def log_node_intersection_range(node, logger, n_prefix_tab=1):
    logger.debug('\t' * n_prefix_tab + 'intersection_range: ')
    for element in node.intersection_range:
        logger.debug('\t' * (n_prefix_tab + 1) + element + ': ' +
                     str([str(boundary) for boundary in node.intersection_range[element]]))


def log_node_filter_time(node, logger, n_prefix_tab = 1):
    logger.debug('\t' * n_prefix_tab + 'Filtering time break down: ')
    logger.debug('\t' * (n_prefix_tab + 1) + 'Value Filtering Time: ' + str(node.value_filtering_time))
    logger.debug('\t' * (n_prefix_tab + 1) + 'Connected Element Filtering Time: '
                 + str(node.connected_element_filtering_time))
    logger.debug('\t' * (n_prefix_tab + 1) + 'Check Descendants Time: ' + str(node.check_lower_level_time))
    logger.debug('\t' * (n_prefix_tab + 1) + 'Init Children Time: ' + str(node.init_children_time))
    logger.debug('\t' * (n_prefix_tab + 1) + 'Filter Children Time: ' + str(node.filter_children_time))


def log_node_validation_time(node, logger, n_prefix_tab=1):
    logger.debug('\t' * n_prefix_tab + 'Validation time break down: ')
    logger.debug('\t' * (n_prefix_tab + 1) + 'Value Validation Time: ' + str(node.value_validation_time))
    logger.debug('\t' * (n_prefix_tab + 1) + 'Structure validation Time: ' + str(node.structure_validation_time))


def log_node_time(node, logger, n_prefix_tab = 1):
    log_node_filter_time(node, logger, n_prefix_tab)
    log_node_validation_time(node, logger, n_prefix_tab)