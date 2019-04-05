import logging
import timeit

from .filter_context_based import Filterer
from .loader import get_index_highest_element
from .validator import node_validation


def initialization(loader):
    """Summary
    This function initialize the limit range for each element root node,
    init link of root node to its connected nodes and its connected tables' nodes

    :param loader: a loader tha contains all the info
    :return: initial limit range for each element
    """
    logger = logging.getLogger("Joiner Init")
    logger.setLevel(logging.getLogger("Joiner").level)
    start_initializing_link = timeit.default_timer()
    # Start from root, link XML root of an element with root of its connected element in XML query
    all_elements_name = loader.all_elements_name
    all_elements_root = loader.all_elements_root
    all_tables_root = loader.all_tables_root
    relationship_matrix = loader.relationship_matrix

    initial_limit_range = {}

    for i in range(len(all_elements_name)):
        element_name = all_elements_name[i]
        element_root = all_elements_root[element_name]
        initial_limit_range[element_name] = element_root.boundary[1]
        for j in range(i + 1, len(all_elements_name)):
            if relationship_matrix[i, j] != 0:
                connected_element = all_elements_name[j]
                element_root.link_xml[connected_element] = []
                element_root.link_xml[connected_element].append(all_elements_root[connected_element])

    ###################################################################3
    # Link tables root with XML root of highest element in XML query
    for table_name in all_tables_root:
        table_root = all_tables_root[table_name]

        # find highest element
        table_elements = table_name.split('_')
        highest_element_name = table_elements[get_index_highest_element(all_elements_name, table_name)]
        # link
        all_elements_root[highest_element_name].link_sql[table_name] = []
        all_elements_root[highest_element_name].link_sql[table_name].append(table_root)

    ################################################################
    # PRINT OUT LINK
    for i in range(len(all_elements_name)):
        element = all_elements_name[i]
        element_root = all_elements_root[element]
        logger.debug(element)
        logger.debug('\t' + 'link_xml')
        for connected_element in element_root.link_xml:
            logger.debug('\t' * 2 + connected_element + ": " +
                         str([str(node) for node in element_root.link_xml[connected_element]]))
        logger.debug('\t' + 'link_sql')
        for connected_table_name in element_root.link_sql:
            logger.debug('\t' * 2 + connected_table_name + ": " +
                         str([str(node) for node in element_root.link_sql[connected_table_name]]))

    end_initializing_link = timeit.default_timer()
    logger.info('%s %d', "Initialize link took: ", end_initializing_link - start_initializing_link)

    return initial_limit_range


# def perform_filtering(loader, initial_limit_range: []):
#     """Summary
#     This function use two queue to do filtering, starting from the root node
#
#     :param loader                 : a Loader that contains all info
#     :param initial_limit_range    : list of initial boundaries for each element
#     :return:
#     """
#     logger = logging.getLogger("Joiner Filtering")
#     logger.setLevel(logging.getLogger("Joiner").level)
#
#     all_elements_name = loader.all_elements_name
#     all_elements_root = loader.all_elements_root
#
#     # Push root of XML query RTree root node to queue
#     start_filtering = timeit.default_timer()
#     n_root_node_processed = 0
#     n_root_node_filtered = 0
#
#     query_root_name = all_elements_name[0]
#     queue_query_root_node = queue.Queue()
#     queue_query_root_node.put(all_elements_root[query_root_name])
#     queue_limit_range = queue.Queue()
#     queue_limit_range.put(initial_limit_range)
#
#     while not queue_query_root_node.empty():
#         query_root_node = queue_query_root_node.get()  # type: XMLNode
#         limit_range = queue_limit_range.get()
#
#         n_root_node_processed += 1
#
#         updated_limit_range = full_filtering(query_root_node, all_elements_name, limit_range)
#
#         logger.debug('Node: ' + str(query_root_node))
#         log_node_filter_status(query_root_node, logger.debug, 1)
#         logger.debug('%s %3f', '\t' + 'filter time: ', query_root_node.full_filtering_time)
#         logger.debug('')
#
#         if not query_root_node.filtered:
#             for XML_query_root_node_child in query_root_node.children:
#                 queue_query_root_node.put(XML_query_root_node_child)
#                 queue_limit_range.put(copy.deepcopy(updated_limit_range))
#         else:
#             n_root_node_filtered += 1
#
#     end_filtering = timeit.default_timer()
#     logger.info('%s %3f', 'Total Filtering time', end_filtering - start_filtering)
#     logger.info('%s %d', 'Number of node processed: ', n_root_node_processed)
#     logger.info('%s %d', 'Number of node filtered: ', n_root_node_filtered)
#     logger.info('%s %3f', 'Average filtering time for one node:',
#                 (end_filtering - start_filtering) / n_root_node_processed)


def perform_validation(loader):
    """Summary
    This function use two queue to do filtering, starting from the root node

    :param loader: a Loader that contains all info
    :return:
    """

    logger = logging.getLogger("Joiner Validating")
    logger.setLevel(logging.getLogger("Joiner").level)

    start_validation = timeit.default_timer()

    query_root_node = loader.all_elements_root[loader.all_elements_name[0]]

    leaf_nodes = query_root_node.get_unfiltered_leaf_node()
    logger.info('%s %d', "Number of remaining leaf nodes after Filtering:", len(leaf_nodes))
    logger.debug('Remaining leaf nodes: ' + str([node.boundary for node in leaf_nodes]))

    logger.debug('Start Validating all leaf nodes')
    for node in leaf_nodes:
        node_validation(node, loader.all_elements_name, loader.relationship_matrix)
        logger.debug('\t' + str(node) + ' Filtered: ' + str(node.filtered) + node.reason_of_filtered)

    logger.info('%s %d', "Number of remaining leaf nodes of root after validation:",
                len(query_root_node.get_unfiltered_leaf_node()))
    end_validation = timeit.default_timer()

    logger.info('%s %d', 'validation time', end_validation - start_validation)

    ##################################################################

    logger.info('Results: ')
    for node in query_root_node.get_unfiltered_leaf_node():
        logger.info('Node: ' + str(node))
        for entry in node.entries:
            logger.info('Entry: ' + str(entry))
            for combination in entry.possible_combinations:
                logger.info(str(combination))
    # Return final result
    # get_final_results(all_elements_name, relationship_matrix, all_elements_root[XML_query_root_element])


class Joiner:
    """Summary
    This joiner takes a loader and do the joining
    """

    def __init__(self, loader, loading_method: str):
        logger = logging.getLogger("Joiner")
        logger.info("Started Joiner")
        initial_limit_range = initialization(loader)
        # perform_filtering(loader, initial_limit_range)
        filterer = Filterer(loader, initial_limit_range)
        filterer.perform()

        logger = logging.getLogger("Joiner Validating")
        logger.setLevel(logging.getLogger("Joiner").level)
        query_root_node = loader.all_elements_root[loader.all_elements_name[0]]
        leaf_nodes = query_root_node.get_unfiltered_leaf_node()
        logger.info('%s %d', "Number of remaining leaf nodes after Filtering:", len(leaf_nodes))
        logger.info('Remaining leaf nodes: ')
        for node in leaf_nodes:
            logger.info('\t' + str(node) + ' ---Entries; ' + str(node.get_entries()))

        # perform_validation(loader)
