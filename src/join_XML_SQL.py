import copy
import logging
import queue
import sys
import timeit
from typing import List

import numpy as np

from .DeweyID import *
from .Filterer import full_filtering
from .Loader import Loader, get_index_highest_element
from .Node import Node
from .Result import Result
from .Validator import validation_xml_sql

logger = logging.getLogger("Main")
logger.disabled = False


def join_xml_sql(all_elements_name: [str], all_elements_root: [Node], all_tables_root: [Node], relationship_matrix: []):
    """Summary
    This function join XML DB and SQL DB given a XML query

    :param all_elements_name    : list of all elements' name in XML_query ordered by level order
    :param all_elements_root    : list of all XML element root nodes (based on the order of the XML query)
    :param all_tables_root      : list of all SQL table root nodes (based on the highest order in XML query level orer)
    :param relationship_matrix  : 2 dimension array store relationship between element.
                                Relation[parent, children] = 1, relation[ancestor, descendant] = 2, other = 0
    :return:
    """

    ######################################
    # # # Print Tree
    # print('XML')
    # for element in all_elements_name:
    #     print(element)
    #     all_elements_root[element].print_node()
    #
    # print('SQL')
    # for table_name in all_tables_root.keys():
    #     print(table_name)
    #     all_tables_root[table_name].print_node()

    ################################################################
    # Intialization
    # Start from root, link XML root of an element with root of its connected element in XML query
    start_initializing_link = timeit.default_timer()

    limit_range = {}

    for i in range(len(all_elements_name)):
        element_name = all_elements_name[i]
        element_root = all_elements_root[element_name]
        limit_range[element_name] = element_root.boundary[1]
        for j in range(i + 1, len(all_elements_name)):
            if relationship_matrix[i, j] != 0:
                connected_element = all_elements_name[j]
                element_root.link_xml[connected_element] = []
                element_root.link_xml[connected_element].append(all_elements_root[connected_element])

    ###################################################################3
    # Link tables root with XML root of highest element in XML query
    for table_name in all_tables_root.keys():
        table_root = all_tables_root[table_name]

        # find highest element
        table_elements = table_name.split('_')
        highest_element_name = table_elements[get_index_highest_element(all_elements_name, table_name)]
        # link
        all_elements_root[highest_element_name].link_sql[table_name] = []
        all_elements_root[highest_element_name].link_sql[table_name].append(table_root)

    end_initializing_link = timeit.default_timer()
    logger.info('%s %d', "Initialize link took: ", end_initializing_link - start_initializing_link)

    ################################################################
    # PRINT OUT LINK
    # for i in range(len(all_elements_name)):
    #     element = all_elements_name[i]
    #     element_root = all_elements_root[element]
    #     print(element)
    #     print('link_xml')
    #     for connected_element in element_root.link_xml.keys():
    #         print(connected_element)
    #         for connected_element_root in element_root.link_xml[connected_element]:
    #             print(connected_element_root.boundary)
    #     print('link_sql')
    #     for connected_table_name in element_root.link_sql.keys():
    #         print(connected_table_name)
    #         for connected_table_root in element_root.link_sql[connected_table_name]:
    #             print(connected_table_root.boundary)

    ##################################################################
    # Push root of XML query RTree root node to queue
    start_filtering = timeit.default_timer()

    query_root_name = all_elements_name[0]
    queue_query_root_node = queue.Queue()
    queue_query_root_node.put(all_elements_root[query_root_name])
    queue_limit_range = queue.Queue()
    queue_limit_range.put(limit_range)

    n_root_node_full_filtered = 0

    while not queue_query_root_node.empty():
        query_root_node = queue_query_root_node.get()  # type: Node
        limit_range = queue_limit_range.get()

        start_one_node_filtering = timeit.default_timer()

        updated_limit_range = full_filtering(query_root_node, all_elements_name, limit_range)

        end_one_node_filtering = timeit.default_timer()

        n_root_node_full_filtered += 1
        logger.info('%s %s %s %d %s %s %s', 'query root node', query_root_node.to_string(), 'filter time:',
                    end_one_node_filtering - start_one_node_filtering, 'filtered:', query_root_node.filtered, query_root_node.reason_of_filtered)

        if not query_root_node.filtered:
            for XML_query_root_node_child in query_root_node.children:
                queue_query_root_node.put(XML_query_root_node_child)
                queue_limit_range.put(copy.deepcopy(updated_limit_range))
        # print("###############")

    end_filtering = timeit.default_timer()
    logger.info('%s %d','Filtering time', end_filtering - start_filtering)
    logger.info('%s %d', 'Average filtering time for one node:',
                (end_filtering - start_filtering) / n_root_node_full_filtered)
    # print("######################################")

    ##################################################################
    # Print filtered tree
    # all_elements_root[XML_query_root_element].print_node_not_filtered_with_link()

    ##################################################################
    # Perform validation
    start_validation = timeit.default_timer()
    validation_xml_sql(all_elements_name, relationship_matrix, all_elements_root[query_root_name])
    end_validation = timeit.default_timer()

    logger.info('%s %d', 'validation time', end_validation - start_validation)

    ##################################################################

    logger.info('Results: ')
    for node in all_elements_root[query_root_name].get_unfiltered_leaf_node():
        logger.info('Node: ' + node.to_string())
        for entry in node.entries:
            logger.info('Entry: ' + str(entry))
            for combination in entry.possible_combinations:
                logger.info(str(combination))
    # Return final result
    # get_final_results(all_elements_name, relationship_matrix, all_elements_root[XML_query_root_element])


def main():
    if len(sys.argv) < 3:
        raise ValueError('Missing arguments. Requires 2 arguments in the following order: folder_name, max_n_children')
    folder_name = sys.argv[1]
    max_n_children = int(sys.argv[2])
    # sys.stdout = open("io/" + folder_name + "/max_children_" + str(max_n_children) + ".txt", 'w')
    loader = Loader(folder_name, max_n_children)
    join_xml_sql(loader.all_elements_name, loader.all_elements_root, loader.all_tables_root, loader.relationship_matrix)

