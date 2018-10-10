import logging
from typing import List, Dict

from .Combination import Combination
from .DeweyID import relationship_satisfied
from .Entry import Entry
from .Node import Node


def entries_value_validation(validating_node, all_elements_name):
    logger = logging.getLogger("Entries Value Validator")
    logger.debug("Start entries value validating " + validating_node.to_string())

    # validating_node.value_validation_visited = True
    validating_node_entries = validating_node.get_entries()
    logger.debug('Validating Node Entries: ' + str([str(entry) for entry in validating_node_entries]))

    remaining_entries = []
    cursors = {}
    table_entries = {}  # type: Dict[str, List[Entry]]
    table_dimension = {}  # type: Dict[str, int]
    table_elements = {}
    for table_name in validating_node.link_sql:
        cursors[table_name] = 0
        entries = []
        for node in validating_node.link_sql[table_name]:
            for entry in node.get_entries():
                entries.append(entry)
        table_entries[table_name] = entries
        table_elements[table_name] = table_name.split('_')
        table_dimension[table_name] = table_elements[table_name].index(validating_node.name)

    for entry in validating_node_entries:
        logger.debug('Checking Entry: ' + str(entry))
        entry_satisfy = True

        # This entry possible combination of each element
        init_combination = Combination(all_elements_name)
        init_combination.index[validating_node.name] = entry.coordinates[0]
        init_combination.value[validating_node.name] = entry.coordinates[1]
        possible_value_combinations = [init_combination]

        # Check for all table if this entry can match with a table entry
        for table_name in validating_node.link_sql:
            logger.debug('\t' + 'Checking table: ' + table_name)
            # if not entry_satisfy:
            #     break

            elements = table_elements[table_name]
            this_table_combinations = []  # type: List[Combination]

            # if entry does not match a table -> ignore this entry

            while cursors[table_name] < len(table_entries[table_name]):
                table_entry = table_entries[table_name][cursors[table_name]]
                dimension = table_dimension[table_name]
                logger.debug('cursor: ' + str(cursors[table_name]) + ' table entry: ' + str(table_entry))
                # this entry is 'less' than all table_entries -> skip this entry
                if entry.coordinates[1] < table_entry.coordinates[dimension]:
                    logger.debug('\t' * 2 + 'out of range')
                    # entry_satisfy = False
                    break
                # this entry is larger than this table_entry -> move to next table_entry
                if entry.coordinates[1] > table_entry.coordinates[dimension]:
                    cursors[table_name] += 1
                # this entry match a table entry -> save combination, move to next table entry
                else:
                    combination = Combination(all_elements_name)
                    for i in range(len(elements)):
                        combination.value[elements[i]] = table_entry.coordinates[i]
                    combination.index[validating_node.name] = entry.coordinates[0]
                    this_table_combinations.append(combination)
                    cursors[table_name] += 1

            if not this_table_combinations:
                logger.debug('\t' + 'no possible combination')
                entry_satisfy = False
                break

            # else -> Update possible combinations
            updated_possible_combinations = []
            for combination in this_table_combinations:
                for existing_combination in possible_value_combinations:
                    if combination.match_with(existing_combination):
                        updated_possible_combinations.append(combination.combine_with(existing_combination))

            if not updated_possible_combinations:
                logger.debug('\t' + 'empty updated combination')
                entry_satisfy = False
                break
            else:
                possible_value_combinations = updated_possible_combinations

        if entry_satisfy:
            remaining_entries.append(entry)
            entry.possible_combinations = possible_value_combinations

    logger.debug('Remaining Entries: ' + str([str(entry) for entry in remaining_entries]))
    logger.debug('Possible combination')
    for entry in remaining_entries:
        logger.debug(str(entry))
        for combination in entry.possible_combinations:
            logger.debug('\t' + str(combination))
    # if no entry satisfy -> filter this validating node
    if not remaining_entries:
        validating_node.filtered = True
        validating_node.reason_of_filtered = "Entries Value Validation: no entry satisfy"
    # else update validating_node entry
    validating_node.entries = remaining_entries
    logger.debug('End Value Validation: ' + validating_node.to_string())


def entries_structure_validation(validating_node: Node, all_elements_name: [str], relationship_matrix: [[int]]):
    logger = logging.getLogger("Entries Structure Validator")
    logger.debug("Start entries structure validating " + validating_node.to_string())

    validating_node_index = all_elements_name.index(validating_node.name)
    remaining_entries = []

    for validating_entry in validating_node.entries:  # type: Entry
        logger.debug('Checking entry: ' + str(validating_entry))
        # Check with each connected element, if found no match -> Skip this entry
        for connected_element in validating_node.link_xml:
            logger.debug('Checking connected element ' + connected_element)
            updated_possible_combination = []  # type: List[Combination]
            connected_index = all_elements_name.index(connected_element)
            relationship = relationship_matrix[validating_node_index, connected_index]
            # logger.debug('Relationship: ' + str(relationship))

            # Go through each connected node, perform node value validation and check entry pairwise
            connected_nodes = validating_node.link_xml[connected_element]  # type: List[Node]
            for connected_node in connected_nodes:  # type: Node
                logger.debug('\t Checking connected node: ' + connected_node.to_string())
                node_validation(connected_node, all_elements_name, relationship_matrix)

                if connected_node.entries:
                    logger.debug('\t' + 'Connected node still have entries')
                    for connected_entry in connected_node.entries:  # type: Entry
                        logger.debug('\t' * 2 + 'connected_entry ' + str(connected_entry))
                        for connected_combination in connected_entry.possible_combinations:  # type: Combination
                            for validating_entry_combination in validating_entry.possible_combinations:
                                logger.debug('\t' * 2 + 'connected_combination: ' + str(connected_combination))
                                logger.debug('\t' * 2 + 'validating_entry_combination: ' + str(validating_entry_combination))
                                # logger.debug('Value Satisfy: ' + str(connected_combination.match_with(validating_entry_combination)))
                                # logger.debug('Structure Satisfy: ' + str(relationship_satisfied(validating_entry_combination.index[validating_node.name],
                                #                                connected_combination.index[connected_element],
                                #                                relationship)))
                                if (connected_combination.match_with(validating_entry_combination)) and \
                                        relationship_satisfied(validating_entry_combination.index[validating_node.name],
                                                               connected_combination.index[connected_element],
                                                               relationship):
                                    logger.debug('\t' * 3 + 'MATCH')
                                    updated_possible_combination.append(validating_entry_combination.combine_with(
                                        connected_combination))

            if updated_possible_combination:
                logger.debug('Has combination')
                for combination in updated_possible_combination:
                    logger.debug(str(combination))
                validating_entry.possible_combinations = updated_possible_combination
                remaining_entries.append(validating_entry)

    if not remaining_entries:
        validating_node.filtered = True
        validating_node.reason_of_filtered = "Entries Structure Validation: no entry satisfy"


def node_validation(node: Node, all_elements_name: [str], relationship_matrix: [[int]]):
    logger = logging.getLogger("Node Validator")
    logger.debug('Start validaing node ' + node.to_string())
    if node.validated:
        logger.debug('Node already validated')
        return
    node.validated = True
    entries_value_validation(node, all_elements_name)
    if node.filtered:
        logger.debug('Node got filtered by entries value validator')
        return
    entries_structure_validation(node, all_elements_name, relationship_matrix)
    logger.debug('End validating node' + node.to_string())


def validation_xml_sql(all_elements_name: List[str], relationship_matrix: List[List[int]], query_root_node: Node):
    """
    Perform validation of filtered Node
    :param all_elements_name:
    :param relationship_matrix:
    :param query_root_node: root node of the root of query
    :return:
    """

    logger = logging.getLogger("Validator Main")

    leaf_nodes = query_root_node.get_unfiltered_leaf_node()
    logger.info('%s %d', "Number of remaining leaf nodes after Filtering:", len(leaf_nodes))
    logger.debug('Remaining leaf nodes: ' + str([node.boundary for node in leaf_nodes]))

    logger.debug('Start Validating all leaf nodes')
    for node in leaf_nodes:
        node_validation(node, all_elements_name, relationship_matrix)
        logger.debug('\t' + node.to_string() + ' Filtered: ' + str(node.filtered) + node.reason_of_filtered)

    # print("#######################")
    logger.info('%s %d', "Number of remaining leaf nodes of root after validation:",
                len(query_root_node.get_unfiltered_leaf_node()))
    # logger.info("All Possible results:")
    #
    # for result in all_results:
    #     logger.info(result)
    # print("#######################")

    # print('###############################')
    # for node in XML_query_root_leaf_nodes:
    #     print('Node ', node.boundary)
    #     if not node.filtered:
    #         for entry in node.validated_entries:
    #             print('\t', entry.coordinates)
    #             print('\t', 'Entry link_xml')
    #             for connected_element in entry.link_xml.keys():
    #                 print('\t', connected_element)
    #                 for entry_XML in entry.link_xml[connected_element]:
    #                     print('\t' * 2, entry_XML.coordinates)
    #             print('\t', 'Entry link_sql')
    #             for table_name in entry.link_sql.keys():
    #                 print('\t', table_name)
    #                 for entry_SQL in entry.link_sql[table_name]:
    #                     print('\t' * 2, entry_SQL.coordinates)
