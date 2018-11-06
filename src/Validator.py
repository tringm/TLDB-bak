import logging
import timeit

from typing import List, Dict

from .Combination import Combination
from .DeweyID import relationship_satisfied
from .Entry import Entry
from .Node import Node


def match_entry(entry1: Entry, table1: str, entry2: Entry, table2: str):
    """
    This is an adhoc function that check if entry2 matches with entry1 assuming that entry2 always has less elements
    than entry 1
    :param entry1:
    :param table1: name of table
    :param entry2:
    :param table2:
    :return:
    """
    table1_elements = table1.split('_')
    table2_elements = table2.split('_')
    if len(table1_elements) > len(table2_elements):
        raise ValueError('Something is wrong' + str(entry2) + 'supposed to has less elements than ' + str(entry1))
    for i in range(len(entry2.coordinates)):
        element_tbl2 = table2_elements[i]
        idx_tbl1 = table1_elements.index(element_tbl2)
        if entry1.coordinates[idx_tbl1] != entry2.coordinates[i]:
            return False

    return True


def entries_value_validation(validating_node, all_elements_name):
    start_value_validation = timeit.default_timer()
    validating_node_index = all_elements_name.index(validating_node.name)

    logger = logging.getLogger("Entries Value Validator")
    logger.setLevel(logging.getLogger("Node Validator").level)
    logger.debug('\t' * validating_node_index + 'Start entries value validating ' + str(validating_node))

    # validating_node.value_validation_visited = True
    validating_node_entries = validating_node.get_entries()

    remaining_entries = []
    cursors = {}
    table_entries = {}  # type: Dict[str, List[Entry]]
    table_dimension = {}  # type: Dict[str, int]
    table_elements = {}

    table_names = list(validating_node.link_sql.keys())
    table_names.sort(key=lambda name: len(name), reverse=True)

    for table_name in table_names:
        cursors[table_name] = 0
        entries = []
        for node in validating_node.link_sql[table_name]:
            entries += node.get_entries()
        table_entries[table_name] = entries
        table_elements[table_name] = table_name.split('_')
        table_dimension[table_name] = table_elements[table_name].index(validating_node.name)

    for entry in validating_node_entries:
        logger.verbose('\t' * (validating_node_index + 1) + 'Checking Entry: ' + str(entry))
        entry_satisfy = True

        # This entry possible combination of each element
        init_combination = Combination(all_elements_name)
        init_combination.index[validating_node.name] = entry.coordinates[0]
        init_combination.value[validating_node.name] = entry.coordinates[1]

        matching_entries = []
        matching_entries_table_name = ''
        # possible_value_combinations = [init_combination]

        # Check for all table if this entry can match with a table entry
        for table_name in table_names:
            logger.verbose('\t' * (validating_node_index + 2) + 'Checking table: ' + table_name)
            # if not entry_satisfy:
            #     break

            elements = table_elements[table_name]
            # this_table_combinations = []  # type: List[Combination]

            this_table_matching_entries = []

            # if entry does not match a table -> ignore this entry
            while cursors[table_name] < len(table_entries[table_name]):
                table_entry = table_entries[table_name][cursors[table_name]]
                dimension = table_dimension[table_name]
                logger.verbose('\t' * (validating_node_index + 3)
                               + 'cursor: ' + str(cursors[table_name]) + ' table entry: ' + str(table_entry))
                # this entry is 'less' than all table_entries -> skip this entry
                if entry.coordinates[1] < table_entry.coordinates[dimension]:
                    logger.verbose('\t' * (validating_node_index + 4) + 'NOT MATCH, move to next table or end')
                    # entry_satisfy = False
                    break
                # this entry is larger than this table_entry -> move to next table_entry
                if entry.coordinates[1] > table_entry.coordinates[dimension]:
                    logger.verbose('\t' * (validating_node_index + 4) + 'NOT MATCH, move to next table entry')
                    cursors[table_name] += 1
                # this entry match a table entry -> save combination, move to next table entry
                else:
                    logger.verbose('\t' * (validating_node_index + 4) + 'MATCH')
                    cursors[table_name] += 1
                    this_table_matching_entries.append(table_entry)

            # This table does not have any matching entry
            if not this_table_matching_entries:
                logger.verbose('\t' * (validating_node_index + 2) + '---> No match entry, ignored')
                entry_satisfy = False
                break

            # Initialize matching entries if not
            if not matching_entries:
                matching_entries = this_table_matching_entries
                matching_entries_table_name = table_name

            # Combine existing matching entries with this table matching entries
            else:
                updated_matching_entries = []
                for matching_entry in matching_entries:
                    for this_table_entry in this_table_matching_entries:
                        if match_entry(matching_entry, matching_entries_table_name, this_table_entry, table_name):
                            updated_matching_entries.append(matching_entry)

                if not updated_matching_entries:
                    logger.verbose('\t' * (validating_node_index + 2) + '---> No match entry with existing table')
                    entry_satisfy = False
                    break

                else:
                    matching_entries = updated_matching_entries

        if entry_satisfy:
            remaining_entries.append(entry)
            entry.matching_entries = {matching_entries_table_name: matching_entries}

    # if no entry satisfy -> filter this validating node
    if not remaining_entries:
        validating_node.filtered = True
        validating_node.reason_of_filtered = "Entries Value Validation: no entry satisfy"
        logger.debug('\t' * (validating_node_index + 1) + '###')
        logger.debug('\t' * (validating_node_index + 1) + 'FILTERED: ' + validating_node.reason_of_filtered)
    # else update validating_node entry
    else:
        validating_node.entries = remaining_entries

    logger.debug('\t' * (validating_node_index + 1) + 'Remaining Entries: ' +
                 str([str(entry) for entry in remaining_entries]))
    logger.debug('\t' * (validating_node_index + 1) + 'Matching Entries:' +
                 str([str(entry) for entry in entry.matching_entries]))

    logger.debug('\t' * validating_node_index + 'End Value Validation: ' + str(validating_node))
    end_value_validation = timeit.default_timer()
    validating_node.value_validation_time = end_value_validation - start_value_validation


def entries_structure_validation(validating_node: Node, all_elements_name: [str], relationship_matrix: [[int]]):
    start_structure_validation_time = timeit.default_timer()

    logger = logging.getLogger("Entries Structure Validator")
    logger.setLevel(logging.getLogger("Node Validator").level)
    validating_node_index = all_elements_name.index(validating_node.name)
    logger.debug('\t' * validating_node_index + "Start entries structure validating " + str(validating_node))
    remaining_entries = []

    connected_elements = list(validating_node.link_xml.keys())
    connected_elements.sort(key=lambda element: all_elements_name.index(element))

    for validating_entry in validating_node.entries:  # type: Entry
        logger.debug('Checking entry: ' + str(validating_entry))
        # Check with each connected element, if found no match -> Skip this entry
        for connected_element in connected_elements:
            logger.debug('Checking connected element ' + connected_element)
            updated_possible_combination = []  # type: List[Combination]
            connected_index = all_elements_name.index(connected_element)
            relationship = relationship_matrix[validating_node_index, connected_index]
            # logger.debug('Relationship: ' + str(relationship))

            # Go through each connected node, perform node value validation and check entry pairwise
            connected_nodes = validating_node.link_xml[connected_element]  # type: List[Node]
            for connected_node in connected_nodes:  # type: Node
                logger.debug('\t Checking connected node: ' + str(connected_node))
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
    logger.debug('Start validating node ' + str(node))
    if node.validated:
        logger.debug('Node already validated')
        return
    node.validated = True
    entries_value_validation(node, all_elements_name)
    if node.filtered:
        logger.debug('Node got filtered by entries value validator')
        return
    entries_structure_validation(node, all_elements_name, relationship_matrix)
    logger.debug('End validating node' + str(node))