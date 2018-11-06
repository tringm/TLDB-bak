import logging
import timeit

from typing import List, Dict

from src.Entry import Entry
from .DeweyID import relationship_satisfied
from .Entry import Entry
from .Node import Node


def match_entry(entry1: Entry, elements1: list(str), entry2: Entry, elements2: list(str)):
    """
    This is an adhoc function that check if entry2 matches with entry1 assuming that entry2 always has less elements
    than entry 1
    :param entry1:
    :param elements1: list of elements of entry1
    :param entry2:
    :param elements2:
    :return:
    """
    if len(elements2) > len(elements1):
        raise ValueError('Something is wrong' + str(entry2) + 'supposed to has less elements than ' + str(entry1))
    for i in range(len(entry2.coordinates)):
        element_tbl2 = elements2[i]
        idx_tbl1 = elements1.index(element_tbl2)
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


    # If this node doesn't have any linked table -> matching entries = all its entries
    if not validating_node.link_sql:
        logger.debug('\t' * validating_node_index + 'No link sql ')
        for e in validating_node_entries:
            e.matching_value_entries = (None, e)
        end_value_validation = timeit.default_timer()
        validating_node.value_validation_time = end_value_validation - start_value_validation
        return

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

        matching_v = []  # type: List[Dict[str, List[int]]]

        # Check for all table if this entry can match with a table entry
        for table_name in table_names:
            logger.verbose('\t' * (validating_node_index + 2) + 'Checking table: ' + table_name)
            # if not entry_satisfy:
            #     break

            elements = table_elements[table_name]
            dimension = table_dimension[table_name]

            this_table_matching_v_e = []  # type: List[Entry] 

            # if entry does not match a table -> ignore this entry
            while cursors[table_name] < len(table_entries[table_name]):
                table_entry = table_entries[table_name][cursors[table_name]]
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
                # this entry match a table entry -> save this entry, move to next table entry
                else:
                    logger.verbose('\t' * (validating_node_index + 4) + 'MATCH')
                    cursors[table_name] += 1
                    this_table_matching_v_e.append(table_entry)

            # This table does not have any matching entry
            if not this_table_matching_v_e:
                logger.verbose('\t' * (validating_node_index + 2) + '---> No match entry, ignored')
                entry_satisfy = False
                break

            # Initialize matching entries if not
            if not matching_v:
                for e in this_table_matching_v_e:
                    matching_v.append(zip(elements, e.coordinates))
                matching_v_e = this_table_matching_v_e
                matching_v_e_elements = table_name.split('_')

            # Combine existing matching entries with this table matching entries
            else:
                logger.verbose('\t' * (validating_node_index + 3) + 'Matching with prev entries')
                updated_matching_v_e = []
                for m_e in matching_v_e:
                    for tbl_e in this_table_matching_v_e:
                        if match_entry(m_e, matching_v_e_elements, tbl_e, elements):
                            logger.verbose('\t' * (validating_node_index + 4) + 'Match: ' + str(m_e) + ' ' + str(tbl_e))
                            updated_matching_v_e.append(m_e)
                            break

                if not updated_matching_v_e:
                    logger.verbose('\t' * (validating_node_index + 2) + '---> No match entry with existing table')
                    entry_satisfy = False
                    break

                else:
                    matching_v_e = updated_matching_v_e

        if entry_satisfy:
            remaining_entries.append(entry)
            entry.matching_value_entries = (matching_v_e_elements, matching_v_e)

    # if no entry satisfy -> filter this validating node
    if not remaining_entries:
        validating_node.filtered = True
        validating_node.reason_of_filtered = "Entries Value Validation: no entry satisfy"
        logger.debug('\t' * (validating_node_index + 1) + '###')
        logger.debug('\t' * (validating_node_index + 1) + 'FILTERED: ' + validating_node.reason_of_filtered)
        end_value_validation = timeit.default_timer()
        validating_node.value_validation_time = end_value_validation - start_value_validation
        return
    # else update validating_node entry
    else:
        validating_node.entries = remaining_entries

    logger.debug('\t' * validating_node_index + 'Remaining Entries: ')
    for entry in validating_node.entries:
        logger.debug('\t' * (validating_node_index + 1) + str(entry) + ' Matching entries: '
                     + str([str(e) for e in entry.matching_value_entries]))

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

    for validating_e in validating_node.entries:  # type: Entry
        logger.verbose('\t' * (validating_node_index + 1) + 'Checking entry: ' + str(validating_e))
        # Check with each connected element, if found no match -> Skip this entry
        for c_e in connected_elements:
            logger.verbose('\t' * (validating_node_index + 2) + 'Checking connected element ' + c_e)
            rls = relationship_matrix[validating_node_index, all_elements_name.index(c_e)]

            # Go through each connected node, perform node value validation and check entry pairwise
            c_e_nodes = validating_node.link_xml[c_e]  # type: List[Node]
            for c_e_node in c_e_nodes:  # type: Node
                logger.verbose('\t' * (validating_node_index + 3) + 'Checking connected node: ' + str(c_e_node))
                node_validation(c_e_node, all_elements_name, relationship_matrix)

                if c_e_node.entries:
                    logger.verbose('\t' * (validating_node_index + 4) + 'Connected node still have entries')
                    for c_e_entry in c_e_node.entries:  # type: Entry
                        logger.verbose('\t' * (validating_node_index + 4) + 'Connected entry ' + str(c_e_entry))

                        for matchi



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

    end_structure_validation_time = timeit.default_timer()
    validating_node.structure_validation_time = end_structure_validation_time - start_structure_validation_time


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