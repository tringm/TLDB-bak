import RTree_XML
import RTree_SQL
import queue
import os
from Node import Node
from Entry import Entry


def load_elements(folder_name, elements_name, max_n_children):
    """Summary
    Load multiple elements and return list of root node of each element's R_Tree
    Args:
        folder_name (string)
        elements_name (list of string): list of name of elements in XML query
        max_n_children (int): maximum number of children in R_Tree
    
    Returns:
        dict[string, Node]: dict with key is name of element and value is corresponding root node of RTree_XML
    """
    elements_root = {}
    for element_name in elements_name:
        elements_root[element_name] = build_RTree_XML(folder_name, element_name, max_n_children)
    return(elements_root)

def build_RTree_XML(folder_name, element_name, max_n_children, dimension = 1):
    """Summary
    Build a RTree for an XML element
    
    Args:
        folder_name (string): 
        element_name (string)
        max_n_children (int): maximum number of children in RTree
        dimension (int, optional): dimenstion to be sorted for RTree, currently 1 (value)
    
    Returns:
        Node: root node of built RTree
    """
    entries = RTree_XML.load(folder_name, element_name)
    root = RTree_XML.bulk_loading(entries, max_n_children, dimension)
    return root

def load_tables(folder_name, elements_name, max_n_children):
    """Summary
    Load all tables inside a folder (files that end with _table.dat)
    Args:
        folder_name (string):
        elements_name (list of string): list of elements in XML query
        max_n_children (int): maximum number of children in R_Tree
    
    Returns:
        dict[string, Node]: dict with key is name of table and value is corresponding root node of RTree_SQL
    """
    tables_root = {}
    for file in os.listdir("../data/" + folder_name):
        if 'table' in file:
            table_elements = table_name[:-10].split('_')
            tables_root[table_elements] = build_RTree_SQL(folder_name, file_name, elements_name, max_n_children)
    return tables_root


def build_RTree_SQL(folder_name, table_name, elements_name, max_n_children):
    """Summary
    Build a RTree for a SQL table with each element is a column. This RTree is sorted by element that is at highest level (smallest index in elements list) in XML query tree
    
    Args:
        folder_name (string):
        table_name (string)
        elements_name (list of string): list of elements_name in XML query
        max_n_children (int): maximum number of children in RTree
    
    Returns:
        Node: root node of built RTree
    """
    entries = RTree_SQL.load(folder_name, table_name)
    
    # Find which element in table is higest level and sort RTree based on this element 
    table_elements = table_name[:-10].split('_')
    index = []
    for table_element in table_elements:
        index.append(elements_name.index(table_element))
    dimension = np.argmin(np.asarray(index))
    root = RTree_SQL.bulk_loading(entries, max_n_children, dimension)
    return root



def value_filtering_1d(filtering_node, condition_nodes, max_level):
    """Summary
    This function do value filtering for a R_Tree_XML node based on stored previous positions (condition_nodes) in a 1 dimension R_Tree_SQL 
    Args:
        filtering_node (Node): A Node in R_Tree_XML to be checked for filtering
        condition_nodes ([Node]): list of Nodes in R_Tree_SQL to be used as condition for filtering
        max_level (int): maximum number of level traversing down the R_Tree_SQL
    
    Returns:
        [Node]: List of Nodes to be checked in next level. Empty if this filtering node is filtered
    """
    condition_nodes = condition_nodes
    # Traverse max_level
    for level in range(max_level):
        # print('level', level)

        next_level = []
        in_range = False
        for i in range(len(condition_nodes)):
            condition_node = condition_nodes[i]
            # print('Node ', i, ' boundary:', condition_node.boundary)

            # if this condition node range contains filtering node value range then add children (unless is leaf node)
            if (((condition_node.boundary[0][0] >= filtering_node.boundary[1][0]) and (condition_node.boundary[0][0] <= filtering_node.boundary[1][1])) or
                ((condition_node.boundary[0][0] <= filtering_node.boundary[1][0]) and (condition_node.boundary[0][1] >= filtering_node.boundary[1][0])) or
                ((condition_node.boundary[0][1] <= filtering_node.boundary[1][1]) and (condition_node.boundary[0][0] >= filtering_node.boundary[1][0])) or
                ((condition_node.boundary[0][1] >= filtering_node.boundary[1][1]) and (condition_node.boundary[0][0] <= filtering_node.boundary[1][1]))):
                # print('is in range')
                in_range = True
                # if has children -> add children else add itself
                if condition_node.children:
                    for child in condition_node.children:
                        next_level.append(child)
                else:
                    next_level.append(condition_node)

        # print('next_level')
        # for i in range(len(next_level)):
            # print(next_level[i].boundary)
        

        # No condition nodes are in range -> Filter this filtering node
        # Else update condition nodes
        if not in_range:
            # print('No node in range')
            filtering_node.filtered = True
            return []
        if next_level:
            # print('Next level is not empty')
            condition_nodes = next_level
    return next_level


def value_filtering_2d(A_XML_node, D_XML_node, A_D_SQL_nodes, max_level):
    """Summary
    This function do value filtering for 2 R_Tree_XML nodes based on stored previous positions (condition_nodes) in a 2-dimension R_Tree_SQL 
    
    Args:
        A_XML_node (Node): R_Tree_XML Node
        D_XML_node (Node): R_Tree_XML Node
        A_D_SQL_nodes ([Node]): List of R_Tree_SQL Nodes
        max_level (int): maximum number of level traversing down the R_Tree_SQL
    
    Returns:
        [Node]: List of Nodes to be checked in next level. Empty if this pair of A and D is not in range
    """
    # print('value_filtering_2d')

    # print('A_XML_node: ', A_XML_node.boundary)
    # print('D_XML_node: ', D_XML_node.boundary)

    condition_nodes = A_D_SQL_nodes
    for level in range(max_level):
        next_level = []
        in_range = False
        for i in range(len(condition_nodes)):
            condition_node = condition_nodes[i]
            # print('Node ', i, ' boundary:', condition_node.boundary)

            # print('condition_node: ', condition_node.boundary)
            A_in_range = False
            if (((condition_node.boundary[0][0] >= A_XML_node.boundary[1][0]) and (condition_node.boundary[0][0] <= A_XML_node.boundary[1][1])) or
                ((condition_node.boundary[0][0] <= A_XML_node.boundary[1][0]) and (condition_node.boundary[0][1] >= A_XML_node.boundary[1][0])) or
                ((condition_node.boundary[0][1] <= A_XML_node.boundary[1][1]) and (condition_node.boundary[0][0] >= A_XML_node.boundary[1][0])) or
                ((condition_node.boundary[0][1] >= A_XML_node.boundary[1][1]) and (condition_node.boundary[0][0] <= A_XML_node.boundary[1][1]))):
                A_in_range = True
            D_in_range = False
            if (((condition_node.boundary[1][0] >= D_XML_node.boundary[1][0]) and (condition_node.boundary[1][0] <= D_XML_node.boundary[1][1])) or
                ((condition_node.boundary[1][0] <= D_XML_node.boundary[1][0]) and (condition_node.boundary[1][1] >= D_XML_node.boundary[1][0])) or
                ((condition_node.boundary[1][1] <= D_XML_node.boundary[1][1]) and (condition_node.boundary[1][0] >= D_XML_node.boundary[1][0])) or
                ((condition_node.boundary[1][1] >= D_XML_node.boundary[1][1]) and (condition_node.boundary[1][0] <= D_XML_node.boundary[1][1]))):
                D_in_range = True
            # print('A_in_range: ', A_in_range)
            # print('D_in_range: ', D_in_range)
            if (A_in_range and D_in_range):
                in_range = True
                # if has children -> add children else add itself
                if condition_node.children:
                    for child in condition_node.children:
                        next_level.append(child)
                else:
                    next_level.append(condition_node)

        # print('next_level')
        # for i in range(len(next_level)):
            # print(next_level[i].boundary)
        

        # No condition nodes are in range -> Filter this filtering node
        # Else update condition nodes
        if not in_range:
            return []
        if next_level:
            # print('Next level is not empty')
            condition_nodes = next_level
    return next_level 


def ancestor_descendant_filter(A_XML, D_XML):
    """Summary
    This function check if a node D_XML is descendant of node A_XML
    Args:
        A_XML (Node): R_Tree_XML Node to be checked if is ancestor
        D_XML (Node): R_Tree_XML Node to be checked if is descendant
    
    Returns:
        Bool: True if A_XML is ancestor of D_XML
    """
    A_XML_structure_boundary = A_XML.boundary[0]
    D_XML_structure_boundary = D_XML.boundary[0]
    if ((A_XML_structure_boundary[0] <= D_XML_structure_boundary[0]) and (A_XML_structure_boundary[1] >= D_XML_structure_boundary[1])):
        return True
    return False



def pairwise_filtering_ancestor_descendant(A_XML, D_XML, A_SQL, D_SQL, A_D_SQL, max_level):
    """Summary
    Structure filtering (D is descendant of A) and value filtering based on SQL tables
    
    Args:
        A_XML (Node): Root node of RTree_XML of A (filtering tree)
        D_XML (Node): Root node of RTree_XML of D (filtering tree)
        A_SQL (Node): Root node of RTree_SQL of table of A (requirement tree)
        D_SQL (Node): Root node of RTree_SQL of table of D (requirement tree)
        A_D_SQL (Node): Root node of RTree_SQL of tables of A_D (requirement tree)
        max_level (int): maximum number of level to be travelled in requirement trees when filtering
    """
    queue_D_XML = queue.Queue()
    queue_D_XML.put(D_XML)

    # List of satisified nodes in each requirement trees (D_SQL, A_D_SQL), inherit from the parent
    # In the beginning -> all root node
    D_XML.position_D_SQL = []
    D_XML.position_A_D_SQL = []
    D_XML.position_A_XML = []
    D_XML.position_D_SQL.append(D_SQL)
    D_XML.position_A_D_SQL.append(A_D_SQL)
    D_XML.position_A_XML.append(A_XML)
    A_XML.position_A_SQL = []
    A_XML.position_A_SQL.append(A_SQL)

    while not queue_D_XML.empty():
        node_D_XML = queue_D_XML.get()
        print()
        print('node_D_XML', node_D_XML.boundary)
        print('node_D_XML.position_D_SQL')
        for i in range(len(node_D_XML.position_D_SQL)):
            print(node_D_XML.position_D_SQL[i].boundary)

        new_position_D_SQL = value_filtering_1d(node_D_XML, node_D_XML.position_D_SQL, max_level)
        print('new_position_D_SQL')
        for i in range(len(new_position_D_SQL)):
            print(new_position_D_SQL[i].boundary)
        # print('filtered', node_D_XML.filtered)
        # if this node_D_XML is not value-filtered
        if new_position_D_SQL:
            # Update its children position_D_SQL
            # print('update children position_D_SQL')
            for node_D_XML_child in node_D_XML.children:
                node_D_XML_child.position_D_SQL = new_position_D_SQL
                # print(node_D_XML_child.boundary)
                # for i in range(len(node_D_XML_child.position_D_SQL)):
                    # print(node_D_XML_child.position_D_SQL[i].boundary)

            # Checking each A_XML node
            has_pairable_A_XML = False

            for i in range(len(node_D_XML.position_A_XML)):
                node_A_XML = node_D_XML.position_A_XML[i]

                print('node_A_XML')
                print(node_A_XML.boundary)
                print('node_A_XML.position_A_SQL')
                for j in range(len(node_A_XML.position_A_SQL)):
                    print(node_A_XML.position_A_SQL[i].boundary)


                # If this node_A_XML is filtered (through value filtering) 
                if node_A_XML.filtered:
                    continue

                # !!!Warning: Since checking pairwise A_XML_node and D_XML_node, A_XML_node can be value-filtered multiple time

                # Value filtering for node_A_XML
                new_position_A_SQL = value_filtering_1d(node_A_XML, node_A_XML.position_A_SQL, max_level)
                print('new_position_A_SQL')
                for j in range(len(new_position_A_SQL)):
                    print(new_position_A_SQL[j].boundary)
                # if this node_A_XML is not value-filtered
                if new_position_A_SQL:
                    
                    # Update its children position_D_SQL
                    for node_A_XML_child in node_A_XML.children:
                        node_A_XML_child.position_A_SQL = new_position_A_SQL

                    # Structure filtering A_D
                    if ancestor_descendant_filter(node_A_XML, node_D_XML):
                        print('Structure requirement met')
                        # Value_filtering A_D
                        print('position_A_D_SQL')
                        for j in range(len(node_D_XML.position_A_D_SQL)):
                            print(node_D_XML.position_A_D_SQL[j].boundary)                            
                        new_position_A_D_SQL = value_filtering_2d(node_A_XML, node_D_XML, node_D_XML.position_A_D_SQL, max_level)
                        print('new_position_A_D_SQL')
                        for j in range(len(new_position_A_D_SQL)):
                            print(new_position_A_D_SQL[j].boundary)                            
                        # if not filtered
                        if new_position_A_D_SQL:
                            has_pairable_A_XML = True

                            for node_D_XML_child in node_D_XML.children:
                                if not hasattr(node_D_XML_child, 'position_A_D_SQL'):
                                    node_D_XML_child.position_A_D_SQL = []
                                if not hasattr(node_D_XML_child, 'position_A_XML'):
                                    node_D_XML_child.position_A_XML = []
                                # !!!Warning: this can be bad, it should be a specific pair of A and D. This solution just add all 
                                for A_D_SQL_node in new_position_A_D_SQL:
                                    node_D_XML_child.position_A_D_SQL.append(A_D_SQL_node)

                                if node_A_XML.children:    
                                    for node_A_XML_child in node_A_XML.children:
                                        node_D_XML_child.position_A_XML.append(node_A_XML_child)
                                else:
                                    node_D_XML_child.position_A_XML.append(node_A_XML)
                                queue_D_XML.put(node_D_XML_child)
                else:
                    node_A_XML.filtered = True

            if not has_pairable_A_XML:
                node_D_XML_child.filtered = True
                                
                     
                    

        # print('new_position_D_SQL')
        # for i in range(len(new_position_D_SQL)):
        #   print(new_position_D_SQL[i].boundary)

def filtering(folder_name, elements_name, relationship_matrix, max_n_children):
    # Loading XML and SQL database into R_Tree
    elements_root = load_elements(folder_name, elements_name, max_n_children)
    tables_root = load_tables(folder_name)

    # Intialization
    # Start from root, link to root of connected node in XML query
    for i in range(len(elements_name)):
        element_name = elements_name[i]
        element_root = elements_root[elements_name]
        for j in range(i + 1, elements_name):
            if (relationship_matrix[i, j] != 0):
                connected_element = elements_name[j]
                elements_root.link[connected_element] = []
                elements_root.link[connected_element].append(elements_root[connected_element])

    root_XML_query = elements_name[0]
    queue = queue.Queue()
    queue.put(elements_root[root_XML_query])

    while not queue.empty():
        current_node = queue.get()

        # Ancestor Descendent filtering
        # Check with each connected element of this current_node
        for connected_element in current_node.link.keys():
            connected_element_nodes = current_node.link[connected_element]              # list of nodes in the connected element that are connected to current_node
            link_nodes_removed = np.zeros(len(connected_element_nodes))                       # array to store if a node in connected_element_nodes should be removed
            has_one_satisfied_node = False
            for i in range(len(connected_element_nodes)):
                if ancestor_descendant_filter(current_node, connected_element_nodes[i]):
                    has_one_satisfied_node = True
                else link_nodes_removed[i] = 1

            # If found no satisfied node -> filter current_node
            if not has_one_satisfied_node:
                current_node.filtered = True
                break

            # Update link of this connected_element
            new_link_nodes = []
            for i in range(len(connected_element)):
                if link_nodes_removed[i] = 0:
                    new_link_nodes.append(connected_element_nodes[i])
            current_node.link[connected_element] = new_link_nodes

        # if this current_node is not filtered by structure -> Do Value filtering
        if not current_node.filtered:



root_L_XML = build_RTree_XML('test_1', 'L', 2)
print('L_XML')
root_L_XML.print_node()

# root_L_SQL = build_RTree_SQL('L', 2, 0)
# print('L_SQL')
# root_L_SQL.print_node()

# root_K_XML = build_RTree_XML('K', 2)
# print('K_XML')
# root_K_XML.print_node()

# root_K_SQL = build_RTree_SQL('K', 2, 0)
# print('K_SQL')
# root_K_SQL.print_node()

# root_L_K_SQL = build_RTree_SQL('L_K', 2, 1)
# print('L_K_SQL')
# root_L_K_SQL.print_node()

# pairwise_filtering_ancestor_descendant(root_L_XML, root_K_XML, root_L_SQL, root_K_SQL, root_L_K_SQL, 1)
# root_L_XML.print_node_not_filtered()
# root_K_XML.print_node_not_filtered()