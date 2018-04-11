import RTree_XML
import RTree_SQL
import queue
from Node import Node
from Entry import Entry


def build_RTree_XML(element_name, max_n_children, dimension = 1):
	"""Summary
	Build a RTree for an XML element
	Args:
		element_name (string): 
		max_n_children (int): maximum number of children in RTree
	
	Returns:
		Node: root node of built RTree
	"""
	entries = RTree_XML.load_element(element_name)
	root = RTree_XML.bulk_loading(entries, max_n_children, dimension)
	return root


def build_RTree_SQL(table_name, max_n_children, dimension):
	"""Summary
	Build a RTree for a SQL table with each dimension is a column
	Args:
		table_name (string): 
		max_n_children (int): maximum number of children in RTree
	"""
	entries = RTree_SQL.load(table_name)
	root = RTree_SQL.bulk_loading(entries, max_n_children, dimension)
	return root

root_A_XML = build_RTree_XML('A', 2)
root_A_XML.print_node()

root_A_B_C_SQL = build_RTree_SQL('A_B_C', 2, 1)
# root_A_B_C_SQL.print_node()
# print(root_A_XML.boundary[0][0])

root_A_SQL = build_RTree_SQL('A', 2, 0)
root_A_SQL.print_node()
# print(root_A_SQL.boundary[0])



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
    """
    condition_nodes = A_D_SQL_nodes
    for level in range(max_level):
    	next_level = []
    	in_range = False
		for i in range(len(condition_nodes)):
			condition_node = condition_nodes[i]
			# print('Node ', i, ' boundary:', condition_node.boundary)

			A_in_range = False
			if (((condition_node.boundary[0][0] >= A_XML_node.boundary[1][0]) and (condition_node.boundary[0][0] <= A_XML_node.boundary[1][1])) or
				((condition_node.boundary[0][0] <= A_XML_node.boundary[1][0]) and (condition_node.boundary[0][1] >= A_XML_node.boundary[1][0])) or
				((condition_node.boundary[0][1] <= A_XML_node.boundary[1][1]) and (condition_node.boundary[0][0] >= A_XML_node.boundary[1][0])) or
				((condition_node.boundary[0][1] >= A_XML_node.boundary[1][1]) and (condition_node.boundary[0][0] <= A_XML_node.boundary[1][1]))):
				A_in_range = True
			D_in_range = False
			if (((condition_node.boundary[1][0] >= D_XML_node.boundary[1][0]) and (condition_node.boundary[1][0] <= A_XML_node.boundary[1][1])) or
				((condition_node.boundary[1][0] <= D_XML_node.boundary[1][0]) and (condition_node.boundary[1][1] >= A_XML_node.boundary[1][0])) or
				((condition_node.boundary[1][1] <= D_XML_node.boundary[1][1]) and (condition_node.boundary[1][0] >= A_XML_node.boundary[1][0])) or
				((condition_node.boundary[1][1] >= D_XML_node.boundary[1][1]) and (condition_node.boundary[1][0] <= A_XML_node.boundary[1][1]))):
				D_in_range = True
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
			# print('No node in range')
			A_XML_node.filtered = True
			D_XML_node.filtered = True
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
	if ((A_XML_structure_boundary[0] <= D_XML_structure_boundary[0]) && (A_XML_structure_boundary[1] >= D_XML_structure_boundary[1])):
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
	A_XML.positions_A_SQL = []
	A_XML.positions_A_SQL.append(A_SQL)

	while not queue_D_XML.empty():
		node_D_XML = queue_D_XML.get()

		new_position_D_SQL = value_filtering_1d(node_D_XML, node_D_XML.position_D_SQL, max_level)

		# if this node_D_XML is not value-filtered
		if new_position_D_SQL:
			# Update its children position_D_SQL
			for node_D_XML_child in node_D_XML.children:
				node_D_XML_child.D_SQL = new_position_D_SQL

			# Checking each A_XML node
			has_pairable_A_XML = False

			for i in range(len(node_D_XML.position_A_XML)):
				node_A_XML = node_D_XML.position_A_XML[i]


				# If this node_A_XML is filtered (through value filtering) 
				if node_A_XML.filtered:
					continue


				# !!!Warning: Since checking pairwise A_XML_node and D_XML_node, A_XML_node can be value-filtered multiple time

				# Value filtering for node_A_XML
				new_position_A_SQL = value_filtering_1d(node_A_XML, node_A_XML.position_A_SQL, max_level)
				# if this node_A_XML is not value-filtered
				if new_position_A_SQL:
					
					# Update its children position_D_SQL
					for node_A_XML_child in node_A_XML.children:
						node_A_XML_child.A_SQL = new_position_A_SQL

					# Structure filtering A_D
					if ancestor_descendant_filter(node_A_XML, node_D_XML):
						# Value_filtering A_D
						new_position_A_D_SQL = value_filtering_2d(node_A_XML, node_D_XML, node_D_XML.position_A_D_SQL, max_level)
						# if not filtered
						if new_position_A_D_SQL:
							has_pairable_A_XML = True

							for node_D_XML_child in node_D_XML.children:
								# !!!Warning: this can be bad, it should be a specific pair of A and D. This solution just add all 
								for A_D_SQL_node in new_position_A_D_SQL:
									node_D_XML_child.position_A_D_SQL.append(A_D_SQL_node)
								for node_A_XML_child in node_A_XML.children:
									node_D_XML_child.position_A_XML.append(node_A_XML_child)
								queue_D_XML.put(node_D_XML_child)
				else:
					node_A_XML.filtered = True

			if not has_pairable_A_XML:
				node_D_XML_child.filtered = True
								
					 
					

		# print('new_position_D_SQL')
		# for i in range(len(new_position_D_SQL)):
		# 	print(new_position_D_SQL[i].boundary)



pairwise_filtering_ancestor_descendant(None, root_A_XML, None, root_A_SQL, None, 4)


