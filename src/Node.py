from Entry import Entry
import math 

class Node:

	"""RTree Node
	
	Attributes:
	    filtered (bool): True if this node if filtered
	    filter_visisted(bool): True if this node has been full filtered before
	    validation_visited (bool): True if this node has been validated before
	    link_XML (dict): a dict contains a list of nodes for each linked element (children)
	    link_SQL (dict): a dict contains a list of nodes for each linked tables (tables that has this element as highest element in XML query)
	    max_n_children (int): maximum number of child Node
	    parent (Node): parent Node
	    boundary [[int, int], [int, int]]: MBR
	    						  Each tuple contains list of 2 ints [lower_bound, upper_bound] of a dimension
	    children [Node]: list of children nodes
	    entries [Entry]: list of entries (if this node is a leaf node)
	    validated_entries [Entry]: list of entries contained in this node (including children) to be used in validation
	    element_name (String): name of this node (element name for RTree_XML node and table_name for R_Tree_SQL node)
	"""
	
	def __init__(self, max_n_children):
		self.max_n_children = max_n_children
		self.parent = None
		self.children = []
		self.boundary = []
		self.entries = []
		self.validated_entries = []
		self.filtered = False
		self.value_filtering_visited = False
		self.validation_visited = False
		self.link_XML = {}
		self.link_SQL = {}
		self.link_SQL_range = {}
		self.name = ""
	
	# def update_boundary(self, coordinates):
	# 	n_dimensions = len(coordinates)

	# 	# if boundary is empty
	# 	if (not self.boundary):
	# 		for i in range(n_dimensions):
	# 			self.boundary.append([coordinates[i], coordinates[i]])
	# 	# Go through each dimension
	# 	for i in range(n_dimensions):
	# 		boundary[i][0] = min(boundary[i][0], coordinates[i])
	# 		boundary[i][1] = max(boundary[i][1], coordinates[i])


	
	# def dynamic_add(self, entry : Entry):
	# 	# If this node is a leaf node
	# 	if (len(self.children) == 0):
	# 		# add entry to this node
	# 		self.entries.append(entry)
	# 		# if the boundary is empty or entry is not inside the boundary -> update boundary
	# 		if (not boundary) or (not entry.is_inside(boundary)):
	# 			self.update_boundary(entry.coordinates)
	# 		# if this leaf node contains more than allowed entries -> split
	# 		if (len(self.entries) > max_n_entries):
	# 			self.split

	def get_entries(self):
		"""Summary
		This function return all entries contained in this node (included children's entries)
		Returns:
		    list of Entry: 
		"""
		# if this node is a leaf node
		if (len(self.entries) > 0):
			return self.entries
		entries = []
		# else get all child entry
		for child in self.children:
			child_entries = child.get_entries()
			for child_entry in child_entries:
				entries.append(child_entry)
		return entries

	def get_leaf_node_not_filtered(self):
		"""Summary
		This function return a list of nodes which are the unfiltered leaves of this node
		
		Returns:
		    list of Node:
		"""
		# if this node is leaf
		leaf_nodes = []
		
		if not self.filtered:
			if len(self.entries) > 0:
				return [self]

			for child in self.children:
				leaf_nodes_child = child.get_leaf_node_not_filtered()
				for node in leaf_nodes_child:
					leaf_nodes.append(node)
		return leaf_nodes

	def print_node(self, level = 0):
		"""Summary
		Simple implementation to print this node and its children
		Args:
		    level (int, optional): current level for printing
		"""
		if (len(self.entries) > 0):
			print('\t' * (level + 1), 'NODE ', self.boundary, 'is Leaf')
			for i in range(len(self.entries)):
				print('\t' * (level + 1), self.entries[i].coordinates)
		else:
			print('\t' * (level + 1), 'NODE ', self.boundary)
			for child in self.children:
				child.print_node(level + 1)

	def print_node_not_filtered_with_link(self, level = 0):
		"""Summary
		Simple implementation to print this node and its children
		Args:
		    level (int, optional): current level for printing
		"""
		if not self.filtered:
			if (len(self.entries) > 0):
				print('\t' * level, 'NODE ', self.boundary, 'is Leaf')
				print('\t' * (level + 1), 'Linked XML: ')
				for connected_element_name in self.link_XML.keys():
					print('\t' * (level + 1), connected_element_name, end = " ")
					for node in self.link_XML[connected_element_name]:
						print(node.boundary, end = " ")
					print()
				print('\t' * (level + 1), 'Linked SQL: ')
				for table_name in self.link_SQL.keys():
					print('\t' * (level + 1), table_name, end = " ")
					for node in self.link_SQL[table_name]:
						print(node.boundary, end = " ")
					print()
				print('\t' * (level + 1), 'Entries')
				for i in range(len(self.entries)):
					print('\t' * (level + 1), self.entries[i].coordinates)
					
			else:
				print('\t' * level, 'NODE ', self.boundary)
				print('\t' * (level + 1), 'Linked XML: ')
				for connected_element_name in self.link_XML.keys():
					print('\t' * (level + 1), connected_element_name, end = " ")
					for node in self.link_XML[connected_element_name]:
						print(node.boundary, end = " ")
					print()
				print('\t' * (level + 1), 'Linked SQL: ')
				for table_name in self.link_SQL.keys():
					print('\t' * (level + 1), table_name, end = " ")
					for node in self.link_SQL[table_name]:
						print(node.boundary, end = " ")
					print()

				for child in self.children:
					child.print_node_not_filtered_with_link(level + 1)


	def print_node_not_filtered(self, level = 0):
		"""Summary
		Simple implementation to print this node and its children
		Args:
		    level (int, optional): current level for printing
		"""
		if not self.filtered:
			if (len(self.entries) > 0):
				print('\t' * (level + 1), 'NODE ', self.boundary, 'is Leaf')
				for i in range(len(self.entries)):
					print('\t' * (level + 1), self.entries[i].coordinates)
			else:
				print('\t' * (level + 1), 'NODE ', self.boundary)
				for child in self.children:
					child.print_node_not_filtered(level + 1)