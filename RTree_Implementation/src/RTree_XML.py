# Create a RTree structure for an XML element


# NOTE: Current conditions
# Data: loading_data is contained in a upper level directory - data
# 		 seperated files containing id and value for each element
# 		 (element_v.dat for value and element_id.dat for id)
# Coordinates of an Entry [[], int]: list contains index and value
# Index: Currently using new index system instead of Dewey
# 		  list of int [left_pos, rightpos]
# 					

import queue
import math

from Node import Node
from Entry import Entry


def load_text_file(filename):
	"""Summary
	Load text file by lines
	Args:
	    filename (String): 
	
	Returns:
	    list [String]: 
	"""
	with open(filename) as f:
		content = f.readlines()
	content = [x.strip() for x in content]
	return content


def load_element(element_name):
	"""Summary
	This function load elements value and ids, and put them as coordinate in Entry
	Entry[0] = id, Entry[1] = value
	
	Args:
	    element_name (string):
	
	Returns:
	    list [Entry]: 
	
	Raises:
	    ValueError: raised if values file and ids file has different length
	
	"""
	id_file_path = "../data/" + element_name + "_id.dat" 
	value_file_path = "../data/" + element_name + "_v.dat" 
	ids = load_text_file(id_file_path)
	values = load_text_file(value_file_path)
	if (len(ids) != len(values)):
		raise ValueError('Id and value files have different size')
	entries = []
	for i in range(len(ids)):
		entry_id = [int(x) for x in ids[i].split()]						# Convert list of string id -> list of int
		entries.append(Entry([entry_id, int(values[i])]))

		# for Dewey index
		# entries.append(Entry([ids[i], int(values[i])]))				# Convert value from string to int
	return entries


def sort_entries(entries, dimension):
	"""Summary
	Quick sort list of entries according to value of a dimension
	WARNING: currently only working for dimension 1 (values)
	
	Args:
	    dimension (int): dimension of Entry to be sorted
	    entries [Entry]: list of input entries
	
	Returns:
	    [Entry]: sorted entries
	
	Raises:
	    ValueError: raised if dimension != 1
	"""
	def partition(entries, low, high, dimension):
		i = low - 1
		pivot_coordinates = entries[high].coordinates
		for j in range(low, high):
			j_coordinate = entries[j].coordinates
			if (j_coordinate[dimension] <= pivot_coordinates[dimension]):
				i += 1
				entries[i], entries[j] = entries[j], entries[i]
		entries[i + 1], entries[high] = entries[high], entries[i + 1]
		return (i + 1)
	def quickSort(entries, low, high, dimension):
		if low < high:
			partition_index = partition(entries, low, high, dimension)
			quickSort(entries, low, partition_index - 1, dimension)
			quickSort(entries, partition_index + 1, high, dimension)

	if (dimension != 1):
		raise ValueError('Currently only support the value dimension of coordinates')

	quickSort(entries, 0, len(entries) - 1, dimension)


def get_boundary_entries(entries):
	"""Summary
	Find the MBR of list of Entries 
	Args:
	    entries [Entry]: list of input entries
	
	Returns:
	    ([index_low, index_high], [value_low, value_high]): tuple of index and value boundary
	"""
	first_entry_coordinates = entries[0].coordinates
	index_low = first_entry_coordinates[0][0]
	index_high = first_entry_coordinates[0][1]
	value_low = first_entry_coordinates[1]
	value_high = first_entry_coordinates[1]
	for i in range(len(entries)):
		entry_coordinates = entries[i].coordinates
		index_low = min(index_low, entry_coordinates[0][0])
		index_high = max(index_high, entry_coordinates[0][1])
		value_low = min(value_low, entry_coordinates[1])
		value_high = max(value_high, entry_coordinates[1])
	return ([index_low, index_high], [value_low, value_high])


def bulk_loading(entries, max_n_children, dimension):
	"""Summary
	Bulk loading RTree based on Overlap Minimizing Top-down Bulk Loading Algorithm
	Args:
	    entries [Entry]: list of input entries
	    max_n_children (int): maximum number of children in a node of RTree
	    dimension (int): dimension to cut when bulk loading (currently only value dimension - 1)
	
	Returns:
	    Node: root node of loaded RTree
	
	Raises:
	    ValueError: Check dimension and max_n_children
	"""
	if (dimension != 1):
		raise ValueError('Currently only support the value dimension of coordinates')
	if (max_n_children == 1):
		raise ValueError('Maximum number of children nodes must be > 1')

	# sort entries based on value
	sort_entries(entries, dimension)
	n_entries = len(entries)
	# Configuration
	queue_node = queue.Queue()																			# Queue for node at each level
	queue_range = queue.Queue()																			# Queue for range of entries contained in a node at each level

	# Initialization
	# Create root node
	root = Node(max_n_children)
	root.boundary = get_boundary_entries(entries)

	queue_node.put(root)
	queue_range.put([0, n_entries])


	while not queue_node.empty():
		current_node = queue_node.get()
		current_range = queue_range.get()
		current_n_entries = current_range[1] - current_range[0] 										# Number of entries contained in this current node
		height = math.ceil(math.log(current_n_entries, max_n_children))									# Calculate the height of this subtree based on max_n_children
		# print('height ', height)
		# print('current_range ', current_range)
		# print('current_n_entries ', current_n_entries)

		# if current node contains has n_entries <= max_n_children then this is a leaf and proceed to add entries
		if (current_n_entries <= max_n_children):
			# print("Add entries in leaf")
			# print('before n entries ', len(current_node.entries))
			adding_entries = entries[current_range[0]:current_range[1]]
			for i in range(len(adding_entries)):
				current_node.entries.append(adding_entries[i])
			# print("After n entries ", len(current_node.entries))

		else:
			# print('Adding new nodes')
			n_entries_subtree = max_n_children ** (height - 1)											# Number of entries contained in the subtree of this node
			n_slices = math.floor(math.sqrt(math.ceil(current_n_entries / n_entries_subtree)))          # Number of slices according to the formula of OMT

			# divide into n_slice + 1 nodes, add to current node
			for i in range(n_slices + 1):
				# print('Node ', i)
				range_low = current_range[0]  + i * max_n_children
				range_high = range_low + max_n_children 
				# last group might have more than max_n_children
				if (i == n_slices):
					range_high = current_range[1]
				# print('range ', range_low, range_high)
				subtree_node = Node(max_n_children)
				subtree_node.parent = current_node
				subtree_node.boundary = get_boundary_entries(entries[range_low:range_high])
				current_node.children.append(subtree_node)
				# print('boundary ', subtree_node.boundary)
				queue_node.put(subtree_node)
				queue_range.put([range_low, range_high])
	return root






# print(get_boundary_entries(A))

# Test sorting entries
# sort_entries(A, 1)
# print('sorted')
# for i in range(len(A)):
# 	entry = A[i]
# 	print(entry.coordinates)

# Test queue 
# Go home and cry 
# queue = queue.Queue()
# for i in range(10):
# 	queue.put(i)
# while not queue.empty():
# 	print(queue.get())

