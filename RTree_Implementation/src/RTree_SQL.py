# NOTE: Current conditions
# Data: loading_data is contained in a upper level directory - data
# Coordinates of an Entry []: list contains values of each column

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
		content = f.readlines()[1:]											# Skip first line containing labels
	content = [x.strip() for x in content]
	return content


def load(folder_name, table_name):
	"""Summary
	This function load contents of the SQL table file and put them as coordinate in Entry
	
	Args:
	    folder_name (string): name of folder containing data files
	    element_name (string): name of element to be loaded
	
	Returns:
	    list of Entries of loaded table
	"""
	file_path = "../data/" + folder_name + "/" + table_name + "_table.dat"
	content = load_text_file(file_path)
	entries = []
	for i in range(len(content)):
		entries.append(Entry([int(x) for x in content[i].split()]))			# Convert list of string -> list of int
	return entries


def sort_entries(entries, dimension):
	"""Summary
	Quick sort list of entries according to value of a dimension
	
	Args:
	    dimension (int): dimension of Entry to be sorted
	    entries [Entry]: list of input entries
	
	Returns:
	    [Entry]: sorted list of  entries
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

	quickSort(entries, 0, len(entries) - 1, dimension)


def get_boundary_entries(entries):
	"""Summary
	Find the MBR of list of Entries 
	Args:
	    entries [Entry]: list of input entries
	
	Returns:
	    (): n_tuple of boundary for n_dimension
	"""
	boundary = []
	first_entry_coordinates = entries[0].coordinates
	for dimension in range(len(first_entry_coordinates)):
		low = first_entry_coordinates[dimension]
		high = first_entry_coordinates[dimension]
		for i in range(len(entries)):
			entry_coordinates = entries[i].coordinates
			low = min(low, entry_coordinates[dimension])
			high = max(high, entry_coordinates[dimension])
		boundary.append([low, high])
	return boundary



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

