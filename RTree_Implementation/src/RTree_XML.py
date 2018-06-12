# Create a RTree structure for an XML element


# NOTE: Current conditions
# Data: loading_data is contained in a upper level directory - data
# 		 seperated files containing id and value for each element
# 		 (element_v.dat for value and element_id.dat for id)
# Coordinates of an Entry [[], int]: list contains index (string, int) and value

import queue
import math

from Node import Node
from Entry import Entry
from Dewey_Index import *
import timeit

def load_text_file(file_path):
	"""Summary
	Load text file by lines
	Args:
	    file_path (String): 
	
	Returns:
	    list [String]: 
	"""
	with open(file_path) as f:
		content = f.readlines()
	content = [x.strip() for x in content]
	return content


def load(folder_name, element_name):
	"""Summary
	This function load elements value and ids, and put them as coordinate in Entry
	Entry[0] = id, Entry[1] = value
	
	Args:
	    folder_name (string): name of folder containing data files
	    element_name (string): name of element to be loaded
	
	Returns:
	    list of Entries of loaded element
	
	Raises:
	    ValueError: raised if values file and ids file has different length
	
	"""
	id_file_path = "../data/" + folder_name + "/" + element_name + "_id.dat" 
	value_file_path = "../data/" + folder_name + "/" + element_name + "_v.dat" 
	ids = load_text_file(id_file_path)
	values = load_text_file(value_file_path)
	if (len(ids) != len(values)):
		raise ValueError('Id and value files have different size')
	entries = []
	for i in range(len(ids)):
		# Range Index
		# entry_id = [int(x) for x in ids[i].split()]						# Convert list of string id -> list of int
		# entries.append(Entry([entry_id, int(values[i])]))

		# Dewey index
		entries.append(Entry([ids[i], float(values[i])]))				# Convert value from string to int
	return entries


def sort_entries_recursive(entries, dimension):
	"""Summary
	Quick sort list of entries according to value of a dimension (recursively)
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


def sort_entries(entries, dimension):
	"""Summary
	Quick sort list of entries according to value of a dimension (Recursive way)
	
	Args:
	    dimension (int): dimension of Entry to be sorted
	    entries [Entry]: list of input entries
	
	Returns:
	    [Entry]: sorted list of  entries
	"""
	def partition(entries, low, high, dimension):
		pivot = entries[low]
		i = low + 1
		j = high
		# print(entries[i].coordinates)
		# print(entries[j].coordinates)
		while 1:
			while i <= j and entries[i].coordinates[dimension] <= pivot.coordinates[dimension]:
				i += 1
			while j >= i and entries[j].coordinates[dimension] >= pivot.coordinates[dimension]:
				j -= 1
			if j <= i:
				break
			entries[i], entries[j] = entries[j], entries[i]

		entries[low], entries[j] = entries[j], entries[low]
		return j
	# def partition(entries, low, high, dimension):
	# 	i = low - 1
	# 	pivot_coordinates = entries[high].coordinates
	# 	for j in range(low, high):
	# 		j_coordinate = entries[j].coordinates
	# 		if (j_coordinate[dimension] <= pivot_coordinates[dimension]):
	# 			i += 1
	# 			entries[i], entries[j] = entries[j], entries[i]
	# 	entries[i + 1], entries[high] = entries[high], entries[i + 1]
	# 	return (i + 1)


	# print('sorting entries')

	temp_stack = []
	low = 0
	high = len(entries) - 1
	temp_stack.append((low, high))
	while temp_stack:
		pos = temp_stack.pop()
		low, high = pos[0], pos[1]
		# print(low, high)
		partition_index = partition(entries, low, high, dimension)
		if partition_index - 1 > low:
			temp_stack.append((low, partition_index - 1))
		if partition_index + 1 < high:
			temp_stack.append((partition_index + 1, high))

	# print('end sorting')

def get_boundary_entries(entries):
	"""Summary
	Find the MBR of list of Entries 
	Args:
	    entries [Entry]: list of input entries
	
	Returns:
	    ([index_low, index_high], [value_low, value_high]): tuple of index and value boundary
	"""
	# print(len(entries))
	first_entry_coordinates = entries[0].coordinates
	index_low = first_entry_coordinates[0]
	index_high = first_entry_coordinates[0]
	value_low = first_entry_coordinates[1]
	value_high = first_entry_coordinates[1]
	for i in range(len(entries)):
		entry_coordinates = entries[i].coordinates
		# Update Index range
		if compare_DeweyId(entries[i].coordinates[0], index_low):
			index_low = entries[i].coordinates[0]
		if compare_DeweyId(index_high, entries[i].coordinates[0]):
			index_high = entries[i].coordinates[0]
		# Update value range
		value_low = min(value_low, entry_coordinates[1])
		value_high = max(value_high, entry_coordinates[1])
	return ([index_low, index_high], [value_low, value_high])


def bulk_loading(entries, element_name, max_n_children, dimension):
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


	'''
	!!!!!!!!!!!!!!!!! BAD PRACTICE
	'''

	print(element_name)

	if element_name != "Orderline":
		start_sorting = timeit.default_timer()
		# sort entries based on value
		sort_entries(entries, dimension)
		end_sorting = timeit.default_timer()
		print('sorting took: ', end_sorting - start_sorting)

	n_entries = len(entries)
	# Configuration
	queue_node = queue.Queue()																			# Queue for node at each level
	queue_range = queue.Queue()																			# Queue for range of entries contained in a node at each level

	# Initialization
	# Create root node
	root = Node(max_n_children)
	root.boundary = get_boundary_entries(entries)
	root.name = element_name

	queue_node.put(root)
	queue_range.put([0, n_entries])

	while not queue_node.empty():
		current_node = queue_node.get()
		current_range = queue_range.get()
		current_n_entries = current_range[1] - current_range[0] 										# Number of entries contained in this current node
		height = math.ceil(round(math.log(current_n_entries, max_n_children), 5))									# Calculate the height of this subtree based on max_n_children
		# print('*******')
		# print('current_range ', current_range)
		# print('current_n_entries ', current_n_entries)
		# print('height', height)

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
			# n_slices = math.floor(math.sqrt(math.ceil(current_n_entries / n_entries_subtree)))          # Number of slices according to the formula of OMT
			n_slices = math.ceil(current_n_entries/n_entries_subtree)

			# if n_entries_subtree == current_n_entries:
			# 	n_slices = 0

			# print('n_entries_subtree', n_entries_subtree)
			# print('n_slices', n_slices)

			# divide into n_slice + 1 nodes, add to current node
			for i in range(n_slices):
				n_entries_slice = current_n_entries

				# print('Children Node ', i)
				range_low = current_range[0]  + i * n_entries_subtree
				range_high = range_low + n_entries_subtree

				# last group might have more than max_n_children
				if (i == n_slices - 1):
					range_high = current_range[1]
				# print('range ', range_low, range_high)

				# if range_high == range_low:
				# 	break

				subtree_node = Node(max_n_children)
				subtree_node.parent = current_node
				subtree_node.boundary = get_boundary_entries(entries[range_low:range_high])
				subtree_node.name = element_name
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

