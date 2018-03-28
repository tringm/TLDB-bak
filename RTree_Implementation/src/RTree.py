# NOTE: Current conditions
# Data : loading_data is contained in a upper level directory - data
# 		 seperated files containing id and value for each element
# 		 (element_v.dat for value and element_id.dat for id)
# Coordinates of an Entry : tuple of list and int [[index], value]
# Index : Currently using new index system instead of Dewey
# 		  list of int [left_pos, rightpos]
# 					



import numpy as np
import pandas as pd
import os.path
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
		entries.append(Entry([ids[i], int(values[i])]))				# Convert value from string to int
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

	return quickSort(entries, 0, len(entries) - 1, dimension)


def find_boundary_entries(entries):
	"""Summary
	Find the MBR of list of Entries 
	Args:
	    entries [Entry]: list of input entries
	
	Returns:
	    [index, value]: tuple of index and value
	"""
	boundary = []
	min_index = 
	for i in range(len(entries)):

	return boundary




def rtree_bulk_loading(entries, max_n_children, dimension):
	if (dimension != 1):
		raise ValueError('Currently only support the value dimension of coordinates')

	# sort entries based on value
	entries = sort_entries(entries, dimension)

	n_entries = len(entries)
	# Configuration
	queue_node = queue.Queue()																			# Queue for node at each level
	queue_range = queue.Queue()																			# Queue for range of a node at each level

	# Initialization
	root = Node(max_n_children)
	queue_node.put(root)
	queue_node.put([0, n_entries])

	height = math.ceil(math.log(n_entries, max_n_children))												# Calculate the hight of the tree based on max_n_children
	level = 1																							# Current level to calculate split
	n_node_prev_level = 1
	

	while not queue_node.empty():
		current_node = queue_node.get()
		current_range = queue_range.get()
		current_n_entries = current_range[1] - current_range[0] 										# Number of entries contained in this current node

		# if current node contains has n_entries <= max_n_children then this is a leaf and proceed to add entries
		if (current_n_entries <= max_n_children):


		else:
			n_entries_subtree = max_n_children ** (height - level)										# Number of entries contained in the subtree of this node
			n_slices = math.floor(math.sqrt(math.ceil(current_n_entries / n_entries_subtree)))          # Number of slices according to the formula of OMT

			for i in range(n_slices + 1):




# Test sorting entries
# A = load_element('A')
# for i in range(len(A)):
# 	entry = A[i]
# 	print(entry.coordinates)
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