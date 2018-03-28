from Entry import Entry
import math 

class Node:

	"""RTree Node
	
	Attributes:
		boundary (list of tuple): MBR
								  Each tuple contains list of 2 ints [lower_bound, upper_bound] of a dimension
		children (list of Node): children Nodes
		entries (list of Entry): contains entries if this Node is a leaf Node
		max_n_children (int): maximum number of child Node
		parent (Node): parent Node
	
	Deleted Attributes:
		n_dimensions (int): number of dimensions of an entry
	"""
	
	def __init__(self, max_n_children):
		self.parent = None
		self.max_n_children = max_n_children
		self.children = []
		# self.n_dimensions = 0
		self.boundary = []
		self.entries = []

	# def get_Parent(self):
	# 	return self.parent
	# def get_max_n_children(self):
	# 	return self.max_n_children
	# def get_children(self):
	# 	return self.children
	# # def get_n_dimensions(self):
	# # 	return self.n_dimensions
	# def get_boundary(self):
	# 	return self.boundary
	# def get_entries(self):
	# 	return self.entries
	# def set_Parent(self, parent):
	# 	self.parent = parent
	# def set_max_n_children(self, max_n_children):
	# 	self.max_n_children = max_n_children
	# def set_childre(self, children):
	# 	self.children = children
	# # def set_n_dimensions(self, n_dimensions):
	# # 	self.n_dimensions = n_dimensions
	# def set_boundary(self, boundary):
	# 	self.boundary = boundary
	# def set_entries(self, entries):
	# 	self.entries = entries
	# 	
	
	def update_boundary(self, coordinates):
		n_dimensions = len(coordinates)

		# if boundary is empty
		if (not self.boundary):
			for i in range(n_dimensions):
				self.boundary.append([coordinates[i], coordinates[i]])
		# Go through each dimension
		for i in range(n_dimensions):
			boundary[i][0] = min(boundary[i][0], coordinates[i])
			boundary[i][1] = max(boundary[i][1], coordinates[i])




	
	def dynamic_add(self, entry : Entry):
		# If this node is a leaf node
		if (len(self.children) == 0):
			# add entry to this node
			self.entries.append(entry)
			# if the boundary is empty or entry is not inside the boundary -> update boundary
			if (not boundary) or (not entry.is_inside(boundary)):
				self.update_boundary(entry.coordinates)
			# if this leaf node contains more than allowed entries -> split
			if (len(self.entries) > max_n_entries):
				self.split
			