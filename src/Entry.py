class Entry:

	"""Summary
	Entry
	
	Attributes:
	    coordinates [id, value]:  list contains index and value for RTree_XML Node
	    coordinates [v1, v2, etc.]: list contains value for RTree_SQL Node
	    link_XML (dict): a dict contains a list of entries contained in linked elements
	    link_SQL (dict): a dict contains a list of entries contained in linked tables 
	"""
	
	def __init__(self, coordinates):
		self.coordinates = coordinates
		self.link_XML = {}
		self.link_SQL = {}
		self.possible_combinations = []

	def is_inside(self, boundary):
		"""Summary
		Check if this entry is insde a boun
		Args:
		    boundary (list of tuple): list of tuple containing coordinates of a dimension. 
		    						  Each tuple contains 2 coordinates (lower_bound, upper_bound) of a dimension
		
		Returns:
		    Boolean: Description
		"""
		is_inside = True
		coordinates = self.coordinates
		# Checking each dimension
		for i in range(len(coordinates)):
			lower_bound = boundary[i][0]
			upper_bound = boundary[i][1]
			if ((coordinates[i] < lower_bound) or (coordinates > upper_bound)):
				is_inside = False
				break

		return is_inside

	def is_similar(entry):
		if len(this.coordinates) != len(entry.coordinates):
			return False

		for i in range(len(this.coordinates)):
			if this.coordinates[i] != entry.coordinates[i]:
				return False

		return True

