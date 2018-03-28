class Entry:
	def __init__(self, coordinates):
		self.coordinates = coordinates

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

