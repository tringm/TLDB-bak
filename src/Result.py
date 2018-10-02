class Result:
	"""Summary
	Entry
	
	Attributes:
	    coordinates [id, value]:  list contains index and value for RTree_XML Node
	    coordinates [v1, v2, etc.]: list contains value for RTree_SQL Node
	    link_XML (dict): a dict contains a list of entries contained in linked elements
	    link_SQL (dict): a dict contains a list of entries contained in linked tables 
	"""

	def __init__(self, size):
		self.index = [None] * size
		self.value = [None] * size

	def is_final_result(self):
		for i in range(len(self.index)):
			if self.index[i] is None:
				return False

		return True

	def __str__(self):
		zip_list = zip(self.index, self.value)
		zip_list_string = ['(%s, %s)' % tuple for tuple in zip_list]
		return ', '.join(zip_list_string)
