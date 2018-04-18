# This file is library for all functions for Dewey Index

def compare_DeweyId(id1, id2):
	"""Summary
	This function compare between 2 Dewey index
	If id1 "less than" id2 -> return true
	"less than" is defined as having smaller element starting from left or having less length if all elements are equal
	Args:
	    id1 (TYPE): Description
	    id2 (TYPE): Description
	
	Returns:
	    TYPE: Description
	"""
	id1 = id1.split('.')
	id2 = id2.split('.')
	for i in range(len(id1)):
		# if id1 is longer than id2
		if i == len(id2):
			return False
		# compare element wise
		if (int(id1[i]) < int(id2[i])):
			return True
		if (int(id1[i]) > int(id2[i])):
			return False
	# All element are equal and has shorter length
	return True


A = '1.2.3.4.5'
B = '1.2.3.4'

print(compare_DeweyId(A, B))

