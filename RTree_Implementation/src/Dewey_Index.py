# This file is library for all functions for Dewey Index

def compare_DeweyId(id1, id2):
	"""Summary
	This function compare between 2 Dewey index
	If id1 "less than" id2 -> return true
	"less than" is defined as having smaller element starting from left or having less length if all elements are equal
	Args:
	    id1 (String): Dewey Id
	    id2 (String): Dewey Id
	
	Returns:
	    Bool: True if id1 < id2
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


def is_ancestor(id1, id2):
	"""Summary
	This function check if a Dewey ID is an ancestor of another Dewey ID
	Args:
	    id1 (String): Dewey Id
	    id2 (String): Dewey Id
	
	Returns:
	    Bool: true if id1 is ancestor of id2
	"""
	id1 = id1.split('.')
	id2 = id2.split('.')
	# id2 is shorter -> can't be descendant
	if len(id2) <= len(id1):
		return False
	# Compare element wise
	for i in range(len(id1)):
		if (int(id1[i]) != int(id2[i])):
			return False
	return True


def is_parent(id1, id2):
	"""Summary
	This function check if a Dewey ID is parent of another Dewey ID
	Args:
	    id1 (String): Dewey Id
	    id2 (String): Dewey Id
	
	Returns:
	    Bool: true if id1 is parent of id2
	"""
	id1 = id1.split('.')
	id2 = id2.split('.')
	# id2 is shorter -> can't be descendant
	if len(id2) != (len(id1) + 1):
		return False
	# Compare element wise
	for i in range(len(id1)):
		if (int(id1[i]) != int(id2[i])):
			return False
	return True	



