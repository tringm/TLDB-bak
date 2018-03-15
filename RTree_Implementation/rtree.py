"""Summary
"""
from rtree import index
import numpy as np
import pandas as pd

def load_data_xml(element_name, content, folder_path):
	"""Summary
	Function to load XML element (after using Dewey indexing)
	Note: Rules based on current naming convention:
	element_type.data	
	The file must have extension.dat

	Args:
	    element_name (string): Name of XML element
	    content (string): Can either be "id" (index) or "v" (value)
	    folder_path (string): Path to folder containing Dewey indexed data files
	
	Returns:
	    numpy array: loaded result
	"""
	path = folder_path + "/" + element_name + "_" + content + ".dat"
	return np.loadtxt(path)

def load_data_sql(elements, folder_path):
	"""Summary
	Function to load SQL table
	Args:
	    elements (list of string): list of name of elements (must be in correct order)
	    folder_path (string): Path to folder containg SQL data file
	
	Returns:
	    DataFrame: loaded result
	"""
	elements_string = ""
	for element in elements:
		elements_string += element + "_"
	path = folder_path + "/" + elements_string + "table.dat"
	with open (path, 'r') as f:
		df = pd.read_csv(f, sep='\s+')
	return(df)

def split_sql_tables(tables):
	"""Summary
	Function to get each element's list of unique values from multiple table
	Args:
	    tables (list of DataFrame): list of SQL tables
	
	Returns:
	    DataFrame: DataFrame contains each element's list of unique values
	"""
	# Get all elements in tables
	elements = set()
	for i in range(len(tables)):
		table = tables[i]
		print(table.columns.values)
		elements.add(table.columns)
	print(elements)
	return

BC_table = load_data_sql(["B", "C"], "data")
ABC_table = load_data_sql(["A", "B", "C"], "data")
split_sql_tables([BC_table, ABC_table])