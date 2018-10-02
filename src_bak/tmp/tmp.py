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


# file_path = "../data/xiye_test_1/Orderline_id.dat" 
# xd = load_text_file(file_path)
# print(len(xd))

# new_path = "../data/xiye_test_1/Orderline_v.dat" 
# with open(new_path, 'a') as f:
# 	for i in range(len(xd)):
# 		f.write(str(i) + "\n")
# f.close()


# a_list = [0, 1, 2, 3]
# print(a_list.index(4))

# import pickle

# with open('asin_v', 'rb') as file:
# 	asin = pickle.load(file)
# with open('Orderline_v', 'rb') as file:
# 	Orderline = pickle.load(file)
# with open('Orderline_asin_table', 'rb') as file:
# 	Orderline_asin_table = pickle.load(file)

# for i in range(10):
# 	print(Orderline[i])

# for i in range(10):
# 	print(asin[i])

# for i in range(len(Orderline_asin_table)):
# 	print(Orderline_asin_table[i])

print(3 == None)
print(None == None)
