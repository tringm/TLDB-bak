import os
import numpy as np

# for file in os.listdir("../data/test_1"):
# 	if 'table' in file:
# 		print(file)


# A = ['A', 'B', 'C', 'D', 'E']
# print(A[0:2])

# s = 'A_B_C_table.dat'
# print(s[:-10])
# print(s[:-10].split('_'))


l1 = ['A', 'B', 'C', 'D']
l2 = ['C', 'D', 'A']
l2_index = []
for l2_e in l2:
	l2_index.append(l1.index(l2_e))
print(l2_index)
print(np.argmax(np.asarray(l2_index)))
