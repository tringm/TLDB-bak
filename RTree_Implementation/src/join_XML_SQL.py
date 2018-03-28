import RTree
from Node import Node
from Entry import Entry

A = RTree.load_element('A')
for i in range(len(A)):
	entry = A[i]
	print(entry.coordinates)

root = RTree.bulk_loading(A, 2, 1)
root.print_node()