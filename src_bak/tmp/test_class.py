class xD:
	def __init__(self, A):
		self.A = A


def add_B(node, B):
	# print(node.B)
	node.B = B
	print(node.B)


tmp = xD(3)
print(tmp.A)
add_B(tmp, 16)
add_B(tmp, 24)
