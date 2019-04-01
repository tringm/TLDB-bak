from tldb.core.structure import entry
from tldb.core.main import SQLNode
from math import ceil

node_type = SQLNode
tree_name = 'A_B_C'


def divide_into_slices(items, n_slices, n_slice_member):
    divided = []
    for i in range(n_slices):
        if i != n_slices - 1:
            divided.append([item for item in items[n_slice_member * i:n_slice_member * (i + 1)]])
        else:
            divided.append([item for item in items[n_slice_member * i:len(items)]])
    return divided


def entries_to_leaf_nodes(some_entries):
    if len(some_entries) > max_n_children:
        raise ValueError('Slicing error: A leaf ' + str(some_entries) + 'has more than ' + str(max_n_children))
    node = node_type(max_n_children, entries=some_entries, name=tree_name, dimension=dimension)
    node.boundary = get_boundaries_from_entries(some_entries)
    return node


def child_nodes_to_parent_node(child_nodes):
    if len(child_nodes) > max_n_children:
        raise ValueError('Slicing error: ' + str(child_nodes) + 'children node ' + str(max_n_children))
    node = node_type(max_n_children, children=child_nodes, name=tree_name, dimension=dimension)
    node.boundary = get_boundaries_from_nodes(child_nodes)
    for child_node in child_nodes:
        child_node.parent = node
    return node


max_n_children = 2
dimension = 1
n_dimension = 3
dimension_order = [dimension] + [d for d in range(n_dimension) if d != dimension]
entries = [entry([23, 39, 88]), entry([34, 86, 52]), entry([30, 84, 80]), entry([84, 17, 23]), entry([74, 19, 9]),
           entry([22, 38, 100]), entry([38, 88, 70]), entry([38, 45, 38]), entry([94, 87, 94]), entry([80, 22, 15])]

# First partition entries into nodes
# Number of slicing step = n_dimension - 1
groups = [entries]
for slice_step in range(n_dimension):
    updated_groups = []
    for group in groups:
        n_leaves = len(group) / max_n_children
        quick_sort_entries(group, dimension_order[slice_step])
        # dividing in to slice
        n_slices = ceil(n_leaves**(1/(n_dimension - slice_step)))
        n_slice_member = ceil(len(group) / n_slices)
        for slice in divide_into_slices(group, n_slices, n_slice_member):
            updated_groups.append(slice)
    groups = updated_groups

# Initialize each group of entry into nodes
nodes = [[entries_to_leaf_nodes(group) for group in groups]]

while len(nodes[0]) != 1:
    for slice_step in range(n_dimension):
        upper_layer_nodes = []
        for nodes_group in nodes:
            n_upper_layer = len(nodes_group) / max_n_children
            quick_sort_nodes(nodes_group, dimension_order[slice_step])
            n_slices = ceil(n_upper_layer ** (1/(n_dimension - slice_step)))
            slice_size = ceil(len(nodes_group) / n_slices)
            for slice in divide_into_slices(nodes_group, n_slices, slice_size):
                upper_layer_nodes.append(slice)
        nodes = upper_layer_nodes
    nodes = [[child_nodes_to_parent_node(group_nodes) for group_nodes in nodes]]
