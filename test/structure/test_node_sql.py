def test_range_search():
    folder_name = "simple_small"
    max_n_children = 2
    method = 'str'
    loader = Loader(folder_name, max_n_children, method)
    root_a_b_d = loader.all_tables_root['A_B_D']
    log_tree_from_root(root_a_b_d, print)
    # print('Results:', root_a_b_d.range_search([[6, 118], [17, 600], [13, 72]]))
    # print('Expect:', root_a_b_d.get_leaf_nodes())
    # print('###')
    print('Results:', root_a_b_d.range_search([[6, 79], [17, 600], [13, 19]]))
    print('Expect:', root_a_b_d.children[0].children)

    print('Results:', root_a_b_d.range_search([[6, 79], [550, 600], [13, 19]]))
    print('Expect:', root_a_b_d.children[0].children[1])

test_range_search()