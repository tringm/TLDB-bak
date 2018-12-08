from core.main.io_support.logger_support import *
from core.main.structure.DeweyID import DeweyID


def test_desc_range_search():
    folder_name = "simple_small"
    max_n_children = 2
    method='str'
    loader = Loader(folder_name, max_n_children, method)
    root_A = loader.all_elements_root['A']  # type: XMLNode
    log_tree_from_root(root_A, print)
    print('Results:', root_A.desc_range_search([DeweyID('1'), DeweyID('2.5')], [35, 39]))
    print('Expect:', None)

    print('Results:', root_A.desc_range_search([DeweyID('1'), DeweyID('2.5')], [34, 45]))
    print('Expect:', root_A.children[0].children[0].children)


def test_map_to_children():
    folder_name = "simple_small"
    max_n_children = 2
    method = 'str'
    loader = Loader(folder_name, max_n_children, method)
    root_A = loader.all_elements_root['A']  # type: XMLNode
    print('Results:', list(map(lambda x: x.children, root_A.children)))
    print('Expect:', [child.children for child in root_A.children])


# test_desc_range_search()
test_map_to_children()