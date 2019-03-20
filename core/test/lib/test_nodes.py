from core.main.io_support.logger_support import *
from core.main.lib.nodes import *

# TODO: Install/Implement expect


def test_sql_nodes_range_search():
    loader = Loader('simple_small', 2, 'str')
    # log_loader(loader, print)

    parent = loader.all_tables_root['A_B_D']  # type: Node
    child = loader.all_tables_root['A_B_D'].children[0]
    print('Results:', sql_nodes_range_search([parent, child], [[6, 119], [17, 600], [13, 72]]))
    print('Expect:', str(parent.get_leaf_nodes()))
    # Expect all leaf nodes


test_sql_nodes_range_search()
