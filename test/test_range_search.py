import logging

from src.structure.Node import *
from src.io_support.logger_support import *

folder_name = "simple_small"
max_n_children = 2
method='str'
loader = Loader(folder_name, max_n_children, method)
# initial_limit_range = initialization(loader)

logging.VERBOSE = 5
logging.addLevelName(logging.VERBOSE, "VERBOSE")
logging.Logger.verbose = lambda inst, msg, *args, **kwargs: inst.log(logging.VERBOSE, msg, *args, **kwargs)
logging.verbose = lambda msg, *args, **kwargs: logging.log(logging.VERBOSE, msg, *args, **kwargs)

root_A = loader.all_elements_root['A']  # type: XMLNode
log_tree_from_root(root_A, print)
print(root_A.desc_range_search([DeweyID('1'), DeweyID('2.5')], [40, 40], 3))


# first_table = loader.all_tables_root['A_B_D']  # type: SQLNode
# log_tree_from_root(first_table, print)
# print(first_table.range_search([[80, 81], [18, 32], [40, 45]], 2))