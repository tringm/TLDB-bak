from core.main.operator import filterer
from core.main.io_support.logger_support import *
import logging

set

folder_name = "simple_small"
max_n_children = 2
method='str'
loader = Loader(folder_name, max_n_children, method)
# initial_limit_range = initialization(loader)

logging.VERBOSE = 5
logging.addLevelName(logging.VERBOSE, "VERBOSE")
logging.Logger.verbose = lambda inst, msg, *args, **kwargs: inst.log(logging.VERBOSE, msg, *args, **kwargs)
logging.verbose = lambda msg, *args, **kwargs: logging.log(logging.VERBOSE, msg, *args, **kwargs)


filterer = filterer(loader, initial_limit_range)
filterer.perform()
filterer.node_value_filter_str(filterer.roots[filterer.elements[0]])