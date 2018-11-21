from src.operation.Loader import Loader
from src.operation.Joiner import Joiner
import logging


folder_name = "orderline_price_asin_small"
max_n_children = 10
method = 'str'

logging.basicConfig(filename="io/" + folder_name + "/" + "result_" + str(max_n_children) + '_' + method
                             + ".log", level=logging.INFO)

logging.VERBOSE = 5
logging.addLevelName(logging.VERBOSE, "VERBOSE")
logging.Logger.verbose = lambda inst, msg, *args, **kwargs: inst.log(logging.VERBOSE, msg, *args, **kwargs)
logging.verbose = lambda msg, *args, **kwargs: logging.log(logging.VERBOSE, msg, *args, **kwargs)

# logging.getLogger("Loader").setLevel(logging.VERBOSE)
# logging.getLogger("RTree").setLevel(logging.VERBOSE)
# logging.getLogger("Joiner").setLevel(logging.DEBUG)
# logging.getLogger("Main Filterer").setLevel(logging.VERBOSE)
# logging.getLogger("Value Filterer").disabled = True
# logging.getLogger("Connected Element").disabled = True
# logging.getLogger("Traversing Query").disabled = True
# logging.getLogger("Init Children").disabled = True

# logging.getLogger("Node Validator").setLevel(logging.DEBUG)
# logging.getLogger("Entries Value Validator").setLevel(logging.DEBUG)
# logging.getLogger("Entries Structure Validator").setLevel(logging.DEBUG)

loader = Loader(folder_name, max_n_children, method)
joiner = Joiner(loader)

logger = logging.getLogger("Main")
logger.info("#####################")
logger.info("#####################")
logger.info("#####################")
