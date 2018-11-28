from src.operation.Loader import Loader
from src.operation.Joiner import Joiner
from pathlib import Path
import logging

folder_name = "orderline_price_asin_small_original"
max_n_children = 10
method = 'str'
n_try = 0
while Path('io/' + folder_name + '/'
           + 'result_' + str(max_n_children) + '_' + method + '_try' + str(n_try) + '.log').exists():
    n_try += 1

logging.basicConfig(filename="io/" + folder_name + "/" + "result_" + str(max_n_children) + '_' + method
                             + '_try' + str(n_try) + ".log", level=logging.INFO)

logging.VERBOSE = 5
logging.addLevelName(logging.VERBOSE, "VERBOSE")
logging.Logger.verbose = lambda inst, msg, *args, **kwargs: inst.log(logging.VERBOSE, msg, *args, **kwargs)
logging.verbose = lambda msg, *args, **kwargs: logging.log(logging.VERBOSE, msg, *args, **kwargs)

# logging.getLogger("Loader").setLevel(logging.VERBOSE)
# logging.getLogger("RTree").setLevel(logging.VERBOSE)
# logging.getLogger("Joiner").setLevel(logging.DEBUG)
# # logging.getLogger("Main Filterer").setLevel(logging.VERBOSE)
logging.getLogger("Filterer").setLevel(logging.VERBOSE)
logging.getLogger("Full Filterer").setLevel(logging.VERBOSE)
# logging.getLogger("Value Filterer").disabled = True
# logging.getLogger("Connected Element Filterer").disabled = True
# logging.getLogger("Check Lower Level").disabled = True
# # logging.getLogger("Traversing Query").disabled = True
# logging.getLogger("Init Children Link").disabled = True
# logging.getLogger("Filter Children").disabled = True
# logging.getLogger("Filter Children Link SQL").disabled = True

# logging.getLogger("Node Validator").setLevel(logging.DEBUG)
# logging.getLogger("Entries Value Validator").setLevel(logging.DEBUG)
# logging.getLogger("Entries Structure Validator").setLevel(logging.DEBUG)

loader = Loader(folder_name, max_n_children, method)
joiner = Joiner(loader, method)

logger = logging.getLogger("Main")
logger.info("#####################")
logger.info("#####################")
logger.info("#####################")
