from src.Loader import Loader
from src.Joiner import Joiner
import logging


folder_name = "simple_small"
max_n_children = 10

logging.basicConfig(filename="io/" + folder_name + "/" + "result" + str(max_n_children) + ".log",
                    level=logging.INFO)

logging.VERBOSE = 5
logging.addLevelName(logging.VERBOSE, "VERBOSE")
logging.Logger.verbose = lambda inst, msg, *args, **kwargs: inst.log(logging.VERBOSE, msg, *args, **kwargs)
logging.verbose = lambda msg, *args, **kwargs: logging.log(logging.VERBOSE, msg, *args, **kwargs)

# logging.getLogger("Loader").setLevel(logging.VERBOSE)
logging.getLogger("Joiner").setLevel(logging.VERBOSE)
# logging.getLogger("Main Filterer").setLevel(logging.DEBUG)
# logging.getLogger("Value Filterer").disabled = True
# logging.getLogger("Connected Element").disabled = True
# logging.getLogger("Traversing Query").disabled = True
# logging.getLogger("Init Children").disabled = True

# logging.getLogger("Node Validator").setLevel(logging.DEBUG)
# logging.getLogger("Entries Value Validator").setLevel(logging.DEBUG)
# logging.getLogger("Entries Structure Validator").setLevel(logging.DEBUG)

loader = Loader(folder_name, max_n_children, 'stripe')
joiner = Joiner(loader)

logger = logging.getLogger("Main")
logger.info("#####################")
logger.info("#####################")
logger.info("#####################")
