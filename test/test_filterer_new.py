import logging
from pathlib import Path
from config import *

from src.io_support.logger_support import *
from src.operation.FiltererContextBased import Filterer
# from src.operation.Filterer_new_no_logger import Filterer
from src.operation.Joiner import initialization

import pickle

logging.VERBOSE = 5
logging.addLevelName(logging.VERBOSE, "VERBOSE")
logging.Logger.verbose = lambda inst, msg, *args, **kwargs: inst.log(logging.VERBOSE, msg, *args, **kwargs)
logging.verbose = lambda msg, *args, **kwargs: logging.log(logging.VERBOSE, msg, *args, **kwargs)

logging.TIMER = 15
logging.addLevelName(logging.TIMER, "TIMER")
logging.Logger.timer = lambda inst, msg, *args, **kwargs: inst.log(logging.TIMER, msg, *args, **kwargs)
logging.timer = lambda msg, *args, **kwargs: logging.log(logging.TIMER, msg, *args, **kwargs)

folder_name = "simple_small"
max_n_children = 2
method = 'str'


n_try = 0
while Path('./io/' + folder_name + '/'
           + 'result_' + str(max_n_children) + '_' + method + '_try' + str(n_try) + '.log').exists():
    n_try += 1

logging.basicConfig(filename="./io/" + folder_name + "/" + "result_" + str(max_n_children) + '_' + method
                             + '_try' + str(n_try) + ".log", level=logging.VERBOSE)
# logging.getLogger("Filterer").setLevel(logging.INFO)
# logging.getLogger("Init Link").setLevel(logging.INFO)
# logging.getLogger("Filter With Context").setLevel(logging.INFO)
# logging.getLogger("Loader").disabled = True
# logging.getLogger("Filterer").disabled = True
# logging.getLogger("Init Link").disabled = True
# logging.getLogger("Filter With Context").disabled = True


loader = Loader(folder_name, max_n_children, method)

# path = data_path() / folder_name / 'data.pkl'
# with path.open('wb') as f:
#     pickle.dump(loader, f)

# path = data_path() / folder_name / 'data.pkl'
# with path.open('rb') as f:
#     loader = pickle.load(f)

# log_loader(loader,print)

initial_limit_range = initialization(loader)
filterer = Filterer(loader, initial_limit_range)
filterer.perform()
