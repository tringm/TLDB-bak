import logging
from pathlib import Path

from src.io_support.logger_support import *
from src.operation.Filterer_new import Filterer
from src.operation.Joiner import initialization

logging.VERBOSE = 5
logging.addLevelName(logging.VERBOSE, "VERBOSE")
logging.Logger.verbose = lambda inst, msg, *args, **kwargs: inst.log(logging.VERBOSE, msg, *args, **kwargs)
logging.verbose = lambda msg, *args, **kwargs: logging.log(logging.VERBOSE, msg, *args, **kwargs)

folder_name = "orderline_price_asin_small"
max_n_children = 10
method = 'stripe'


n_try = 0
while Path('./io/' + folder_name + '/'
           + 'result_' + str(max_n_children) + '_' + method + '_try' + str(n_try) + '.log').exists():
    n_try += 1

logging.basicConfig(filename="./io/" + folder_name + "/" + "result_" + str(max_n_children) + '_' + method
                             + '_try' + str(n_try) + ".log", level=logging.INFO)

loader = Loader(folder_name, max_n_children, method)


# log_loader(loader,print)

initial_limit_range = initialization(loader)
filterer = Filterer(loader, initial_limit_range)
filterer.perform()
