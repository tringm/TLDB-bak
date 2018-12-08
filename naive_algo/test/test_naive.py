import logging

from naive_algo.src.Loader import Loader
from naive_algo.src.Filterer import Filterer

logging.VERBOSE = 5
logging.addLevelName(logging.VERBOSE, "VERBOSE")
logging.Logger.verbose = lambda inst, msg, *args, **kwargs: inst.log(logging.VERBOSE, msg, *args, **kwargs)
logging.verbose = lambda msg, *args, **kwargs: logging.log(logging.VERBOSE, msg, *args, **kwargs)

logging.TIMER = 15
logging.addLevelName(logging.TIMER, "TIMER")
logging.Logger.timer = lambda inst, msg, *args, **kwargs: inst.log(logging.TIMER, msg, *args, **kwargs)
logging.timer = lambda msg, *args, **kwargs: logging.log(logging.TIMER, msg, *args, **kwargs)

# folder_name = "orderline_price_asin_big"
folder_name = "simple_small"

loader = Loader(folder_name)
filterer = Filterer(loader)

filterer.perform()
