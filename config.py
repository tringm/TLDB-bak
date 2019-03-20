from pathlib import Path
import logging


def root_path():
    return Path(__file__).parent


def set_up_logger():
    logging.VERBOSE = 5
    logging.addLevelName(logging.VERBOSE, "VERBOSE")
    logging.Logger.verbose = lambda inst, msg, *args, **kwargs: inst.log(logging.VERBOSE, msg, *args, **kwargs)
    logging.verbose = lambda msg, *args, **kwargs: logging.log(logging.VERBOSE, msg, *args, **kwargs)

    logging.TIMER = 15
    logging.addLevelName(logging.TIMER, "TIMER")
    logging.Logger.timer = lambda inst, msg, *args, **kwargs: inst.log(logging.TIMER, msg, *args, **kwargs)
    logging.timer = lambda msg, *args, **kwargs: logging.log(logging.TIMER, msg, *args, **kwargs)
