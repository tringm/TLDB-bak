import datetime
import logging
from pathlib import Path


def root_path():
    return Path(__file__).parent


def set_up_logger(log_folder:Path = None, log_name:str = None, default_logging_level=logging.INFO):
    if not log_folder:
        log_folder = root_path() / 'io' / 'out'
    log_folder.mkdir(parents=True, exist_ok=True)
    if not log_name:
        log_name = 'main'
    if (log_folder / f"{log_name}.log").exists():
        log_name = log_name + str(datetime.datetime.now())

    log_path = log_folder / f"{log_name}.log"
    logging.basicConfig(filename=str(log_path), level=default_logging_level,
                        format='%(asctime)-5s %(name)-5s %(levelname)-10s %(message)s',
                        datefmt='%H:%M:%S')
    logging.VERBOSE = 5
    logging.addLevelName(logging.VERBOSE, "VERBOSE")
    logging.Logger.verbose = lambda inst, msg, *args, **kwargs: inst.log(logging.VERBOSE, msg, *args, **kwargs)
    logging.verbose = lambda msg, *args, **kwargs: logging.log(logging.VERBOSE, msg, *args, **kwargs)

    logging.TIMER = 15
    logging.addLevelName(logging.TIMER, "TIMER")
    logging.Logger.timer = lambda inst, msg, *args, **kwargs: inst.log(logging.TIMER, msg, *args, **kwargs)
    logging.timer = lambda msg, *args, **kwargs: logging.log(logging.TIMER, msg, *args, **kwargs)
