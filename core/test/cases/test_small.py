from config import *

from core.main.io_support.logger_support import *
from core.main.operator.FiltererContextBased import Filterer
from core.main.operator.Joiner import initialization

set_up_logger()

folder_name = "orderline_price_asin_small"
max_n_children = 10
method = 'str'


n_try = 0

file_name = method + '_' + str(max_n_children) + '_try' + str(n_try) + '.log'
log_path = core_io_out_path() / folder_name / file_name

while log_path.exists():
    n_try += 1
    file_name = method + '_' + str(max_n_children) + '_try' + str(n_try) + '.log'
    log_path = core_io_out_path() / folder_name / file_name

logging.basicConfig(filename=str(log_path), level=logging.DEBUG)
# logging.basicConfig(filename="./io/" + folder_name + "/" + "result_" + str(max_n_children) + '_' + method
#                              + '_try' + str(n_try) + ".log", level=logging.INFO)
# logging.getLogger("Filterer").setLevel(logging.INFO)
# logging.getLogger("Init Link").setLevel(logging.INFO)
# logging.getLogger("Filter With Context").setLevel(logging.INFO)
# logging.getLogger("Loader").disabled = True
# logging.getLogger("Filterer").disabled = True
# logging.getLogger("Init Link").disabled = True
# logging.getLogger("Filter With Context").disabled = True

loader = Loader(folder_name, max_n_children, method)

initial_limit_range = initialization(loader)
filterer = Filterer(loader, initial_limit_range)
filterer.perform()
