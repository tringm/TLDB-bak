from config import *

from core.main.io_support.logger_support import *
from core.main.operation.FiltererContextBased import Filterer
# from core.operation.Filterer_new_no_logger import Filterer
from core.main.operation.Joiner import initialization


set_up_logger()

folder_name = "simple_small"
max_n_children = 2
method = 'str'


n_try = 0

file_name = method + '_' + str(max_n_children) + '_try' + str(n_try) + '.log'
log_path = core_io_out_path() / folder_name / file_name

while log_path.exists():
    n_try += 1
    file_name = method + '_' + str(max_n_children) + '_try' + str(n_try) + '.log'
    log_path = core_io_out_path() / folder_name / file_name

logging.basicConfig(filename=str(log_path), level=logging.INFO)
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
