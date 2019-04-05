from tldb.core.client import TLDB
from tldb.core.operator.join import ComplexXMLSQLJoin
from tldb.core.structure.context import RangeContext
from tldb.server.query.xml_query import XMLQuery
from config import root_path, set_up_logger
import logging

set_up_logger()
output_folder = root_path() / 'test' / 'io' / 'out' / 'cases'


def get_log_path():
    log_name = f"test_simple_small_try_{n_try}.log"
    log_path = output_folder / log_name
    return log_path


n_try = 0
log_path = get_log_path()
while log_path.exists():
    n_try += 1
    log_path = get_log_path()

logging.basicConfig(filename=str(log_path), level=logging.VERBOSE)

tldb = TLDB('local')
tldb.load_from_folder(root_path() / 'test' / 'io' / 'in' / 'cases' / 'simple_small')
xml_query = XMLQuery('A_B_C_D')
xml_query.load_from_matrix_file(root_path() / 'test' / 'io' / 'in' / 'cases' / 'simple_small' / 'XML_query.dat')

attributes = xml_query.traverse_order
initial_range = RangeContext(attributes,
                             [tldb.get_object('A_B_C_D').get_attribute(a).index_structure.root.boundary for a in attributes])
join_op = ComplexXMLSQLJoin(tldb, xml_query, initial_range)
join_op.perform()