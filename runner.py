from src.join_XML_SQL import main
import logging

logging.basicConfig(filename="io/orderline_price_asin_small/result.log", level=logging.INFO)
logger = logging.getLogger("RTree")
logger.level = logging.INFO
logging.getLogger("Loader").disabled = True
logging.getLogger("Main Filterer").disabled = True
logging.getLogger("Value Filterer").disabled = True
logging.getLogger("Connected Element").disabled = True
logging.getLogger("Traversing Query").disabled = True
logging.getLogger("Init Children").disabled = True
logging.getLogger("Entries Value Validator").disabled = True
logging.getLogger("Entries Structure Validator").disabled = True

logger = logging.getLogger("Runner")
main()
logger.info("#####################")
logger.info("#####################")
logger.info("#####################")
