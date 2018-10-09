from src.join_XML_SQL import main
import logging

logging.basicConfig(filename="io/simple_small/result.log", level=logging.DEBUG)
logger = logging.getLogger("RTree")
logger.level = logging.INFO
logger = logging.getLogger("Loader")
logger.disabled = True
logger = logging.getLogger("Runner")
main()
logger.info("#####################")
logger.info("#####################")
logger.info("#####################")
