import logging
import timeit


class TLDBContext:
    def __init__(self, name, master):
        self.name = name
        self.master = master

    def read_csv(self, path, hedaers, delimiter=',', method='str', max_n_children=128, inferSchema=True):
        # TODO: auto detect headers
        with open(path) as f:
            content = f.readlines()
        content = [line.strip() for line in content]
        
    #
    #
    # def read_text(self, path, format, method='str',max_n_children=128, inferSchema=True):
    #     logger = logging.getLogger("Loader")
    #     logger.info("Start loading ", path)
    #     start_loading = timeit.default_timer()
    #
    #
    #
    #
    #     self.method = load_method
    #     self.max_n_children = max_n_children
    #     self.all_elements_name, self.relationship_matrix = load_xml_query(folder_name)
    #     self.all_elements_root = load_elements(folder_name, self.all_elements_name, self.max_n_children, load_method)
    #     self.all_tables_root = load_tables(folder_name, self.all_elements_name, self.max_n_children, load_method)
    #     end_loading = timeit.default_timer()
    #     self.total_loading_time = end_loading - start_loading
    #     logger.info('%s %.3f', "Total loading time:", self.total_loading_time)
    #
    #
    #
    #
    #
