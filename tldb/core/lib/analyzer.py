from abc import ABC, abstractmethod


class AnalyzedContent:
    def __init__(self, format):
        self.format = format


class Analyzer(ABC):
    def __init__(self):
        return
    @abstractmethod
    def analyze(self, content):
        """
        Returned contents are list of Entries
        :param content:
        :return:
        """
        pass


# class CsvAnalyzer(Analyzer):
#     def __init__(self):
#         super().__init__()
#
#     def analyze(self, content):
#
#
#
#         for i in range(len(content)):
#             entries.append(Entry([float(x) for x in content[i].split()]))  # Convert list of string -> list of int