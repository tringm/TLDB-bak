class Query:
    def __init__(self, query_type: str):
        self._type = query_type

    @property
    def type(self):
        return self._type
