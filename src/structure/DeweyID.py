class DeweyID:
    def __init__(self, string_id = ''):
        self._id = string_id
        self._components = self._id.split('.')

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value
        self._components = self.id.split('.')