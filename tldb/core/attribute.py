class TLDBAttribute:
    def __init__(self, name: str, tldb_object, index_structure=None):
        self.name = name
        self.tldb_object = tldb_object
        self.index_structure = index_structure

    def __repr__(self):
        return 'TLDBAttribute' + ':' + self.tldb_object.name + '.' + self.name

    def __str__(self):
        rep_string = 'TLDBAttribute' + ':' + self.tldb_object.name + '.' + self.name + '\n'
        rep_string += 'INDEX STRUCTURE: \n' + str(self.index_structure)
        return rep_string
