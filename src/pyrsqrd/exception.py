class ParseException(Exception):
    __module__ = 'builtins'

def parse_error(tok, expected):
    raise ParseException(f"Unexpected token {tok}. Expected one of {expected}.")



class TableError(Exception):
    """
    Raised if attempting table creation using a reserved word as a table name
    """
    def __init__(self, *msgs):
         super().__init__(' '.join(msgs))

class ColumnError(Exception):
    """
    Raised when:
    - attempting to create column using a reserved word
    - attempting to create more than 1 column of type serial in a table
    """
    def __init__(self, *msgs):
         super().__init__(' '.join(msgs))
