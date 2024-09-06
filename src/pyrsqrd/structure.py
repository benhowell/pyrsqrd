from collections.abc import Mapping, Sequence
from typing import TypeAlias, Any, Callable
from dataclasses import dataclass
from functools import reduce

ColumnDefinition: TypeAlias = Sequence[str, type, bool | None]
Record: TypeAlias = list[Any]
#RecordSet: TypeAlias = list[Record]
WhereConstraint: TypeAlias = tuple[Callable,str,Any]
OrderByConstraint: TypeAlias = tuple[str,str]

class serial:
    id = 0
    def __new__(cls):
        serial.id += 1
        obj = object().__new__(cls)
        obj.__dict__['val'] = serial.id
        return serial.id

    def __repr__(self):
        return 'serial(%s)' % (self.id)

    def __str__(self):
        #return '%s' % (self.id)
        return 'serial'

class _ImmutableMap(Mapping):
    def __init__(self, *args, **kwargs):
        self.__dict__ = dict(*args, **kwargs)
    def __getitem__(self, key):
        return self.__dict__[key]
    def __len__(self):
        return len(self.__dict__)
    def __iter__(self):
        return iter(self.__dict__)


class _Map:
    def __init__(self, *args, **kwargs):
        self.map = _ImmutableMap(*args, **kwargs)

    #def __str__(self):
    #   return str(self.map.__dict__)

    def __repr__(self):
         return '_Map({})'.format(self.map.__dict__)

    def __len__(self):
        return len(self.map.keys())

    def __iter__(self):
        yield from self.map.values()

    def __contains__(self, key):
        return self.map.__contains__(key)

    def keys(self):
        return self.map.keys()

    def values(self):
        return self.map.values()

    def get(self, key=None):
        if key:
            return self.map.get(key)
        return list(self.map.values())

    def set(self, pv: tuple[str], x: Any):
        new = self.copy()
        reduce(lambda x,y: x.get(y),pv, new.map).values = x
        return new

    def copy(self):
        return _Map(self.__dict__['map'])

    def add(self, x):
        new = self.copy()
        new.map.__dict__[x.name] = x
        return new

    def remove(self, x):
        new = self.copy()
        del new.map.__dict__[x]
        return new



@dataclass(slots=True)
class Column:
    """Database-like column."""
    name: str
    vtype: type
    nullable: bool
    values: tuple[Any] = tuple()

    def get(self, slot):
        return getattr(self, slot)

    def __len__(self): # row count
        return len(self.values)

    def __iter__(self):
        yield from self.values


@dataclass(slots=True)
class Table:
    """Database-like table."""
    name: str
    columns: _Map = _Map()
    comment: str = None

    def get(self, slot):
        return getattr(self, slot)

    def column_count(self):
        return len(self.columns.keys())

    def __len__(self): # row count
        return len(self.columns.get()[0])

    #def __str__(self):
    #    return str(self.columns)

    def __repr__(self):
        return 'Table(name={}, columns={})'.format(self.name,self.columns)

    def __iter__(self):
        yield from self.columns.values()


@dataclass(slots=True)
class RecordSet:
    """
    Postgresql inspired query return type.
    Similar in structure and concept to SETOF record and RETURNS TABLE.
    RecordSet offers much the same functionality, post database query, as
    database querying itself.
    """
    name: str
    columns: list[str]
    rows: list[Record]

    # def get(self, slot):
    #     return getattr(self, slot)

    # def column_count(self):
    #     return len(self.columns.keys())

    # def __len__(self): # row count
    #     return len(self.columns.get()[0])

    # #def __str__(self):
    # #    return str(self.columns)

    # def __repr__(self):
    #     return 'Table(name={}, columns={})'.format(self.name,self.columns)

    # def __iter__(self):
    #     yield from self.columns.values()
