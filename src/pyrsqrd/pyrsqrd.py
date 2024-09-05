import os
import csv
from typing import TypeAlias, Any, Callable
from dataclasses import dataclass
from operator import itemgetter, add, eq
from collections.abc import Mapping, Sequence
from functools import reduce
import ast

ColumnDefinition: TypeAlias = Sequence[str, type, bool | None]
Record: TypeAlias = list[Any]
RecordSet: TypeAlias = list[Record]
WhereConstraint: TypeAlias = tuple[Callable,str,Any]
OrderByConstraint: TypeAlias = tuple[str,str]

NULL = True


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
        print("yo")
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



#https://blog.codinghorror.com/why-objects-suck/

# Reactive structured queryable relational data


#col_line_ascii = lambda c: c.join('-' * (m+pad) for m in max)
#row_border_ascii = '  +' + col_line_ascii('+') + '+'


def print_table(t: Table, header: bool = True, row_borders=True,
                format: str = 'unicode', default_missing: str = '(none)'):

    pad = 4
    ncols = len(t[0])
    max = [0 for i in range(len(t[0]))]
    for r in t:
        for i, v in enumerate(r):
            if len(str(v)) > max[i]:
                max[i] = len(str(v))

    if format == 'ascii':
        h='—'   # horizontal # '-' # '~' # '='
        v='|'   # vertical   # '|'
        tc='.' # top corners
        bc="'" # bottom corners
        m='  '  # margin

        row_format = m + v + '{0:<' + str(max[0]+pad) + '}' + v + ''.join(
            '{' + str(i) + ':<' + str(max[i]+pad) +'}' + v for i in range(1, ncols))
        col_line = lambda h,v: v.join(h * (m+pad) for m in max)
        top_border = m + tc + col_line(h,h) + tc
        row_border = m + v + col_line(h,v) + v
        bottom_border = m + bc + col_line(h,h) + bc

    else: # format == 'unicode':
        row_format = '  │{0:<' + str(max[0]+pad) + '}│' + ''.join(
            '{' + str(i) + ':<' + str(max[i]+pad) + '}│' for i in range(1, ncols))
        col_line = lambda c: c.join('─' * (m+pad) for m in max)
        top_border = '  ┌' + col_line('┬') + '┐'
        row_border = '  ├' + col_line('┼') + '┤'
        bottom_border = '  └' + col_line('┴') + '┘'

    print(top_border)
    if header:
        print(row_format.format(pad=pad,max=max, *t[0]))
        if not row_borders:
            print(row_border)
        t = t[1:]
    for r in t:
        if row_borders:
            print(row_border)
        print(row_format.format(
            *mapv(r, lambda v: str(v) if v is not None else default_missing),
            pad=pad,max=max))
    print(bottom_border)


def dict_mapv(v,f,*a):
    return {x[0]:x[1] for x in (f(v,*a) for v in v)}

def mapv(v,f,*a):
    return [f(v,*a) for v in v]

def filterv(v,f,*a):
    return [v for v in v if f(v,*a)]

def filteriv(v,f,r='v',*a):
    return [iv if r=='both' else iv[0] if r=='i' else iv[1] # v
            for iv in enumerate(v) if f(iv,*a)]

def equal(x: str, y: Any) -> WhereConstraint:
    return eq,x,y

debug = True

class Pyrsqrd:

    def __init__(self, name: str ='pyst'):
        self.name: str = name
        self.__tables: _Map = _Map()

    def __str__(self):
        return str(self.__tables)

    def __repr__(self):
        return 'Pyrsqrd(name={}, tables={})'.format(self.name, self.__tables)

    # def __repr__(self):
    #     return '{}, Pyrsqrd({})'.format(
    #         super(Pyrsqrd, self).__repr__(), self.__tables)

    def create_table(self,
            table: str,
            columns: Sequence[ColumnDefinition],
            comment: str | None = None):

        if debug:
            print("CREATE TABLE",table,columns)

        n_serial = len(filterv(columns, lambda c: c[1]==serial))
        if n_serial > 1:
            print("[Pyrsqrd] CREATE TABLE failed")
            print("-    Number of serial columns is greater than 1")

        cols = _Map(dict_mapv(
            columns,
            lambda v: (
                v[0],
                Column(v[0],v[1], True if len(v) < 3 else v[-1]))))
        self.__tables = self.__tables.add(Table(table, cols, comment))

    def drop_table(self, table: str):
        if debug:
            print("DROP TABLE",table)

        self.__tables = self.__tables.remove(table)
        #ON DELETE CASCADE???

    def tables(self, table: str = None) -> Table | list[Table]:
        if table:
            return self.__tables.get(table)
        return list(self.__tables.get())


    def table_metadata(self) -> tuple[RecordSet]:
        tables = self.__tables.get()
        th = ['Table name', 'Column count', 'Row count', 'Comment']
        tt = mapv(tables, lambda t: [t.name, t.column_count(),
            len(t), t.comment])
        tt.insert(0,th)

        ch = ['Table name','Column name', 'Data type',
            'Nullable', 'Auto increment']
        ct = reduce(add,mapv(tables, lambda t: mapv(t,
            lambda c: [t.name, c.name, str(c.vtype),
                       c.nullable, c.vtype == serial])))
        ct.insert(0,ch)

        return tt,ct


    def columns(self, table: str, column: str = None) -> Column | list[Column]:
        if column:
            return self.tables(table).columns.get(column)
        return list(self.tables(table).columns.get())

    def not_null_columns(self, columns: list[Column], in_cols: str) -> list[Column]:
        return filterv(columns,
            lambda c: not c.nullable \
            and not c.vtype == serial \
            and not c.name in in_cols)

    def values(self, table: str, column: str) -> tuple[Any]:
        return self.columns(column).values

    def add_value(self, table: str, column: str, value: Any):
        self.__tables = self.__tables.set((table,'columns',column), value)


    def insert(self,
               table: str,
               columns: Sequence[str],
               values: Sequence[Any]) -> int:
        """
        Database-like INSERT statement function
        """

        # db cols:
        dbcols = self.columns(table)

        if debug:
            print("INSERT INTO",table,"COLUMNS",columns,"VALUES",values)

        # check not NULL columns have been supplied...
        nncols = mapv(self.not_null_columns(dbcols, columns), lambda c: c.name)
        if nncols:
            print("[Pyrsqrd] Insert failed")
            print("-    Column and value must be provided for all non-nullable columns")
            print("-    Column/value missing for columns:",nncols)
            return 0

        # check column/value numbers match...
        n_columns = len(columns)
        n_values = len(values)
        if not n_columns == n_values:
            print("[Pyrsqrd] INSERT failed")
            print("-    Number of columns ({}) to values ({}) mismatch".format(
                n_columns, n_values))
            print("-    Columns:",columns)
            print("-    Values:",values)
            return 0

        cvm = dict(zip(columns, values))
        insert_id = 0 #FIXME: needs to be PK field

        for column in dbcols:
            if column.vtype == serial:
                insert_id = serial()
                self.add_value(table, column.name, column.values + (insert_id,))
            else:
                nval = cvm.get(column.name, None)
                if nval is not None:
                    if type(nval) != column.vtype:
                        nval = ast.literal_eval(nval)
                    if nval is not None:
                        self.add_value(table, column.name, column.values + (nval,))
                    else:
                        print("[Pyrsqrd] INSERT failed")
                        print("-    Value type ({}) does not match column type ({})".format(
                              type(nval), column.vtype))
                elif column.nullable:
                    self.add_value(table, column.name, column.values + (None,))
                else:
                    print("[Pyrsqrd] INSERT failed")
                    print("-    No value supplied for non-nullable column ({})".format(
                        column.name))
        return insert_id


    def select(self,
               table: str,
               columns: Sequence[str] | None = None,
               where: Sequence[WhereConstraint] | None = None,
               order_by: Sequence[OrderByConstraint] | None = None):

        # get full table for cases where the
        # where condition column(s) are not in select columns
        cs = self.columns(table)
        ks = mapv(cs, lambda c: c.name)
        vs = list(map(list, zip(*cs)))

        # filter by where conditions...
        # if where:
        #     for i,w in enumerate(where):
        #         print("iw:",i,w)
        #         idx = ks.index(w[1])
        #         vs = [print(t[1]) for t in enumerate(vs)]
        #         vs = filteriv(vs, lambda v: w[0](v[1], w[2]),'v')

        print("ks:",ks)
        #print("cs",[print(x.name) for x in cs])
        # filter select columns...
        if type(columns) == str:
            columns = [columns]

        if columns and '*' in columns:
             if len(columns) > 1:
                cs = cs + filterv(cs, lambda c: c.name in columns)
                print("cs",cs)
        elif columns:
            cs = filterv(cs, lambda c: c.name in columns)
        #else: # all columns


        # if debug:
        #     _c = ', '.join(columns)
        #     _w = "WHERE " + ' '.join(where) if where else ''
        #     _o = "ORDER BY " +' '.join(order_by) if order_by else None
        #     print("SELECT {} FROM {}{} {}".format(_c,table,_w,_o))


FIXME: this is meant to be a vector of order bys
        if order_by:
            if len(order_by) < 2:
                d = False # reverse=False == 'ASC'
            else:
                d = order_by[1] == 'DESC'
            idx = ks.index(order_by[0])
            vs.sort(key=itemgetter(idx), reverse=d)
        vs.insert(0, ks)
        return vs



db = Pyrsqrd("Record-shop")

# db.create_table(
#     'customer',
#     (('id', serial, not NULL),
#      ('first_name', str, not NULL),
#      ('last_name', str, not NULL),
#      ('age', int, not NULL),
#      ('alive', bool)),
#     'Holds customer data')

# db.create_table(
#     table = 'customer',
#     columns = [
#         ('id', serial, not NULL),
#         ('first_name', str, not NULL),
#         ('last_name', str, not NULL),
#         ('age', int, not NULL),
#         ('alive', bool)],
#     comment = 'Holds customer data')

db.create_table(
    'customer',
    [
     ('id', serial, not NULL),
     ('first_name', str, not NULL),
     ('last_name', str, not NULL),
     ('age', int, not NULL),
     ('alive', bool)],
    'Holds customer data')


db.create_table(
    table = 'album',
    columns = [
        ('id', serial, not NULL),
        ('title', str, not NULL),
        ('artist_id', int, not NULL)])

db.create_table(
    table = 'artist',
    columns = [
        ['id', serial, not NULL],
        ['name', str, not NULL]])

db.create_table(
    table = 'customer_album',
    columns = [
        ('id', serial, not NULL),
        ('customer_id', int, not NULL),
        ('album_id', int, not NULL)])



PWD = os.path.dirname(os.path.abspath(__file__))
RD = os.path.join(PWD, '..', '..')

table = 'customer'
columns = ('first_name','last_name','age','alive')
customer_csv = os.path.join(RD, 'customer.csv')

with open(customer_csv, "r") as file:
    reader = csv.reader(file, delimiter=',',lineterminator='\n')
    for r in reader:
        id = db.insert(table=table, columns=columns[0:len(r)], values=r)
        print("insert id:",id)


customers = db.select(
    table = 'customer',
    columns = ('*',),
    order_by = ('last_name', 'ASC'))
print_table(customers, row_borders=False)




# or a column as string

# customers = db.select(
#     table = 'customer',
#     columns = ('*','first_name', 'last_name','age'),
#     order_by = ('last_name', 'DESC'))

# print("")
# print_table(customers, row_borders=False)
# print("")

# tmd, cmd = db.table_metadata()
# print_table(tmd,row_borders=False)
# print_table(cmd,row_borders=False)


customers = db.select(
    table = 'customer',
    columns = ['first_name', 'last_name'],
    where = [equal('alive', True)],
    order_by = [('last_name', 'DESC')]
    )

print_table(customers, row_borders=False)






#print("tables: ", db.tables())
#print("db: ", db)

#db.drop_table('artist')

#print("tables: ", db.tables())
#print("table: ", db.tables('artist'))


#id = db.insert(
#    table = 'customer',
#    columns = ('first_name','last_name','age','alive'),
#    values = ('Jane', 'Doe', 18, False))

# id = db.insert(
#     table = 'customer',
#     columns = ('first_name','last_name','age','alive'),
#     values = ('Bob', 'Builder', 37, True))


# def table(name: str) -> str:
#     return name

# def columns(names: str | Sequence[str]) -> Sequence[str]:
#     if type(names) == str:
#             names = [names]
#     return names

# def where(constraints: Sequence[WhereConstraint]) -> Sequence[WhereConstraint]:
#     return constraints


# def where(self, constriants: Sequence[WhereConstraint]):
#     #for c in constriants:
#     def pf():
#         return constriants
#     return pf


# def order_by(constraints: Sequence[OrderByConstraint]) -> Sequence[OrderByConstraint]:
#     return constraints

#([('alive' == True)]),

# def equal(x: str, y: Any) -> Callable:
#     def _equal():
#         return eq(x,y)
#     return _equal






# [['Table name', 'Column name', 'Data type', 'Nullable', 'Auto increment'],
#  ['customer', 'id', "<class '__main__.serial'>", False, True],
#  ['customer', 'first_name', "<class 'str'>", False, False],
#  ['customer', 'last_name', "<class 'str'>", False, False],
#  ['customer', 'age', "<class 'int'>", False, False],
#  ['customer', 'alive', "<class 'bool'>", True, False],
#  ['album', 'id', "<class 'int'>", False, False],
#  ['album', 'title', "<class 'str'>", False, False],
#  ['album', 'artist_id', 'False', True, False],
#  ['artist', 'id', "<class 'int'>", False, False],
#  ['artist', 'name', 'False', True, False],
#  ['customer_album', 'id', "<class '__main__.serial'>", False, True],
#  ['customer_album', 'customer_id', "<class 'int'>", False, False],
#  ['customer_album', 'album_id', 'False', True, False]]


# operator.lt(a, b)
# operator.le(a, b)
# operator.eq(a, b)
# operator.ne(a, b)
# operator.ge(a, b)
# operator.gt(a, b)

# operator.and_(a, b)
# operator.or_(a, b)
# operator.xor(a, b)
# operator.countOf(a, b)



# +-----------------------+-------------------+-----------------------------------.
# | Operation             | Syntax            | Function                          |
# +-----------------------+-------------------+-----------------------------------+
# | Addition              | a + b             | add(a, b)                         |
# | Concatenation         | seq1 + seq2       | concat(seq1, seq2)                |
# | Containment Test      | obj in seq        | contains(seq, obj)                |
# | Division              | a / b             | truediv(a, b)                     |
# | Division              | a // b            | floordiv(a, b)                    |
# | Bitwise And           | a &amp; b         | and_(a, b)                        |
# | Bitwise Exclusive Or  | a ^ b             | xor(a, b)                         |
# | Bitwise Inversion     | ~ a               | invert(a)                         |
# | Bitwise Or            | a | b             | or_(a, b)                         |
# | Exponentiation        | a ** b            | pow(a, b)                         |
# | Identity              | a is b            | is_(a, b)                         |
# | Identity              | a is not b        | is_not(a, b)                      |
# | Indexed Assignment    | obj[k] = v        | setitem(obj, k, v)                |
# | Indexed Deletion      | del obj[k]        | delitem(obj, k)                   |
# | Indexing              | obj[k]            | getitem(obj, k)                   |
# | Left Shift            | a &lt;&lt; b      | lshift(a, b)                      |
# | Modulo                | a % b             | mod(a, b)                         |
# | Multiplication        | a * b             | mul(a, b)                         |
# | Matrix Multiplication | a @ b             | matmul(a, b)                      |
# | Negation (Arithmetic) | - a               | neg(a)                            |
# | Negation (Logical)    | not a             | not_(a)                           |
# | Positive              | + a               | pos(a)                            |
# | Right Shift           | a &gt;&gt; b      | rshift(a, b)                      |
# | Slice Assignment      | seq[i:j] = values | setitem(seq, slice(i, j), values) |
# | Slice Deletion        | del seq[i:j]      | delitem(seq, slice(i, j))         |
# | Slicing               | seq[i:j]          | getitem(seq, slice(i, j))         |
# | String Formatting     | s % obj           | mod(s, obj)                       |
# | Subtraction           | a - b             | sub(a, b)                         |
# | Truth Test            | obj               | truth(obj)                        |
# | Ordering              | a &lt; b          | lt(a, b)                          |
# | Ordering              | a &lt;= b         | le(a, b)                          |
# | Equality              | a == b            | eq(a, b)                          |
# | Difference            | a != b            | ne(a, b)                          |
# | Ordering              | a &gt;= b         | ge(a, b)                          |
# | Ordering              | a &gt; b          | gt(a, b)                          |
# +-----------------------+-------------------+-----------------------------------+




# # customers = db.select(
# #     args={
# #         'v': '*',
# #         't': 'customer',
# #         'w': [
# #             ('alive', '==', True)]
# #     })
# # print("customers: ", customers)



# # values
# # table
# # join
# # where
# # group
# # order_by
# # parameter_map
