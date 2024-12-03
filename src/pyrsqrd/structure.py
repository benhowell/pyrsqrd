import ast
from operator import itemgetter
from operator import (eq, ne, gt, ge, lt, le, and_, or_, not_)
from functools import reduce
from collections import namedtuple
from copy import deepcopy

from exception import TableError, ColumnError
from util import RESERVED, is_listy, filterv

def serial(column):
    return 1 if len(column) == 0 else max(column)+1

def valid_columns(dbcols, incols):
    return filterv(dbcols,
        lambda c: not c.nullable \
        and not c.vtype == serial \
        and not c.label in incols)

def merge_fields(tables):
    fs = []
    sv = []
    for ti,x in enumerate(tables):
        for fi,f in enumerate(x.columns._fields):
            sx = 0
            nf = f
            c = x.columns[fi]
            while True:
                if nf not in fs:
                    fs.append(nf)
                    sv.append(c._replace(label=nf, idx=len(sv)))
                    break
                else:
                    sx+=1
                    nf = nf+'_'+str(sx)
    return fs,sv

def make_columns(fields, values):
    Columns = namedtuple('Columns', fields)
    setattr(Columns, 'column', lambda self, name: getattr(self, name))
    setattr(Columns, 'copy', lambda self: self._replace())
    return Columns(*values)

def make_tables(fields, values):
    Tables = namedtuple('Tables', fields)
    return Tables(*values)

def make_table(name, idx, columns):
    return Table(name, idx, columns)


class Column(namedtuple(
    'Column', ['label', 'idx', 'vtype', 'nullable', 'fq_label', 'values',
               'alias', 'fq_alias'],
    defaults=(None,None))):
    __slots__ = ()

    def copy(self):
        return self._replace()

    def set_alias(self, alias, fq_alias):
        if alias in RESERVED:
            raise ColumnError(
                str("[Pyrsqrd] '{}' is reserved and cannot be used as ") + str(
                    "a column alias".format(alias)))
        return self._replace(alias=alias,fq_alias=fq_alias)

    as_alias = set_alias

    def __count__(self): return len(self.values)

    def __eq__(self, x): return (eq, self.values)

    def __ne__(self, x): return (ne, self.values)

    def __gt__(self, x): return (gt, self.values)

    def __ge__(self, x): return (ge, self.values)

    def __lt__(self, x): return (lt, self.values)

    def __le__(self, x): return (le, self.values)

    def __and__(self, x): return (and_, self.values)

    def __or__(self, x): return (or_, self.values)

    def __not__(self, x): return (not_, self.values)





class Table(namedtuple(
    'Table', ['label', 'idx', 'columns', 'constraints', 'alias', 'comment'],
    defaults=(None,None,None,None,None))):
    __slots__ = ()

    def __getattr__(self, field):
        return self.columns.column(field)

    def __deepcopy__(self, memo):
        id_self = id(self)
        _copy = memo.get(id_self)
        if _copy is None:
            _copy = type(self)(
                deepcopy(self.label, memo),
                deepcopy(self.idx, memo),
                deepcopy(self.columns, memo),
                deepcopy(self.constraints, memo),
                deepcopy(self.alias, memo),
                deepcopy(self.comment, memo))
            memo[id_self] = _copy
        return _copy

    def copy(self):
        return deepcopy(self)

    @property
    def fields(self):
        return tuple(self.columns._fields)


    def insert(self, columns, values):

        # check not NULL columns have been supplied...
        nncols = valid_columns(self.columns, columns)
        if nncols:
            raise ColumnError(
                str("[Pyrsqrd] INSERT failed. Values must be provided ") + str(
                    "for all non-nullable columns. Column/value missing ") + str(
                        "for columns: {}").format(nncols))

        # check column/value numbers match...
        n_columns = len(columns)
        n_values = len(values)
        if not n_columns == n_values:
            raise ColumnError(
                str("[Pyrsqrd] INSERT failed. Number of columns ({}) ") + str(
                    "to values, ({}) mismatch").format(n_columns, n_values))

        cvm = dict(zip(columns, values))
        insert_id = 0 #FIXME: needs to be PK field

        row = tuple()
        for csc in self.columns:
            if csc.vtype == serial:
                insert_id = serial(self.column(csc.label))
                row = row + (insert_id,)
            else:
                nval = cvm.get(csc.label, None)
                if nval is not None:
                    if isinstance(nval,str) and type(nval) != csc.vtype:
                        nval = ast.literal_eval(nval)
                    if nval is not None:
                        row = row + (nval,)
                    else:
                        print("[Pyrsqrd] INSERT failed")
                        print("-    Value type ({}) does not match column type ({})".format(
                              type(nval), csc.vtype))
                        return 0
                elif csc.nullable:
                    row = row + (None,)
                else:
                    print("[Pyrsqrd] INSERT failed")
                    print("-    No value supplied for non-nullable column ({})".format(
                        csc.label))
                    return 0

        t = self.append_row(row)
        self.tables = self.tables._replace(**{table:t})
        return insert_id



    def select(self,*args):
        def __ex(col):
            raise ColumnError(
                str("[Pyrsqrd] SELECT column not found. Column: {}".format(col)))
        nfs = []
        ncols = []
        for arg in args:
            if isinstance(arg, Column):
                nfs.append(arg.label)
                ncols.append(arg)
            elif is_listy(arg):
                if isinstance(arg[0], Column):
                    c = arg[0]
                else:
                    c = self.column(arg[0])
                if len(arg) == 3 and arg[1].upper() == 'AS':
                    a = arg[2]
                elif len(arg) == 2:
                    a = arg[1]
                else:
                    raise ColumnError(
                    str("[Pyrsqrd] Wrong number of args for SELECT column. ") + str(
                        "Column: {}".format(arg)))
                if a not in RESERVED:
                    c = c.set_alias(a, self.alias+'.'+a if self.alias else None)
                    nfs.append(a)
                    ncols.append(c)
                else:
                    raise ColumnError(
                        str("[Pyrsqrd] '{}' is reserved and cannot be used ") + str(
                             "as a column alias".format(a)))
            elif arg=='*':
                cs = [c for c in self.columns]
                nfs = nfs + [c.label for c in cs]
                ncols = ncols + cs
            elif c := self.column(arg):
                nfs.append(c.label)
                ncols.append(c)
            #elif FIXME fn or something (e.g. SUM, AVG, etc)
            else:
                __ex(c)
        return make_table(self.label, -1, make_columns(nfs,ncols))


    def crossjoin(self,x):
        ncols = sum([len(self.columns), len(x.columns)])
        v = [[] for _ in range(ncols)]
        f,sv = merge_fields([self,x])
        return make_table('result', -1, make_columns(f,sv))


    def column(self, label):
        if label in self.fields:
            return getattr(self.columns, label)
        for c in self.columns:
            if label in [c.label, c.fq_label, c.alias, c.fq_alias]:
                return c
        return None

    def set_alias(self, alias):
        if alias in RESERVED:
            raise ColumnError(
                "[Pyrsqrd] '{}' is reserved and cannot be used as a table alias".format(
                    alias))
        return self._replace(alias = alias)

    as_alias = set_alias

    def set_column_alias(self, col, alias):
        if not isinstance(col, Column):
            col = self.column(col)


        if alias in RESERVED:
            raise ColumnError(
                "[Pyrsqrd] '{}' is reserved and cannot be used as a column alias".format(
                    alias))

        talias = self.alias
        return self._replace(
            columns=self.columns._replace(
                **{col.label:col._replace(
                    alias=alias,fq_alias=talias+'.'+alias if talias else None)}))

    @property
    def rows(self):
        return tuple(zip(*[c.values for c in self.columns]))

    def set_rows(self,rows):
        cols = list(zip(*rows))
        return self._replace(columns = self.columns._replace(
            **{self.columns[i].label:self.columns[i]._replace(
                values = cols[i]) for (i,v) in enumerate(cols)}))

    def append_row(self, row):
        return self._replace(columns = self.columns._replace(
            **{self.columns[i].label:self.columns[i]._replace(
                values = self.columns[i].values+(v,))
               for (i,v) in enumerate(row)}))

    def order_by(self, exprs):
        # sort expressions in reverse order
        # see: https://docs.python.org/3/howto/sorting.html#sort-stability-and-complex-sorts
        rows = self.rows
        for e in reversed(exprs):
            rows = sorted(rows, key=itemgetter(e[0].idx), reverse=e[1])

        # replace columns in sorted order
        cols = list(zip(*rows))
        return self._replace(columns = self.columns._replace(
            **{self.columns[i].label:self.columns[i]._replace(
                values = cols[i]) for (i,v) in enumerate(cols)}))
