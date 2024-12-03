import os
import csv
from operator import (eq, ne, gt, ge, lt, le, and_, or_, not_)

from typing import Any, Callable

from collections import namedtuple
from collections.abc import Sequence


from structure import make_tables, make_table, make_columns, valid_columns
from structure import Column, serial
from exception import TableError, ColumnError
from util import alias_args, is_listy, print_table, pop_seq
from util import RESERVED, NULL, as_, AS

#https://blog.codinghorror.com/why-objects-suck/
# Reactive structured queryable relational data

debug = False

class Pyrsqrd:

    def __init__(self, label='pyrsqrd'):
        self.__label = label
        self.tables = None

    def __str__(self):
        return str(self.tables)

    def __repr__(self):
        return 'Pyrsqrd(label={}, tables={})'.format(self.__label, self.tables)

    def __getattr__(self, name):
        if name in self.tables._fields:
            return getattr(self.tables, name).copy()
        for t in self.tables:
            if name in [t.label, t.alias]:
                return t.copy()
        return None

    def table(self, label):
        return self.__getattr__(label)

    def label(self):
        return self.__label

    def __ft(self, t, ts):
        return next(
            (x for x in ts if t in [x.label, x.alias]), None)


    def merge_fields(self, tables):
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


    #FIXME: need to build rows according to join condition here...
    #       - need to break where fn into conditions/expressions and application
    #         in order to reuse the condition part for joins,where, etc.
    def merge_tables(self, t1, t2, kind='cross', expr=None):
        tables = [t1, t2]
        ncols = sum([len(x.columns) for x in tables])
        v = [[] for _ in range(ncols)]
        #for r in t1.rows:
        #    for rr in t2.rows:
        #        for i,x in enumerate(r + rr):
        #            v[i].append(x)
        f,sv = self.merge_fields([t1,t2])
        return make_table('result', -1, make_columns(f,sv))


    # NOTE: valid from-items: table, function, (sub)select

    # CROSS JOIN
    def cross_join(self, t1, t2):
        return self.merge_tables(t1, t2)

    # INNER JOIN
    def inner_join(self, t, ts, expr):
        for e in expr:
            jt = self.__ft(e[0][2], ts) if is_listy(e[0]) else self.__ft(e[0], ts)
            nt = self.merge_tables(t, jt)
            nt = self.where(nt,[e[1]])
        return nt

    # [LEFT] OUTER JOIN
    # NOTE: to perform a right outer join, just reverse the table order
    def outer_join(self, t, ts, expr):
        pass


    #FIXME: use _replace and allow for duplicates

    #FIXME: allow field duplication
    #ValueError: Encountered duplicate field name: 't1_name'
    def filter_columns(self,t,columns):
        def __ex(col):
            raise ColumnError(
                str("[Pyrsqrd] SELECT column not found. Column: {}".format(col)))
        fs = []
        sv = []
        for c in columns:
            if is_listy(c):
                csc = t.column(c[0])
                if not csc.alias == c[2]:
                    __ex(c[2])
                else:
                    c = c[0]
            csc = t.column(c)
            if csc is not None:
                fs.append(csc.label)
                sv.append(csc)
            elif c=='*' or c== '*.*':
                for f in t.fields:
                    fs.append(f)
                    sv.append(t.column(f))
            elif '.' in c:
                _t,_c = c.split('.')
                if _t=='*':
                    for csc in t.columns:
                        if _c == csc.fq_label.split('.')[1]:
                            fs.append(csc.label)
                            sv.append(csc)
                elif _c=='*':
                    for csc in t.columns:
                        if _t == csc.fq_label.split('.')[0]:
                            fs.append(csc.label)
                            sv.append(csc)
            #elif FIXME fn or something (e.g. SUM, AVG, etc)
            else:
                __ex(c)
        return make_table('result', -1, make_columns(fs,sv))



    def apply_constraint(self, row, cns):
        # post-order execution of nested condition sequence
        acc = []
        def __ex(constraint,acc):
            for x in constraint:
                if is_listy(x):
                    __ex(x,acc)
            # unwinding...
            if isinstance(constraint[0], Callable):
                if isinstance(constraint[1][0], Callable):
                    v = acc[:2]
                    acc = acc[3:] + [constraint[0](*v)]
                else:
                    v = []
                    for i in constraint[1]:
                        if c := t.column(i):
                            v.append(row[c.idx])
                        else:
                            v.append(i)
                    acc.append(constraint[0](*v))
            return acc
        return __ex(cns, acc)[0]




    def compile_constraints(self, t, conditions):
        # build constraint fn...
        cns = None
        for x in conditions:
            if isinstance(x, Sequence):
                for i,a in enumerate(x):
                    if isinstance(a,Callable):
                        fna = pop_seq(x,i)
                        cns = fna if cns is None else cns + (fna,)
                        break
            elif isinstance(x, Callable):
                cns = (x,cns)
        return cns


    def where(self,t,where):

        # build where constraint fn...
        # post-order execution of nested constraint sequence
        def __constraint(row,constraint):
            acc = []
            def __ex(constraint,acc):
                for x in constraint:
                    if is_listy(x):
                        __ex(x,acc)
                # unwinding...
                if isinstance(constraint[0], Callable):
                    if isinstance(constraint[1][0], Callable):
                        v = acc[:2]
                        acc = acc[3:] + [constraint[0](*v)]
                    else:
                        v = []
                        for i in constraint[1]:
                            if c := t.column(i):
                                v.append(row[c.idx])
                            else:
                                v.append(i)
                        acc.append(constraint[0](*v))
                return acc
            return __ex(constraint,acc)[0]


        cns = self.compile_constraints(t, where)
        nr = []
        for r in t.rows:
            rok = __constraint(r,cns)
            if rok:
                nr.append(r)
        t = t.set_rows(nr)
        return t


    def create_table(self, table, columns, constraints=None, comment=None):

        if debug:
            print("CREATE TABLE",table, columns)

        if table.lower() in RESERVED:
            raise TableError(
                "[Pyrsqrd] '{}' is reserved and cannot be used as a table label".format(
                    table))

        def __validate_serial(n, t):
            if t==serial:
                n+=1
                if n > 1:
                    raise ColumnError(
                        str("[Pyrsqrd] Table definition {} contains more than 1",
                            " column of type 'serial'.").format(table))

        #FIXME: consolidate exceptions, make fns etc
        def __validate_col_label(n):
            if n.lower() in RESERVED:
                raise ColumnError(
                    "[Pyrsqrd] '{}' is reserved and cannot be used as a column label".format(
                    'columns'))


        cv = []
        f = []
        n_serial = 0
        for c in columns:
            #print(c[0])
            __validate_serial(n_serial, c[1])
            __validate_col_label(c[0])

            cv.append(
                Column(
                    c[0], len(cv), c[1],
                    True if len(c) < 3 else c[-1],
                    table+'.'+c[0], tuple()))

            f.append(c[0])
            #v.append(tuple())

        if not self.tables:
            t = make_table(table, 0, make_columns(f, cv))
            self.tables = make_tables((table,), (t,))
        else:
            tf = self.tables._fields + (table,)
            t = make_table(table, len(self.tables), make_columns(f, cv))
            self.tables = make_tables(tf, list(self.tables) + [t])



    #FIXME: allow multiple rows in values
    def insert(self, table, columns, values):
        """
        Database-like INSERT statement function
        """

        t = self.table(table)

        if debug:
            print("INSERT INTO",table,"COLUMNS",columns,"VALUES",values)

        # check not NULL columns have been supplied...
        nncols = valid_columns(t.columns, columns)
        if nncols:
            raise ColumnError(
                str("[Pyrsqrd] INSERT failed. Values must be provided for all non-nullable columns. Column/value missing for columns: {}").format(nncols))


        # check column/value numbers match...
        n_columns = len(columns)
        n_values = len(values)
        if not n_columns == n_values:
            raise ColumnError(
                str("[Pyrsqrd] INSERT failed. Number of columns ({}) to values",
                    " ({}) mismatch").format(n_columns, n_values))

        cvm = dict(zip(columns, values))
        insert_id = 0 #FIXME: needs to be PK field

        row = tuple()
        for csc in t.columns:
            if csc.vtype == serial:
                insert_id = serial(t.column(csc.label))
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

        t = t.append_row(row)
        self.tables = self.tables._replace(**{table:t})
        return insert_id




    @alias_args({
        'tables': ['table','tables','t'],
        'columns': ['column','columns','c'],
        'cross_join': ['cross_join','cj'],
        'inner_join': ['join','j','inner_join','ij'],
        'left_join': ['left_join','left_outer_join','lj'],
        'right_join': ['right_join','right_outer_join','rj'],
        'full_join': ['full_join','full_outer_join','fj']})
    def select(self,
               tables = None,
               columns = None,
               cross_join = None,
               inner_join = None,
               left_join = None,
               right_join = None,
               full_join = None,
               where = None,
               order_by = None):

        """
        Database-like SELECT statement function
        """

        # retrieves table and applies alias if specified
        def __table(label):
            if is_listy(label):
                return self.table(label[0]).set_alias(label[2])
            else:
                return self.table(label)


        #FIXME: loop over columns, loop over tables, if found, call t.set_alias(c)
        def __apply_aliases(t,columns):
            for c in columns:
                if is_listy(c):
                    if c[2] not in RESERVED:
                        cscs = t.schema.columns
                        for csc in cscs:
                            if c[0] in [csc.label, csc.fq_label]:
                                return t.set_column_alias(csc,c[2])
                    else:
                        raise ColumnError(
                            "[Pyrsqrd] '{}' is reserved and cannot be used as a column alias".format(
                            c[2]))
            return t


        if isinstance(tables,str):
            tables = [tables]
        if isinstance(columns,str):
            columns = [columns]

        # FROM more than two tables is not valid
        if len(tables) > 2:
            raise TableError(
                    str("[Pyrsqrd] SELECT must contain a maximum of 2 tables."),
                    str(" Perhaps you need a join or subquery?"))


        #... get tables...
        ts = [__table(t) for t in tables]

        cj = None
        if cross_join:
            cj = __table(cross_join)

        ij = None
        if inner_join:
            ij = [__table(t) for t in [x[0] for x in inner_join]]

        lj = None
        if left_join:
            lj = [__table(t) for t in [x[0] for x in left_join]]

        rj = None
        if right_join:
            rj = [__table(t) for t in [x[0] for x in right_join]]

        fj = None
        if full_join:
            fj = [__table(t) for t in [x[0] for x in full_join]]



        #FIXME: might need to find all tables in joins, etc. before normalising
        #... now normalise columns (resolve aliases, expand wildcards, etc)...
        ts = [__apply_aliases(t,columns) for t in ts]



        if len(ts) == 2:
            t = self.cross_join(ts[0], ts[1]) # 2 tables: implicit CROSS JOIN
        elif cross_join:
            t = self.cross_join(ts[0], cj)
        else:
            t = ts[0]

        #FIXME
        #... all the join types...

        #FIXME: if USING, the join cols should:
        #    - only come from left table if wildcard (*) is used
        #    - only come from left table if bare col name (col) is used
        #    - only come from left table if left table col is specified (t1.col)
        #    - only come from right table if right table col specified (t2.col)
        #    - come from both tables if both table cols specified (t1.col, t2.col)
        #FIXME: NATURAL is just a USING with all columns
        if inner_join:
            t = self.inner_join(t, ij, inner_join)

        if left_join:
            t = self.left_join(t, lj, left_join)



        #FIXME
        #... now do where...
        if where:
            t = self.where(t,where)

        #FIXME
        #... filter the columns now...
        t = self.filter_columns(t,columns)

        #FIXME
        #... finally, order by...
        o = []
        if order_by:
            print(order_by)
            for x in order_by:
                # FIXME:
                # TODO: allow all valid expressions
                # NOTE: only uses columns atm
                exp = x[0]
                r = t.schema.column(x[0])
                d = True if 'DESC' in x[1:] else False # reverse=True == DESC
                o.append((r,d))
            print(o)
            t = t.order_by(o)
        return t





#jt = t1.innerjoin(t2).on(eq(t1.employer_id, t2.id)).where(ne(t1.name, 'john')).orderby(t1.name).desc()

#select().from().innerjoin().where().orderby()

# select columns
#     from table inner join another_table on table.thing op another_table.thing
#     where conditions
#     order by column direction
#     limit int





db = Pyrsqrd("Record-shop")

# t1:
#
#  num | name
# -----+------
#    1 | a
#    2 | b
#    3 | c

# and t2:

#  num | value
# -----+-------
#    1 | xxx
#    3 | yyy
#    5 | zzz


#FIXME: test constraints and comments
def test_fn_create_insert():
    db.create_table(
        't1',
        [('num', int, not NULL),
        ('name', str, not NULL)])

    db.create_table(
        't2',
        [('num', int, not NULL),
        ('value', str, not NULL)])


    for r in ((1,'a'), (2,'b'), (3,'c')):
        id = db.insert(
            table='t1', columns=('num','name'), values=r)

    for r in ((1,'xxx'), (3,'yyy'), (5,'zzz')):
        id = db.insert(
            table='t2', columns=('num','value'), values=r)



def test_oo_create_insert():
    db.create_table(
        't1',
        [('num', int, not NULL),
        ('name', str, not NULL)])

    db.create_table(
        't2',
        [('num', int, not NULL),
        ('value', str, not NULL)])


    t1 = db.table('t1')
    for r in ((1,'a'), (2,'b'), (3,'c')):
        #t1, id = t1.insert(columns=('num','name'), values=r)
        t1, id = db.insert(table=t1, columns=('num','name'), values=r)
    print(t1)
    print_table(t1)


    t2 = db.table('t2')
    for r in ((1,'xxx'), (3,'yyy'), (5,'zzz')):
        #t2, id = t2.insert(columns=('num','name'), values=r)
        t2, id = db.insert('t2', ('num','name'), r)
    print(t2)
    print_table(t2)






##################################
# TEST
##################################

def test_parse():
    from parser import parse

    parse("SELECT name, num AS nn FROM t1 AS ta INNER JOIN t2 AS taa ON ta.num = taa.num")

    parse("SELECT t1.name,t2.name FROM t1 AS XX CROSS JOIN (SELECT * FROM (SELECT * FROM t2)) AS YY WHERE (name = 'yyy' AND num = 3.4) OR num = 4.3 ORDER BY t2.name")

    parse("""
        SELECT *
        FROM t1
        CROSS JOIN t2
        where
        (t2.value = 'yyy' OR t1.num > 2) and t2.num <= 4
        ORDER BY t2.value DESC
        LIMIT 5
        OFFSET 3
        """)


# column args can be referenced by name as string or passed as column object
# set alias with two or three args: name, ['AS'], alias
# set table alias on table object or in db.table call
# set column alias on column object or in table.select call
def test_oo():
    t1 = db.table('t1').as_alias('mongrel')
    print(t1)
    print_table(t1, table_label=True)

    t1 = db.table('t1').select('name').as_alias('mongrel')
    print(t1)
    print_table(t1, table_label=True)

    t1 = db.table('t1').select(('name', 'AS', 'bully'))
    print(t1)
    print_table(t1)

    t1 = db.table('t1')
    t2 = db.table('t2')
    t3 = t1.crossjoin(t2)
    print(t3)
    print_table(t3)

    t1 = db.table('t1')
    t2 = db.table('t2')
    t3 = t1.crossjoin(t2).select('num','value')
    print(t3)
    print_table(t3)

    t1 = db.table('t1')
    t2 = db.table('t2')
    t3 = t1.crossjoin(t2).select(t1.name, t2.value)
    print(t3)
    print_table(t3)

    t1 = db.table('t1')
    t2 = db.table('t2')
    t3 = t1.crossjoin(t2).select((t1.name, 'jesus'), t2.value)
    print(t3)
    print_table(t3)

    t1 = db.table('t1')
    t2 = db.table('t2')
    t3 = t1.crossjoin(t2).select((t1.name, 'as', 'jesus2'), t2.value)
    print(t3)
    print_table(t3)


    #t1.innerjoin(t2).on(t1.num == t2.num)


# session.query(User).join(Document).join(DocumentsPermissions).filter(
#     User.email == "user@email.com").all()



# query.join(Address, User.id==Address.user_id) # explicit condition
# query.join(User.addresses)                    # specify relationship from left to right
# query.join(Address, User.addresses)           # same, with explicit target
# query.join('addresses')                       # same, using a string









    #c = t1.num
    #print(c,type(c))

    #c = t1.num == 3
    #print(c,type(c))

    #c = t1.column('num')
    #print(c,type(c))




    #for i,r in enumerate(t1.rows):
    #    r.num == t2.rows[i].num
    #t1.innerjoin(t2).on(t1.num == t2.num)


def test_fn():
    # Column wildcard
    # INNER JOIN with table alias
    result = db.select(
        columns = '*',
        tables = 't1',
        inner_join = [
            (('t2', as_, 'bastard'), ('t1.num', eq, 't2.num'))],
        where = [('t1.name', eq, 'a'), and_, ('t1.num', ne, 6)])
    print(result)
    print_table(result)


    # Column selection
    # INNER JOIN
    result = db.select(
        columns = ['name', 'value'],
        tables = 't1',
        inner_join = [
            ('t2', ('t1.num', eq, 't2.num'))])
    print(result)
    print_table(result)


if __name__ == "__main__":
    #test_fn_create_insert()
    test_oo_create_insert()
    #test_parse()
    #test_fn()
    test_oo()
    exit()


##################################
# END TEST
##################################
