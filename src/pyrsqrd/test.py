#------------------------------#
# CROSS JOIN


#  num | name | num | value
# -----+------+-----+-------
#    1 | a    |   1 | xxx
#    1 | a    |   3 | yyy
#    1 | a    |   5 | zzz
#    2 | b    |   1 | xxx
#    2 | b    |   3 | yyy
#    2 | b    |   5 | zzz
#    3 | c    |   1 | xxx
#    3 | c    |   3 | yyy
#    3 | c    |   5 | zzz
# (9 rows)


# Multiple FROM tables (implicit CROSS JOIN)
# Column wildcard
result = db.select(columns = '*', tables = ['t1', 't2'])
print(result)
print_table(result)


# Multiple FROM tables (implicit CROSS JOIN)
# Column wildcard
# Single where clause
result = db.select(
    columns = '*',
    tables = ['t1', 't2'],
    where = [('t2.value', eq, 'yyy')])
print(result)
print_table(result)


# Explicit cross join syntax
# Column wildcard
result = db.select(columns = '*', table = 't1', cross_join = 't2')
print(result)
print_table(result)


# Explicit cross join syntax
# Column wildcard
# Single where clause
result = db.select(columns = '*', table = 't1', cross_join = 't2',
                   where = [('t2.value', eq, 'zzz')])
print(result)
print_table(result)


# Explicit cross join syntax
# Column subset selection
# Single where clause
result = db.select(columns = ['t1.name', 't2.value'],
                   table = 't1',
                   cross_join = 't2',
                   where = [('t2.value', eq, 'zzz')])
print(result)
print_table(result)


#FIXME: filter_columns fails: cannot put duplicate field into namedtuple
# Explicit cross join syntax
# Column subset selection with duplicate column
# Single where clause
# result = db.select(columns = ['t1.name', 't2.value', 't1.name'],
#                    table = 't1',
#                    cross_join = 't2',
#                    where = [('t2.value', eq, 'zzz')])
# print(result)
# print_table(result)


# Multiple FROM tables (implicit CROSS JOIN)
# Column wildcard
# Multiple WHERE constraints
result = db.select(
    columns = '*',
    tables = ['t1', 't2'],
    where = [('t2.value', eq, 'yyy'), or_, ('t1.num', gt, 2), and_, ('t2.num', le, 4)])
print(result)
print_table(result)



# Column alias
# SELECT function keyword alias (tables aliased as table)
result = db.select(
    columns = ['num', ('name', as_, 'mongrel')],
    table = 't1')
print(result)
print_table(result)



# Column alias
# ORDER BY constraint using column alias
result = db.select(
    columns = ['num', ('name', as_, 'mongrel')],
    table = 't1',
    order_by = [('mongrel', 'DESC')])
print(result)
print_table(result)


# FIXME: implement NULL order
# sort_expression1 [ASC | DESC] [NULLS { FIRST | LAST }]
# Sort expression(s) can be any expression that would be valid in the query's select list.

# Multiple FROM tables (implicit CROSS JOIN)
# Multiple column aliases
# Complex ORDER BY with multiple expressions using column aliases
result = db.select(
    columns = ['num', ('name', as_, 'mongrel'), ('t2.num', as_, 'jesus'), 'value'],
    tables = ['t1', 't2'],
    where = [('jesus', ne, 3)],
    order_by= [('mongrel', 'DESC'), ('jesus', 'DESC')])
print(result)
print_table(result)


# Multiple FROM tables (implicit CROSS JOIN)
# Multiple column aliases
# Complex ORDER BY with multiple expressions using column aliases
# Table alias
result = db.select(
    columns = ['num', ('name', as_, 'mongrel'), ('t2.num', as_, 'jesus'), 'value'],
    tables = [('t1', as_, 'bully' ), 't2'],
    where = [('jesus', ne, 3)],
    order_by= [('bully.mongrel', 'DESC'), ('jesus', 'DESC')])
print(result)
print_table(result)

#tables = 't1'
#tables = ['t1']
#tables = [('t1','bully')]
#tables = [('t1', as_, 'bully' )],
#tables = ['t1 AS bully'],
#??
#should I drop the as_ ???





#------------------------------#
# INNER JOIN

# => SELECT * FROM t1 INNER JOIN t2 ON t1.num = t2.num;
#  num | name | num | value
# -----+------+-----+-------
#    1 | a    |   1 | xxx
#    3 | c    |   3 | yyy
# (2 rows)

# => SELECT * FROM t1 INNER JOIN t2 USING (num);
#  num | name | value
# -----+------+-------
#    1 | a    | xxx
#    3 | c    | yyy
# (2 rows)

# => SELECT * FROM t1 NATURAL INNER JOIN t2;
#  num | name | value
# -----+------+-------
#    1 | a    | xxx
#    3 | c    | yyy
# (2 rows)



# Column wildcard
# INNER JOIN with table alias
result = db.select(
   columns = '*',
   tables = 't1',
   inner_join = [
       (('t2', as_, 'bastard'), ('t1.num', eq, 't2.num'))])
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



#FIXME: USING not implemented
# Column wildcard
# INNER JOIN with table alias
# result = db.select(
#    columns = '*',
#    tables = 't1',
#    inner_join = [
#        ('t2', USING, ('num',))])
# print(result)
# print_table(result)

#FIXME: t1 NATURAL INNER JOIN t2

#FIXME: statement alias, func alias


# => SELECT * FROM t1 LEFT JOIN t2 ON t1.num = t2.num;
#  num | name | num | value
# -----+------+-----+-------
#    1 | a    |   1 | xxx
#    2 | b    |     |
#    3 | c    |   3 | yyy
# (3 rows)

# => SELECT * FROM t1 LEFT JOIN t2 USING (num);
#  num | name | value
# -----+------+-------
#    1 | a    | xxx
#    2 | b    |
#    3 | c    | yyy
# (3 rows)

# Column wildcard
# LEFT JOIN
result = db.select(
   columns = '*',
   tables = 't1',
   left_join = [
       ('t2', ('t1.num', eq, 't2.num'))])
print(result)
print_table(result)
