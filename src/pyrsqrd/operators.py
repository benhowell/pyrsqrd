# operator.lt(a, b)
#def less_than_or_equal(x: str, y: Any) -> WhereConstraint:
#    return eq,x,y

# operator.le(a, b)

# # operator.eq(a, b)
# def eq(x: str, y: Any) -> WhereConstraint:
#     return operator.eq,x,y
# equal = eq

# # operator.ne(a, b)
# def ne(x: str, y: Any) -> WhereConstraint:
#     return operator.ne,x,y
# not_equal = ne

# # operator.ge(a, b)
# def ge(x: str, y: Any) -> WhereConstraint:
#     return operator.ge,x,y
# greater_or_equal = ge

# # operator.gt(a, b)
# def gt(x: str, y: Any) -> WhereConstraint:
#     return operator.gt,x,y
# greater = gt


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


# Aggregations 	Union, GroupBy, Transform, IterateBy, InnerJoin, LeftJoin, CrossJoin
# Aggregation Helpers 	union, union_inplace, group_by, transform, iterate_by,
#              inner_join, left_join, cross_join

# Properties 	rows, columns, data, dtypes, size, ndim, shape
# Iter Methods 	iterrows, itertuples, itercols
# Functional Methods 	row_map, tuple_map, col_map, pipe
# Dict-like Methods 	keys, values, items, get, update, update_inplace,
#           update_dtypes, update_dtypes_inplace
# Other Helper Methods 	select, head, copy, rename, rename_inplace, coalesce,
#            coalesce_inplace, _coalesce_dtypes, delete, delete_inplace

















    # def drop_table(self, table: str):
    #     if debug:
    #         print("DROP TABLE",table)

    #     self.__tables = self.__tables.remove(table)
    #     #ON DELETE CASCADE???

    # def tables(self, table: str = None) -> Table | list[Table]:
    #     if table:
    #         return self.__tables.get(table)
    #     return list(self.__tables.get())




# AND = and_
# OR =  or_
# NOT = not_
# XOR = xor
# def COUNT(): return True # this fn should operate on whole
#     # table (*), column (col), distinct col (DISTINCT col),
# #s.count(x): total number of occurrences of x in s


# # add, eq, ne, gt, ge, lt, le, and_, or_,
# # not_, xor, is_, is_not, add, sub, mod, mul, neg,
# # pos, truediv, floordiv

# # abs()
# # bin()
# # bool()
# # bytearray()
# # bytes()
# # complex()
# # float()
# # hex()
# # len()
# # max()
# # min()
# # oct()
# # ord()
# # pow()
# # round()
# # sum()
# # super()




# #NOTE: whereconstraint tuples should contain exactly 1 callable.
# #NOTE: callable can be prefix, infix, or postfix.

# #NOTE: column name args must be a string in format: 'table.column'.
# #      e.g. 'employee.age' (column named 'age' from the 'employee' table)
# #      this can provide the basis for joins, etc. later on

# #NOTE: args can be any type and in any number.
# #NOTE: args will always be passed to callable in the order they appear in the tuple.

# customers = db.select(
#     table = 'customer',
#     columns = ['first_name', 'last_name'],
#     where = [
#         ('customer.alive', eq, True),
#         AND,
#         ('customer.age', ge, 37),
#         OR,
#         ('customer.age', lt, 23)],
#     order_by = [('last_name', 'DESC')]
#     )



# # print_recordset(customers, metadata=True,
# #                 row_borders=True,
# #                 column_labels=True, format='ascii')
# # print_recordset(customers, metadata=True,
# #                 row_borders=True,
# #                 column_labels=True, format='unicode')

























# def pipe(data, *funcs):
#     """ Pipe a value through a sequence of functions

#     I.e. ``pipe(data, f, g, h)`` is equivalent to ``h(g(f(data)))``

#     We think of the value as progressing through a pipe of several
#     transformations, much like pipes in UNIX

#     ``$ cat data | f | g | h``

#     >>> double = lambda i: 2 * i
#     >>> pipe(3, double, str)
#     '6'

#     See Also:
#         compose
#         compose_left
#         thread_first
#         thread_last
#     """
#     for func in funcs:
#         data = func(data)
#     return data



# def thread_first(val, *forms):
#     """ Thread value through a sequence of functions/forms

#     >>> def double(x): return 2*x
#     >>> def inc(x):    return x + 1
#     >>> thread_first(1, inc, double)
#     4

#     If the function expects more than one input you can specify those inputs
#     in a tuple.  The value is used as the first input.

#     >>> def add(x, y): return x + y
#     >>> def pow(x, y): return x**y
#     >>> thread_first(1, (add, 4), (pow, 2))  # pow(add(1, 4), 2)
#     25

#     So in general
#         thread_first(x, f, (g, y, z))
#     expands to
#         g(f(x), y, z)

#     See Also:
#         thread_last
#     """
#     def evalform_front(val, form):
#         if callable(form):
#             return form(val)
#         if isinstance(form, tuple):
#             func, args = form[0], form[1:]
#             args = (val,) + args
#             return func(*args)
#     return reduce(evalform_front, forms, val)



# def thread_last(val, *forms):
#     """ Thread value through a sequence of functions/forms

#     >>> def double(x): return 2*x
#     >>> def inc(x):    return x + 1
#     >>> thread_last(1, inc, double)
#     4

#     If the function expects more than one input you can specify those inputs
#     in a tuple.  The value is used as the last input.

#     >>> def add(x, y): return x + y
#     >>> def pow(x, y): return x**y
#     >>> thread_last(1, (add, 4), (pow, 2))  # pow(2, add(4, 1))
#     32

#     So in general
#         thread_last(x, f, (g, y, z))
#     expands to
#         g(y, z, f(x))

#     >>> def iseven(x):
#     ...     return x % 2 == 0
#     >>> list(thread_last([1, 2, 3], (map, inc), (filter, iseven)))
#     [2, 4]

#     See Also:
#         thread_first
#     """
#     def evalform_back(val, form):
#         if callable(form):
#             return form(val)
#         if isinstance(form, tuple):
#             func, args = form[0], form[1:]
#             args = args + (val,)
#             return func(*args)
#     return reduce(evalform_back, forms, val)


# # values
# # table
# # join
# # where
# # group
# # order_by
# # parameter_map
