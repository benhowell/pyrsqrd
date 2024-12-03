import re, re._parser, re._compiler
from re._constants import BRANCH, SUBPATTERN

STATEMENT = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE']
OPEXP = ['integer', 'double', 'column', 'column_alias', 'string']

from exception import parse_error

class Scanner:
    def __init__(self, lexicon, flags=0):
        if isinstance(flags, re.RegexFlag):
            flags = flags.value
        self.lexicon = lexicon
        # combine phrases into a compound pattern
        p = []
        s = re._parser.State()
        s.flags = flags
        for phrase, action in lexicon:
            gid = s.opengroup()
            p.append(re._parser.SubPattern(s, [
                (SUBPATTERN, (gid, 0, 0, re._parser.parse(phrase, flags))),
                ]))
            s.closegroup(gid, p[-1])
        p = re._parser.SubPattern(s, [(BRANCH, (None, p))])
        self.scanner = re._compiler.compile(p)
    def scan(self, string):
        self.result = []
        append = self.result.append
        match = self.scanner.scanner(string).match
        i = 0
        while True:
            m = match()
            if not m:
                break
            j = m.end()
            if i == j:
                break
            action = self.lexicon[m.lastindex-1][1]
            if callable(action):
                self.match = m
                #print(m.group())
                action = action(self, m.group())
            if action is not None:
                append(action)
            i = j
        return self.result, string[i:]
    def last(self,n):
        if len(self.result) >= n:
            return self.result[-n:]
        return None


#FIXME: join on subselect
#FIXME: FROM subselect
#FIXME: WHERE subselect???

class Lexer:
    """
    FIXME
    """
    def __init__(self):

        #FIXME: SERIAL, PRIMARY KEY, FOREIGN KEY
        self.patterns = [
            #(r"[" " \\n]",      lambda s,t: self.__ignore("whitespace",t)),
            #(r",",              lambda s,t: self.__ignore("comma",t)),
            (r"[" " \\n]",      lambda s,t: None),
            (r",",              lambda s,t: None),
            (r"SELECT",         lambda s,t: self.__select(t)),
            (r"FROM",           lambda s,t: self.__from(t)),
            (r"CROSS\s{1}JOIN", lambda s,t: self.__crossjoin(t)),
            (r"INNER\s{1}JOIN", lambda s,t: self.__innerjoin(t)),
            (r"OUTER\s{1}JOIN", lambda s,t: self.__outerjoin(t)),
            (r"ON",             lambda s,t: self.__on(t)),
            (r"WHERE",          lambda s,t: self.__where(t)),
            (r"ORDER\s{1}BY",   lambda s,t: self.__orderby(t)),
            (r"LIMIT",          lambda s,t: self.__limit(t)),
            (r"OFFSET",         lambda s,t: self.__offset(t)),
            (r"ASC",            lambda s,t: self.__asc(t)),
            (r"DESC",           lambda s,t: self.__desc(t)),
            (r"AS",             lambda s,t: self.__as(t)),
            (r"AND",            lambda s,t: self.__operator("logic", "and_", t)),
            (r"OR",             lambda s,t: self.__operator("logic", "or_", t)),
            (r"NOT",            lambda s,t: self.__operator("logic", "not_", t)),
            (r"NULL",           lambda s,t: self.__null(t)),

            # all columns with table qualifier
            (r"[a-z_]+[a-z0-9_]*[.][*]", lambda s,t: self.__qcolumns(t)),

            # column with table or table_alias qualifier
            (r"[a-z_]+[a-z0-9_]*[.][a-z0-9_]+", lambda s,t: self.__qcolumn(t)),

            # string: string value in single quotes
            (r"'[a-z_]+[a-z0-9_]*'", lambda s,t: self.__string(t.replace("'",""))),

            # name: could be alias, column or table name
            (r"[a-z_]+[a-z0-9_]*", lambda s,t: self.__name(t)),

            (r"[-+]?[\d]*[.][\d+]", lambda s,t: self.__number("double",t)),
            (r"[-+]?[\d+]",     lambda s,t: self.__number("integer",t)),
            (r">=",             lambda s,t: self.__operator("bool", "ge", t)),
            (r">",              lambda s,t: self.__operator("bool", "gt", t)),
            (r"<=",             lambda s,t: self.__operator("bool", "le", t)),
            (r"<",              lambda s,t: self.__operator("bool", "lt", t)),
            (r"!=",             lambda s,t: self.__operator("bool", "ne", t)),
            (r"=",              lambda s,t: self.__operator("bool", "eq", t)),
            (r"\*",             lambda s,t: self.__columns(t)),
            (r"\(",             lambda s,t: self.__lparen(t)),
            (r"\)",             lambda s,t: self.__rparen(t)),
            (r";",              lambda s,t: self.__terminator(t))]

        self.scanner = Scanner(self.patterns, flags=re.IGNORECASE)


    def scan(self, txt):
        self.kw = None
        self.table_alias = {}
        self.column_alias = {}
        return self.scanner.scan(txt)


    def __kw(self,v):
        if last := self.scanner.last(1):
            e = last[0]['expecting']
            if v.upper() in e:
                self.kw = v.upper()
            else:
                parse_error(v.upper(),e)
        else:
            self.kw = v.upper()


    def __select(self,v):
        self.__kw(v)
        return {"type": "keyword", "value": v, "expecting": ['column', 'asterisk']}

    def __from(self,v):
        self.__kw(v)
        return {"type": "keyword", "value": v, "expecting": ['table','lparen']}

    def __crossjoin(self,v):
        self.__kw(v)
        return {"type": "keyword", "value": v, "expecting": ['table','lparen']}

    def __innerjoin(self,v):
        self.__kw(v)
        return {"type": "keyword", "value": v, "expecting": ['table','lparen']}

    def __outerjoin(self,v):
        self.__kw(v)
        return {"type": "keyword", "value": v, "expecting": ['table','lparen']}

    def __on(self,v):
        self.__kw(v)
        return {"type": "keyword", "value": v, "expecting": [
            'column', 'column_alias','table_alias_column']}

    def __where(self,v):
        self.__kw(v)
        return {"type": "keyword", "value": v, "expecting": ['table','table_alias','lparen']}

    def __orderby(self,v):
        self.__kw(v)
        return {"type": "keyword", "value": v, "expecting": ['column','column_alias']}

    def __limit(self,v):
        self.__kw(v)
        return {"type": "keyword", "value": v, "expecting": ['integer']}

    def __offset(self,v):
        self.__kw(v)
        return {"type": "keyword", "value": v, "expecting": ['integer']}

    def __asc(self,v):
        return {"type": "order", "value": v,
                "expecting": ['column','column_alias', 'terminator', 'LIMIT']}

    def __desc(self,v):
        return {"type": "order", "value": v,
                "expecting": ['column','column_alias', 'terminator', 'LIMIT']}

    def __as(self,v):
        return {"type": "as", "value": v, "expecting": ['name']}

    # type logic: AND | OR
    # type bool: >= | > | <= | < | != | =
    def __operator(self,t,st,v):
        return {"type": t, "value": v, "subtype": st, "expecting": OPEXP}

    def __null(self,v):
        return {"type": "null", "value": v, "expecting": []}

    def __lparen(self,v):
        return {"type": "lparen", "value": v, "expecting": ['SELECT', 'column', 'lparen']}

    def __rparen(self,v):
        return {"type": "rparen", "value": v, "expecting": [
            'as', 'and_', 'or_', 'name', 'rparen', 'lparen', 'terminator']}

    def __terminator(self,v):
        return {"type": "terminator", "value": v, "expecting": []}

    def __ignore(self,st,v):
        return {"type": "ignore", "subtype": st, "value": v}

    def __number(self,t,v):
        e = []
        if self.kw == 'LIMIT' and t == 'integer':
            e = ['OFFSET']
        elif self.kw == 'OFFSET' and t == 'integer':
            e = ['terminator']
        elif self.kw == 'WHERE':
            last = self.scanner.last(1)
            p = last[0]
            if p['value'] == 'WHERE':
                e = ['bool']
            elif p['type'] == 'bool':
                e = ['lparen','rparen','logic','ORDER BY','LIMIT']
        elif self.kw == 'ON' and p['type'] == 'bool':
            e = ['logic', 'INNER JOIN','OUTER JOIN','WHERE','ORDER BY','LIMIT']
        return {"type": t, "value": v, "expecting": e}


    def __columns(self,v):
        return {"type": "columns", "value": v, "expecting": ['column', 'FROM']}

    # indeterminate qualified column
    # determines type: table_column, table_alias_column
    def __qcolumn(self,t):
        tc = t.split('.')
        if self.table_alias.get(tc[0],None):
            return self.__table_alias_column(t)
        else:
            return self.__table_column(t)

    # derived from qcolumn
    def __table_alias_column(self,v):
        return {"type": 'table_alias_column', "value": v, "expecting": [
            'column', 'as', 'FROM', 'bop', 'and_', 'or_',
            'rparen','ASC', 'DESC', 'terminator']}

    # derived from qcolumn
    def __table_column(self,v):
        return {"type": "table_column", "value": v, "expecting": [
            'column', 'as', 'FROM', 'bop', 'and_', 'or_',
            'rparen','ASC', 'DESC', 'terminator']}

    # indeterminate qualified columns
    # determines type: table_columns, table_alias_columns
    def __qcolumns(self,t):
        tc = t.split('.')
        if self.table_alias.get(tc[0],None):
            return self.__table_alias_columns(t)
        else:
            return self.__table_columns(t)

    # derived from qcolumns
    def __table_alias_columns(self,v):
        return {"type": "table_alias_columns", "value": v, "expecting": [
            'column', 'FROM']}

    # derived from qcolumns
    def __table_columns(self,v):
        return {"type": "table_columns", "value": v, "expecting": [
            'column', 'FROM']}

    # indeterminate name
    # determines type: column, table, column_alias, table_alias
    def __name(self,t):
        last = self.scanner.last(1)
        print("last",last)
        p = last[0]

        print("kw",self.kw)


        if self.kw == 'SELECT' and p['type'] == 'as':
            return self.__column_alias(t)
        elif self.kw == 'SELECT':
            return self.__column(t)
        elif self.kw == 'FROM' and p['type'] == 'as':
            return self.__table_alias(t)
        elif self.kw == 'FROM':
            return self.__table(t)
        elif self.kw in ['INNER JOIN', 'OUTER JOIN', 'CROSS JOIN']:
            if self.table_alias.get(t,None):
                return self.__table_alias(t)
            else:
                return self.__table(t)
        elif self.kw in ['ON','WHERE','ORDER BY']:
            if self.column_alias.get(t,None):
                return self.__column_alias(t)
            else:
                return self.__column(t)

    # derived from name
    def __column_alias(self,v):
        return {"type": "column_alias", "value": v,
            "expecting": ['column', 'FROM', 'bop', 'and_', 'or_',
                        'rparen','ASC', 'DESC', 'terminator']}

    # derived from name
    def __column(self,v):
        return {"type": "column", "value": v, "expecting": [
            'column', 'as', 'FROM', 'bop', 'and_', 'or_',
            'rparen','ASC', 'DESC', 'terminator']}

    def __table_alias(self,v):
        return {"type": "table_alias", "value": v, "expecting": [
            'ON','CROSS JOIN', 'INNER JOIN','OUTER JOIN','WHERE', 'ORDER BY',
            'LIMIT', 'rparen', 'terminator']}

    def __table(self,v):
        return {"type": "table", "value": v, "expecting": [
            'ON','CROSS JOIN', 'INNER JOIN','OUTER JOIN','WHERE', 'ORDER BY',
            'LIMIT', 'rparen', 'terminator'
            'as', 'alias']}

    def __string(self,v):
        return {"type": "string", "value": v, "expecting": []}



class Node(object):
    def __init__(self, val):
        self.value = val # Value of node
        self.children = [] # Can have multiple children

    def add_child(self, node):
        self.children.append(node)
        return node

    def print_df(self, mode='all'):
        print(self.value['value'],"   ["+self.value['type']+"]")

        # if mode == 'symbol':
        #     print(self.value, end=' ')
        # elif mode == 'token':
        #     if isinstance(self.value, dict):
        #         if x := self.value.get('token', None):
        #             print(x, end=' ')
        # elif mode == 'token_type':
        #     if isinstance(self.value, dict):
        #         if x := self.value.get('token_type', None):
        #             print(x, end=' ')
        # else: # mode == 'all'
        #     print(self.value, end=' ')

        if len(self.children) > 0:
            for c in self.children:
                c.print_df(mode)


def tokenise(txt):
    """
    Token generator.
    Yields tokens, one at a time, in order.
    """
    tokens, unknown = Lexer().scan(txt.strip())
    if(len(unknown) != 0):
        raise Exception(f"Syntax error near: {unknown.split(' ')[0]}")
    else:
        for x in tokens:
            if x["type"] == "ignore":
                continue
            else:
                yield x


def advance(nxtok, tokens):
    return nxtok, next(tokens, None)


def push(obj, l, depth):
    while depth:
        l = l[-1]
        depth -= 1
    l.append(obj)




# Small subset of SQL supported.
# - select columns from (table/subselect) cross/inner/outer join where orderby (direction)
# - wildcards and table qualifiers can be used for column reference
# - all aliases MUST prepend the 'AS' keyword (i.e. columns, tables, and subselects)
# - all ORDER BY expressions MUST include direction of order (i.e. ASC or DESC)
# - logic operators (AND, OR, NOT)
# - boolean operators
# - where clauses can use parens to determine operator precedence.
# - LIMIT
# - OFFSET
# - NO GROUP BY and no aggregates.
# - NO arithmetic operators


def parse(txt):

    print(txt)

    tokens = tokenise(txt+';')
    tok = None
    nxtok = None

    column_aliases = {}
    table_aliases = {}

    stack = []
    depth = 0
    kw = None


    # empty head, first token
    tok, nxtok = advance(nxtok, tokens)

    # valid statements start with a statement keyword.
    # - i.e. SELECT, INSERT, UPDATE, DELETE, CREATE
    if nxtok['value'] not in STATEMENT:
        parse_error(nxtok, STATEMENT)

    # advance to first token, second token
    tok, nxtok = advance(nxtok, tokens)
    tree = Node(tok)

    while nxtok != None:
        print()
        print(tok['value'],"[",tok['type'],"]","->",nxtok['value'],"[",nxtok['type'],"]")

        # push down automata to capture expressions in parens
        try:
            if tok['type'] == 'lparen':
                push([], stack, depth)
                depth += 1
            elif tok['type'] == 'rparen':
                depth -= 1
            elif depth > 0:
                push(tok['value'], stack, depth)
        except IndexError:
            raise ValueError('Parentheses mismatch')


        # if tok['type'] == 'keyword':
        #     kw = tok['value']
        #     if len(tok['expected']) > 0:
        #         if nxtok['type'] not in tok['expected']:
        #             parse_error(nxtok, tok['expected'])

        # if kw == 'SELECT':
        #     if tok['value'] == 'SELECT':
        #         # SELECT *
        #         if nxtok['type'] == 'asterisk':
        #             nxtok = token.columns | {"value": nxtok['value']}
        #         # SELECT column
        #         elif nxtok['type'] == 'name':
        #             nxtok = token.column | {"value": nxtok['value']}
        #         # else SELECT column (fully qualified column: table.column)

        #     elif tok['value'] == 'AS':
        #         if nxtok['type'] == 'name':
        #             column_aliases = column_aliases | {
        #                 tok['previous']['value']: nxtok['value'],
        #                 nxtok['value']: tok['previous']['value']}
        #             nxtok = token.column_alias | {"value": nxtok['value']}
        #         else:
        #             parse_error(nxtok, tok['expected'])

            # elif tok['type'] == ['column']:
            #     nxtok could be name, column, AS, FROM
            # elif tok['type'] == ['columns']:
            #     nxtok could be column, FROM
            # elif tok['type'] == ['column_alias']:
            #     nxtok could be name, column, AS, FROM












            #i.e. inner join
            #so that we can know if name (alias or table) needs to expect ON
            #e.g. "INNER JOIN t1 as yo ON t1.man == t2.joey"

            #FIXME: upper or lower. choose.

            #FIXME: need a stack to put parens on.

            #FIXME: need to track aliases

            # elif nxtok['type'] == 'asterisk':
            #     if tok['value'] == 'SELECT':
            #         nxtok = token.columns | {
            #             "value": nxtok['value']}


            # elif nxtok['type'] == 'name':
            #     # add inferred_type to token
            #     if tok['value'] == 'SELECT':
            #         nxtok = token.column | {"value": nxtok['value']}
            #     elif tok['type'] == 'column':
            #         nxtok = token.column | {
            #             "supertype": "name", "value": nxtok['value']}
            #     elif tok['value'] == 'AS':
            #         if tok['previous']['type'] == 'column':
            #             column_aliases = column_aliases | {
            #                 tok['previous']['value']: nxtok['value'],
            #                 nxtok['value']: tok['previous']['value']}
            #             nxtok = token.column_alias | {"value": nxtok['value']}
            #         elif tok['previous']['type'] == 'table':
            #             table_aliases = table_aliases | {
            #                 tok['previous']['value']: nxtok['value'],
            #                 nxtok['value']: tok['previous']['value']}
            #             nxtok = token.table_alias | {"value": nxtok['value']}

            #         #FIXME: need to set as expression_alias and have the whole
            #         #subselect classified as a statement itself

            #         elif tok['previous']['type'] == 'rparen':
            #             table_aliases = table_aliases | {
            #                 tok['previous']['value']: nxtok['value'],
            #                 nxtok['value']: tok['previous']['value']}
            #             nxtok = token.table_alias | {"value": nxtok['value']}
            #     elif tok['value'] == 'FROM':
            #         nxtok = token.table | {"value": nxtok['value']}

            #     #FIXME: this could be column, column_alias
            #     elif tok['value'] == 'WHERE':
            #         nxtok = token.column | {"value": nxtok['value']}

            #     elif tok['value'] == 'CROSS JOIN':
            #         nxtok = token.table | {"value": nxtok['value']}

            #     elif tok['value'] == 'INNER JOIN':
            #         nxtok = token.table | {"value": nxtok['value']}

            #     elif tok['value'] == 'OUTER JOIN':
            #         nxtok = token.table | {"value": nxtok['value']}

            #     # elif tok['type'] == 'lparen':
            #     #     nxtok = token.table_alias | {
            #     #         #"root": tok['value'],
            #     #         "value": nxtok['value']
            #     #         }
            #     elif tok['type'] == 'rparen':
            #         table_aliases = table_aliases | {
            #                 tok['previous']['value']: nxtok['value'],
            #                 nxtok['value']: tok['previous']['value']}
            #         nxtok = token.table_alias | {"value": nxtok['value']}

            # elif nxtok['type'] not in tok['expected'] \
            #     and nxtok['supertype'] not in tok['expected']:
            #     parse_error(nxtok, tok['expected'])

        tok, nxtok = advance(nxtok, tokens)
        tree.add_child(Node(tok))
    print("column aliases:", column_aliases)
    print("table aliases:", table_aliases)
    print("FINAL STACK",stack)
    if depth > 0:
        raise ValueError('Parentheses mismatch')
    else:
        print("parens match!")
    tree.print_df()
