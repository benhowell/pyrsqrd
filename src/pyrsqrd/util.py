#from copy import deepcopy
#from structure import Table

NULL = True
RESERVED = ['table', 'tables', 'column', 'columns', 'label', 'idx',
            'vtype', 'nullable', 'fq_label', 'values','alias','fq_alias',
            'constraints', 'comment']

AS = as_ = 'as'
USING = using_ = 'using'


def dict_mapv(v,f,*a):
    return {x[0]:x[1] for x in (f(v,*a) for v in v)}

def mapv(v,f,*a):
    return [f(v,*a) for v in v]

def mapkv(m,f,*a):
    return {f(k,v,*a) for (k,v) in m.items()}

def filterkv(m,f,*a):
    return {k:v for (k,v) in m.items() if f(k,v,*a)}

def flattenv(v):
    return [x for xs in v for x in xs]

def filterv(v,f,*a):
    return [v for v in v if f(v,*a)]

def filteriv(v,f,r='v',*a):
    return [iv if r=='both' else iv[0] if r=='i' else iv[1] # v
            for iv in enumerate(v) if f(iv,*a)]

#or (isinstance(x, tuple) and getattr(x, '_fields', None) is None) \
def is_listy(x):
    if isinstance(x, list) \
        or isinstance(x, tuple) \
        or isinstance(x, set): \
        return True
    return False

def pop_seq(seq, idx):
    """Returns item at idx in seq (pop), and resulting seq"""
    return seq[idx], seq[:idx] + seq[idx+1:]



# decorator (or fn) that allows parameters to be aliased.
# e.g. "last_name" can be aliased with the items  ["surname",...]
def alias_args(m: dict):
    def caller(f):
        def dec(*args, **kwargs):
            _kw = dict(kwargs)
            for k,w in kwargs.items():
                if not k in m:
                    for a,v in m.items():
                        if k in v:
                            _kw[a]=w
                            del _kw[k]
            return f(*args, **_kw)
        return dec
    return caller



def print_table(t: 'Table',
                table_label = False,
                column_labels = True,
                row_count = True,
                row_borders = False,
                format = 'unicode',
                default_missing = '(none)'):
    pad = 4
    max = []
    if column_labels:
        labels = mapv(t.columns,
            lambda l: l.alias if l.alias is not None else l.label)
        # determine widest label for each column...
        max = [len(x) for x in labels]

    # determine widest value in each column...
    for r in t.rows:
        for i, v in enumerate(r):
            if v is None:
                v = default_missing
            if len(str(v)) > max[i]:
                max[i] = len(str(v))

    if format == 'ascii':
        h='—'               # horizontal # '-' # '~' # '='
        v=['|','+','+','—'] # verticals
        r=['|','|']         # row ends
        c=['.','.',"'","'"] # corners
        m='  '              # margin

    else: # format == 'unicode':
        h='─'               # horizontal
        v=['│','┬','┼','┴'] # verticals
        r=['├','┤']         # row ends
        c=['┌','┐','└','┘'] # corners
        m='  '              # margin

    col_line = lambda h,v: v.join(h * (m+pad) for m in max)
    top_border = m + c[0] + col_line(h,h) + c[1]
    top_border_cols = m + c[0] + col_line(h,v[1]) + c[1]

    bottom_border = m + c[2] + col_line(h,v[3]) + c[3]
    col_top_border = m + r[0] + col_line(h,v[1]) + r[1]
    row_border = m + r[0] + col_line(h,v[2]) + r[1]

    meta_fmt = m + v[0] + '{0:<' + str(
        sum((max[i]+pad+1) for i in range(
            1, len(t.columns))) + (max[0]+pad)) + '}' + v[0]

    row_format = m + v[0] + '{0:<' + str(
        max[0]+pad) + '}' + v[0] + ''.join('{' + str(i) + ':<' + str(
            max[i]+pad) + '}' + v[0] for i in range(1, len(t.columns)))

    rb = False
    if not table_label and not column_labels:
        rb = True
    elif table_label:
        print(top_border)
        print(meta_fmt.format(pad=pad,max=max, *[t.alias if t.alias else t.label]))
        print(col_top_border)

    if column_labels:
        labels = mapv(t.columns,
            lambda l: l.alias if l.alias is not None else l.label)
        if not table_label:
            print(top_border_cols)
        print(row_format.format(pad=pad,max=max, *labels))
        if not row_borders:
            print(row_border)

    for r in t.rows:
        if rb:
            rb = False
            print(top_border_cols)
        elif row_borders:
            print(row_border)
        print(row_format.format(
            *mapv(r, lambda v: str(v) if v is not None else default_missing),
            pad=pad,max=max))
    print(bottom_border)
    if row_count:
        print(m + '(' + str(len(t.rows)) + ' rows)')
    print("")
