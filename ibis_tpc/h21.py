import ibis


def tpc_h21(con, NATION='SAUDI ARABIA'):
    '''Suppliers Who Kept Orders Waiting Query (Q21)

    This query identifies certain suppliers who were not able to ship required
    parts in a timely manner.'''

    supplier = con.table('supplier')
    lineitem = con.table('lineitem')
    orders = con.table('orders')
    nation = con.table('nation')

    L2 = lineitem.view()
    L3 = lineitem.view()

    q = supplier
    q = q.join(lineitem, supplier.s_suppkey == lineitem.l_suppkey)
    q = q.join(orders, orders.O_ORDERKEY == lineitem.l_orderkey)
    q = q.join(nation, supplier.s_nationkey == nation.n_nationkey)
    q = q.materialize()
    q = q[
        q.l_orderkey.name("l1_orderkey"),
        q.O_ORDERSTATUS,
        q.l_receiptdate,
        q.l_commitdate,
        q.l_suppkey.name("l1_suppkey"),
        q.s_name,
        q.n_name,
    ]
    q = q.filter([
        q.O_ORDERSTATUS == 'F',
        q.l_receiptdate > q.l_commitdate,
        q.n_name == nation,
        ((L2.l_orderkey == q.l1_orderkey) & (l2.l_suppkey != q.l1_suppkey)).any(),
        ~(((L3.l_orderkey == q.l1_orderkey) & (l3.l_suppkey != q.l1_suppkey) & (l3.l_receiptdate > l3.l_commitdate)).any()),
    ])

    gq = q.group_by([q.s_name])
    q = gq.aggregate(numwait=q.count())
    q = q.sort_by([ibis.desc(q.numwait), q.s_name])
    return q.limit(100)
