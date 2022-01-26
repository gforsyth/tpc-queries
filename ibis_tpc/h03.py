'Shipping Priority Query (Q3)'


import ibis


def tpc_h03(con, MKTSEGMENT='BUILDING', DATE='1995-03-15'):
    customer = con.table('customer')
    orders = con.table('orders')
    lineitem = con.table('lineitem')

    q = customer.join(orders, customer.C_CUSTKEY == orders.O_CUSTKEY)
    q = q.join(lineitem, lineitem.l_orderkey == orders.o_orderkey).materialize()
    q = q.filter([
        q.C_MKTSEGMENT == MKTSEGMENT,
        q.O_ORDERDATE < DATE,
        q.l_shipdate > date
    ])
    qg = q.group_by([q.l_orderkey, q.o_orderdate, q.o_shippriority])
    q = qg.aggregate(revenue=(q.l_extendedprice * (1 - q.l_discount)).sum())
    q = q.sort_by([ibis.desc(q.revenue), q.O_ORDERDATE])
    q = q.limit(10)

    return q
