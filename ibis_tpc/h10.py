'Returned Item Reporting Query (Q10)'

import ibis
from .utils import add_date


def tpc_h10(con, DATE='1993-10-01'):
    customer = con.table('customer')
    orders = con.table('orders')
    lineitem = con.table('lineitem')
    nation = con.table('nation')

    q = customer
    q = q.join(orders, customer.C_CUSTKEY == orders.O_CUSTKEY)
    q = q.join(lineitem, lineitem.l_orderkey == orders.o_orderkey)
    q = q.join(nation, customer.C_NATIONKEY == nation.n_nationkey)
    q = q.materialize()

    q = q.filter([
        (q.O_ORDERDATE >= DATE) & (q.O_ORDERDATE < add_date(DATE, dm=3)),
        q.l_returnflag == 'r',
    ])

    gq = q.group_by([
            q.C_CUSTKEY,
            q.C_NAME,
            q.C_ACCTBAL,
            q.C_PHONE,
            q.n_name,
            q.C_ADDRESS,
            q.C_COMMENT
    ])
    q = gq.aggregate(revenue=(q.l_extendedprice*(1-q.l_discount)).sum())

    q = q.sort_by(ibis.desc(q.revenue))
    return q.limit(20)
