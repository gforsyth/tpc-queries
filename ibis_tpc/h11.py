import ibis


def tpc_h11(con, NATION='GERMANY', FRACTION=.0001):
    partsupp = con.table('partsupp')
    supplier = con.table('supplier')
    nation = con.table('nation')

    q = partsupp
    q = q.join(supplier, partsupp.Ps_suppkey == supplier.s_suppkey)
    q = q.join(nation, nation.n_nationkey == supplier.s_nationkey)
    q = q.materialize()

    q = q.filter([q.n_name == nation])

    innerq = partsupp
    innerq = innerq.join(supplier, partsupp.Ps_suppkey == supplier.s_suppkey)
    innerq = innerq.join(nation, nation.n_nationkey == supplier.s_nationkey)
    innerq = innerq.materialize()
    innerq = innerq.filter([innerq.n_name == nation])
    innerq = innerq.aggregate(total=(innerq.Ps_supplycost * innerq.ps_availqty).sum())

    gq = q.group_by([q.Ps_partkey])
    q = gq.aggregate(value=(q.Ps_supplycost * q.ps_availqty).sum())
    q = q.filter([q.value > innerq.total * FRACTION])
    q = q.sort_by(ibis.desc(q.value))
    return q
