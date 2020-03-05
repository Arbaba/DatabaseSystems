select
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
from
    tpch0_001_customer,
    tpch0_001_orders,
    tpch0_001_lineitem
where
    o_orderkey in (
        select
            l_orderkey
        from
            tpch0_001_lineitem
        group by
            l_orderkey
        having
            sum(l_quantity) > 200
    )
    and c_custkey = o_custkey
    and o_orderkey = l_orderkey
group by
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
order by
    o_totalprice desc,
    o_orderdate
;
