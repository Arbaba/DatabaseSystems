select
    sum(l_extendedprice) / 7.0 as avg_yearly
from
    tpch0_001_lineitem,
    tpch0_001_part
where
    p_partkey = l_partkey
    and p_brand = 'Brand#23'
    and p_container = 'MED BAG'
    and l_quantity < (
        select
            0.2 * avg(l_quantity)
        from
            tpch0_001_lineitem
        where
            l_partkey = p_partkey
    )
;
