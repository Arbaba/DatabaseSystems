select
    s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
from
    tpch0_001_part,
    tpch0_001_supplier,
    tpch0_001_partsupp,
    tpch0_001_nation,
    tpch0_001_region
where
    p_partkey = ps_partkey
    and s_suppkey = ps_suppkey
    and p_size = 15
    and p_type LIKE '%TIN'
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'EUROPE'
    and ps_supplycost = (
        select
            min(ps_supplycost)
        from
            tpch0_001_partsupp,
            tpch0_001_supplier,
            tpch0_001_nation,
            tpch0_001_region
        where
            p_partkey = ps_partkey
            and s_suppkey = ps_suppkey
            and s_nationkey = n_nationkey
            and n_regionkey = r_regionkey
            and r_name = 'EUROPE'
    )
order by
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
limit 100
;
