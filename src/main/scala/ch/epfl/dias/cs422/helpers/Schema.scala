package ch.epfl.dias.cs422.helpers

import org.apache.calcite.sql.`type`.SqlTypeName

object Schema {
  val SF = "0_001"

  def nullable(t: SqlTypeName): (SqlTypeName, Boolean) = (t, true)
  //FIXME: consider nullable attributes

  val schema = Map(
    "data" -> List(
      "col0"              -> SqlTypeName.INTEGER,
      "col1"              -> SqlTypeName.INTEGER,
      "col2"              -> SqlTypeName.INTEGER,
      "col3"              -> SqlTypeName.INTEGER,
      "col4"              -> SqlTypeName.INTEGER,
      "col5"              -> SqlTypeName.INTEGER,
      "col6"              -> SqlTypeName.INTEGER,
      "col7"              -> SqlTypeName.INTEGER,
      "col8"              -> SqlTypeName.INTEGER,
      "col9"              -> SqlTypeName.INTEGER,
    ),
    "empty_table" -> List(
      "col0"              -> SqlTypeName.INTEGER,
      "col1"              -> SqlTypeName.INTEGER,
      "col2"              -> SqlTypeName.INTEGER,
      "col3"              -> SqlTypeName.INTEGER,
      "col4"              -> SqlTypeName.INTEGER,
      "col5"              -> SqlTypeName.INTEGER,
      "col6"              -> SqlTypeName.INTEGER,
      "col7"              -> SqlTypeName.INTEGER,
      "col8"              -> SqlTypeName.INTEGER,
      "col9"              -> SqlTypeName.INTEGER,
    ),
    "order_small" -> List(
      "col0"              -> SqlTypeName.INTEGER,
      "col1"              -> SqlTypeName.INTEGER,
      "col2"              -> SqlTypeName.CHAR,
      "col3"              -> SqlTypeName.DECIMAL,
      "col4"              -> SqlTypeName.DATE,
      "col5"              -> SqlTypeName.VARCHAR,
      "col6"              -> SqlTypeName.VARCHAR,
      "col7"              -> SqlTypeName.INTEGER,
      "col8"              -> SqlTypeName.VARCHAR,
    ),
    "lineitem_small" -> List(
      "col0"              -> SqlTypeName.INTEGER,
      "col1"              -> SqlTypeName.INTEGER,
      "col2"              -> SqlTypeName.INTEGER,
      "col3"              -> SqlTypeName.INTEGER,
      "col4"              -> SqlTypeName.DECIMAL,
      "col5"              -> SqlTypeName.DECIMAL,
      "col6"              -> SqlTypeName.DECIMAL,
      "col7"              -> SqlTypeName.DECIMAL,
      "col8"              -> SqlTypeName.VARCHAR,
      "col9"              -> SqlTypeName.VARCHAR,
      "col10"             -> SqlTypeName.DATE,
      "col11"             -> SqlTypeName.DATE,
      "col12"             -> SqlTypeName.DATE,
      "col13"             -> SqlTypeName.VARCHAR,
      "col14"             -> SqlTypeName.VARCHAR,
      "col15"             -> SqlTypeName.VARCHAR,
    ),
    "tpch" + SF + "_part" -> List(
      "p_partkey"         -> SqlTypeName.INTEGER,
      "p_name"            -> SqlTypeName.VARCHAR,
      "p_mfgr"            -> SqlTypeName.VARCHAR,
      "p_brand"           -> SqlTypeName.VARCHAR,
      "p_type"            -> SqlTypeName.VARCHAR,
      "p_size"            -> SqlTypeName.INTEGER,
      "p_container"       -> SqlTypeName.VARCHAR,
      "p_retailprice"     -> SqlTypeName.DECIMAL,
      "p_comment"         -> SqlTypeName.VARCHAR,
    ),
    "tpch" + SF + "_supplier" -> List(
      "s_suppkey"         -> SqlTypeName.INTEGER,
      "s_name"            -> SqlTypeName.VARCHAR,
      "s_address"         -> SqlTypeName.VARCHAR,
      "s_nationkey"       -> SqlTypeName.INTEGER,
      "s_phone"           -> SqlTypeName.VARCHAR,
      "s_acctbal"         -> SqlTypeName.DECIMAL,
      "s_comment"         -> SqlTypeName.VARCHAR,
    ),
    "tpch" + SF + "_partsupp" -> List(
      "ps_partkey"         -> SqlTypeName.INTEGER,
      "ps_suppkey"         -> SqlTypeName.INTEGER,
      "ps_availqty"        -> SqlTypeName.INTEGER,
      "ps_supplycost"      -> SqlTypeName.DECIMAL,
      "ps_comment"         -> SqlTypeName.VARCHAR,
    ),
    "tpch" + SF + "_customer" -> List(
      "c_custkey"          -> SqlTypeName.INTEGER,
      "c_name"             -> SqlTypeName.VARCHAR,
      "c_address"          -> SqlTypeName.VARCHAR,
      "c_nationkey"        -> SqlTypeName.INTEGER,
      "c_phone"            -> SqlTypeName.VARCHAR,
      "c_acctbal"          -> SqlTypeName.DECIMAL,
      "c_mktsegment"       -> SqlTypeName.VARCHAR,
      "c_comment"          -> SqlTypeName.VARCHAR,
    ),
    "tpch" + SF + "_orders" -> List(
      "o_orderkey"        -> SqlTypeName.INTEGER,
      "o_custkey"         -> SqlTypeName.INTEGER,
      "o_orderstatus"     -> SqlTypeName.CHAR,
      "o_totalprice"      -> SqlTypeName.DECIMAL,
      "o_orderdate"       -> SqlTypeName.DATE,
      "o_orderpriority"   -> SqlTypeName.VARCHAR,
      "o_clerk"           -> SqlTypeName.VARCHAR,
      "o_shippriority"    -> SqlTypeName.INTEGER,
      "o_comment"         -> SqlTypeName.VARCHAR,
    ),
    "tpch" + SF + "_lineitem" -> List(
      "l_orderkey"        -> SqlTypeName.INTEGER,
      "l_partkey"         -> SqlTypeName.INTEGER,
      "l_suppkey"         -> SqlTypeName.INTEGER,
      "l_linenumber"      -> SqlTypeName.INTEGER,
      "l_quantity"        -> SqlTypeName.DECIMAL,
      "l_extendedprice"   -> SqlTypeName.DECIMAL,
      "l_discount"        -> SqlTypeName.DECIMAL,
      "l_tax"             -> SqlTypeName.DECIMAL,
      "l_returnflag"      -> SqlTypeName.VARCHAR,
      "l_linestatus"      -> SqlTypeName.VARCHAR,
      "l_shipdate"        -> SqlTypeName.DATE,
      "l_commitdate"      -> SqlTypeName.DATE,
      "l_receiptdate"     -> SqlTypeName.DATE,
      "l_shipinstruct"    -> SqlTypeName.VARCHAR,
      "l_shipmode"        -> SqlTypeName.VARCHAR,
      "l_comment"         -> SqlTypeName.VARCHAR,
    ),
    "tpch" + SF + "_nation" -> List(
      "n_nationkey"       -> SqlTypeName.INTEGER,
      "n_name"            -> SqlTypeName.VARCHAR,
      "n_regionkey"       -> SqlTypeName.INTEGER,
      "n_comment"         -> SqlTypeName.VARCHAR,
    ),
    "tpch" + SF + "_region" -> List(
      "r_regionkey"       -> SqlTypeName.INTEGER,
      "r_name"            -> SqlTypeName.VARCHAR,
      "r_comment"         -> SqlTypeName.VARCHAR,
    )
  )

}
