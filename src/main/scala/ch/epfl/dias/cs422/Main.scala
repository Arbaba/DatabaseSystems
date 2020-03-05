package ch.epfl.dias.cs422

import ch.epfl.dias.cs422.helpers.builder.Factories
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import ch.epfl.dias.cs422.helpers.{PrintUtil, SqlPrepare}

object Main {
  def main(args: Array[String]): Unit = {
    val sql = """
select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    tpch0_001_lineitem
where
    l_shipdate <= date '1998-12-01' - interval '90' day
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus
      """

    val prep = SqlPrepare(Factories.VOLCANO_INSTANCE, "rowstore")
    val rel = prep.prepare(sql)

    PrintUtil.printTree(rel)

    for (i <- 1 to 10) {
      println("Iteration " + i + " :")
      rel.asInstanceOf[Operator].foreach(println)
//      // equivalent:
//      rel.open()
//      breakable {
//        while (true) {
//          val n = rel.next()
//          if (n == Nil) break // It's not safe to call next again after it returns Nil
//          println(n)
//        }
//      }
//      rel.close()
    }
  }
}