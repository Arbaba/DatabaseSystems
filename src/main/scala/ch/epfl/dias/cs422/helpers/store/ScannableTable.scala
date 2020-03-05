package ch.epfl.dias.cs422.helpers.store

import java.io.FileNotFoundException
import java.nio.file.{NoSuchFileException, Path, Paths}
import java.util

import ch.epfl.dias.cs422.helpers.SqlPrepare
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import com.google.common.collect.ImmutableList
import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.schema.{Statistic, Statistics}
import org.apache.calcite.sql.`type`.SqlTypeName

import scala.io.Source
import scala.jdk.CollectionConverters._

class ScannableTable(val schema: List[(String, _ >: SqlTypeName with (SqlTypeName, Boolean))], val name: String) extends AbstractTable with org.apache.calcite.schema.ScannableTable {
  private val path: Path = try {
    Paths.get("input", this.name + ".tbl")
  } catch {
    case _: NoSuchFileException => null
  }
//  private var reader: InputTable = _

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    typeFactory.createStructType(
      schema.map(e => new util.Map.Entry[String, RelDataType] {
        override def getKey: String = e._1

        override def getValue: RelDataType = {
          typeFactory.createTypeWithNullability(typeFactory.createSqlType(
            e._2 match {
              case (t: SqlTypeName, true) => t
              case t: SqlTypeName => t
            }), e._2 match {
            case (_, true) => true
            case _ => false
          })
        }

        override def setValue(v: RelDataType): RelDataType = null
      }).asJava
    )
  }

  private lazy val stat: Statistic = {
    try {
      val f = Source.fromFile(path.toUri)
      val s = Statistics.of(f.getLines.size * 1.0, ImmutableList.of())
      f.close()
      s
    } catch {
      case _: FileNotFoundException => Statistics.UNKNOWN
    }
  }

  override def getStatistic: Statistic = stat

  def getRowCount: Long = stat.getRowCount.toLong
//
//  def open(cluster: RelOptCluster): Unit = {
//    reader = new InputTable(getRowType(, SqlPrepare.cluster.getTypeFactory), path, SqlPrepare.cluster)
//  }
//
//  def readNext(): IndexedSeq[Any] = {
//    reader.readNext()
//  }
//
//  def close(): Unit = {
//    reader.close()
//  }

  private lazy val rowStore = new RowStore(readAll().toIndexedSeq)
  private lazy val columnStore = new ColumnStore(readAll().toIndexedSeq.transpose, getRowCount)
  private lazy val paxStore = new PAXStore(readAll().toIndexedSeq.sliding(RelOperator.blockSize, RelOperator.blockSize).toIndexedSeq.map(b => b.transpose), getRowCount)

  private[helpers] def getAsRowStore: RowStore = rowStore

  private[helpers] def getAsColumnStore: ColumnStore = columnStore

  private[helpers] def getAsPAXStore: PAXStore = paxStore

  private def readAll(): List[IndexedSeq[Any]] = {
    val reader = new InputTable(getRowType(SqlPrepare.cluster.getTypeFactory), path, SqlPrepare.cluster)
    val tmp = reader.readAll()
    reader.close()
    tmp
  }

  override def scan(root: DataContext): Enumerable[Array[AnyRef]] = ???
}
