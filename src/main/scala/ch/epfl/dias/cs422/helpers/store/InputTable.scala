package ch.epfl.dias.cs422.helpers.store

import java.nio.file.{Files, Path}

import au.com.bytecode.opencsv.CSVReader
import ch.epfl.dias.cs422.helpers.rel.RelOperatorUtil
import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.rex.{RexInputRef, RexLiteral}
import org.apache.calcite.schema.impl.AbstractTable

import scala.jdk.CollectionConverters._

object InputTable{
  private var accessCnt = 0
}

class InputTable(val rowType: RelDataType, val path: Path, val cluster: RelOptCluster) extends AbstractTable with org.apache.calcite.schema.ScannableTable {
  private val reader: CSVReader = new CSVReader(Files.newBufferedReader(path), '|')

  private val types = rowType.getFieldList.asScala.toIndexedSeq.map(t => cluster.getTypeFactory.createTypeWithNullability(t.getType, false)).zipWithIndex
  private val evaluators = RelOperatorUtil.eval(cluster, types.map{e => new RexInputRef(e._2 /*.getIndex*/, e._1 /*.getType*/)},
    cluster.getTypeFactory.createStructType(
      types.map(e=>e._1).asJava, rowType.getFieldNames
    )/*rowType*/)

  private def parseLine(values: Array[String]) = {
    val ts = types
    if (values.length == types.length + 1)
      assert(values.length == types.length || (values.length == types.length + 1 && values(values.length - 1) == ""))
    val inp = ts.zip(values).map(e => try {
      RexLiteral.fromJdbcString(e._1._1, e._1._1.getSqlTypeName, e._2).getValue3
    } catch {
      case _: Throwable => e._2
    })

    evaluators(inp)
  }

  def readNext(): IndexedSeq[Any] = {
    val values = reader.readNext()
    if (values == null) return null
    parseLine(values)
  }

  def readAll(): List[IndexedSeq[Any]] = reader.readAll().asScala.toList.map(parseLine)

  def close(): Unit = {
    reader.close()
  }

  override def scan(root: DataContext): Enumerable[Array[AnyRef]] = ???

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = rowType
}
