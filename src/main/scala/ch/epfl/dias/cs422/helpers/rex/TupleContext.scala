package ch.epfl.dias.cs422.helpers.rex

import org.apache.calcite.DataContext
import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.linq4j.QueryProvider
import org.apache.calcite.schema.SchemaPlus

class TupleContext extends DataContext {
  override def getRootSchema: SchemaPlus = ???

  override def getTypeFactory: JavaTypeFactory = ???

  override def getQueryProvider: QueryProvider = ???

  override def get(name: String): AnyRef = ???

  def field(index: Int): Any = ???
}
