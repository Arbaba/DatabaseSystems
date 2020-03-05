package ch.epfl.dias.cs422.helpers.rex

import java.lang.reflect.{Modifier, Type}
import java.util

import com.google.common.collect.ImmutableList
import org.apache.calcite.DataContext
import org.apache.calcite.adapter.enumerable.RexToLixTranslator
import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.config.CalciteSystemProperty
import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.linq4j.tree.{BlockBuilder, Expression, Expressions}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex._
import org.apache.calcite.sql.validate.SqlConformanceEnum
import org.apache.calcite.util.{BuiltInMethod, Util}

import scala.jdk.CollectionConverters._



class RexExecutor extends RexExecutorImpl(new TupleContext){
  private def compile(rexBuilder: RexBuilder, constExps: util.List[RexNode], getter: RexToLixTranslator.InputGetter, rowType: RelDataType): String = {
    val programBuilder = new RexProgramBuilder(rowType, rexBuilder)
    for (node <- constExps.asScala) {
      programBuilder.addProject(node, "c" + programBuilder.getProjectList.size)
    }
    val javaTypeFactory = new JavaTypeFactoryImpl(rexBuilder.getTypeFactory.getTypeSystem)
    val blockBuilder = new BlockBuilder
    val root0_ = Expressions.parameter(classOf[Any], "root0")
    val root_ = DataContext.ROOT
    blockBuilder.add(Expressions.declare(Modifier.FINAL, root_, Expressions.convert_(root0_, classOf[DataContext])))
    val conformance = SqlConformanceEnum.DEFAULT
    val program = programBuilder.getProgram
    val expressions = RexToLixTranslator.translateProjects(program, javaTypeFactory, conformance, blockBuilder, null, root_, getter, null)
    blockBuilder.add(Expressions.return_(null, Expressions.newArrayInit(classOf[Array[AnyRef]], expressions)))
    val methodDecl = Expressions.methodDecl(Modifier.PUBLIC, classOf[Array[AnyRef]], BuiltInMethod.FUNCTION1_APPLY.method.getName, ImmutableList.of(root0_), blockBuilder.toBlock)
    val code = Expressions.toString(methodDecl)
    if (CalciteSystemProperty.DEBUG.value) Util.debugCode(System.out, code)
    code
  }

  override def getExecutable(rexBuilder: RexBuilder, exps: util.List[RexNode], rowType: RelDataType): RexExecutable = {
    getExecutable(rexBuilder, exps, rowType, (rowType: RelDataType, typeFactory: JavaTypeFactory) => new DataContextInputGetter(rowType, typeFactory))
  }

  def getExecutable(rexBuilder: RexBuilder, exps: util.List[RexNode], rowType: RelDataType, getter: (RelDataType, JavaTypeFactory) => RexToLixTranslator.InputGetter): RexExecutable = {
    val typeFactory = new JavaTypeFactoryImpl(rexBuilder.getTypeFactory.getTypeSystem)
    val code = compile(rexBuilder, exps, getter(rowType, typeFactory), rowType)
//    println(code)
    new RexExecutable(code, "generated Rex code")
  }

  private class DataContextInputGetter(rowType: RelDataType, typeFactory: JavaTypeFactory) extends RexToLixTranslator.InputGetter{
    override def field(list: BlockBuilder, index: Int, storageType: Type): Expression = {
//      val expr = DataContext.ROOT.asInstanceOf[TupleContext].fieldAsExpression(index)
//      if (expr!= null) return expr

      val method = classOf[TupleContext].getMethod("field", classOf[Int])
      val context = RexToLixTranslator.convert(DataContext.ROOT, classOf[TupleContext])
      val recFromCtx = Expressions.call(context, method, Expressions.constant(index))
      val recordAccess = recFromCtx
      var sType = storageType
      if (sType == null) {
        val fieldType = rowType.getFieldList.get(index).getType
        sType = typeFactory.getJavaClass(fieldType)
      }
      RexToLixTranslator.convert(recordAccess, sType)
    }
  }

}
