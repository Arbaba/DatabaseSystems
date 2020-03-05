package ch.epfl.dias.cs422

import java.io.IOException
import java.lang.management.ManagementFactory
import java.nio.file.{Files, Path, Paths}
import java.sql.SQLException
import java.time.Duration.ofSeconds
import java.util.function.BiConsumer
import java.util.stream.Stream

import ch.epfl.dias.cs422.helpers.SqlPrepare
import ch.epfl.dias.cs422.helpers.builder.Factories
import ch.epfl.dias.cs422.helpers.store.InputTable
import org.apache.calcite.runtime.SqlFunctions
import org.apache.calcite.sql.`type`.SqlTypeName
import org.junit.jupiter.api.Assertions.assertTimeoutPreemptively
import org.junit.jupiter.api.DynamicContainer.dynamicContainer
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api._
import org.junit.jupiter.api.function.Executable
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import scala.util.control.Breaks._

object QueryTest {
  private val logger = LoggerFactory.getLogger(classOf[QueryTest])
  private val isDebug = ManagementFactory.getRuntimeMXBean.getInputArguments.toString.indexOf("jdwp") >= 0;

  @BeforeAll
  @throws[SQLException]
  def init() = { // warm-up connection and load classes
  }

  @AfterAll
  @throws[SQLException]
  def deinit() = { // Print timing results
  }

  /**
   * Helper function to create a list of tests based on a folder structure
   *
   * @param path path to a folder, root of the tree structure under consideration
   * @param test function to execute per sql query
   * @return a stream of DynamicTests
   * @throws IOException for exception relating to traversing the root of the tree
   */
  @throws[IOException]
  private def testsFromFileTree(path: Path, test: BiConsumer[String, Path]): Stream[DynamicNode] = {
    Stream
      .concat(
        Files
          .list(path)
          // we want to control the order of traversal, otherwise we would use the Files.walk function
          .filter(Files.isRegularFile(_))
          .filter(_.getFileName.toString.endsWith(".sql"))
          .sorted // sorted in order to guarantee the order between different invocations
          .map(file => {
            try { // find file containing the verification set: same path/filename but extended with .resultset
              val rFile = Paths.get(file.toString + ".resultset")
              val resultFile: Path = if (Files.exists(rFile) && Files.isRegularFile(rFile)) rFile else null

              // clean the sql command from comments and final ';'
              var sql = new String(Files.readAllBytes(file))
              sql = sql.replaceAll("--[^\n]*", "").trim
              assert(sql.lastIndexOf(';') == sql.length - 1)
              val q = sql.substring(0, sql.length - 1)
              // create the test
              dynamicTest(file.getFileName.toString, () => {
                assertTimeoutPreemptively(ofSeconds(if (isDebug) Long.MaxValue / 1000 else 60), new Executable {
                  override def execute(): Unit = test.accept(q, resultFile)
                })
              }).asInstanceOf[DynamicNode]
            } catch {
              case e: IOException =>
                logger.warn(e.getMessage)
                null
            }
          }),
        Files
          .list(path)
          .filter(x => !(x.getFileName.toString == "current"))
          .filter(Files.isDirectory(_))
          .sorted
          .map(file => {
            try dynamicContainer(file.getFileName.toString, testsFromFileTree(file, test)).asInstanceOf[DynamicNode]
            catch {
              case e: IOException =>
                logger.warn(e.getMessage)
                null
            }
          })
      )
      .filter((x: DynamicNode) => x != null)
  }
}

class QueryTest {
  @throws[SQLException]
  @throws[IOException]
  def testQueryOrdered(sql: String, resultFile: Path, prep: SqlPrepare[_]): Unit = {
    val (q, t) = prep.runQuery(sql)
    val validTable = new InputTable(t, resultFile, prep.cluster)
    var validationSet = List[List[Any]]()
    breakable {
      while (true) {
        val next = validTable.readNext()
        if (next == null) break
        validationSet = validationSet :+ next.toList
      }
    }
    Assertions.assertEquals(validationSet.size, q.size, "Result set has wrong size")
    validationSet.zip(q).foreach(
      e => {
        t.getFieldList.asScala.zip(e._1).zip(e._2).foreach {
          case ((tp, expected), actual) =>
            tp.getType.getSqlTypeName match {
              case SqlTypeName.DECIMAL | SqlTypeName.INTEGER | SqlTypeName.BIGINT =>
                val relError = try {
                  SqlFunctions.divideAny(SqlFunctions.minusAny(actual, expected), expected)
                } catch {
                  case _: ArithmeticException => expected
                }
                Assertions.assertTrue(SqlFunctions.geAny(relError, -0.001), "Inaccurate result: " + actual + " vs " + expected)
                Assertions.assertTrue(SqlFunctions.leAny(relError, +0.001), "Inaccurate result: " + actual + " vs " + expected)
              case _ => Assertions.assertEquals(expected, actual)
            }
        }
      }
    )
  }

  @TestFactory
  @throws[IOException]
  private[cs422] def tests = {
    List(
      "volcano (row store)" -> SqlPrepare(Factories.VOLCANO_INSTANCE, "rowstore"),
      "operator-at-a-time (row store)" -> SqlPrepare(Factories.OPERATOR_AT_A_TIME_INSTANCE, "rowstore"),
      "block-at-a-time (row store)" -> SqlPrepare(Factories.BLOCKATATIME_INSTANCE, "rowstore"),
      "late-operator-at-a-time (row store)" -> SqlPrepare(Factories.LAZY_OPERATOR_AT_A_TIME_INSTANCE, "rowstore"),
      "volcano (column store)" -> SqlPrepare(Factories.VOLCANO_INSTANCE, "columnstore"),
      "operator-at-a-time (column store)" -> SqlPrepare(Factories.OPERATOR_AT_A_TIME_INSTANCE, "columnstore"),
      "block-at-a-time (column store)" -> SqlPrepare(Factories.BLOCKATATIME_INSTANCE, "columnstore"),
      "late-operator-at-a-time (column store)" -> SqlPrepare(Factories.LAZY_OPERATOR_AT_A_TIME_INSTANCE, "columnstore"),
      "volcano (pax store)" -> SqlPrepare(Factories.VOLCANO_INSTANCE, "paxstore"),
      "operator-at-a-time (pax store)" -> SqlPrepare(Factories.OPERATOR_AT_A_TIME_INSTANCE, "paxstore"),
      "block-at-a-time (pax store)" -> SqlPrepare(Factories.BLOCKATATIME_INSTANCE, "paxstore"),
      "late-operator-at-a-time (pax store)" -> SqlPrepare(Factories.LAZY_OPERATOR_AT_A_TIME_INSTANCE, "paxstore"),
    ).map { case (key, prep) =>
      dynamicContainer(key,
        QueryTest
          .testsFromFileTree(
            Paths.get(classOf[QueryTest].getResource("/tests").getPath),
            (sql: String, resultFile: Path) => {
              if (QueryTest.isDebug) {
                testQueryOrdered(sql, resultFile, prep)
              } else {
                Assertions.assertDoesNotThrow(new Executable {
                  override def execute(): Unit = testQueryOrdered(sql, resultFile, prep)
                })
              }
            }
          )
      ).asInstanceOf[DynamicNode]
    }.asJava
  }
}