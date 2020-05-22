/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.spec

import java.io.File
import java.io.PrintWriter

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.logical.builder.Resolver
import org.neo4j.cypher.internal.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.logical.plans.UserFunctionSignature
import org.neo4j.cypher.internal.plandescription.InternalPlanDescription
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.InputValues
import org.neo4j.cypher.internal.runtime.NoInput
import org.neo4j.cypher.internal.runtime.QueryStatistics
import org.neo4j.cypher.internal.runtime.debug.DebugLog
import org.neo4j.cypher.internal.spi.TransactionBoundPlanContext
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.api.Kernel
import org.neo4j.kernel.api.procedure.CallableProcedure
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade
import org.neo4j.kernel.impl.query.NonRecordingQuerySubscriber
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.query.RecordingQuerySubscriber
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.AssertableLogProvider
import org.neo4j.logging.LogProvider
import org.neo4j.values.AnyValue
import org.neo4j.values.AnyValues
import org.neo4j.values.virtual.ListValue
import org.scalactic.Equality
import org.scalactic.TolerantNumerics
import org.scalactic.source.Position
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Tag
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.util.Random

object RuntimeTestSuite {
  val ANY_VALUE_ORDERING: Ordering[AnyValue] = Ordering.comparatorToOrdering(AnyValues.COMPARATOR)
}

/**
 * Contains helpers, matchers and graph handling to support runtime acceptance test,
 * meaning tests where the query is
 *
 *  - specified as a logical plan
 *  - executed on a real database
 *  - evaluated by it's results
 */
abstract class RuntimeTestSuite[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                           val runtime: CypherRuntime[CONTEXT],
                                                           workloadMode: Boolean = false)
  extends CypherFunSuite
  with AstConstructionTestSupport
  with RuntimeExecutionSupport[CONTEXT]
  with GraphCreation[CONTEXT]
  with BeforeAndAfterEach
  with Resolver {

  private var managementService: DatabaseManagementService = _
  private var graphDb: GraphDatabaseService = _
  protected var runtimeTestSupport: RuntimeTestSupport[CONTEXT] = _
  private var kernel: Kernel = _
  val ANY_VALUE_ORDERING: Ordering[AnyValue] = Ordering.comparatorToOrdering(AnyValues.COMPARATOR)
  val logProvider: AssertableLogProvider = new AssertableLogProvider()

  override protected def beforeEach(): Unit = {
    DebugLog.beginTime()
    managementService = edition.newGraphManagementService()
    graphDb = managementService.database(DEFAULT_DATABASE_NAME)
    kernel = graphDb.asInstanceOf[GraphDatabaseFacade].getDependencyResolver.resolveDependency(classOf[Kernel])
    logProvider.clear()
    runtimeTestSupport = createRuntimeTestSupport(graphDb, edition, workloadMode, logProvider)
    runtimeTestSupport.start()
    runtimeTestSupport.startTx()
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    runtimeTestSupport.stopTx()
    DebugLog.log("")
    shutdownDatabase()
    super.afterEach()
  }

  protected def createRuntimeTestSupport(graphDb: GraphDatabaseService,
                                         edition: Edition[CONTEXT],
                                         workloadMode: Boolean,
                                         logProvider: LogProvider): RuntimeTestSupport[CONTEXT] = {
    new RuntimeTestSupport[CONTEXT](graphDb, edition, workloadMode, logProvider)
  }

  protected def shutdownDatabase(): Unit = {
    if (managementService != null) {
      runtimeTestSupport.stop()
      managementService.shutdown()
      managementService = null
    }
  }

  override def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit pos: Position): Unit = {
    super.test(testName, Tag(runtime.name) +: testTags: _*)(testFun)
  }

  // HELPERS

  def getConfig: Config = {
    graphDb.asInstanceOf[GraphDatabaseAPI].getDependencyResolver.resolveDependency(classOf[Config])
  }

  override def getLabelId(label: String): Int = {
    tx.kernelTransaction().tokenRead().nodeLabel(label)
  }

  override def getPropertyKeyId(prop: String): Int = {
    tx.kernelTransaction().tokenRead().propertyKey(prop)
  }

  override def procedureSignature(name: QualifiedName): ProcedureSignature = {
    val ktx = tx.kernelTransaction()
    TransactionBoundPlanContext.procedureSignature(ktx, name)
  }

  override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] = {
    val ktx = tx.kernelTransaction()
    TransactionBoundPlanContext.functionSignature(ktx, name)
  }

  def select[X](things: Seq[X],
                selectivity: Double = 1.0,
                duplicateProbability: Double = 0.0,
                nullProbability: Double = 0.0): Seq[X] = {
    val rng = new Random(42)
    for {thing <- things if rng.nextDouble() < selectivity
         dup <- if (rng.nextDouble() < duplicateProbability) Seq(thing, thing) else Seq(thing)
         nullifiedDup = if (rng.nextDouble() < nullProbability) null.asInstanceOf[X] else dup
         } yield nullifiedDup
  }

  // EXECUTE

  override def execute(logicalQuery: LogicalQuery,
                       runtime: CypherRuntime[CONTEXT],
                       input: InputDataStream,
                       subscriber: QuerySubscriber): RuntimeResult = runtimeTestSupport.execute(logicalQuery, runtime, input, subscriber)

  override def execute(logicalQuery: LogicalQuery,
                       runtime: CypherRuntime[CONTEXT],
                       inputStream: InputDataStream): RecordingRuntimeResult = runtimeTestSupport.execute(logicalQuery, runtime, inputStream)

  override def executeAndConsumeTransactionally(logicalQuery: LogicalQuery,
                                                runtime: CypherRuntime[CONTEXT],
                                                parameters: Map[String, Any] = Map.empty
                                               ): IndexedSeq[Array[AnyValue]] = runtimeTestSupport.executeAndConsumeTransactionally(logicalQuery, runtime)

  override def execute(executablePlan: ExecutionPlan): RecordingRuntimeResult = runtimeTestSupport.execute(executablePlan)

  override def buildPlan(logicalQuery: LogicalQuery,
                         runtime: CypherRuntime[CONTEXT]): ExecutionPlan = runtimeTestSupport.buildPlan(logicalQuery, runtime)

  override def buildPlanAndContext(logicalQuery: LogicalQuery,
                                   runtime: CypherRuntime[CONTEXT]): (ExecutionPlan, CONTEXT) = runtimeTestSupport.buildPlanAndContext(logicalQuery, runtime)

  override def profile(logicalQuery: LogicalQuery,
                       runtime: CypherRuntime[CONTEXT],
                       inputDataStream: InputDataStream = NoInput): RecordingRuntimeResult = runtimeTestSupport.profile(logicalQuery, runtime, inputDataStream)

  override def profileNonRecording(logicalQuery: LogicalQuery,
                                   runtime: CypherRuntime[CONTEXT],
                                   inputDataStream: InputDataStream = NoInput): NonRecordingRuntimeResult =
    runtimeTestSupport.profileNonRecording(logicalQuery, runtime, inputDataStream)

  override def executeAndContext(logicalQuery: LogicalQuery,
                                 runtime: CypherRuntime[CONTEXT],
                                 input: InputValues
                                ): (RecordingRuntimeResult, CONTEXT) = runtimeTestSupport.executeAndContext(logicalQuery, runtime, input)

  override def executeAndExplain(logicalQuery: LogicalQuery,
                                 runtime: CypherRuntime[CONTEXT],
                                 input: InputValues
                                ): (RecordingRuntimeResult, InternalPlanDescription) = runtimeTestSupport.executeAndExplain(logicalQuery, runtime, input)

  def printQueryProfile(fileName: String, maxAllocatedMemory: Long, logToStdOut: Boolean, lastAllocation: Long, stackTrace: Option[String]): Unit = {
    val pw = new PrintWriter(new File(fileName))
    val logString = new StringBuilder("Estimation of max allocated memory: ")
    logString.append(maxAllocatedMemory)
    if (lastAllocation > 0) {
      logString.append("\nLast allocation before peak reached: ")
      logString.append(lastAllocation)
      logString.append("\nEstimation of max allocated memory before peak reached: ")
      logString.append(maxAllocatedMemory - lastAllocation)
    }
    if (stackTrace.isDefined) {
      logString.append("\nStack trace of the allocation where peak was reached: ")
      logString.append(stackTrace.get)
    }
    if (logToStdOut) println(logString)
    try {
      pw.println(logString)
    } finally {
      pw.close()
    }
  }

  def printQueryProfile(fileName: String, queryProfile: QueryProfile, logToStdOut: Boolean = false, lastAllocation: Long = 0, stackTrace: Option[String] = None): Unit = {
    printQueryProfile(fileName, queryProfile.maxAllocatedMemory(), logToStdOut, lastAllocation, stackTrace)
  }

    // PROCEDURES

  def registerProcedure(proc: CallableProcedure): Unit = {
    kernel.registerProcedure(proc)
  }

  // TX

  def tx: InternalTransaction = runtimeTestSupport.tx

  /**
   * Call this to ensure that everything is commited and a new TX is opened. Be sure to not
   * use data from the previous tx afterwards. If you need to, get them again from the new
   * tx by id.
   */
  def restartTx(): Unit = runtimeTestSupport.restartTx()

  // MATCHERS

  private val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(0.0001)

  def tolerantEquals(expected: Double, x: Number): Boolean =
    doubleEquality.areEqual(expected, x.doubleValue())

  protected def beColumns(columns: String*): RuntimeResultMatcher =
    new RuntimeResultMatcher(columns)

  class RuntimeResultMatcher(expectedColumns: Seq[String]) extends Matcher[RecordingRuntimeResult] {

    private var rowsMatcher: RowsMatcher = AnyRowsMatcher
    private var maybeStatisticts: Option[QueryStatistics] = None

    def withStatistics(stats: QueryStatistics): RuntimeResultMatcher = {
      maybeStatisticts = Some(stats)
      this
    }

    def withSingleRow(values: Any*): RuntimeResultMatcher = withRows(singleRow(values: _*))

    def withRows(rows: Iterable[Array[_]], listInAnyOrder: Boolean = false): RuntimeResultMatcher = withRows(inAnyOrder(rows, listInAnyOrder))
    def withNoRows(): RuntimeResultMatcher = withRows(NoRowsMatcher)

    def withRows(rowsMatcher: RowsMatcher): RuntimeResultMatcher = {
      if (this.rowsMatcher != AnyRowsMatcher)
        throw new IllegalArgumentException("RowsMatcher already set")
      this.rowsMatcher = rowsMatcher
      this
    }

    override def apply(left: RecordingRuntimeResult): MatchResult = {
      val columns = left.runtimeResult.fieldNames().toIndexedSeq
      if (columns != expectedColumns) {
        MatchResult(matches = false, s"Expected result columns $expectedColumns, got $columns", "")
      } else if (maybeStatisticts.exists(_ != left.runtimeResult.queryStatistics())) {
        MatchResult(matches = false, s"Expected statistics ${left.runtimeResult.queryStatistics()}, got ${maybeStatisticts.get}", "")
      } else {
        val rows = consume(left)
        rowsMatcher.matches(columns, rows) match {
          case RowsMatch => MatchResult(matches = true, "", "")
          case RowsDontMatch(msg) => MatchResult(matches = false, msg, "")
        }
      }
    }
  }

  def consume(left: RecordingRuntimeResult): IndexedSeq[Array[AnyValue]] = {
    val seq = left.awaitAll()
    left.runtimeResult.close()
    seq
  }

  def consumeNonRecording(left: NonRecordingRuntimeResult): Long = {
    val count = left.awaitAll()
    left.runtimeResult.close()
    count
  }

  def inOrder(rows: Iterable[Array[_]], listInAnyOrder: Boolean = false): RowsMatcher = {
    val anyValues = rows.map(row => row.map(ValueUtils.asAnyValue)).toIndexedSeq
    EqualInOrder(anyValues, listInAnyOrder)
  }

  def inAnyOrder(rows: Iterable[Array[_]], listInAnyOrder: Boolean = false): RowsMatcher = {
    val anyValues = rows.map(row => row.map(ValueUtils.asAnyValue)).toIndexedSeq
    EqualInAnyOrder(anyValues, listInAnyOrder)
  }

  def singleColumn(values: Iterable[Any], listInAnyOrder: Boolean = false): RowsMatcher = {
    val anyValues = values.map(x => Array(ValueUtils.asAnyValue(x))).toIndexedSeq
    EqualInAnyOrder(anyValues, listInAnyOrder)
  }

  def singleColumnInOrder(values: Iterable[Any], listInAnyOrder: Boolean = false): RowsMatcher = {
    val anyValues = values.map(x => Array(ValueUtils.asAnyValue(x))).toIndexedSeq
    EqualInOrder(anyValues, listInAnyOrder)
  }

  def singleRow(values: Any*): RowsMatcher = {
    val anyValues = Array(values.toArray.map(ValueUtils.asAnyValue))
    EqualInAnyOrder(anyValues)
  }

  def rowCount(value: Int): RowsMatcher = {
    RowCount(value)
  }

  def matching(func: PartialFunction[Any, _]): RowsMatcher = {
    CustomRowsMatcher(matchPattern(func))
  }

  def groupedBy(columns: String*): RowOrderMatcher = new GroupBy(None, None, columns: _*)

  def groupedBy(nGroups: Int, groupSize: Int, columns: String*): RowOrderMatcher = new GroupBy(Some(nGroups), Some(groupSize), columns: _*)

  def sortedAsc(column: String): RowOrderMatcher = new Ascending(column)

  def sortedDesc(column: String): RowOrderMatcher = new Descending(column)

  case class DiffItem(missingRow: ListValue, fromA: Boolean)

}

case class RecordingRuntimeResult(runtimeResult: RuntimeResult, recordingQuerySubscriber: RecordingQuerySubscriber) {
  def awaitAll(): IndexedSeq[Array[AnyValue]] = {
    runtimeResult.consumeAll()
    runtimeResult.close()
    recordingQuerySubscriber.getOrThrow().asScala.toIndexedSeq
  }

  def pageCacheHits: Long = runtimeResult.asInstanceOf[ClosingRuntimeResult].pageCacheHits
  def pageCacheMisses: Long = runtimeResult.asInstanceOf[ClosingRuntimeResult].pageCacheMisses

}

case class NonRecordingRuntimeResult(runtimeResult: RuntimeResult, nonRecordingQuerySubscriber: NonRecordingQuerySubscriber) {
  def awaitAll(): Long = {
    runtimeResult.consumeAll()
    runtimeResult.close()
    nonRecordingQuerySubscriber.assertNoErrors()
    nonRecordingQuerySubscriber.recordCount()
  }

  def pageCacheHits: Long = runtimeResult.asInstanceOf[ClosingRuntimeResult].pageCacheHits
  def pageCacheMisses: Long = runtimeResult.asInstanceOf[ClosingRuntimeResult].pageCacheMisses
}

case class ContextCondition[CONTEXT <: RuntimeContext](test: CONTEXT => Boolean, errorMsg: String)
