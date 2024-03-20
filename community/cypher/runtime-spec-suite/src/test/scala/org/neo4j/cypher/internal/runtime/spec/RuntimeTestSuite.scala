/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.logical.builder.Resolver
import org.neo4j.cypher.internal.logical.plans.Prober
import org.neo4j.cypher.internal.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.logical.plans.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.logical.plans.UserFunctionSignature
import org.neo4j.cypher.internal.options.CypherDebugOptions
import org.neo4j.cypher.internal.plandescription.InternalPlanDescription
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.InputValues
import org.neo4j.cypher.internal.runtime.NoInput
import org.neo4j.cypher.internal.runtime.TestSubscriber
import org.neo4j.cypher.internal.runtime.debug.DebugLog
import org.neo4j.cypher.internal.spi.TransactionBoundPlanContext
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.kernel.api.Kernel
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.procedure.CallableProcedure
import org.neo4j.kernel.api.procedure.CallableUserAggregationFunction
import org.neo4j.kernel.api.procedure.CallableUserFunction
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade
import org.neo4j.kernel.impl.query.NonRecordingQuerySubscriber
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.query.RecordingQuerySubscriber
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.lock.LockType
import org.neo4j.lock.ResourceType
import org.neo4j.lock.ResourceTypes
import org.neo4j.logging.AssertableLogProvider
import org.neo4j.logging.LogProvider
import org.neo4j.values.AnyValue
import org.neo4j.values.AnyValues
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.virtual.ListValue
import org.scalactic.Equality
import org.scalactic.TolerantNumerics
import org.scalactic.source.Position
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Tag
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

import java.io.File
import java.io.PrintWriter
import java.time.LocalTime
import java.time.OffsetTime
import java.time.chrono.ChronoLocalDate
import java.time.chrono.ChronoLocalDateTime
import java.time.chrono.ChronoZonedDateTime
import java.time.format.DateTimeFormatter
import java.util
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.mutable.ArrayBuffer
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
  def debugOptions: CypherDebugOptions = CypherDebugOptions.default
  val isParallel: Boolean = runtime.name.toLowerCase == "parallel"
  val runOnlySafeScenarios: Boolean = !System.getenv().containsKey("RUN_EXPERIMENTAL")

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
    new RuntimeTestSupport[CONTEXT](graphDb, edition, workloadMode, logProvider, debugOptions)
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

  override def getRelTypeId(relType: String): Int = {
    tx.kernelTransaction().tokenRead().relationshipType(relType)
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
                                                parameters: Map[String, Any] = Map.empty,
                                                profileAssertion: Option[QueryProfile => Unit] = None
                                               ): IndexedSeq[Array[AnyValue]] = runtimeTestSupport.executeAndConsumeTransactionally(logicalQuery, runtime, parameters, profileAssertion)

  override def execute(executablePlan: ExecutionPlan, readOnly: Boolean, periodicCommit: Boolean): RecordingRuntimeResult =
    runtimeTestSupport.execute(executablePlan, readOnly, periodicCommit)

  override def buildPlan(logicalQuery: LogicalQuery,
                         runtime: CypherRuntime[CONTEXT]): ExecutionPlan = runtimeTestSupport.buildPlan(logicalQuery, runtime)

  override def buildPlanAndContext(logicalQuery: LogicalQuery,
                                   runtime: CypherRuntime[CONTEXT]): (ExecutionPlan, CONTEXT) = runtimeTestSupport.buildPlanAndContext(logicalQuery, runtime)

  override def profile(logicalQuery: LogicalQuery,
                       runtime: CypherRuntime[CONTEXT],
                       inputDataStream: InputDataStream = NoInput): RecordingRuntimeResult = runtimeTestSupport.profile(logicalQuery.copy(doProfile = true), runtime, inputDataStream)

  override def profileNonRecording(logicalQuery: LogicalQuery,
                                   runtime: CypherRuntime[CONTEXT],
                                   inputDataStream: InputDataStream = NoInput): NonRecordingRuntimeResult =
    runtimeTestSupport.profileNonRecording(logicalQuery.copy(doProfile = true), runtime, inputDataStream)

  override def profileWithSubscriber(logicalQuery: LogicalQuery,
                                     runtime: CypherRuntime[CONTEXT],
                                     subscriber: QuerySubscriber,
                                     inputDataStream: InputDataStream = NoInput): RuntimeResult =
    runtimeTestSupport.profileWithSubscriber(logicalQuery, runtime, subscriber, inputDataStream)

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

  def registerFunction(function: CallableUserFunction): Unit = {
    kernel.registerUserFunction(function)
  }

  def registerUserAggregation(func: CallableUserAggregationFunction): Unit = {
    kernel.registerUserAggregationFunction(func)
  }

  // TX

  def tx: InternalTransaction = runtimeTestSupport.tx

  def newTx(): InternalTransaction = runtimeTestSupport.startNewTx()

  def withNewTx(consumer: InternalTransaction => Unit): Unit = {
    // replace with scala.util.Using once we are on Scala 2.13
    val transaction = newTx()
    try {
      consumer(transaction)
    } finally {
      transaction.close()
    }
  }

  /**
   * Call this to ensure that everything is commited and a new TX is opened. Be sure to not
   * use data from the previous tx afterwards. If you need to, get them again from the new
   * tx by id.
   */
  def restartTx(txType: KernelTransaction.Type = runtimeTestSupport.getTransactionType): Unit = runtimeTestSupport.restartTx(txType)

  // MATCHERS

  private val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(0.0001)

  def tolerantEquals(expected: Double, x: Number): Boolean =
    doubleEquality.areEqual(expected, x.doubleValue())

  protected def beColumns(columns: String*): RuntimeResultMatcher =
    new RuntimeResultMatcher(columns)

  class RuntimeResultMatcher(expectedColumns: Seq[String]) extends Matcher[RecordingRuntimeResult] {

    private var rowsMatcher: RowsMatcher = AnyRowsMatcher
    private var maybeStatistics: Option[QueryStatisticsMatcher] = None
    private var maybeLockedNodes: Option[LockResourceMatcher] = None
    private var maybeLockedRelationships: Option[LockResourceMatcher] = None

    private var maybeLocks: Option[LockMatcher] = None

    def withNoUpdates(): RuntimeResultMatcher = withStatistics()

    def withStatistics(
                        nodesCreated: Int = 0,
                        nodesDeleted: Int = 0,
                        relationshipsCreated: Int = 0,
                        relationshipsDeleted: Int = 0,
                        labelsAdded: Int = 0,
                        labelsRemoved: Int = 0,
                        propertiesSet: Int = 0,
                        transactionsCommitted: Int = 1,
                      ): RuntimeResultMatcher = {
      maybeStatistics = Some(new QueryStatisticsMatcher(
        nodesCreated,
        nodesDeleted,
        relationshipsCreated,
        relationshipsDeleted,
        labelsAdded,
        labelsRemoved,
        propertiesSet,
        transactionsCommitted,
      ))
      this
    }

    def withLockedNodes(nodeIds: Set[Long]): RuntimeResultMatcher = {
      maybeLockedNodes = Some(new LockResourceMatcher(nodeIds, ResourceTypes.NODE))
      this
    }

    def withLockedRelationships(relationshipId: Set[Long]): RuntimeResultMatcher = {
      maybeLockedRelationships = Some(new LockResourceMatcher(relationshipId, ResourceTypes.RELATIONSHIP))
      this
    }

    def withLocks(locks: (LockType, ResourceType)*): RuntimeResultMatcher = {
      maybeLocks = Some(new LockMatcher(locks))
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
      } else {
        maybeStatistics
          .map(s => s.apply(left.runtimeResult.queryStatistics()))
          .filter(_.matches == false)
          .getOrElse {
            val rows = consume(left)
            maybeLocks
              .map(_.apply(()))
              .filter(_.matches == false)
              .getOrElse {
                maybeLockedNodes
                  .map(_.apply(()))
                  .filter(_.matches == false)
                  .getOrElse {
                    maybeLockedRelationships
                      .map(_.apply(()))
                      .filter(_.matches == false)
                      .getOrElse {
                        rowsMatcher.matches(columns, rows) match {
                          case RowsMatch => MatchResult(matches = true, "", "")
                          case RowsDontMatch(msg) => MatchResult(matches = false, msg, "")
                        }
                      }
                  }
              }
          }
      }
    }
  }

  /*
   * locks.accept() does not keep the order of when the locks was taken, therefore we don't assert on the order of the locks.
   */
  class LockResourceMatcher(expectedLocked: Set[Long], expectedResourceType: ResourceType) extends Matcher[Unit] {
     override def apply(left: Unit): MatchResult = {
      val locksList = new util.ArrayList[Long]
      runtimeTestSupport.locks.accept(
        (_: LockType, resourceType: ResourceType, _: Long, resourceId: Long, _: String, _: Long, _: Long) => {
          if (resourceType == expectedResourceType) locksList.add(resourceId)
        }
      )

      val actualLocked = locksList.asScala.toSet
      MatchResult(
        matches = actualLocked == expectedLocked,
        rawFailureMessage = s"expected ${expectedResourceType.name} locked=$expectedLocked but was $actualLocked",
        rawNegatedFailureMessage = ""
      )
    }
  }

  class LockMatcher(expectedLocked: Seq[(LockType, ResourceType)]) extends Matcher[Unit] {

    private val ordering = new Ordering[(LockType, ResourceType)] {
      override def compare(x: (LockType, ResourceType),
                           y: (LockType, ResourceType)): Int = {
        val (xLock, xType) = x
        val (yLock, yType) = y
        val comparison = xLock.compareTo(yLock)
        if (comparison == 0) {
          Integer.compare(xType.typeId(), yType.typeId())
        } else {
          comparison
        }
      }
    }
    override def apply(left: Unit): MatchResult = {
      val actualLockList = ArrayBuffer.empty[(LockType, ResourceType)]
      runtimeTestSupport.locks.accept(
        (lockType: LockType, resourceType: ResourceType, _: Long, _: Long, _: String, _: Long, _: Long) =>

          actualLockList.append((lockType, resourceType))
      )

      MatchResult(
        matches = expectedLocked.sorted(ordering) == actualLockList.sorted(ordering),
        rawFailureMessage = s"expected locks ${expectedLocked.mkString(", ")} but got ${actualLockList.mkString(", ")}",
        rawNegatedFailureMessage = ""
      )
    }
  }

  class QueryStatisticsMatcher(
                                nodesCreated: Int,
                                nodesDeleted: Int,
                                relationshipsCreated: Int,
                                relationshipsDeleted: Int,
                                labelsAdded: Int,
                                labelsRemoved: Int,
                                propertiesSet: Int,
                                transactionsCommitted: Int,
                              ) extends Matcher[QueryStatistics] {

    override def apply(left: QueryStatistics): MatchResult = {
      def transactionsCommittedDoesNotMatch: Option[MatchResult] = {
        left match {
          case qs: org.neo4j.cypher.internal.runtime.QueryStatistics =>
            // FIXME: we currently do not account for the outermost transaction because that is out of cypher's control
            if (transactionsCommitted - 1 != qs.transactionsCommitted) {
              Some(MatchResult(matches = false, s"expected transactionsCommitted=$transactionsCommitted but was ${qs.transactionsCommitted + 1}", ""))
            } else {
              None
            }
          case _ =>
            if (transactionsCommitted != 1) {
              Some(MatchResult(matches = false, s"expected transactionsCommitted=$transactionsCommitted but can only match on org.neo4j.cypher.internal.runtime.QueryStatistics and was $left", ""))
            } else {
              None
            }
        }
      }

      if (nodesCreated != left.getNodesCreated) {
        MatchResult(matches = false, s"expected nodesCreated=$nodesCreated but was ${left.getNodesCreated}", "")
      } else if (nodesDeleted != left.getNodesDeleted) {
        MatchResult(matches = false, s"expected nodesDeleted=$nodesDeleted but was ${left.getNodesDeleted}", "")
      } else if (relationshipsCreated != left.getRelationshipsCreated) {
        MatchResult(matches = false, s"expected relationshipCreated=$relationshipsCreated but was ${left.getRelationshipsCreated}", "")
      } else if (relationshipsDeleted != left.getRelationshipsDeleted) {
        MatchResult(matches = false, s"expected relationshipsDeleted=$relationshipsDeleted but was ${left.getRelationshipsDeleted}", "")
      } else if (labelsAdded != left.getLabelsAdded) {
        MatchResult(matches = false, s"expected labelsAdded=$labelsAdded but was ${left.getLabelsAdded}", "")
      } else if (labelsRemoved != left.getLabelsRemoved) {
        MatchResult(matches = false, s"expected labelsRemoved=$labelsRemoved but was ${left.getLabelsRemoved}", "")
      } else if (propertiesSet != left.getPropertiesSet) {
        MatchResult(matches = false, s"expected propertiesSet=$propertiesSet but was ${left.getPropertiesSet}", "")
      } else if (transactionsCommittedDoesNotMatch.nonEmpty)  {
        transactionsCommittedDoesNotMatch.get
      } else {
        MatchResult(matches = true, "", "")
      }
    }
  }

  def consume(left: RecordingRuntimeResult): IndexedSeq[Array[AnyValue]] = {
    val seq = left.awaitAll()
    left.runtimeResult.close()
    seq
  }

  def rollbackAndRestartTx(txType: KernelTransaction.Type): Unit =
    runtimeTestSupport.rollbackAndRestartTx(txType)

  def request(numberOfRows: Long, left: RecordingRuntimeResult): Unit = {
    left.runtimeResult.request(numberOfRows)
    left.runtimeResult.await()
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

  def disallowValues(columnValuePredicates: Seq[(String, AnyValue => Boolean)]): RowsMatcher = {
    DisallowValues(columnValuePredicates)
  }

  def matching(func: PartialFunction[Any, _]): RowsMatcher = {
    CustomRowsMatcher(matchPattern(func))
  }

  def groupedBy(columns: String*): RowOrderMatcher = new GroupBy(None, None, columns: _*)

  def groupedBy(nGroups: Int, groupSize: Int, columns: String*): RowOrderMatcher = new GroupBy(Some(nGroups), Some(groupSize), columns: _*)

  def sortedAsc(column: String): RowOrderMatcher = new Ascending(column)

  def sortedDesc(column: String): RowOrderMatcher = new Descending(column)

  case class DiffItem(missingRow: ListValue, fromA: Boolean)

  def failProbe(failAfterRowCount: Int): Prober.Probe = new Prober.Probe {
    val c = new AtomicInteger(0)
    override def onRow(row: AnyRef): Unit = {
      if ( c.incrementAndGet() == failAfterRowCount ) {
        throw new RuntimeException(s"Probe failed as expected (row count=$c)")
      }
    }
  }

  def toExpression(value: Any): Expression = {
    def resolve(function: FunctionInvocation): Expression = {
      if (function.needsToBeResolved) ResolvedFunctionInvocation(functionSignature)(function).coerceArguments else function
    }
    value match {
      case date: ChronoLocalDate => resolve(function("date", literalString(date.format(DateTimeFormatter.ISO_LOCAL_DATE))))
      case dateTime: ChronoLocalDateTime[_] => resolve(function("localdatetime", literalString(dateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))))
      case time: LocalTime => resolve(function("localtime", literalString(time.format(DateTimeFormatter.ISO_LOCAL_TIME))))
      case duration: DurationValue => resolve(function("duration", literalString(duration.prettyPrint())))
      case offsetTime: OffsetTime => resolve(function("time", literalString(offsetTime.format(DateTimeFormatter.ISO_OFFSET_TIME))))
      case dateTime: ChronoZonedDateTime[_] => resolve(function("datetime", literalString(dateTime.format(DateTimeFormatter.ISO_ZONED_DATE_TIME))))
      case other => literal(other)
    }
  }
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

object RecordingRuntimeResult {
  def apply(runtimeResult: RuntimeResult, testSubscriber: TestSubscriber): RecordingRuntimeResult =
    RecordingRuntimeResult(runtimeResult, TestSubscriberWrappingRecordingQuerySubscriber(testSubscriber))
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

case class TestSubscriberRuntimeResult(runtimeResult: RuntimeResult, testSubscriber: TestSubscriber) {
  def awaitAll(): IndexedSeq[Array[AnyValue]] = {
    runtimeResult.consumeAll()
    runtimeResult.close()
    testSubscriber.allSeen.map(_.toArray).toArray.asInstanceOf[IndexedSeq[Array[AnyValue]]]
  }

  def pageCacheHits: Long = runtimeResult.asInstanceOf[ClosingRuntimeResult].pageCacheHits
  def pageCacheMisses: Long = runtimeResult.asInstanceOf[ClosingRuntimeResult].pageCacheMisses
}

case class ContextCondition[CONTEXT <: RuntimeContext](test: CONTEXT => Boolean, errorMsg: String)

case class TestSubscriberWrappingRecordingQuerySubscriber(testSubscriber: TestSubscriber) extends RecordingQuerySubscriber {
  override def getOrThrow: util.List[Array[AnyValue]] = {
    assertNoErrors()
    util.Arrays.asList[Array[AnyValue]](testSubscriber.allSeen.map(_.toArray): _*)
  }

  override def assertNoErrors(): Unit = {}

  override def queryStatistics: QueryStatistics = {
    testSubscriber.queryStatistics
  }
}
