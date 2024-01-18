/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.cypher_worker_limit
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.InterpretedRuntimeName
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.ParallelRuntimeName
import org.neo4j.cypher.internal.PipelinedRuntimeName
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.SlottedRuntimeName
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.frontend.phases.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.logical.plans.Prober
import org.neo4j.cypher.internal.options.CypherDebugOptions
import org.neo4j.cypher.internal.runtime.InputValues
import org.neo4j.cypher.internal.runtime.TestSubscriber
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.spec.execution.RuntimeTestSupportExecution
import org.neo4j.cypher.internal.runtime.spec.matcher.RuntimeResultMatchers
import org.neo4j.cypher.internal.runtime.spec.resolver.RuntimeTestResolver
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanCombinationRewriter.TestPlanCombinationRewriterHint
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.graphdb.config.Setting
import org.neo4j.io.fs.EphemeralFileSystemAbstraction
import org.neo4j.kernel.api.Kernel
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.procedure.CallableProcedure
import org.neo4j.kernel.api.procedure.CallableUserAggregationFunction
import org.neo4j.kernel.api.procedure.CallableUserFunction
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade
import org.neo4j.kernel.impl.query.NonRecordingQuerySubscriber
import org.neo4j.kernel.impl.query.RecordingQuerySubscriber
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.AssertableLogProvider
import org.neo4j.logging.InternalLogProvider
import org.neo4j.values.AnyValue
import org.neo4j.values.AnyValues
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.storable.PointValue
import org.neo4j.values.storable.Value
import org.scalactic.source.Position
import org.scalatest.Args
import org.scalatest.Assertion
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Status
import org.scalatest.SucceededStatus
import org.scalatest.Tag

import java.time.LocalTime
import java.time.OffsetTime
import java.time.chrono.ChronoLocalDate
import java.time.chrono.ChronoLocalDateTime
import java.time.chrono.ChronoZonedDateTime
import java.time.format.DateTimeFormatter
import java.util
import java.util.Locale

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Random
import scala.util.Using

object RuntimeTestSuite {
  val ANY_VALUE_ORDERING: Ordering[AnyValue] = Ordering.comparatorToOrdering(AnyValues.COMPARATOR)
  def isParallel(runtime: CypherRuntime[_]): Boolean = runtime.name.toLowerCase(Locale.ROOT) == "parallel"
}

/**
 * Contains helpers, matchers and graph handling to support runtime acceptance test,
 * meaning tests where the query is
 *
 *  - specified as a logical plan
 *  - executed on a real database
 *  - evaluated by it's results
 */
abstract class BaseRuntimeTestSuite[CONTEXT <: RuntimeContext](
  baseEdition: Edition[CONTEXT],
  val runtime: CypherRuntime[CONTEXT],
  workloadMode: Boolean = false,
  testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint]
) extends CypherFunSuite
    with AstConstructionTestSupport
    with RuntimeTestSupportExecution[CONTEXT]
    with GraphCreation[CONTEXT]
    with BeforeAndAfterEach
    with RuntimeResultMatchers[CONTEXT]
    with RuntimeTestResolver[CONTEXT] {

  protected var managementService: DatabaseManagementService = _
  protected var dbmsFileSystem: EphemeralFileSystemAbstraction = _
  protected var graphDb: GraphDatabaseService = _
  protected var systemDb: GraphDatabaseService = _
  protected var runtimeTestSupport: RuntimeTestSupport[CONTEXT] = _
  protected var kernel: Kernel = _
  val ANY_VALUE_ORDERING: Ordering[AnyValue] = Ordering.comparatorToOrdering(AnyValues.COMPARATOR)
  var logProvider: AssertableLogProvider = _
  def debugOptions: CypherDebugOptions = CypherDebugOptions.default
  val isParallel: Boolean = RuntimeTestSuite.isParallel(runtime)

  def canFuse: Boolean = {
    val runtimeUsed = runtime.name.toLowerCase(Locale.ROOT)
    val fuseablePipeline = runtimeUsed == "pipelined" || runtimeUsed == "parallel"

    fuseablePipeline && !edition
      .getSetting(GraphDatabaseInternalSettings.cypher_operator_engine)
      .contains(GraphDatabaseInternalSettings.CypherOperatorEngine.INTERPRETED)
  }

  def canFuseOverPipelines: Boolean = canFuse && !isParallel

  val runOnlySafeScenarios: Boolean = !System.getenv().containsKey("RUN_EXPERIMENTAL")

  protected var edition: Edition[CONTEXT] = baseEdition

  def isParallelWithOneWorker: Boolean = if (isParallel) {
    edition.getSetting(cypher_worker_limit) match {
      case Some(workerLimit) if workerLimit == 1 => true
      case _                                     => false
    }
  } else
    false

  def setAdditionalConfigs(configs: Array[(Setting[_], Object)]): Unit = {
    require(managementService == null)
    require(graphDb == null)
    require(systemDb == null)
    require(runtimeTestSupport == null)
    require(kernel == null)
    edition = edition.copyWith(configs: _*)
  }

  private[this] var runtimeTestParameters: RuntimeTestParameters = _
  private[this] var includeOnlyTestNames: Set[String] = _
  private[this] var customSuiteName: Option[String] = None
  private[this] var customSuiteId: Option[String] = None

  def setRuntimeTestParameters(params: RuntimeTestParameters): Unit = {
    require(runtimeTestSupport == null) // We expect this to be called before we construct runtimeTestSupport
    runtimeTestParameters = params
  }

  def setIncludeOnlyTestNames(includeTestNames: Set[String]): Unit = {
    if (includeTestNames.nonEmpty) {
      includeOnlyTestNames = includeTestNames
    }
  }

  protected def restartDB(): Unit = {
    logProvider = new AssertableLogProvider()
    val dbms = edition.newGraphManagementService(logProvider)
    managementService = dbms.dbms
    dbmsFileSystem = dbms.filesystem
    graphDb = managementService.database(DEFAULT_DATABASE_NAME)
    systemDb = managementService.database(SYSTEM_DATABASE_NAME)
    kernel = graphDb.asInstanceOf[GraphDatabaseFacade].getDependencyResolver.resolveDependency(classOf[Kernel])
  }

  protected def createRuntimeTestSupport(): Unit = {
    logProvider.clear()
    runtimeTestSupport = createRuntimeTestSupport(graphDb, edition, runtime, workloadMode, logProvider)
    if (runtimeTestParameters != null) {
      runtimeTestSupport.setRuntimeTestParameters(augmentedRuntimeTestParameters, isParallel)
    }
    runtimeTestSupport.start()
    runtimeTestSupport.startTx()
  }

  private def augmentedRuntimeTestParameters: RuntimeTestParameters = {
    val augmented =
      if (
        runtimeTestParameters != null && runtimeTestParameters.planCombinationRewriter.isDefined && testPlanCombinationRewriterHints.nonEmpty
      ) {
        runtimeTestParameters.copy(planCombinationRewriter =
          Some(runtimeTestParameters.planCombinationRewriter.get.copy(hints =
            runtimeTestParameters.planCombinationRewriter.get.hints.union(testPlanCombinationRewriterHints)
          ))
        )
      } else {
        runtimeTestParameters
      }
    augmented
  }

  protected def createRuntimeTestSupport(
    graphDb: GraphDatabaseService,
    edition: Edition[CONTEXT],
    runtime: CypherRuntime[CONTEXT],
    workloadMode: Boolean,
    logProvider: InternalLogProvider
  ): RuntimeTestSupport[CONTEXT] = {
    new RuntimeTestSupport[CONTEXT](graphDb, edition, runtime, workloadMode, logProvider, debugOptions)
  }

  protected def shutdownDatabase(): Unit = {
    try {
      if (managementService != null) {
        runtimeTestSupport.stop()
        managementService.shutdown()
      }
    } finally {
      managementService = null
      dbmsFileSystem = null
      runtimeTestSupport = null
      kernel = null
      graphDb = null
      systemDb = null
      // NOTE: AssertFusingSucceeded relies on logProvider not being null, so delay setting it to null
      //       in case a test case explicitly calls shutdownDatabase() (looking at you, SchedulerTracerTestBase...)
      if (logProvider != null) {
        logProvider.clear()
      }
    }
  }

  override def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit pos: Position): Unit = {
    super.test(testName, Tag(runtime.name) +: testTags: _*)({
      testFun
      // Close the transaction here so that any errors resulting from that will be visible as test failures
      if (runtimeTestSupport != null) {
        runtimeTestSupport.stopTx()
      }
    })
  }

  override protected def runTest(testName: String, args: Args): Status = {
    if (includeOnlyTestNames == null || includeOnlyTestNames.contains(testName)) {
      super.runTest(testName, args)
    } else {
      SucceededStatus // Maybe not optimal. If we could filter before run that would be better.
    }
  }

  // HELPERS

  def getConfig: Config = {
    graphDb.asInstanceOf[GraphDatabaseAPI].getDependencyResolver.resolveDependency(classOf[Config])
  }

  def select[X](
    things: Seq[X],
    selectivity: Double = 1.0,
    duplicateProbability: Double = 0.0,
    nullProbability: Double = 0.0
  ): Seq[X] = {
    val rng = new Random(42)
    for {
      thing <- things if rng.nextDouble() < selectivity
      dup <- if (rng.nextDouble() < duplicateProbability) Seq(thing, thing) else Seq(thing)
      nullifiedDup = if (rng.nextDouble() < nullProbability) null.asInstanceOf[X] else dup
    } yield nullifiedDup
  }

  // A little helper to collect dependencies on runtime name in one place
  sealed trait Runtime
  case object Interpreted extends Runtime
  case object Slotted extends Runtime
  case object Pipelined extends Runtime
  case object Parallel extends Runtime

  protected def runtimeUsed: Runtime = {
    runtime.name.toUpperCase match {
      case InterpretedRuntimeName.name => Interpreted
      case SlottedRuntimeName.name     => Slotted
      case PipelinedRuntimeName.name   => Pipelined
      case ParallelRuntimeName.name    => Parallel
    }
  }

  def countRows(logicalQuery: LogicalQuery, runtime: CypherRuntime[CONTEXT], input: InputValues = NO_INPUT): Long = {
    val (result, _) = runtimeTestSupport.executeAndContextNonRecording(logicalQuery, runtime, input)
    result.awaitAll()
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
   * Call this to ensure that everything is committed and a new TX is opened. Be sure to not
   * use data from the previous tx afterwards. If you need to, get them again from the new
   * tx by id.
   */
  def restartTx(txType: KernelTransaction.Type = runtimeTestSupport.defaultTransactionType): Unit =
    runtimeTestSupport.restartTx(txType)

  def consume(left: RecordingRuntimeResult): IndexedSeq[Array[AnyValue]] = left.awaitAll()

  def request(numberOfRows: Long, left: RecordingRuntimeResult): Unit = {
    left.runtimeResult.request(numberOfRows)
    left.runtimeResult.await()
  }

  def toExpression(value: Any): Expression = {
    def resolve(function: FunctionInvocation): Expression = {
      if (function.needsToBeResolved) ResolvedFunctionInvocation(functionSignature)(function).coerceArguments
      else function
    }
    val valueToConvert = value match {
      case neo4jValue: Value => neo4jValue.asObject()
      case other             => other
    }
    valueToConvert match {
      case date: ChronoLocalDate =>
        resolve(function("date", literalString(date.format(DateTimeFormatter.ISO_LOCAL_DATE))))
      case dateTime: ChronoLocalDateTime[_] =>
        resolve(function("localdatetime", literalString(dateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))))
      case time: LocalTime =>
        resolve(function("localtime", literalString(time.format(DateTimeFormatter.ISO_LOCAL_TIME))))
      case duration: DurationValue => resolve(function("duration", literalString(duration.prettyPrint())))
      case offsetTime: OffsetTime =>
        resolve(function("time", literalString(offsetTime.format(DateTimeFormatter.ISO_OFFSET_TIME))))
      case dateTime: ChronoZonedDateTime[_] =>
        resolve(function("datetime", literalString(dateTime.format(DateTimeFormatter.ISO_ZONED_DATE_TIME))))
      case point: PointValue =>
        val coordExpr = point.coordinate().toSeq.zip(Seq("x", "y", "z"))
          .map { case (value, key) => key -> literal(value) }
        val crsExpr = "crs" -> literal(point.getCRS.getType)
        resolve(function("point", mapOf((coordExpr :+ crsExpr): _*)))
      case array: Array[_] => listOf(array.map(toExpression): _*)
      case other           => literal(other)
    }
  }

  def runtimeTestUtils: RuntimeTestUtils = edition.runtimeTestUtils

  protected def queryStatisticsProbe(assertion: QueryStatistics => Assertion): QueryStatisticsProbe = {
    QueryStatisticsProbe(assertion, runtimeTestUtils)
  }

  protected def recordingProbe(variablesToRecord: String*): Prober.Probe with RecordingRowsProbe = {
    if (isParallel)
      ThreadSafeRecordingProbe(variablesToRecord: _*)
    else
      RecordingProbe(variablesToRecord: _*)
  }

  /** Hack to make TC report test results correctly for certain nested suites */
  def setSuiteNamePrefix(prefix: String): Unit = {
    customSuiteName = Some(s"$prefix.${super.suiteName}")
    customSuiteId = Some(s"$prefix.${super.suiteId}")
  }

  override def suiteId: String = customSuiteId.getOrElse(super.suiteId)
  override def suiteName: String = customSuiteName.getOrElse(super.suiteName)
}

/**
 * Used for the general case where you want to clear the entire database before each test.
 */
abstract class RuntimeTestSuite[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  workloadMode: Boolean = false,
  testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint] = Set.empty[TestPlanCombinationRewriterHint]
) extends BaseRuntimeTestSuite[CONTEXT](edition, runtime, workloadMode, testPlanCombinationRewriterHints) {

  override protected def beforeEach(): Unit = {
    DebugSupport.TIMELINE.beginTime()
    restartDB()
    createRuntimeTestSupport()
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    try {
      if (runtimeTestSupport != null) {
        runtimeTestSupport.stopTx()
      }
      DebugSupport.TIMELINE.log("")
    } finally {
      try {
        shutdownDatabase()
      } finally {
        logProvider = null
      }
      super.afterEach()
    }
  }
}

/**
 * Used for the case when you want create the database once and run multiple test
 * against that database. Useful for when a bigger database is required.
 *
 * NOTE: The database is not cleared between tests so if any tests in the extending class are
 * doing updates these will be visible to other tests, use with caution.
 */
abstract class StaticGraphRuntimeTestSuite[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  workloadMode: Boolean = false,
  testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint] = Set.empty[TestPlanCombinationRewriterHint]
) extends BaseRuntimeTestSuite[CONTEXT](edition, runtime, workloadMode, testPlanCombinationRewriterHints)
    with BeforeAndAfterAll {

  def shouldSetup: Boolean

  override protected def beforeEach(): Unit = {
    if (shouldSetup) {
      DebugSupport.TIMELINE.beginTime()
      createRuntimeTestSupport()
    }
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    if (shouldSetup) {
      runtimeTestSupport.stopTx()
      DebugSupport.TIMELINE.log("")
    }
    super.afterEach()
  }

  override protected def beforeAll(): Unit = {
    if (shouldSetup) {
      restartDB()
      createRuntimeTestSupport()
      createGraph()
    }
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    if (shouldSetup) {
      shutdownDatabase()
    }
    super.afterAll()
  }

  /**
   * Creates a graph that are used by all tests in the class
   */
  protected def createGraph(): Unit
}

trait RuntimeTestResult {
  def runtimeResult: RuntimeResult
  def resultConsumptionController: RuntimeTestResultConsumptionController
  def pageCacheHits: Long = runtimeResult.asInstanceOf[ClosingRuntimeTestResult].pageCacheHits
  def pageCacheMisses: Long = runtimeResult.asInstanceOf[ClosingRuntimeTestResult].pageCacheMisses
}

trait RuntimeTestResultConsumptionController {
  def consume(runtimeResult: RuntimeResult)
}

case object ConsumeAllThenCloseResultConsumer extends RuntimeTestResultConsumptionController {

  override def consume(runtimeResult: RuntimeResult): Unit = {
    Using.resource(runtimeResult)(r => r.consumeAll())
  }
}

case class ConsumeNByNThenCloseResultConsumer(nRowsPerRequest: Int) extends RuntimeTestResultConsumptionController {

  override def consume(runtimeResult: RuntimeResult): Unit = {
    Using.resource(runtimeResult) { r =>
      do {
        r.request(nRowsPerRequest)
      } while (r.await())
    }
  }
}

case class ConsumeSlowlyNByNThenCloseResultConsumer(nRowsPerRequest: Int, sleepNanos: Int)
    extends RuntimeTestResultConsumptionController {

  override def consume(runtimeResult: RuntimeResult): Unit = {
    Using.resource(runtimeResult) { r =>
      do {
        Thread.sleep(0L, sleepNanos)
        r.request(nRowsPerRequest)
      } while (r.await())
    }
  }
}

case class RecordingRuntimeResult(
  runtimeResult: RuntimeResult,
  recordingQuerySubscriber: RecordingQuerySubscriber,
  resultConsumptionController: RuntimeTestResultConsumptionController = ConsumeAllThenCloseResultConsumer
) extends RuntimeTestResult {

  def awaitAll(): IndexedSeq[Array[AnyValue]] = {
    resultConsumptionController.consume(runtimeResult)
    recordingQuerySubscriber.getOrThrow().asScala.toIndexedSeq
  }
}

object RecordingRuntimeResult {

  def apply(runtimeResult: RuntimeResult, testSubscriber: TestSubscriber): RecordingRuntimeResult =
    RecordingRuntimeResult(runtimeResult, TestSubscriberWrappingRecordingQuerySubscriber(testSubscriber))
}

case class NonRecordingRuntimeResult(
  runtimeResult: RuntimeResult,
  nonRecordingQuerySubscriber: NonRecordingQuerySubscriber,
  resultConsumptionController: RuntimeTestResultConsumptionController = ConsumeAllThenCloseResultConsumer
) extends RuntimeTestResult {

  def awaitAll(): Long = {
    resultConsumptionController.consume(runtimeResult)
    nonRecordingQuerySubscriber.assertNoErrors()
    nonRecordingQuerySubscriber.recordCount()
  }
}

case class TestSubscriberRuntimeResult(runtimeResult: RuntimeResult, testSubscriber: TestSubscriber) {

  def awaitAll(): IndexedSeq[Array[AnyValue]] = {
    runtimeResult.consumeAll()
    runtimeResult.close()
    testSubscriber.allSeen.map(_.toArray).toArray.asInstanceOf[IndexedSeq[Array[AnyValue]]]
  }

  def pageCacheHits: Long = runtimeResult.asInstanceOf[ClosingRuntimeTestResult].pageCacheHits
  def pageCacheMisses: Long = runtimeResult.asInstanceOf[ClosingRuntimeTestResult].pageCacheMisses
}

case class ContextCondition[CONTEXT <: RuntimeContext](test: CONTEXT => Boolean, errorMsg: String)

case class TestSubscriberWrappingRecordingQuerySubscriber(testSubscriber: TestSubscriber)
    extends RecordingQuerySubscriber {

  override def getOrThrow: util.List[Array[AnyValue]] = {
    assertNoErrors()
    util.Arrays.asList[Array[AnyValue]](testSubscriber.allSeen.map(_.toArray): _*)
  }

  override def assertNoErrors(): Unit = {}

  override def queryStatistics: QueryStatistics = {
    testSubscriber.queryStatistics
  }
}
