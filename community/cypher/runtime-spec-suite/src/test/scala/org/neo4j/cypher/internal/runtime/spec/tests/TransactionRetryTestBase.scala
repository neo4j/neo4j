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
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenFail
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.hasRetry
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsRetryParameters
import org.neo4j.cypher.internal.logical.plans.Prober
import org.neo4j.cypher.internal.logical.plans.TransactionConcurrency
import org.neo4j.cypher.internal.logical.plans.TransactionConcurrency.Concurrent
import org.neo4j.cypher.internal.logical.plans.TransactionConcurrency.Serial
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.InputValues
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RowsMatcher
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSupport
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSupport.WorkloadMode
import org.neo4j.cypher.internal.runtime.spec.StaticGraphRuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanCombinationRewriter.NoRewrites
import org.neo4j.cypher.internal.runtime.spec.tests.TransactionRetryTestBase.DEFAULT_RETRY_TIMEOUT_SECONDS
import org.neo4j.cypher.internal.runtime.spec.tests.TransactionRetryTestBase.ExpectedRange
import org.neo4j.cypher.internal.runtime.spec.tests.TransactionRetryTestBase.ExpectedRange.AtLeast
import org.neo4j.cypher.internal.runtime.spec.tests.TransactionRetryTestBase.ExpectedRange.AtMost
import org.neo4j.cypher.internal.runtime.spec.tests.TransactionRetryTestBase.ExpectedRange.Exactly
import org.neo4j.cypher.internal.runtime.spec.tests.TransactionRetryTestBase.ExpectedRange.NoExpectation
import org.neo4j.cypher.internal.util.test_helpers.TimeLimitedCypherTest
import org.neo4j.exceptions.StatusWrapCypherException
import org.neo4j.exceptions.TransactionRetryAbortedException
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.exceptions.Status.HasStatus
import org.neo4j.kernel.api.query.ExtendedQueryStatistics
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.logging.InternalLogProvider
import org.neo4j.util.Table
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.IntegralValue
import org.neo4j.values.storable.NumberValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.scalatest.Assertions.withClue
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.LockSupport

import scala.util.Random

abstract class TransactionRetryTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int
) extends StaticGraphRuntimeTestSuite[CONTEXT](edition, runtime, testPlanCombinationRewriterHints = Set(NoRewrites))
    with TimeLimitedCypherTest {

  test("simple debug") {
    val input = inputValues((1 to 6).map(Array[Any](_)): _*)
    var thrown = false
    val query = new LogicalQueryBuilder(this)
      .produceResults(inputVarName, statusVarName)
      .transactionApply(
        batchSize = 3,
        concurrency = Serial,
        onErrorBehaviour = OnErrorRetryThenFail,
        maybeReportAs = Some(statusVarName),
        maybeRetryParameters = Some(InTransactionsRetryParameters(timeout = Some(literalFloat(1)))(pos))
      )
      .|.prober((row: AnyRef, state: AnyRef) => {
        val i = row.asInstanceOf[CypherRow]
          .getByName(inputVarName)
          .asInstanceOf[NumberValue]
          .longValue()

        if (i == 5 && !thrown) {
          thrown = true
          throw new TestTransientError("Test exception")
        }
      })
      .|.argument(inputVarName)
      .input(variables = Seq(inputVarName))
      .build(readOnly = false)

    val result = execute(query, runtime, input)
    result.awaitAll()
    // println(result.table()) // (Left on purpose, enable to inspect result)
  }

  // TODO
  // write a test for the case where we have two single row inner buffers and one of them fails when the two are out of sync

  // Interactive test case
  test("debug me - one failed batch in the middle") {
    val params = TestCaseParameters(
      concurrency = Serial,
      onErrorBehaviour = OnErrorRetryThenFail,
      applyOrForeach = ApplyOrForeach.Apply,
      reportStatus = false,
      batchSize = 2,
      morselSize = 1
    )
    val nBatches = 3
    val retryTimeoutSeconds = 0.5
    val failedBatchNumber = nBatches / 2
    val nErrors = 1
    val config = TestCaseConfig.withErrors(
      params,
      nBatches,
      retryTimeoutSeconds,
      errorsForBatch = (b: Int) => if (b == failedBatchNumber) nErrors else 0 // A batch in the middle has errors
    )
    val result = runTest(config)
    TestCaseExpectation.expectNoFailedBatchesAtLeastOneRetry(
      config
    ).verify(result)
  }

  test("debug me - one failed batch last") {
    val params = TestCaseParameters(
      concurrency = Serial,
      onErrorBehaviour = OnErrorRetryThenFail,
      applyOrForeach = ApplyOrForeach.Apply,
      reportStatus = false,
      batchSize = 2
    )
    val nBatches = 3
    val retryTimeoutSeconds = 0.5
    val failedBatchNumber = nBatches - 1
    val nErrors = 1
    val config = TestCaseConfig.withErrors(
      params,
      nBatches,
      retryTimeoutSeconds,
      errorsForBatch = (b: Int) => if (b == failedBatchNumber) nErrors else 0 // A batch in the middle has errors
    )
    val result = runTest(config)
    TestCaseExpectation.expectNoFailedBatchesAtLeastOneRetry(
      config
    ).verify(result)
  }

  test("debug me - a single error in the first and middle batch") {
    val params = TestCaseParameters(
      concurrency = Serial,
      onErrorBehaviour = OnErrorRetryThenFail,
      applyOrForeach = ApplyOrForeach.Apply,
      reportStatus = false,
      batchSize = 2
    )
    val nBatches = 3
    val retryTimeoutSeconds = 0.5
    val config = TestCaseConfig.withErrors(
      params,
      nBatches,
      retryTimeoutSeconds,
      errorsForBatch = (b: Int) => if (b == 0 || b == nBatches / 2) 1 else 0 // The first and middle batch has one error
    )
    val result = runTest(config)
    TestCaseExpectation.expectNoFailedBatchesAtLeastOneRetry(
      config
    ).verify(result)
  }

  test("debug me - a single error in the first and last batch") {
    val params = TestCaseParameters(
      concurrency = Serial,
      onErrorBehaviour = OnErrorRetryThenFail,
      applyOrForeach = ApplyOrForeach.Apply,
      reportStatus = false,
      batchSize = 2
    )
    val nBatches = 3
    val retryTimeoutSeconds = 0.5
    val config = TestCaseConfig.withErrors(
      params,
      nBatches,
      retryTimeoutSeconds,
      errorsForBatch = (b: Int) => if (b == 0 || b == nBatches - 1) 1 else 0 // The first and last batch has one error
    )
    val result = runTest(config)
    TestCaseExpectation.expectNoFailedBatchesAtLeastOneRetry(
      config
    ).verify(result)
  }

  test("debug me - a single error in the first and last batch and one in the middle") {
    val params = TestCaseParameters(
      concurrency = Serial,
      onErrorBehaviour = OnErrorRetryThenFail,
      applyOrForeach = ApplyOrForeach.Apply,
      reportStatus = false,
      batchSize = 2
    )
    val nBatches = 3
    val retryTimeoutSeconds = 0.5
    val config = TestCaseConfig.withErrors(
      params,
      nBatches,
      retryTimeoutSeconds,
      errorsForBatch = (b: Int) =>
        if (b == 0 || b == nBatches - 1 || b == nBatches / 2) 1 else 0 // The first and last batch has one error
    )
    val result = runTest(config)
    TestCaseExpectation.expectNoFailedBatchesAtLeastOneRetry(
      config
    ).verify(result)
  }

  test("debug me - expect failed batch retry aborted") {
    val params = TestCaseParameters(
      concurrency = Serial,
      onErrorBehaviour = OnErrorRetryThenFail,
      applyOrForeach = ApplyOrForeach.Apply,
      reportStatus = false,
      batchSize = 2
    )
    val nBatches = 3
    val retryTimeoutSeconds = 0.5
    val failedBatchNumber = nBatches / 2
    val nErrors = 100
    val config = TestCaseConfig.withErrors(
      params,
      nBatches,
      retryTimeoutSeconds,
      errorsForBatch = (b: Int) => if (b == failedBatchNumber) nErrors else 0 // A batch in the middle has errors
    )
    val result = runTest(config)
    TestCaseExpectation.expectFailedBatchesAtLeastOneRetry(
      config,
      errorMessage = testTransientErrorPrefix,
      failedBatchNumber
    ).verify(result)
  }

  test("debug me - expect no errors no retries") {
    val params = TestCaseParameters(
      concurrency = Serial,
      onErrorBehaviour = OnErrorRetryThenFail,
      applyOrForeach = ApplyOrForeach.Apply,
      reportStatus = false,
      batchSize = 10
    )
    val nBatches = defaultNumberOfBatches
    val retryTimeoutSeconds = shortRetryTimeoutSeconds
    val config = TestCaseConfig.withErrors(
      params,
      nBatches,
      retryTimeoutSeconds,
      errorsForBatch = (b: Int) => if (b == nBatches / 2) 1 else 0 // A batch in the middle has one error
    )
    val result = runTest(config)
    TestCaseExpectation.expectNoFailedBatchesAtLeastOneRetry(config).verify(result)
  }

  test(s"debug me - Transaction retry should fail and not retry client errors") {
    val params = TestCaseParameters(
      concurrency = Serial,
      onErrorBehaviour = OnErrorRetryThenFail,
      applyOrForeach = ApplyOrForeach.Apply,
      reportStatus = false,
      batchSize = 10
    )
    val nBatches = defaultNumberOfBatches
    val failedBatchNumber = nBatches / 2
    val config = TestCaseConfig.withErrors(
      params,
      nBatches,
      errorsForBatch = (b: Int) => if (b == failedBatchNumber) 1 else 0,
      exceptionFactory = TestClientErrorFactory
    )
    val result = runTest(config)
    TestCaseExpectation.expectFailedBatchesNonRetryable(
      config,
      testClientErrorPrefix,
      failedBatchNumber
    ).verify(result)
  }

  test(s"debug me - Custom query on error break") {
    val params = TestCaseParameters(
      concurrency = Serial,
      onErrorBehaviour = OnErrorBreak,
      applyOrForeach = ApplyOrForeach.Apply,
      reportStatus = true,
      batchSize = 1,
      morselSize = 3,
      query = QueryTemplate.Custom
    )
    val nBatches = 3
    val failedBatchNumber = nBatches / 2
    val config = TestCaseConfig.withErrors(
      params,
      nBatches,
      errorsForBatch = (b: Int) => if (b == failedBatchNumber) 1 else 0,
      exceptionFactory = TestClientErrorFactory
    )
    val result = runTest(config)
    TestCaseExpectation.expectFailedBatchesNonRetryable(
      config,
      testClientErrorPrefix,
      failedBatchNumber
    ).verify(result)
  }

  // StaticGraphRuntimeTestSuite
  override def shouldSetup: Boolean = true
  override protected def createGraph(): Unit = {}

  // Constants
  val defaultNumberOfBatches: Int = 20 // sizeHint / 10
  val shortRetryTimeoutSeconds = 0.2
  val inputVarName = "i"
  val statusVarName = "s"
  val random = new Random()
  val seed: Long = System.currentTimeMillis()
  random.setSeed(seed)
  println(s"TransactionRetryTestBase random seed: $seed")

  // Utility classes
  sealed trait QueryTemplate {
    def union: Boolean = false
    def reducerOnRhs: Boolean = false
    def reducerOnTop: Boolean = false
    def limitOnRhs: Boolean = false
    def limitOnTop: Boolean = false
    def nestedApply: Boolean = false

    // override def toString: String = getClass.getSimpleName
  }

  object QueryTemplate {
    case object Simple extends QueryTemplate

    case object Union extends QueryTemplate {
      override def union: Boolean = true
    }

    case object ReducerOnRhs extends QueryTemplate {
      override def reducerOnRhs: Boolean = true
    }

    case object ReducerOnTop extends QueryTemplate {
      override def reducerOnTop: Boolean = true
    }

    case object Complex extends QueryTemplate {
      override def reducerOnTop: Boolean = true
      override def reducerOnRhs: Boolean = true
      override def limitOnRhs: Boolean = true
      override def limitOnTop: Boolean = true
      override def nestedApply: Boolean = true
    }

    case object Custom extends QueryTemplate {
      override def reducerOnTop: Boolean = false
      override def reducerOnRhs: Boolean = true
      override def limitOnRhs: Boolean = false
      override def limitOnTop: Boolean = false
      override def nestedApply: Boolean = false
    }
  }

  sealed trait ApplyOrForeach

  object ApplyOrForeach {
    case object Apply extends ApplyOrForeach
    case object Foreach extends ApplyOrForeach
  }

  case class TestCaseParameters(
    concurrency: TransactionConcurrency,
    onErrorBehaviour: InTransactionsOnErrorBehaviour,
    applyOrForeach: ApplyOrForeach,
    reportStatus: Boolean,
    batchSize: Int,
    morselSize: Int = getConfig.get(GraphDatabaseInternalSettings.cypher_pipelined_batch_size_small),
    query: QueryTemplate = QueryTemplate.Simple
  ) {

    def inExpectedOrder(rows: Iterable[Array[_]]): RowsMatcher = {
      concurrency match {
        case Serial        => inOrder(rows)
        case Concurrent(_) => inAnyOrder(rows)
      }
    }

    override def toString: String =
      s"concurrency=$concurrency onErrorBehaviour=$onErrorBehaviour plan=$applyOrForeach reportStatus=$reportStatus batchSize=$batchSize morselSize=$morselSize, query=$query"

    def codeString: String =
      s"${this.getClass.getSimpleName}(concurrency=$concurrency, onErrorBehaviour=$onErrorBehaviour, applyOrForeach=ApplyOrForeach.$applyOrForeach, reportStatus=$reportStatus, batchSize=$batchSize, morselSize=$morselSize, query=$query)"
  }

  case class TestCaseConfig(
    params: TestCaseParameters,
    nBatches: Int,
    retryTimeoutSeconds: Double = DEFAULT_RETRY_TIMEOUT_SECONDS,
    innerProbe: Prober.Probe = Prober.NoopProbe,
    inputProbe: Prober.Probe = Prober.NoopProbe,
    outputProbe: Prober.Probe = Prober.NoopProbe,
    errorsForBatch: Int => Int = _ => 0
  ) {
    def nRows: Int = nBatches * params.batchSize

    def rollbacksPlusCommit(batch: Int) = errorsForBatch(batch) + 1
  }

  object TestCaseConfig {

    def withErrors(
      params: TestCaseParameters,
      nBatches: Int,
      retryTimeoutSeconds: Double = DEFAULT_RETRY_TIMEOUT_SECONDS,
      errorsForBatch: Int => Int = _ => 0, // Batch number => Number of errors to inject for that batch
      delaysForBatch: Int => Long = _ => 0L, // Batch number => Delay in nanos when executing that batch
      exceptionFactory: String => Throwable = TestTransientErrorFactory
    ): TestCaseConfig = {
      val errorProbe = createErrorProbe(sizeHint, params.batchSize, errorsForBatch, delaysForBatch, exceptionFactory)
      TestCaseConfig(params, nBatches, retryTimeoutSeconds, innerProbe = errorProbe, errorsForBatch = errorsForBatch)
    }

    def withNoErrors(params: TestCaseParameters, nBatches: Int): TestCaseConfig =
      TestCaseConfig(params, nBatches, retryTimeoutSeconds = 0.0)
  }

  sealed trait ErrorMessageExpectation

  object ErrorMessageExpectation {
    case object NoError extends ErrorMessageExpectation
    case class TransientErrorWithPrefix(prefix: String) extends ErrorMessageExpectation
    case class ClientErrorWithPrefix(prefix: String) extends ErrorMessageExpectation
  }

  case class TestCaseExpectation(
    config: TestCaseConfig,
    nResultRows: Option[Int],
    transactionsStarted: ExpectedRange = NoExpectation,
    transactionsCommitted: ExpectedRange = NoExpectation,
    transactionsRolledBack: ExpectedRange = NoExpectation,
    expectedOuterFailure: ErrorMessageExpectation = ErrorMessageExpectation.NoError,
    batchesExpectedStartedStatus: Int => Option[Boolean] = _ => None,
    batchesExpectedCommittedStatus: Int => Option[Boolean] = _ => None,
    batchesExpectedErrorStatus: Int => Option[ErrorMessageExpectation] = _ => None
  ) {

    private def verifyTimeoutException(result: RecordingRuntimeResult): Unit = {
      val exception = intercept[StatusWrapCypherException](result.awaitAll())
      exception.getMessage should include(
        s"Retry timed out with a maximum retry duration of ${config.retryTimeoutSeconds} seconds"
      )

      exception.getCause shouldBe a[TransactionRetryAbortedException]
      val retryAbortedException = exception.getCause.asInstanceOf[TransactionRetryAbortedException]
      retryAbortedException.gqlStatus() shouldBe "50N23"

      exception.getCause.getCause shouldBe a[TestTransientError]
      exception.getCause.getMessage should include(testTransientErrorPrefix)
    }

    private def verifyNonRetryableException(result: RecordingRuntimeResult): Unit = {
      val exception = intercept[TestClientError](result.awaitAll())
      exception.getMessage should startWith(testClientErrorPrefix)
    }

    def verify(result: RecordingRuntimeResult): Unit = {
      val parametersClue =
        s"Failed test parameters:\nval params = ${config.params.codeString}\nval nBatches = ${config.nBatches}\nval retryTimeoutSeconds = ${config.retryTimeoutSeconds}\n\n"
      withClue(parametersClue) {
        expectedOuterFailure match {
          case ErrorMessageExpectation.TransientErrorWithPrefix(_) =>
            verifyTimeoutException(result)
          case ErrorMessageExpectation.ClientErrorWithPrefix(_) =>
            verifyNonRetryableException(result)
          case _ =>
            val resultTable = result.table()
            verifyResult(resultTable)
        }

        // Verify transaction statistics
        val queryStatistics = result.runtimeResult.queryStatistics().asInstanceOf[ExtendedQueryStatistics]
        transactionsStarted.assert(queryStatistics.getTransactionsStarted, "transactions started")
        transactionsCommitted.assert(queryStatistics.getTransactionsCommitted, "transactions committed")
        transactionsRolledBack.assert(queryStatistics.getTransactionsRolledBack, "transactions rolled back")
      }
    }

    private def verifyResult(resultTable: Table): Unit = {
      withClue("Expected result columns: ") {
        resultTable.header shouldBe Seq(inputVarName) ++ (if (config.params.reportStatus) Seq(statusVarName)
                                                          else Seq.empty)
      }
      val rows = resultTable.rows
      nResultRows.foreach { nExpectedRows =>
        withClue("Expected number of rows: ") {
          rows.size shouldBe nExpectedRows
        }
      }
      rows.foreach { row =>
        verifyResultRow(row)
      }
    }

    private def verifyResultRow(row: Seq[AnyValue]): Unit = {
      val i = ValueUtils.asLongValue(row.head).longValue().toInt
      if (config.params.reportStatus) {
        val status = row(1).asInstanceOf[MapValue]
        val started = ValueUtils.asBooleanValue(status.get("started")).booleanValue()
        val committed = ValueUtils.asBooleanValue(status.get("committed")).booleanValue()
        val errorMessage = status.get("errorMessage")
        val batch = i / config.params.batchSize
        withClue(s"Row $i in batch $batch: ") {
          withClue("Started: ") {
            val expected = batchesExpectedStartedStatus(batch)
            expected.foreach(started shouldBe _)
          }
          withClue("Committed: ") {
            val expected = batchesExpectedCommittedStatus(batch)
            expected.foreach(committed shouldBe _)
          }
          withClue("Error message: ") {
            val expected = batchesExpectedErrorStatus(batch)
            expected.foreach {
              case ErrorMessageExpectation.TransientErrorWithPrefix(expectedMessage) =>
                val es = errorMessage.asInstanceOf[TextValue].stringValue()
                es should startWith(transactionRetryAbortedPrefix)
                es should include(expectedMessage)
              case ErrorMessageExpectation.ClientErrorWithPrefix(expectedMessage) =>
                val es = errorMessage.asInstanceOf[TextValue].stringValue()
                es should startWith(expectedMessage)
              case ErrorMessageExpectation.NoError =>
                errorMessage shouldBe Values.NO_VALUE
            }
          }
        }
      }
    }
  }

  object TestCaseExpectation {
    final val TrueForAllBatches = (_: Int) => Some(true)
    final val NoErrorForAllBatches = (_: Int) => Some(ErrorMessageExpectation.NoError)
    final val NoExpectationForAllBatches = (_: Int) => None

    def expectNoErrorsAndNoRetries(config: TestCaseConfig): TestCaseExpectation = {
      val nResultRows = config.nRows
      TestCaseExpectation(
        config,
        Some(nResultRows),
        transactionsStarted = Exactly(config.nBatches),
        transactionsCommitted = Exactly(config.nBatches),
        transactionsRolledBack = Exactly(0),
        expectedOuterFailure = ErrorMessageExpectation.NoError,
        batchesExpectedStartedStatus = TrueForAllBatches,
        batchesExpectedCommittedStatus = TrueForAllBatches,
        batchesExpectedErrorStatus = NoErrorForAllBatches
      )
    }

    def expectNoFailedBatchesAtLeastOneRetry(config: TestCaseConfig): TestCaseExpectation = {
      // TODO: Maybe we can have stronger assertions here?
      expectFailedBatchesAtLeastOneRetry(config, "No error expected", failedBatches = Seq.empty: _*)
    }

    def expectFailedBatchesAtLeastOneRetry(
      config: TestCaseConfig,
      errorMessage: String,
      failedBatches: Int*
    ): TestCaseExpectation = {
      val nResultRows = config.nRows
      val nFailedBatches = failedBatches.size
      val failedBatchesSet = failedBatches.toSet

      config.params.onErrorBehaviour match {
        case OnErrorRetryThenContinue =>
          TestCaseExpectation(
            config,
            Some(nResultRows),
            transactionsStarted = if (nFailedBatches > 0) {
              AtLeast(config.nBatches + nFailedBatches)
            } else {
              Exactly((0 until config.nBatches).map(config.rollbacksPlusCommit).sum)
            },
            transactionsCommitted = Exactly(config.nBatches - nFailedBatches),
            transactionsRolledBack =
              if (nFailedBatches > 0) {
                AtLeast((0 until config.nBatches).map(i =>
                  if (failedBatchesSet.contains(i)) 2 else config.errorsForBatch(i)
                ).sum)
              } else {
                Exactly((0 until config.nBatches).map(config.errorsForBatch).sum)
              },
            expectedOuterFailure = ErrorMessageExpectation.NoError,
            batchesExpectedStartedStatus = TrueForAllBatches,
            batchesExpectedCommittedStatus = (b: Int) => Some(!failedBatchesSet.contains(b)),
            batchesExpectedErrorStatus =
              (b: Int) =>
                Some(
                  if (failedBatchesSet.contains(b))
                    ErrorMessageExpectation.TransientErrorWithPrefix(errorMessage)
                  else ErrorMessageExpectation.NoError
                )
          )
        case eb @ (OnErrorRetryThenBreak | OnErrorRetryThenFail) =>
          TestCaseExpectation(
            config,
            Some(nResultRows),
            transactionsStarted = config.params.concurrency match {
              case Serial if nFailedBatches > 0 =>
                AtLeast((0 until failedBatches.min).map(config.rollbacksPlusCommit).sum + 1)
              case Concurrent(_) if nFailedBatches > 0 =>
                AtLeast(2)
              case _ =>
                Exactly((0 until config.nBatches).map(config.rollbacksPlusCommit).sum)
            },
            transactionsCommitted = config.params.concurrency match {
              case Serial if nFailedBatches > 0        => Exactly(failedBatches.min)
              case Concurrent(_) if nFailedBatches > 0 => AtMost(config.nBatches - nFailedBatches)
              case _                                   => Exactly(config.nBatches)
            },
            transactionsRolledBack = config.params.concurrency match {
              // in serial we will retry each batch in order until we hit the failing batch. here we account for each
              // batch prior to the failing batch, plus the minimum number of expected failures for the failing batch: 1
              case Serial if nFailedBatches > 0 =>
                AtLeast((0 until failedBatches.min).map(config.errorsForBatch).sum + 1)

              // in concurrent we only know that something will fail, not in which order
              case Concurrent(_) if nFailedBatches > 0 =>
                AtLeast(2)

              case _ =>
                Exactly((0 until config.nBatches).map(config.errorsForBatch).sum)
            },
            expectedOuterFailure = eb match {
              case OnErrorRetryThenFail if failedBatches.nonEmpty =>
                ErrorMessageExpectation.TransientErrorWithPrefix(errorMessage)
              case _ =>
                ErrorMessageExpectation.NoError
            },
            batchesExpectedStartedStatus =
              config.params.concurrency match {
                case Serial if nFailedBatches > 0 =>
                  (b: Int) => Some(b <= failedBatches.min)

                case Concurrent(_) if nFailedBatches == 1 =>
                  (b: Int) => if (failedBatchesSet.contains(b)) Some(true) else None

                case Concurrent(_) if nFailedBatches > 1 =>
                  NoExpectationForAllBatches

                case _ => TrueForAllBatches
              },
            batchesExpectedCommittedStatus = config.params.concurrency match {
              case Serial if nFailedBatches > 0 =>
                (b: Int) => Some(b < failedBatches.min)

              case Concurrent(_) if nFailedBatches > 0 =>
                (b: Int) => if (failedBatchesSet.contains(b)) Some(false) else None

              case _ => TrueForAllBatches
            },
            batchesExpectedErrorStatus = config.params.concurrency match {
              case Serial if nFailedBatches > 0 =>
                (b: Int) =>
                  if (b < failedBatches.min) {
                    Some(ErrorMessageExpectation.NoError)
                  } else if (b == failedBatches.min) {
                    Some(ErrorMessageExpectation.TransientErrorWithPrefix(errorMessage))
                  } else {
                    None
                  }

              case Concurrent(_) if nFailedBatches == 1 =>
                (b: Int) =>
                  if (failedBatchesSet.contains(b)) Some(ErrorMessageExpectation.TransientErrorWithPrefix(errorMessage))
                  else None

              case Concurrent(_) if nFailedBatches > 1 =>
                NoExpectationForAllBatches

              case _ =>
                NoErrorForAllBatches
            }
          )
      }
    }

    def expectFailedBatchesNonRetryable(
      config: TestCaseConfig,
      errorMessage: String,
      failedBatches: Int*
    ): TestCaseExpectation = {
      val nResultRows = config.nRows
      val nFailedBatches = failedBatches.size
      val failedBatchesSet = failedBatches.toSet
      config.params.onErrorBehaviour match {
        case OnErrorRetryThenContinue =>
          TestCaseExpectation(
            config,
            Some(nResultRows),
            transactionsStarted = Exactly(config.nBatches),
            transactionsCommitted = Exactly(config.nBatches - nFailedBatches),
            transactionsRolledBack = Exactly(nFailedBatches),
            expectedOuterFailure = ErrorMessageExpectation.NoError,
            batchesExpectedStartedStatus = TrueForAllBatches,
            batchesExpectedCommittedStatus = (b: Int) => Some(!failedBatchesSet.contains(b)),
            batchesExpectedErrorStatus =
              (b: Int) =>
                Some(
                  if (failedBatchesSet.contains(b))
                    ErrorMessageExpectation.ClientErrorWithPrefix(errorMessage)
                  else ErrorMessageExpectation.NoError
                )
          )
        case eb @ (OnErrorRetryThenBreak | OnErrorRetryThenFail) =>
          TestCaseExpectation(
            config,
            Some(nResultRows),
            transactionsStarted = NoExpectation,
            transactionsCommitted = AtMost(config.nBatches - nFailedBatches),
            transactionsRolledBack = eb match {
              case OnErrorRetryThenBreak =>
                AtLeast(nFailedBatches)
              case OnErrorRetryThenFail =>
                AtLeast(1)
            },
            expectedOuterFailure = eb match {
              case OnErrorRetryThenFail if failedBatches.nonEmpty =>
                ErrorMessageExpectation.ClientErrorWithPrefix(errorMessage)
              case _ =>
                ErrorMessageExpectation.NoError
            },
            batchesExpectedStartedStatus = {
              if (nFailedBatches == 1) {
                (b: Int) =>
                  if (failedBatchesSet.contains(b)) Some(true) else None
              } else {
                NoExpectationForAllBatches
              }
            },
            batchesExpectedCommittedStatus = {
              (b: Int) =>
                if (failedBatchesSet.contains(b)) Some(false) else None
            },
            batchesExpectedErrorStatus =
              if (nFailedBatches == 1) {
                (b: Int) =>
                  if (failedBatchesSet.contains(b)) Some(ErrorMessageExpectation.ClientErrorWithPrefix(errorMessage))
                  else None
              } else {
                NoExpectationForAllBatches
              }
          )
        case eb @ OnErrorBreak =>
          TestCaseExpectation(
            config,
            Some(nResultRows),
            transactionsStarted = NoExpectation,
            transactionsCommitted = AtMost(config.nBatches - nFailedBatches),
            transactionsRolledBack = AtLeast(nFailedBatches),
            expectedOuterFailure = ErrorMessageExpectation.NoError,
            batchesExpectedStartedStatus = {
              if (nFailedBatches == 1) {
                (b: Int) =>
                  if (failedBatchesSet.contains(b)) Some(true) else None
              } else {
                NoExpectationForAllBatches
              }
            },
            batchesExpectedCommittedStatus = {
              (b: Int) =>
                if (failedBatchesSet.contains(b)) Some(false) else None
            },
            batchesExpectedErrorStatus =
              if (nFailedBatches == 1) {
                (b: Int) =>
                  if (failedBatchesSet.contains(b)) Some(ErrorMessageExpectation.ClientErrorWithPrefix(errorMessage))
                  else None
              } else {
                NoExpectationForAllBatches
              }
          )
      }
    }
  }

  // ========================================
  // Test multiple combinations of parameters

  // Limit the number of combinations on regular builds and test most combinations on experimental build
  private[this] val concurrencyValuesToTest = Seq(1, 2, 4, 8, 16)
  private[this] val nConcurrencyValuesToTest = if (runOnlySafeScenarios) 1 else 2
  private[this] val batchSizesToTest = Seq(1, 2, 10)
  private[this] val nBatchSizesToTest = if (runOnlySafeScenarios) 1 else 3
  private[this] val morselSizesToTest = Seq(1, 2, 3, 4, 5)
  private[this] val nMorselSizesToTest = if (runOnlySafeScenarios) 1 else 5

  for {
    concurrency <-
      Seq(Serial) ++ (if (!isPipelined)
                        random.shuffle(concurrencyValuesToTest).take(nConcurrencyValuesToTest).map(Concurrent(_))
                      else Seq.empty)
    onErrorBehaviour <- Seq(OnErrorRetryThenFail, OnErrorRetryThenContinue, OnErrorRetryThenBreak)
    applyOrForeach <- Seq(ApplyOrForeach.Apply, ApplyOrForeach.Foreach)
    reportStatus <- if (onErrorBehaviour != OnErrorRetryThenFail) Seq(true, false) else Seq(false)
    batchSize <- random.shuffle(batchSizesToTest).take(nBatchSizesToTest)
    morselSize <- if (isPipelined) random.shuffle(morselSizesToTest).take(nMorselSizesToTest) else Seq(1)
    query <- Seq(QueryTemplate.Simple, QueryTemplate.Complex)
    params =
      TestCaseParameters(concurrency, onErrorBehaviour, applyOrForeach, reportStatus, batchSize, morselSize, query)
  } yield {

    test(s"Transaction retry should succeed when no errors: $params") {
      val nBatches = defaultNumberOfBatches
      val config = TestCaseConfig.withNoErrors(params, nBatches)
      val result = runTest(config)
      TestCaseExpectation.expectNoErrorsAndNoRetries(config).verify(result)
    }

    test(
      s"Transaction retry should succeed when a single error in one batch in the middle and a short timeout: $params"
    ) {
      val nBatches = defaultNumberOfBatches
      val retryTimeoutSeconds = shortRetryTimeoutSeconds
      val config = TestCaseConfig.withErrors(
        params,
        nBatches,
        retryTimeoutSeconds,
        errorsForBatch = (b: Int) => if (b == nBatches / 2) 1 else 0 // A batch in the middle has one error
      )
      val result = runTest(config)
      TestCaseExpectation.expectNoFailedBatchesAtLeastOneRetry(config).verify(result)
    }

    test(s"Transaction retry should succeed when a single error in the last batch and a short timeout: $params") {
      val nBatches = defaultNumberOfBatches
      val retryTimeoutSeconds = shortRetryTimeoutSeconds
      val config = TestCaseConfig.withErrors(
        params,
        nBatches,
        retryTimeoutSeconds,
        errorsForBatch = (b: Int) => if (b == nBatches - 1) 1 else 0 // The last batch has one error
      )
      val result = runTest(config)
      TestCaseExpectation.expectNoFailedBatchesAtLeastOneRetry(config).verify(result)
    }

    test(s"Transaction retry should succeed when a single error in the first batch and a short timeout: $params") {
      val nBatches = defaultNumberOfBatches
      val retryTimeoutSeconds = shortRetryTimeoutSeconds
      val config = TestCaseConfig.withErrors(
        params,
        nBatches,
        retryTimeoutSeconds,
        errorsForBatch = (b: Int) => if (b == 0) 1 else 0 // The last batch has one error
      )
      val result = runTest(config)
      TestCaseExpectation.expectNoFailedBatchesAtLeastOneRetry(config).verify(result)
    }

    test(
      s"Transaction retry should succeed when a single error in the first, last and one batch in the middle and a short timeout: $params"
    ) {
      val nBatches = defaultNumberOfBatches
      val retryTimeoutSeconds = shortRetryTimeoutSeconds
      val config = TestCaseConfig.withErrors(
        params,
        nBatches,
        retryTimeoutSeconds,
        errorsForBatch =
          (b: Int) => if (b == 0 || b == nBatches - 1 || b == nBatches / 2) 1 else 0 // The last batch has one error
      )
      val result = runTest(config)
      TestCaseExpectation.expectNoFailedBatchesAtLeastOneRetry(config).verify(result)
    }

    test(s"Transaction retry should time out when many errors in one batch and a short timeout: $params") {
      val nBatches = defaultNumberOfBatches
      val retryTimeoutSeconds = shortRetryTimeoutSeconds
      val failedBatchNumber = nBatches / 2
      val nErrors = 100
      val config = TestCaseConfig.withErrors(
        params,
        nBatches,
        retryTimeoutSeconds,
        errorsForBatch = (b: Int) => if (b == failedBatchNumber) nErrors else 0 // A batch in the middle has one error
      )
      val result = runTest(config)
      TestCaseExpectation.expectFailedBatchesAtLeastOneRetry(
        config,
        errorMessage = testTransientErrorPrefix,
        failedBatchNumber
      ).verify(result)
    }

    test(s"Transaction retry should succeed when single errors in 2/3 of batches and a short timeout: $params") {
      val nBatches = defaultNumberOfBatches
      val retryTimeoutSeconds = shortRetryTimeoutSeconds
      val config = TestCaseConfig.withErrors(
        params,
        nBatches,
        retryTimeoutSeconds,
        errorsForBatch = (b: Int) => if (b % 3 == 2) 0 else 1 // Every third batch has no errors, the rest has one error
        // delaysForBatch = (b: Int) => if (b % 2 == 0) TimeUnit.MILLISECONDS.toNanos(1) else 0L, // Every second batch has a delay
      )
      val result = runTest(config)
      TestCaseExpectation.expectNoFailedBatchesAtLeastOneRetry(config).verify(result)
    }

    test(s"Transaction retry should time out when many errors in 1/3 of batches and a short timeout: $params") {
      val nBatches = defaultNumberOfBatches
      val retryTimeoutSeconds = shortRetryTimeoutSeconds
      val config = TestCaseConfig.withErrors(
        params,
        nBatches,
        retryTimeoutSeconds,
        errorsForBatch = (b: Int) =>
          b % 3 match {
            case 0 => 1
            case 1 => 100
            case 2 => 0
          },
        delaysForBatch =
          (b: Int) => if (b % 2 == 0) TimeUnit.MILLISECONDS.toNanos(1) else 0L // Every second batch has a delay
      )
      val result = runTest(config)
      val expectedFailed = (0 until nBatches).filter(_ % 3 == 1).toArray
      TestCaseExpectation.expectFailedBatchesAtLeastOneRetry(
        config,
        errorMessage = testTransientErrorPrefix,
        expectedFailed: _*
      ).verify(result)
    }

    test(s"Transaction retry should fail and not retry client errors: $params") {
      val nBatches = defaultNumberOfBatches
      val failedBatchNumber = nBatches / 2
      val config = TestCaseConfig.withErrors(
        params,
        nBatches,
        errorsForBatch = (b: Int) => if (b == failedBatchNumber) 1 else 0,
        exceptionFactory = TestClientErrorFactory
      )
      val result = runTest(config)
      TestCaseExpectation.expectFailedBatchesNonRetryable(
        config,
        testClientErrorPrefix,
        failedBatchNumber
      ).verify(result)
    }
  }

  def runTest(config: TestCaseConfig): RecordingRuntimeResult = {
    val input = createInput(config.nRows)
    val query = createTestQueryWithInput(config)
    val parametersClue =
      s"Test parameters:\nval params = ${config.params.codeString}\nval nBatches = ${config.nBatches}\nval retryTimeoutSeconds = ${config.retryTimeoutSeconds}\n\n"
    println(parametersClue)
    getConfig.setDynamicByUser[Integer](
      GraphDatabaseInternalSettings.cypher_pipelined_batch_size_big,
      config.params.morselSize,
      getClass.getSimpleName
    )
    getConfig.setDynamicByUser[Integer](
      GraphDatabaseInternalSettings.cypher_pipelined_batch_size_small,
      config.params.morselSize,
      getClass.getSimpleName
    )
    val result = execute(query, runtime, input)
    result
  }

  def createInput(nRows: Int): InputValues = {
    val rows = (0 until nRows).map { i =>
      Array[Any](i)
    }
    inputValues(rows: _*)
  }

  def createErrorProbe(
    nRows: Int,
    batchSize: Int,
    errorsToThrowForBatch: Int => Int,
    delayInNanosForBatch: Int => Long,
    exceptionFactory: String => Throwable = TestTransientErrorFactory
  ): Prober.Probe = {
    if (batchSize == 1) {
      new ErrorInjectionProbe(
        nRows,
        errorsToThrowForBatch,
        delayInNanosForBatch,
        getRowNumberByVariable(inputVarName),
        exceptionFactory
      )
    } else {
      val middleIndex = (batchSize + 1) / 2
      val errorsToThrowForRow = (row: Int) => {
        if (row % batchSize == middleIndex) {
          errorsToThrowForBatch(row / batchSize)
        } else {
          0
        }
      }
      val delayInNanosForRow = (row: Int) => {
        if (row % batchSize == middleIndex) {
          delayInNanosForBatch(row / batchSize)
        } else {
          0
        }
      }
      new ErrorInjectionProbe(
        nRows,
        errorsToThrowForRow,
        delayInNanosForRow,
        getRowNumberByVariable(inputVarName),
        exceptionFactory
      )
    }
  }

  def createTestQueryWithInput(config: TestCaseConfig): LogicalQuery = {
    val params = config.params
    val retryParams = if (hasRetry(params.onErrorBehaviour)) {
      Some(InTransactionsRetryParameters(timeout = Some(literalFloat(config.retryTimeoutSeconds)))(pos))
    } else {
      None
    }
    val reportAs = if (params.reportStatus) Some(statusVarName) else None
    val resultColumns = Seq(inputVarName) ++ reportAs
    val qt = config.params.query
    config.params.applyOrForeach match {
      case ApplyOrForeach.Apply =>
        new LogicalQueryBuilder(this)
          .produceResults(resultColumns: _*)
          .planIf(qt.reducerOnTop && reportAs.isEmpty)(_
            .unwind(s"l2 as $inputVarName")
            .aggregation(Seq.empty, Seq(s"COLLECT($inputVarName) AS l2")))
          .planIf(qt.reducerOnTop && reportAs.isDefined)(_
            .projection(s"row.input AS $inputVarName", s"row.status AS $statusVarName")
            .unwind(s"rows as row")
            .aggregation(Seq.empty, Seq(s"COLLECT({input: $inputVarName, status: $statusVarName}) AS rows")))
          .planIf(qt.limitOnTop)(_.limit(Int.MaxValue))
          .prober(config.outputProbe)
          .transactionApply(
            batchSize = params.batchSize,
            concurrency = params.concurrency,
            onErrorBehaviour = params.onErrorBehaviour,
            maybeReportAs = reportAs,
            maybeRetryParameters = retryParams
          )
          .planIf(qt.reducerOnRhs)(_
            .|.unwind(s"l1 as $inputVarName")
            .|.aggregation(Seq.empty, Seq(s"COLLECT($inputVarName) AS l1")))
          .planIf(qt.limitOnRhs)(_.|.limit(Int.MaxValue))
          .planIf(qt.nestedApply)(_
            .|.apply()
            .|.|.union()
            .|.|.|.unwind(s"l0 as $inputVarName")
            .|.|.|.aggregation(Seq.empty, Seq(s"COLLECT($inputVarName) AS l0"))
            .|.|.|.limit(Int.MaxValue)
            .|.|.|.argument()
            .|.|.limit(0)
            .|.|.argument())
          .planIf(qt.union)(_
            .|.union()
            .|.|.limit(0)
            .|.|.unwind(s"u0 as $inputVarName")
            .|.|.aggregation(Seq.empty, Seq(s"COLLECT($inputVarName) AS u0"))
            .|.|.argument(inputVarName))
          .|.prober(config.innerProbe)
          .|.argument(inputVarName)
          .prober(config.inputProbe)
          .input(variables = Seq(inputVarName))
          .build(readOnly = false)

      case ApplyOrForeach.Foreach =>
        new LogicalQueryBuilder(this)
          .produceResults(resultColumns: _*)
          .prober(config.outputProbe)
          .transactionForeach(
            batchSize = params.batchSize,
            concurrency = params.concurrency,
            onErrorBehaviour = params.onErrorBehaviour,
            maybeReportAs = reportAs,
            maybeRetryParameters = retryParams
          )
          .|.prober(config.innerProbe)
          .|.argument(inputVarName)
          .prober(config.inputProbe)
          .input(variables = Seq(inputVarName))
          .build(readOnly = false)
    }
  }

  override protected def createRuntimeTestSupport(
    graphDb: GraphDatabaseService,
    edition: Edition[CONTEXT],
    runtime: CypherRuntime[CONTEXT],
    workloadMode: WorkloadMode,
    logProvider: InternalLogProvider
  ): RuntimeTestSupport[CONTEXT] = {
    new RuntimeTestSupport[CONTEXT](
      graphDb,
      edition,
      runtime,
      workloadMode,
      logProvider,
      debugOptions,
      defaultTransactionType = Type.IMPLICIT
    )
  }

  final private class TestTransientError(message: String) extends RuntimeException(message) with HasStatus {
    override def status: Status = Status.Transaction.Outdated
  }

  final private class TestClientError(message: String) extends RuntimeException(message) with HasStatus {
    override def status: Status = Status.Statement.SyntaxError
  }

  final val transactionRetryAbortedPrefix = "Transaction retry aborted"
  final val testTransientErrorPrefix = "TestTransientError:"
  final val testClientErrorPrefix = "TestClientError:"

  final private val TestTransientErrorFactory = (messageSuffix: String) => {
    new TestTransientError(s"$testTransientErrorPrefix $messageSuffix")
  }

  final private val TestClientErrorFactory = (messageSuffix: String) => {
    new TestClientError(s"$testClientErrorPrefix $messageSuffix")
  }

  class ErrorInjectionProbe(
    nRows: Int,
    errorsToThrowForRow: Int => Int, // Row number => Number of errors to inject
    delayInNanosForRow: Int => Long, // Row number => Delay in nanos before retry
    rowNumberFromCypherRow: AnyRef => Int, // Cypher Row => Row number
    exceptionFactory: String => Throwable
  ) extends Prober.Probe {
    // NOTE: We assume only one thread at a time will access the same array element,
    // and that thread visibility is guaranteed by the scheduler

    val thrownCounts = new Array[Int](nRows)

    override def onRow(row: AnyRef, state: AnyRef): Unit = {
      val i = rowNumberFromCypherRow(row)
      // print(s"Row $i on thread ${Thread.currentThread().getName}\n")
      if (i < nRows) {
        val delay = delayInNanosForRow(i)
        if (delay > 0L) {
          LockSupport.parkNanos(delay)
        }
        val ec = errorsToThrowForRow(i)
        var tc = thrownCounts(i)
        if (tc < ec) {
          tc += 1
          thrownCounts(i) = tc
          val messageSuffix =
            s"Error on row $i ($tc thrown, ${ec - tc} left) on thread ${Thread.currentThread().getName}"
          // print(messageSuffix + "\n")
          throw exceptionFactory(messageSuffix)
        }
      }
    }
  }

  private def getRowNumberByVariable(variableName: String): AnyRef => Int = {
    row =>
      row.asInstanceOf[CypherRow].getByName(variableName).asInstanceOf[IntegralValue].longValue().toInt
  }

  protected def createNodePropertyTestGraph(nNodes: Int): Array[Long] = {
    val nodeIds = new Array[Long](nNodes)
    withNewTx(tx => {
      (0 until nNodes).foreach { i =>
        val node = tx.createNode()
        node.setProperty("prop", i.toLong)
        nodeIds(i) = node.getId
      }
      tx.commit()
    })
    nodeIds
  }

  protected def txAssertionProbe(assertion: InternalTransaction => Unit): Prober.Probe = {
    (row: AnyRef, state: AnyRef) =>
      {
        withNewTx(assertion(_))
      }
  }

  protected def countingProbe(atomicIncr: AtomicInteger): Prober.Probe = {
    (row: AnyRef, state: AnyRef) =>
      {
        atomicIncr.getAndAdd(1)
      }
  }
}

object TransactionRetryTestBase {
  final val DEFAULT_RETRY_TIMEOUT_SECONDS: Double = 0.1

  sealed trait ExpectedRange {
    def assert(value: Long, clue: String = ""): Unit
  }

  object ExpectedRange {

    case object NoExpectation extends ExpectedRange {
      def assert(value: Long, clue: String = ""): Unit = {}
    }

    case class Exactly(n: Long) extends ExpectedRange {

      def assert(value: Long, clue: String = ""): Unit = {
        withClue(s"Expected exactly $n, but was $value $clue: ") {
          value shouldBe n
        }
      }
    }

    case class AtLeast(n: Long) extends ExpectedRange {

      def assert(value: Long, clue: String = ""): Unit = {
        withClue(s"Expected at least $n, but was $value $clue: ") {
          value should be >= n
        }
      }
    }

    case class AtMost(n: Long) extends ExpectedRange {

      def assert(value: Long, clue: String = ""): Unit = {
        withClue(s"Expected at most $n, but was $value $clue: ") {
          value should be <= n
        }
      }
    }

    case class Between(min: Long, max: Long) extends ExpectedRange {

      def assert(value: Long, clue: String = ""): Unit = {
        withClue(s"Expected between $min and $max, but was $value $clue: ") {
          value should (be <= max and be >= min)
        }
      }
    }
  }
}
