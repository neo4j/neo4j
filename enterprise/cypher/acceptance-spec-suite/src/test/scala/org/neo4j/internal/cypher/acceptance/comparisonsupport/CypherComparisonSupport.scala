/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance.comparisonsupport

import org.neo4j.cypher._
import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.kernel.monitoring.Monitors
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory
import org.neo4j.test.TestGraphDatabaseFactory
import org.neo4j.values.virtual.MapValue
import org.opencypher.v9_0.util.Eagerly
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.opencypher.v9_0.util.test_helpers.CypherTestSupport

import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
  * Will run a query across versions and configurations, making sure they all agree on the results and/or errors.
  *
  * For every query tested using `executeWith`, the query will be run against all configurations. Every configuration
  * is expected to either succeed or fail. When new features are added that enable queries in new configurations,
  * acceptance tests will start failing because now a configuration is succeeding that was not successful before.
  *
  * This is expected and useful - it let's us know how a change impacts how many acceptance tests now start
  * succeeding where they weren't earlier.
  */
trait CypherComparisonSupport extends AbstractCypherComparisonSupport {
  self: ExecutionEngineFunSuite =>

  override def eengineExecute(query: String,
                              params: MapValue,
                              context: TransactionalContext,
                              profile: Boolean): Result = eengine.execute(query, params, context, profile)

  override def makeRewinadable(in: Result): RewindableExecutionResult = RewindableExecutionResult(in)

  override def rollback[T](f: => T): T = graph.rollback(f)

  override def inTx[T](f: => T): T = graph.inTx(f)

  override def transactionalContext(query: (String, Map[String, Any])): TransactionalContext = graph.transactionalContext(query = query)

  override def databaseConfig(): collection.Map[Setting[_], String] = {
    Map(GraphDatabaseSettings.cypher_hints_error -> "true")
  }

  override protected def createDatabaseFactory(): TestGraphDatabaseFactory = new TestEnterpriseGraphDatabaseFactory()
}

trait AbstractCypherComparisonSupport extends CypherFunSuite with CypherTestSupport {

  // abstract, can be defined through CypherComparisonSupport
  def eengineExecute(query: String, params: MapValue, context: TransactionalContext, profile: Boolean = false): Result

  def makeRewinadable(in:Result): RewindableExecutionResult

  def rollback[T](f: => T): T

  def inTx[T](f: => T): T

  def transactionalContext(query: (String, Map[String, Any])): TransactionalContext

  def kernelMonitors: Monitors

  // Concrete stuff

  /**
    * Get rid of Arrays and java.util.Map to make it easier to compare results by equality.
    */
  implicit class RichInternalExecutionResults(res: RewindableExecutionResult) {
    def toComparableResultWithOptions(replaceNaNs: Boolean): Seq[Map[String, Any]] = res.toList.toComparableSeq(replaceNaNs)

    def toComparableResult: Seq[Map[String, Any]] = res.toList.toComparableSeq(replaceNaNs = false)
  }

  implicit class RichMapSeq(res: Seq[Map[String, Any]]) {

    import scala.collection.JavaConverters._

    object NanReplacement

    def toComparableSeq(replaceNaNs: Boolean): Seq[Map[String, Any]] = {
      def convert(v: Any): Any = v match {
        case a: Array[_] => a.toList.map(convert)
        case m: Map[_, _] =>
          Eagerly.immutableMapValues(m, convert)
        case m: java.util.Map[_, _] =>
          Eagerly.immutableMapValues(m.asScala, convert)
        case l: java.util.List[_] => l.asScala.map(convert)
        case d: java.lang.Double if replaceNaNs && java.lang.Double.isNaN(d) => NanReplacement
        case m => m
      }

      res.map((map: Map[String, Any]) => map.map {
        case (k, v) => k -> convert(v)
      })
    }
  }

  override protected def initTest() {
    super.initTest()
    kernelMonitors.addMonitorListener(NewPlannerMonitor)
  }

  protected def failWithError(expectedSpecificFailureFrom: TestConfiguration,
                              query: String,
                              message: Seq[String] = Seq.empty,
                              errorType: Seq[String] = Seq.empty,
                              params: Map[String, Any] = Map.empty): Unit = {
    // Never consider Morsel even if test requests it
    val expectedSpecificFailureFromEffective = expectedSpecificFailureFrom - Configs.Morsel

    val explicitlyRequestedExperimentalScenarios = expectedSpecificFailureFromEffective.scenarios intersect Configs.Experimental.scenarios
    val scenariosToExecute = Configs.All.scenarios ++ explicitlyRequestedExperimentalScenarios
    for (thisScenario <- scenariosToExecute) {
      val expectedToFailWithSpecificMessage = expectedSpecificFailureFromEffective.containsScenario(thisScenario)

      val tryResult: Try[RewindableExecutionResult] = Try(innerExecute(s"CYPHER ${thisScenario.preparserOptions} $query", params))
      tryResult match {
        case Success(_) =>
          if (expectedToFailWithSpecificMessage) {
            fail("Unexpectedly Succeeded in " + thisScenario.name)
          }
        // It was not expected to fail with the specified error message, do nothing
        case Failure(e: Throwable) =>  {
          val actualErrorType = e.toString
          if (expectedToFailWithSpecificMessage) {
            if (!correctError(actualErrorType, errorType)) {
              fail("Correctly failed in " + thisScenario.name + " but instead of one the given error types, the error was '" + actualErrorType + "'", e)
            }
            if (!correctError(e.getMessage, message)) {
              fail("Correctly failed in " + thisScenario.name + " but instead of one of the given messages, the error message was '" + e.getMessage + "'", e)
            }
          } else {
            if (correctError(e.getMessage, message) && correctError(actualErrorType, errorType)) {
              fail("Unexpectedly (but correctly!) failed in " + thisScenario.name + " with the correct error. Did you forget to add this config?", e)
            }
          }
        }
      }
    }
  }

  /**
    * Execute query on all compatibility versions and dump the result into a string.
    *
    * Asserts that the same string is created by all versions and return that string. No other preparser
    * options are given except for the version.
    */
  protected def dumpToString(query: String,
                             params: Map[String, Any] = Map.empty): String = {

    case class DumpResult(maybeResult:Try[String], version: Version)

    val paramValue = ExecutionEngineHelper.asMapValue(params)
    val results: Seq[DumpResult] =
      Versions.orderedVersions.map {
        version => {
          val queryText = s"CYPHER ${version.name} $query"
          val txContext = transactionalContext(queryText -> params)
          val maybeResult =
            Try(eengineExecute(queryText, paramValue, txContext).resultAsString())
          DumpResult(maybeResult, version)
        }
      }

    val successes = results.filter(_.maybeResult.isSuccess)
    if (successes.isEmpty)
      fail(s"No compatibility mode managed to execute ´$query´")

    val reference = successes.head.maybeResult.get
    for (result <- results) {
      withClue(s"Failed with version '${result.version.name}'") {
        result.maybeResult.get should equal(reference)
      }
    }
    reference
  }

  /**
    * Execute a query with different pre-parser options and
    * assert which configurations should success and which ones should fail.
    */
  protected def executeWith(expectSucceed: TestConfiguration,
                            query: String,
                            expectedDifferentResults: TestConfiguration = Configs.Empty,
                            planComparisonStrategy: PlanComparisonStrategy = DoNotComparePlans,
                            resultAssertionInTx: Option[RewindableExecutionResult => Unit] = None,
                            executeBefore: () => Unit = () => {},
                            executeExpectedFailures: Boolean = true,
                            params: Map[String, Any] = Map.empty): RewindableExecutionResult = {
    // Never consider Morsel even if test requests it
    val expectSucceedEffective = expectSucceed - Configs.Morsel

    if (expectSucceedEffective.scenarios.nonEmpty) {
      val expectedDifferentResultsEffective = expectedDifferentResults - Configs.Morsel
      val compareResults = expectSucceedEffective - expectedDifferentResultsEffective
      val baseScenario = extractBaseScenario(expectSucceedEffective, compareResults)
      val explicitlyRequestedExperimentalScenarios = expectSucceedEffective.scenarios intersect Configs.Experimental.scenarios

      val positiveResults = ((Configs.All.scenarios ++ explicitlyRequestedExperimentalScenarios) - baseScenario).flatMap {
        thisScenario =>
          executeScenario(thisScenario,
                          query,
                          expectSucceedEffective.containsScenario(thisScenario),
                          executeBefore,
                          params,
                          resultAssertionInTx,
                          executeExpectedFailures)
      }

      //Must be run last and have no rollback to be able to do certain result assertions
      val baseOption = executeScenario(baseScenario,
                                       query,
                                       expectedToSucceed = true,
                                       executeBefore,
                                       params,
                                       resultAssertionInTx = None,
                                       executeExpectedFailures = false,
                                       shouldRollback = false)

      // Assumption: baseOption.get is safe because the baseScenario is expected to succeed
      val baseResult = baseOption.get._2

      positiveResults.foreach {
        case (scenario, result) =>
          planComparisonStrategy.compare(expectSucceedEffective, scenario, result)

          if (compareResults.containsScenario(scenario)) {
            assertResultsSame(result, baseResult, query, s"${scenario.name} returned different results than ${baseScenario.name}")
          } else {
            assertResultsNotSame(result, baseResult, query, s"Unexpectedly (but correctly!)\n${scenario.name} returned same results as ${baseScenario.name}")
          }
      }
      baseResult
    } else {
      /**
        * If we are ending up here we don't expect any config to succeed i.e. Configs.Empty was used.
        * Currently this only happens when we use a[xxxException] should be thrownBy...
        * Consider to not allow this, but always use failWithError instead.
        * For now, don't support plan comparisons and only run som default config without a transaction to get a result.
        */
      if (planComparisonStrategy != DoNotComparePlans) {
        fail("At least one scenario must be expected to succeed to be able to compare plans")
      }

      val baseScenario = TestScenario(Versions.V3_5, Planners.Cost, Runtimes.Interpreted)
      executeBefore()
      val baseResult = innerExecute(s"CYPHER ${baseScenario.preparserOptions} $query", params)
      baseResult
    }
  }

  @deprecated("Rewrite to use executeWith instead")
  protected def assertResultsSameDeprecated(result1: RewindableExecutionResult, result2: RewindableExecutionResult, queryText: String, errorMsg: String, replaceNaNs: Boolean = false): Unit =
    assertResultsSame(result1, result2, queryText, errorMsg, replaceNaNs)

  /**
    * Execute a single CYPHER query (without multiple different pre-parser options). Obtain a RewindableExecutionResult.
    */
  protected def executeSingle(queryText: String, params: Map[String, Any] = Map.empty): RewindableExecutionResult =
    innerExecute(queryText, params)

  private def correctError(actualError: String, possibleErrors: Seq[String]): Boolean = {
    possibleErrors == Seq.empty || (actualError != null && possibleErrors.exists(s => actualError.replaceAll("\\r", "").contains(s.replaceAll("\\r", ""))))
  }

  private def extractBaseScenario(expectSucceed: TestConfiguration, compareResults: TestConfiguration): TestScenario = {
    val scenariosToChooseFrom = if (compareResults.scenarios.isEmpty) expectSucceed else compareResults

    if (scenariosToChooseFrom.scenarios.isEmpty) {
      fail("At least one scenario must be expected to succeed, to be comparable with plan and result")
    }
    val preferredScenario = TestScenario(Versions.V3_5, Planners.Cost, Runtimes.Interpreted)
    if (scenariosToChooseFrom.containsScenario(preferredScenario))
      preferredScenario
    else
      scenariosToChooseFrom.scenarios.head
  }

  private def executeScenario(scenario: TestScenario,
                              query: String,
                              expectedToSucceed: Boolean,
                              executeBefore: () => Unit,
                              params: Map[String, Any],
                              resultAssertionInTx: Option[RewindableExecutionResult => Unit],
                              executeExpectedFailures: Boolean,
                              shouldRollback: Boolean = true): Option[(TestScenario, RewindableExecutionResult)] = {

    def execute() = {
      executeBefore()
      val tryRes =
        if (expectedToSucceed || executeExpectedFailures)
          Try(innerExecute(s"CYPHER ${scenario.preparserOptions} $query", params))
        else Failure(NotExecutedException)
      if (expectedToSucceed && resultAssertionInTx.isDefined) {
        tryRes match {
          case Success(thisResult) =>
            withClue(s"result in transaction for ${scenario.name}\n") {
              resultAssertionInTx.get.apply(thisResult)
            }
          case Failure(_) =>
          // No need to do anything: will be handled by match below
        }
      }
      if (expectedToSucceed) {
        tryRes match {
          case Success(thisResult) =>
            scenario.checkResultForSuccess(query, thisResult)
            Some(scenario -> thisResult)
          case Failure(e) =>
            fail(s"Expected to succeed in ${scenario.name} but got exception", e)
        }
      } else {
        scenario.checkResultForFailure(query, tryRes)
        None
      }
    }

    if (shouldRollback) rollback(execute()) else inTx(execute())
  }

  private def assertResultsSame(result1: RewindableExecutionResult, result2: RewindableExecutionResult, queryText: String, errorMsg: String, replaceNaNs: Boolean = false): Unit = {
    withClue(errorMsg) {
      if (queryText.toLowerCase contains "order by") {
        result1.toComparableResultWithOptions(replaceNaNs) should contain theSameElementsInOrderAs result2.toComparableResultWithOptions(replaceNaNs)
      } else {
        result1.toComparableResultWithOptions(replaceNaNs) should contain theSameElementsAs result2.toComparableResultWithOptions(replaceNaNs)
      }
    }
  }

  private def assertResultsNotSame(result1: RewindableExecutionResult, result2: RewindableExecutionResult, queryText: String, errorMsg: String, replaceNaNs: Boolean = false): Unit = {
    withClue(errorMsg) {
      if (queryText.toLowerCase contains "order by") {
        result1.toComparableResultWithOptions(replaceNaNs) shouldNot contain theSameElementsInOrderAs result2.toComparableResultWithOptions(replaceNaNs)
      } else {
        result1.toComparableResultWithOptions(replaceNaNs) shouldNot contain theSameElementsAs result2.toComparableResultWithOptions(replaceNaNs)
      }
    }
  }

  private def innerExecute(queryText: String, params: Map[String, Any]): RewindableExecutionResult = {
    val innerResult: Result = eengineExecute(queryText, ExecutionEngineHelper.asMapValue(params), transactionalContext(queryText -> params))
    makeRewinadable(innerResult)
  }
}

object NewPlannerMonitor {

  sealed trait NewPlannerMonitorCall {
    def stackTrace: String
  }

  final case class UnableToHandleQuery(stackTrace: String) extends NewPlannerMonitorCall

  final case class NewQuerySeen(stackTrace: String) extends NewPlannerMonitorCall

}

object NewRuntimeMonitor {

  sealed trait NewRuntimeMonitorCall {
    def stackTrace: String
  }

  final case class UnableToCompileQuery(stackTrace: String) extends NewRuntimeMonitorCall

  final case class NewPlanSeen(stackTrace: String) extends NewRuntimeMonitorCall

}

object NotExecutedException extends Exception
