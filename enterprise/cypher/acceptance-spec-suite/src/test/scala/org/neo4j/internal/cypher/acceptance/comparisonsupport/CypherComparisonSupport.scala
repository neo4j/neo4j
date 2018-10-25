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
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory
import org.neo4j.test.TestGraphDatabaseFactory
import org.opencypher.v9_0.util.Eagerly
import org.opencypher.v9_0.util.test_helpers.CypherTestSupport
import org.scalatest.Assertions
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

import scala.util.Failure
import scala.util.Success
import scala.util.Try

trait CypherComparisonSupport extends CypherTestSupport {
  self: ExecutionEngineFunSuite =>

  import CypherComparisonSupport._

  override def databaseConfig(): collection.Map[Setting[_], String] = {
    Map(GraphDatabaseSettings.cypher_hints_error -> "true")
  }

  override protected def createDatabaseFactory(): TestGraphDatabaseFactory = new TestEnterpriseGraphDatabaseFactory()

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
    self.kernelMonitors.addMonitorListener(newPlannerMonitor)
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

  private def correctError(actualError: String, possibleErrors: Seq[String]): Boolean = {
    possibleErrors == Seq.empty || (actualError != null && possibleErrors.exists(s => actualError.replaceAll("\\r", "").contains(s.replaceAll("\\r", ""))))
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

    val paramValue = asMapValue(params)
    val results: Seq[DumpResult] =
      Versions.orderedVersions.map {
        version => {
          val queryText = s"CYPHER ${version.name} $query"
          val txContext = graph.transactionalContext(query = queryText -> params)
          val maybeResult =
            Try(eengine.execute(queryText, paramValue, txContext).resultAsString())
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

  protected def executeWith(expectSucceed: TestConfiguration,
                            query: String,
                            expectedDifferentResults: TestConfiguration = Configs.Empty,
                            planComparisonStrategy: PlanComparisonStrategy = DoNotComparePlans,
                            resultAssertionInTx: Option[RewindableExecutionResult => Unit] = None,
                            executeBefore: () => Unit = () => {},
                            executeExpectedFailures: Boolean = true,
                            params: Map[String, Any] = Map.empty): RewindableExecutionResult = {
    // TODO this is weird
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
                                       rollback = false)

      // Assumption: baseOption.get is safe because the baseScenario is expected to succeed
      val baseResult = baseOption.get._2

      // TODO we do not run this for the base scenario
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
                              rollback: Boolean = true): Option[(TestScenario, RewindableExecutionResult)] = {

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

    // TODO test the case where the default (base?) scenario is expected to fail
    if (rollback) graph.rollback(execute()) else graph.inTx(execute())
  }

  @deprecated("Rewrite to use executeWith instead")
  protected def assertResultsSameDeprecated(result1: RewindableExecutionResult, result2: RewindableExecutionResult, queryText: String, errorMsg: String, replaceNaNs: Boolean = false): Unit =
    assertResultsSame(result1, result2, queryText, errorMsg, replaceNaNs)

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

  // Should this really be deprecated? We have real use cases where we want to get the RewindableExecutionResult
  // But do NOT want comparison support, for example see the query statistics support used in CompositeNodeKeyAcceptanceTests
  @deprecated("Rewrite to use executeWith instead")
  protected def innerExecuteDeprecated(queryText: String, params: Map[String, Any] = Map.empty): RewindableExecutionResult =
    innerExecute(queryText, params)

  private def innerExecute(queryText: String, params: Map[String, Any]): RewindableExecutionResult = {
    val innerResult: Result = eengine.execute(queryText, asMapValue(params), graph.transactionalContext(query = queryText -> params))
    RewindableExecutionResult(innerResult)
  }

  def evaluateTo(expected: Seq[Map[String, Any]]): Matcher[RewindableExecutionResult] = new Matcher[RewindableExecutionResult] {
    override def apply(actual: RewindableExecutionResult): MatchResult = {
      MatchResult(
        matches = actual.toComparableResult == expected.toComparableSeq(replaceNaNs = false),
        rawFailureMessage = s"Results differ: ${actual.toComparableResult} did not equal to $expected",
        rawNegatedFailureMessage = s"Results are equal")
    }
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
object CypherComparisonSupport {

  val newPlannerMonitor = NewPlannerMonitor

  sealed trait PlanComparisonStrategy extends Assertions {
    def compare(expectSucceed: TestConfiguration, scenario: TestScenario, result: RewindableExecutionResult): Unit
  }

  case object DoNotComparePlans extends PlanComparisonStrategy {
    override def compare(expectSucceed: TestConfiguration, scenario: TestScenario, result: RewindableExecutionResult): Unit = {}
  }

  case class ComparePlansWithPredicate(predicate: InternalPlanDescription => Boolean,
                                       expectPlansToFailPredicate: TestConfiguration = TestConfiguration.empty,
                                       predicateFailureMessage: String = "") extends PlanComparisonStrategy {
    override def compare(expectSucceed: TestConfiguration, scenario: TestScenario, result: RewindableExecutionResult): Unit = {
      val comparePlans = expectSucceed - expectPlansToFailPredicate
      if (comparePlans.containsScenario(scenario)) {
        if (!predicate(result.executionPlanDescription())) {
          fail(s"plan for ${scenario.name} did not fulfill predicate.\n$predicateFailureMessage\n${result.executionPlanString()}")
        }
      } else {
        if (predicate(result.executionPlanDescription())) {
          fail(s"plan for ${scenario.name} did unexpectedly fulfill predicate\n$predicateFailureMessage\n${result.executionPlanString()}")
        }
      }
    }
  }

  case class ComparePlansWithAssertion(assertion: InternalPlanDescription => Unit,
                                       expectPlansToFail: TestConfiguration = TestConfiguration.empty) extends PlanComparisonStrategy {
    override def compare(expectSucceed: TestConfiguration, scenario: TestScenario, result: RewindableExecutionResult): Unit = {
      val comparePlans = expectSucceed - expectPlansToFail
      if (comparePlans.containsScenario(scenario)) {
        withClue(s"plan for ${scenario.name}\n") {
          assertion(result.executionPlanDescription())
        }
      } else {
        val tryResult = Try(assertion(result.executionPlanDescription()))
        tryResult match {
          case Success(_) =>
            fail(s"plan for ${scenario.name} did unexpectedly succeed \n${result.executionPlanString()}")
          case Failure(_) =>
          // Expected to fail
        }
      }
    }
  }

  object NotExecutedException extends Exception
}
