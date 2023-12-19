/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher._
import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.NewRuntimeSuccessRateMonitor
import org.neo4j.cypher.internal.compiler.v3_1.{CartesianPoint => CartesianPointv3_1, GeographicPoint => GeographicPointv3_1}
import org.neo4j.cypher.internal.compiler.v3_4.planner.CantCompileQueryException
import org.neo4j.cypher.internal.runtime.InternalExecutionResult
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments.{Planner => IPDPlanner, PlannerVersion => IPDPlannerVersion, Runtime => IPDRuntime, RuntimeVersion => IPDRuntimeVersion}
import org.neo4j.cypher.internal.util.v3_4.Eagerly
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherTestSupport
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.helpers.Exceptions
import org.neo4j.internal.cypher.acceptance.NewRuntimeMonitor.{NewPlanSeen, NewRuntimeMonitorCall, UnableToCompileQuery}
import org.neo4j.test.{TestEnterpriseGraphDatabaseFactory, TestGraphDatabaseFactory}
import org.neo4j.values.storable.{CoordinateReferenceSystem, Values}
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory
import org.scalatest.Assertions
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.util.{Failure, Success, Try}

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
  implicit class RichInternalExecutionResults(res: InternalExecutionResult) {
    def toComparableResultWithOptions(replaceNaNs: Boolean): Seq[Map[String, Any]] = res.toList.toComparableSeq(replaceNaNs)

    def toComparableResult: Seq[Map[String, Any]] = res.toList.toComparableSeq(replaceNaNs = false)
  }

  implicit class RichMapSeq(res: Seq[Map[String, Any]]) {

    import scala.collection.JavaConverters._

    object NanReplacement

    def toComparableSeq(replaceNaNs: Boolean): Seq[Map[String, Any]] = {
      def convert(v: Any): Any = v match {
        case p: GeographicPointv3_1 => Values.pointValue(CoordinateReferenceSystem.get(p.crs.url), p.longitude, p.latitude)
        case p: CartesianPointv3_1 => Values.pointValue(CoordinateReferenceSystem.get(p.crs.url), p.x, p.y)
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
    self.kernelMonitors.addMonitorListener(newRuntimeMonitor)
  }

  protected def failWithError(expectedSpecificFailureFrom: TestConfiguration,
                              query: String,
                              message: Seq[String] = Seq.empty,
                              errorType: Seq[String] = Seq.empty,
                              params: Map[String, Any] = Map.empty): Unit = {
    val explicitlyRequestedExperimentalScenarios = expectedSpecificFailureFrom.scenarios intersect Configs.Experimental.scenarios
    val scenariosToExecute = Configs.AbsolutelyAll.scenarios ++ explicitlyRequestedExperimentalScenarios
    for (thisScenario <- scenariosToExecute) {
      thisScenario.prepare()
      val expectedToFailWithSpecificMessage = expectedSpecificFailureFrom.containsScenario(thisScenario)

      val tryResult: Try[InternalExecutionResult] = Try(innerExecute(s"CYPHER ${thisScenario.preparserOptions} $query", params))
      tryResult match {
        case (Success(_)) =>
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

  protected def executeWith(expectSucceed: TestConfiguration,
                            query: String,
                            expectedDifferentResults: TestConfiguration = Configs.Empty,
                            planComparisonStrategy: PlanComparisonStrategy = DoNotComparePlans,
                            resultAssertionInTx: Option[(InternalExecutionResult) => Unit] = None,
                            executeBefore: () => Unit = () => {},
                            params: Map[String, Any] = Map.empty): InternalExecutionResult = {
    if (expectSucceed.scenarios.nonEmpty) {
      val compareResults = expectSucceed - expectedDifferentResults
      val baseScenario = extractBaseScenario(expectSucceed, compareResults)
      val explicitlyRequestedExperimentalScenarios = expectSucceed.scenarios intersect Configs.Experimental.scenarios

      val positiveResults = ((Configs.AbsolutelyAll.scenarios ++ explicitlyRequestedExperimentalScenarios) - baseScenario).flatMap {
        thisScenario =>
          executeScenario(thisScenario, query, expectSucceed.containsScenario(thisScenario), executeBefore, params, resultAssertionInTx)
      }

      //Must be run last and have no rollback to be able to do certain result assertions
      val baseOption = executeScenario(baseScenario, query, expectedToSucceed = true, executeBefore, params, resultAssertionInTx = None, rollback = false)

      // Assumption: baseOption.get is safe because the baseScenario is expected to succeed
      val baseResult = baseOption.get._2
      //must also check planComparisonStrategy on baseScenario
      planComparisonStrategy.compare(expectSucceed, baseScenario, baseResult)

      positiveResults.foreach {
        case (scenario, result) =>
          planComparisonStrategy.compare(expectSucceed, scenario, result)

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

      val baseScenario = TestScenario(Versions.Default, Planners.Default, Runtimes.Interpreted)
      baseScenario.prepare()
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
    val preferredScenario = TestScenario(Versions.Default, Planners.Default, Runtimes.Interpreted)
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
                              resultAssertionInTx: Option[(InternalExecutionResult) => Unit],
                              rollback: Boolean = true) = {
    scenario.prepare()

    def execute = {
      executeBefore()
      val tryRes = Try(innerExecute(s"CYPHER ${scenario.preparserOptions} $query", params))
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
      tryRes
    }

    val tryResult = if (rollback) graph.rollback(execute) else execute

    if (expectedToSucceed) {
      tryResult match {
        case Success(thisResult) =>
          scenario.checkResultForSuccess(query, thisResult)
          Some(scenario -> thisResult)
        case Failure(e) =>
          fail(s"Expected to succeed in ${scenario.name} but got exception", e)
      }
    } else {
      scenario.checkResultForFailure(query, tryResult)
      None
    }
  }

  @deprecated("Rewrite to use executeWith instead")
  protected def assertResultsSameDeprecated(result1: InternalExecutionResult, result2: InternalExecutionResult, queryText: String, errorMsg: String, replaceNaNs: Boolean = false): Unit =
    assertResultsSame(result1, result2, queryText, errorMsg, replaceNaNs)

  private def assertResultsSame(result1: InternalExecutionResult, result2: InternalExecutionResult, queryText: String, errorMsg: String, replaceNaNs: Boolean = false): Unit = {
    withClue(errorMsg) {
      if (queryText.toLowerCase contains "order by") {
        result1.toComparableResultWithOptions(replaceNaNs) should contain theSameElementsInOrderAs result2.toComparableResultWithOptions(replaceNaNs)
      } else {
        result1.toComparableResultWithOptions(replaceNaNs) should contain theSameElementsAs result2.toComparableResultWithOptions(replaceNaNs)
      }
    }
  }

  private def assertResultsNotSame(result1: InternalExecutionResult, result2: InternalExecutionResult, queryText: String, errorMsg: String, replaceNaNs: Boolean = false): Unit = {
    withClue(errorMsg) {
      if (queryText.toLowerCase contains "order by") {
        result1.toComparableResultWithOptions(replaceNaNs) shouldNot contain theSameElementsInOrderAs result2.toComparableResultWithOptions(replaceNaNs)
      } else {
        result1.toComparableResultWithOptions(replaceNaNs) shouldNot contain theSameElementsAs result2.toComparableResultWithOptions(replaceNaNs)
      }
    }
  }

  // Should this really be deprecated? We have real use cases where we want to get the InternalExecutionResult
  // But do NOT want comparison support, for example see the query statistics support used in CompositeNodeKeyAcceptanceTests
  @deprecated("Rewrite to use executeWith instead")
  protected def innerExecuteDeprecated(queryText: String, params: Map[String, Any] = Map.empty): InternalExecutionResult =
    innerExecute(queryText, params)

  private def innerExecute(queryText: String, params: Map[String, Any]): InternalExecutionResult = {
    val innerResult: Result = eengine.execute(queryText, params, graph.transactionalContext(query = queryText -> params))
    RewindableExecutionResult(innerResult)
  }

  def evaluateTo(expected: Seq[Map[String, Any]]): Matcher[InternalExecutionResult] = new Matcher[InternalExecutionResult] {
    override def apply(actual: InternalExecutionResult): MatchResult = {
      MatchResult(
        matches = actual.toComparableResult == expected.toComparableSeq(replaceNaNs = false),
        rawFailureMessage = s"Results differ: ${actual.toComparableResult} did not equal to $expected",
        rawNegatedFailureMessage = s"Results are equal")
    }
  }
}

class NewRuntimeMonitor extends NewRuntimeSuccessRateMonitor {
  private var traceBuilder = List.newBuilder[NewRuntimeMonitorCall]

  override def unableToHandlePlan(plan: LogicalPlan, e: CantCompileQueryException) {
    traceBuilder += UnableToCompileQuery(Exceptions.stringify(e))
  }

  override def newPlanSeen(plan: LogicalPlan) {
    traceBuilder += NewPlanSeen(plan.toString)
  }

  def trace = traceBuilder.result()

  def clear() {
    traceBuilder.clear()
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
  * For every query tested using `testWith`, the query will be run against all configurations. Every configuration
  * is expected to either succeed or fail. When new features are added that enable queries in new configurations,
  * acceptance tests will start failing because now a configuration is succeeding that was not successful before.
  *
  * This is expected and useful - it let's us know how a change impacts how many acceptance tests now start
  * succeeding where they weren't earlier.
  */
object CypherComparisonSupport {

  val newPlannerMonitor = NewPlannerMonitor

  val newRuntimeMonitor = new NewRuntimeMonitor

  case class Versions(versions: Version*) {
    def +(other: Version): Versions = {
      val newVersions = if (!versions.contains(other)) versions :+ other else versions
      Versions(newVersions: _*)
    }
  }

  object Versions {
    val orderedVersions: Seq[Version] = Seq(V2_3, V3_1, V3_3, V3_4)

    implicit def versionToVersions(version: Version): Versions = Versions(version)

    val oldest: Version = orderedVersions.head
    val latest: Version = orderedVersions.last
    val all = Versions(orderedVersions: _*)

    object V2_3 extends Version("2.3")

    object V3_1 extends Version("3.1")

    object V3_3 extends Version("3.3") {
      // 3.3 has 3.4 runtime
      override val acceptedRuntimeVersionNames = Set("3.4")
    }

    object V3_4 extends Version("3.4")

    object Default extends Version("") {
      override val acceptedRuntimeVersionNames = Set("2.3", "3.1", "3.3", "3.4")
      override val acceptedPlannerVersionNames = Set("2.3", "3.1", "3.3", "3.4")
    }

  }

  case class Version(name: String) {
    // inclusive
    def ->(other: Version): Versions = {
      val fromIndex = Versions.orderedVersions.indexOf(this)
      val toIndex = Versions.orderedVersions.indexOf(other) + 1
      Versions(Versions.orderedVersions.slice(fromIndex, toIndex): _*)
    }

    val acceptedRuntimeVersionNames: Set[String] = Set(name)
    val acceptedPlannerVersionNames: Set[String] = Set(name)

  }


  case class Planners(planners: Planner*)

  object Planners {

    val all = Planners(Cost, Rule)

    implicit def plannerToPlanners(planner: Planner): Planners = Planners(planner)

    object Cost extends Planner(Set("COST", "IDP"), "planner=cost")

    object Rule extends Planner(Set("RULE"), "planner=rule")

    object Default extends Planner(Set("COST", "IDP", "RULE", "PROCEDURE"), "")

  }

  case class Planner(acceptedPlannerNames: Set[String], preparserOption: String)


  case class Runtimes(runtimes: Runtime*)

  object Runtimes {

    // Default behaves different from specifying a specific runtime - thus it's included
    val all = Runtimes(CompiledBytecode, CompiledSource, Slotted, Interpreted, ProcedureOrSchema, Default)

    implicit def runtimeToRuntimes(runtime: Runtime): Runtimes = Runtimes(runtime)

    object CompiledSource extends Runtime(Set("COMPILED"), "runtime=compiled debug=generate_java_source")

    object CompiledBytecode extends Runtime(Set("COMPILED"), "runtime=compiled")

    object Slotted extends Runtime(Set("SLOTTED"), "runtime=slotted")

    object Interpreted extends Runtime(Set("INTERPRETED"), "runtime=interpreted")

    object ProcedureOrSchema extends Runtime(Set("PROCEDURE"), "")

    object Default extends Runtime(Set("COMPILED", "SLOTTED", "INTERPRETED", "PROCEDURE"), "")

  }

  case class Runtime(acceptedRuntimeNames: Set[String], preparserOption: String)


  sealed trait PlanComparisonStrategy extends Assertions {
    def compare(expectSucceed: TestConfiguration, scenario: TestScenario, result: InternalExecutionResult): Unit
  }

  case object DoNotComparePlans extends PlanComparisonStrategy {
    override def compare(expectSucceed: TestConfiguration, scenario: TestScenario, result: InternalExecutionResult): Unit = {}
  }

  case class ComparePlansWithPredicate(predicate: (InternalPlanDescription) => Boolean,
                                       expectPlansToFailPredicate: TestConfiguration = TestConfiguration.empty,
                                       predicateFailureMessage: String = "") extends PlanComparisonStrategy {
    override def compare(expectSucceed: TestConfiguration, scenario: TestScenario, result: InternalExecutionResult): Unit = {
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

  case class ComparePlansWithAssertion(assertion: (InternalPlanDescription) => Unit,
                                       expectPlansToFail: TestConfiguration = TestConfiguration.empty) extends PlanComparisonStrategy {
    override def compare(expectSucceed: TestConfiguration, scenario: TestScenario, result: InternalExecutionResult): Unit = {
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

  /**
    * A single scenario, which can be composed to configurations.
    */
  case class TestScenario(version: Version, planner: Planner, runtime: Runtime) extends Assertions {

    def name: String = {
      val versionName = if (version == Versions.Default) "<default version>" else version.name
      val plannerName = if (planner == Planners.Default) "<default planner>" else planner.preparserOption
      val runtimeName = runtime match {
        case Runtimes.Default => "<default runtime>"
        case Runtimes.ProcedureOrSchema => "<procedure or schema runtime>"
        case _ => runtime.preparserOption
      }
      s"${versionName} ${plannerName} ${runtimeName}"
    }

    def preparserOptions: String = s"${version.name} ${planner.preparserOption} ${runtime.preparserOption}"

    def prepare(): Unit = newRuntimeMonitor.clear()

    def checkResultForSuccess(query: String, internalExecutionResult: InternalExecutionResult): Unit = {
      val (reportedRuntime: String, reportedPlanner: String, reportedVersion: String, reportedPlannerVersion: String) = extractConfiguration(internalExecutionResult)
      if (!runtime.acceptedRuntimeNames.contains(reportedRuntime))
        fail(s"did not use ${runtime.acceptedRuntimeNames} runtime - instead $reportedRuntime was used. Scenario $name")
      if (!planner.acceptedPlannerNames.contains(reportedPlanner))
        fail(s"did not use ${planner.acceptedPlannerNames} planner - instead $reportedPlanner was used. Scenario $name")
      if (!version.acceptedRuntimeVersionNames.contains(reportedVersion))
        fail(s"did not use ${version.acceptedRuntimeVersionNames} runtime version - instead $reportedVersion was used. Scenario $name")
      if (!version.acceptedPlannerVersionNames.contains(reportedPlannerVersion))
        fail(s"did not use ${version.acceptedPlannerVersionNames} planner version - instead $reportedPlannerVersion was used. Scenario $name")
    }

    def checkResultForFailure(query: String, internalExecutionResult: Try[InternalExecutionResult]): Unit = {
      internalExecutionResult match {
        case Failure(_) => // not unexpected
        case Success(result) =>
          val (reportedRuntimeName: String, reportedPlannerName: String, reportedVersionName: String, reportedPlannerVersionName: String) = extractConfiguration(result)

          if (runtime.acceptedRuntimeNames.contains(reportedRuntimeName)
            && planner.acceptedPlannerNames.contains(reportedPlannerName)
            && version.acceptedRuntimeVersionNames.contains(reportedVersionName)) {
            fail(s"Unexpectedly succeeded using $name for query $query, with $reportedVersionName $reportedRuntimeName runtime and $reportedPlannerVersionName $reportedPlannerName planner.")
          }
      }
    }

    private def extractConfiguration(result: InternalExecutionResult): (String, String, String, String) = {
      val arguments = result.executionPlanDescription().arguments
      val reportedRuntime = arguments.collectFirst {
        case IPDRuntime(reported) => reported
      }
      val reportedPlanner = arguments.collectFirst {
        case IPDPlanner(reported) => reported
      }
      val reportedVersion = arguments.collectFirst {
        case IPDRuntimeVersion(reported) => reported
      }
      val reportedPlannerVersion = arguments.collectFirst {
        case IPDPlannerVersion(reported) => reported
      }

      // Neo4j versions 3.2 and earlier do not accurately report when they used procedure runtime/planner,
      // in executionPlanDescription. In those versions, a missing runtime/planner is assumed to mean procedure
      val versionsWithUnreportedProcedureUsage = (Versions.V2_3 -> Versions.V3_1) + Versions.Default
      val (reportedRuntimeName, reportedPlannerName, reportedVersionName, reportedPlannerVersionName) =
        if (versionsWithUnreportedProcedureUsage.versions.contains(version))
          (reportedRuntime.getOrElse("PROCEDURE"), reportedPlanner.getOrElse("PROCEDURE"), reportedVersion.getOrElse("NONE"), reportedPlannerVersion.getOrElse("NONE"))
        else
          (reportedRuntime.get, reportedPlanner.get, reportedVersion.get, reportedPlannerVersion.get)
      (reportedRuntimeName, reportedPlannerName, reportedVersionName, reportedPlannerVersionName)
    }

    def +(other: TestConfiguration): TestConfiguration = other + this
  }

  /**
    * A set of scenarios.
    */
  case class TestConfiguration(scenarios: Set[TestScenario]) {

    def +(other: TestConfiguration): TestConfiguration = TestConfiguration(scenarios ++ other.scenarios)

    def -(other: TestConfiguration): TestConfiguration = TestConfiguration(scenarios -- other.scenarios)

    def containsScenario(scenario: TestScenario): Boolean = this.scenarios.contains(scenario)
  }

  object TestConfiguration {
    def apply(scenarios: TestScenario*): TestConfiguration = {
      TestConfiguration(scenarios.toSet)
    }

    def apply(versions: Versions, planners: Planners, runtimes: Runtimes): TestConfiguration = {
      val scenarios = for (v <- versions.versions;
                           p <- planners.planners;
                           r <- runtimes.runtimes)
        yield TestScenario(v, p, r)
      TestConfiguration(scenarios.toSet)
    }

    def empty: TestConfiguration = {
      TestConfiguration(Nil: _*)
    }

    implicit def scenarioToTestConfiguration(scenario: TestScenario): TestConfiguration = TestConfiguration(scenario)
  }

  object Configs {

    def Compiled: TestConfiguration = TestConfiguration(Versions.V3_4, Planners.Cost, Runtimes(Runtimes.CompiledSource, Runtimes.CompiledBytecode))

    def Interpreted: TestConfiguration =
      TestConfiguration(Versions.Default, Planners.Default, Runtimes(Runtimes.Interpreted, Runtimes.Slotted)) +
        TestConfiguration(Versions.V2_3 -> Versions.V3_1, Planners.all, Runtimes.Default) +
        TestScenario(Versions.Default, Planners.Rule, Runtimes.Default) +
        TestScenario(Versions.V3_3, Planners.Cost, Runtimes.Default)

    def CommunityInterpreted: TestConfiguration =
      TestScenario(Versions.Default, Planners.Default, Runtimes.Interpreted) +
        TestConfiguration(Versions.V2_3 -> Versions.V3_1, Planners.all, Runtimes.Default) +
        TestScenario(Versions.Default, Planners.Rule, Runtimes.Default) +
        TestScenario(Versions.V3_3, Planners.Cost, Runtimes.Default)

    def SlottedInterpreted: TestConfiguration = TestScenario(Versions.Default, Planners.Default, Runtimes.Slotted)

    def DefaultInterpreted: TestConfiguration = TestScenario(Versions.Default, Planners.Default, Runtimes.Interpreted)

    def DefaultRule: TestConfiguration = TestScenario(Versions.Default, Planners.Rule, Runtimes.Default)

    def Cost2_3: TestConfiguration = TestScenario(Versions.V2_3, Planners.Cost, Runtimes.Default)

    def Cost3_1: TestConfiguration = TestScenario(Versions.V3_1, Planners.Cost, Runtimes.Default)

    def Cost3_3: TestConfiguration = TestScenario(Versions.V3_3, Planners.Cost, Runtimes.Default)

    def Cost3_4: TestConfiguration = TestScenario(Versions.V3_4, Planners.Cost, Runtimes.Default)

    def Rule2_3: TestConfiguration = TestScenario(Versions.V2_3, Planners.Rule, Runtimes.Default)

    def Rule3_1: TestConfiguration = TestScenario(Versions.V3_1, Planners.Rule, Runtimes.Default)

    def CurrentRulePlanner: TestConfiguration = TestScenario(Versions.latest, Planners.Rule, Runtimes.Default)

    def Version2_3: TestConfiguration = TestConfiguration(Versions.V2_3, Planners.all, Runtimes.Default)

    def Version3_1: TestConfiguration = TestConfiguration(Versions.V3_1, Planners.all, Runtimes.Default)

    def Version3_3: TestConfiguration = TestConfiguration(Versions.V3_3, Planners.Cost, Runtimes.Default)

    def Version3_4: TestConfiguration =
      TestConfiguration(Versions.V3_4, Planners.Cost, Runtimes(Runtimes.CompiledSource, Runtimes.CompiledBytecode)) +
        TestConfiguration(Versions.Default, Planners.Default, Runtimes(Runtimes.Interpreted, Runtimes.Slotted)) +
        TestScenario(Versions.Default, Planners.Rule, Runtimes.Default)

    def AllRulePlanners: TestConfiguration = TestConfiguration(Versions(Versions.V2_3, Versions.V3_1, Versions.Default), Planners.Rule, Runtimes.Default)

    def BackwardsCompatibility: TestConfiguration = TestConfiguration(Versions.V2_3 -> Versions.V3_1, Planners.all, Runtimes.Default) +
      TestScenario(Versions.V3_3, Planners.Cost, Runtimes.Default)

    def Procs: TestConfiguration = TestScenario(Versions.Default, Planners.Default, Runtimes.ProcedureOrSchema)

    /**
      * Handy configs for things only supported from 3.3 (not rule) and for checking plans
      */
    def OldAndRule: TestConfiguration = Cost2_3 + Cost3_1 + AllRulePlanners

    /**
      * Configs which support CREATE, DELETE, SET, REMOVE, MERGE etc.
      */
    def UpdateConf: TestConfiguration = Interpreted - Cost2_3

    /*
    If you are unsure what you need, this is a good start. It's not really all scenarios, but this is testing all
    interesting scenarios.
     */
    def All: TestConfiguration = AbsolutelyAll - Procs

    def AllExceptSlotted: TestConfiguration = All - SlottedInterpreted

    /**
      * These are all configurations that will be executed even if not explicitly expected to succeed or fail.
      * Even if not explicitly requested, they are executed to check if they unexpectedly succeed to make sure that
      * test coverage is kept up-to-date with new features.
      */
    def AbsolutelyAll: TestConfiguration =
      TestConfiguration(Versions.V3_4, Planners.Cost, Runtimes(Runtimes.CompiledSource, Runtimes.CompiledBytecode)) +
        TestConfiguration(Versions.Default, Planners.Default, Runtimes(Runtimes.Interpreted, Runtimes.Slotted,
                                                                       Runtimes.ProcedureOrSchema)) +
        TestConfiguration(Versions.V2_3 -> Versions.V3_1, Planners.all, Runtimes.Default) +
        TestScenario(Versions.Default, Planners.Rule, Runtimes.Default) +
        TestScenario(Versions.V3_3, Planners.Cost, Runtimes.Default)

    /**
      * These experimental configurations will only be executed if you explicitly specify them in the test expectation.
      * I.e. there will be no check to see if they unexpectedly succeed on tests where they were not explicitly requested.
      */
    def Experimental: TestConfiguration =
      TestConfiguration.empty

    def Empty: TestConfiguration = TestConfiguration.empty

  }

}
