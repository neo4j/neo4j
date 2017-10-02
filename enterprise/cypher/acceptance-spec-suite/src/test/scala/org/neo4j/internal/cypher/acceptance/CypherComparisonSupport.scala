/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.InternalPlanDescription.Arguments.{Planner => IPDPlanner, Runtime => IPDRuntime, Version => IPDVersion}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{CRS, CartesianPoint, GeographicPoint}
import org.neo4j.cypher.internal.compiler.v3_1.{CartesianPoint => CartesianPointv3_1, GeographicPoint => GeographicPointv3_1}
import org.neo4j.cypher.internal.compiler.v3_2.{CartesianPoint => CartesianPointv3_2, GeographicPoint => GeographicPointv3_2}
import org.neo4j.cypher.internal.frontend.v3_3.helpers.Eagerly
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherTestSupport
import org.neo4j.cypher.internal.{InternalExecutionResult, RewindableExecutionResult}
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory
import org.scalacheck.Prop.True
import org.scalatest.Assertions
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

trait CypherComparisonSupport extends CypherTestSupport {
  self: ExecutionEngineFunSuite =>

  import CypherComparisonSupport._

  override def databaseConfig(): collection.Map[Setting[_], String] = {
    Map(GraphDatabaseSettings.cypher_hints_error -> "true")
  }

  override protected def createGraphDatabase(config: collection.Map[Setting[_], String] = databaseConfig()): GraphDatabaseCypherService = {
    new GraphDatabaseCypherService(new TestEnterpriseGraphDatabaseFactory().newImpermanentDatabase(config.asJava))
  }

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
        case p: GeographicPointv3_1 => GeographicPoint(p.longitude, p.latitude, CRS(p.crs.name, p.crs.code, p.crs.url))
        case p: CartesianPointv3_1 => CartesianPoint(p.x, p.y, CRS(p.crs.name, p.crs.code, p.crs.url))
        case p: GeographicPointv3_2 => GeographicPoint(p.longitude, p.latitude, CRS(p.crs.name, p.crs.code, p.crs.url))
        case p: CartesianPointv3_2 => CartesianPoint(p.x, p.y, CRS(p.crs.name, p.crs.code, p.crs.url))
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

  protected def failWithError(expectedSpecificFailureFrom: TestConfiguration, query: String, message: Seq[String], params: (String, Any)*):
  Unit = {
    for (thisScenario <- Configs.AbsolutelyAll.scenarios) {
      thisScenario.prepare()
      val expectedToFailWithSpecificMessage = expectedSpecificFailureFrom.containsScenario(thisScenario)

      val tryResult: Try[InternalExecutionResult] = Try(innerExecute(s"CYPHER ${thisScenario.preparserOptions} $query", params.toMap))
      tryResult match {
        case (Success(_))=>
          if(expectedToFailWithSpecificMessage) {
            fail("Unexpectedly Succeeded in " + thisScenario.name)
          }
          // It was not expected to fail with the specified error message, do nothing
        case Failure(e: CypherException) =>
          if (expectedToFailWithSpecificMessage) {
            if (e.getMessage == null || !message.exists(e.getMessage.contains(_))) {
              fail("Correctly failed in " + thisScenario.name + " but instead of one of the given messages, the error message was '" + e.getMessage + "'")
            }
          } else {
            if (message.exists(e.getMessage.contains(_))) {
              fail("Unexpectedly (but correctly!) failed in " + thisScenario.name + " with the correct message. Did you forget to add this config?")
            }
            // It failed like expected, and we did not specify any message for this config
          }
        case Failure(e: Throwable) => {
          if (expectedToFailWithSpecificMessage) {
            if (e.getMessage == null || !message.exists(e.getMessage.contains(_))) {
              fail(s"Unexpected exception in ${thisScenario.name} with error message " + e.getMessage, e)
            }
          }
        }
      }
    }
  }

  protected def executeWith(expectSucceed: TestConfiguration,
                            query: String,
                            expectedDifferentResults: TestConfiguration = Configs.Empty,
                            planComparisonStrategy: PlanComparisonStrategy = DoNotComparePlans,
                            executeBefore: () => Unit = () => {},
                            params: Map[String, Any] = Map.empty,
                            rollback: Boolean = true): InternalExecutionResult = {
    val compareResults = expectSucceed -  expectedDifferentResults
    val baseScenario =
      if (expectSucceed.scenarios.nonEmpty) extractBaseScenario(expectSucceed, compareResults)
      else TestScenario(Versions.Default, Planners.Default, Runtimes.Interpreted)

    val positiveResults = (Configs.AbsolutelyAll.scenarios - baseScenario).flatMap {
      thisScenario =>
        executeScenario(thisScenario, query, expectSucceed.containsScenario(thisScenario), executeBefore, params, rollback)
    }

    baseScenario.prepare()
    executeBefore()
    val baseResult = innerExecute(s"CYPHER ${baseScenario.preparserOptions} $query", params)
    baseScenario.checkResultForSuccess(query, baseResult)

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
  }

  private def extractBaseScenario(expectSucceed: TestConfiguration, compareResults: TestConfiguration): TestScenario = {
    val scenariosToChooseFrom = if (compareResults.scenarios.isEmpty) expectSucceed else compareResults

    if (scenariosToChooseFrom.scenarios.isEmpty) {
      fail("At least one scenario must be expected to succeed, be comparable with plan and result")
    }
    val preferredScenario = TestScenario(Versions.Default, Planners.Default, Runtimes.Interpreted)
    if (scenariosToChooseFrom.containsScenario(preferredScenario))
      preferredScenario
    else
      scenariosToChooseFrom.scenarios.head
  }

  private def executeScenario(scenario: TestScenario, query: String, expectedToSucceed: Boolean, executeBefore: () => Unit, params: Map[String, Any], rollback: Boolean) = {
    scenario.prepare()
    val tryResult = if (rollback) {
      graph.rollback(
        {
          executeBefore()
          Try(innerExecute(s"CYPHER ${scenario.preparserOptions} $query", params))
        })
    } else {
      executeBefore()
      Try(innerExecute(s"CYPHER ${scenario.preparserOptions} $query", params))
    }

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

  @deprecated("Rewrite to use executeWith instead")
  protected def innerExecuteDeprecated(queryText: String, params: Map[String, Any]): InternalExecutionResult =
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
    val orderedVersions: Seq[Version] = Seq(V2_3, V3_1, V3_2, V3_3)

    implicit def versionToVersions(version: Version): Versions = Versions(version)

    val oldest = orderedVersions.head
    val latest = orderedVersions.last
    val all = Versions(orderedVersions: _*)

    object V2_3 extends Version("2.3")

    object V3_1 extends Version("3.1")

    object V3_2 extends Version("3.2")

    object V3_3 extends Version("3.3")

    object Default extends Version("") {
      override  val acceptedVersionNames = Set("2.3", "3.1", "3.2", "3.3").map("CYPHER " + _)
    }

  }

  case class Version(name: String) {
    // inclusive
    def ->(other: Version): Versions = {
      val fromIndex = Versions.orderedVersions.indexOf(this)
      val toIndex = Versions.orderedVersions.indexOf(other) + 1
      Versions(Versions.orderedVersions.slice(fromIndex, toIndex): _*)
    }

    val acceptedVersionNames : Set[String] = Set("CYPHER " + name)

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

    object Default extends Runtime(Set("COMPILED", "SLOTTED" , "INTERPRETED", "PROCEDURE"), "")

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
  case class CompareResults(assertion: (InternalExecutionResult) => Unit,
                                       expectPlansToFail: TestConfiguration = TestConfiguration.empty) extends PlanComparisonStrategy {
    override def compare(expectSucceed: TestConfiguration, scenario: TestScenario, result: InternalExecutionResult): Unit = {
      val comparePlans = expectSucceed - expectPlansToFail
      if (comparePlans.containsScenario(scenario)) {
        withClue(s"result for ${scenario.name}\n") {
          assertion(result)
        }
      } else {
        val tryResult = Try(assertion(result))
        tryResult match {
          case Success(_) =>
            fail(s"result for ${scenario.name} did unexpectedly succeed \n${result.executionPlanString()}")
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
      internalExecutionResult.executionPlanDescription().arguments.collect {
        case IPDRuntime(reportedRuntime) if (!runtime.acceptedRuntimeNames.contains(reportedRuntime)) =>
          fail(s"did not use ${runtime.acceptedRuntimeNames} runtime - instead $reportedRuntime was used. Scenario $name")
        case IPDPlanner(reportedPlanner) if (!planner.acceptedPlannerNames.contains(reportedPlanner)) =>
          fail(s"did not use ${planner.acceptedPlannerNames} planner - instead $reportedPlanner was used. Scenario $name")
        case IPDVersion(reportedVersion) if(!version.acceptedVersionNames.contains(reportedVersion)) =>
          fail(s"did not use ${version.acceptedVersionNames} version - instead $reportedVersion was used. Scenario $name")
      }
    }

    def checkResultForFailure(query: String, internalExecutionResult: Try[InternalExecutionResult]): Unit = {
      internalExecutionResult match {
        case Failure(_) => // not unexpected
        case Success(result) =>
          val arguments = result.executionPlanDescription().arguments
          val reportedRuntime = arguments.collectFirst {
            case IPDRuntime(reportedRuntime) => reportedRuntime
          }
          val reportedPlanner = arguments.collectFirst {
            case IPDPlanner(reportedPlanner) => reportedPlanner
          }
          val reportedVersion = arguments.collectFirst {
            case IPDVersion(reportedVersion) => reportedVersion
          }

          // Neo4j versions 3.2 and earlier do not accurately report when they used procedure runtime/planner,
          // in executionPlanDescription. In those versions, a missing runtime/planner is assumed to mean procedure
          val versionsWithUnreportedProcedureUsage = (Versions.V2_3 -> Versions.V3_2) + Versions.Default
          val (reportedRuntimeName, reportedPlannerName, reportedVersionName) =
            if (versionsWithUnreportedProcedureUsage.versions.contains(version))
              (reportedRuntime.getOrElse("PROCEDURE"), reportedPlanner.getOrElse("PROCEDURE"), reportedVersion.getOrElse("NONE"))
            else
              (reportedRuntime.get, reportedPlanner.get, reportedVersion.get)

          if (runtime.acceptedRuntimeNames.contains(reportedRuntimeName)
            && planner.acceptedPlannerNames.contains(reportedPlannerName)
            && version.acceptedVersionNames.contains(reportedVersionName)) {
            fail(s"Unexpectedly succeeded using $name for query $query, with $reportedVersionName and $reportedRuntimeName runtime and $reportedPlannerName planner.nMatchAC")
          }
      }
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

    def Compiled: TestConfiguration = TestConfiguration(Versions.V3_3, Planners.Cost, Runtimes(Runtimes.CompiledSource, Runtimes.CompiledBytecode))

    def Interpreted: TestConfiguration =
      TestConfiguration(Versions.Default, Planners.Default, Runtimes(Runtimes.Interpreted, Runtimes.Slotted)) +
        TestConfiguration(Versions.V2_3 -> Versions.V3_2, Planners.all, Runtimes.Default) +
        TestScenario(Versions.Default, Planners.Rule, Runtimes.Default)

    def CommunityInterpreted: TestConfiguration =
      TestScenario(Versions.Default, Planners.Default, Runtimes.Interpreted) +
        TestConfiguration(Versions.V2_3 -> Versions.V3_2, Planners.all, Runtimes.Default) +
        TestScenario(Versions.Default, Planners.Rule, Runtimes.Default)

    def SlottedInterpreted: TestConfiguration = TestScenario(Versions.Default, Planners.Default, Runtimes.Slotted)

    def DefaultInterpreted: TestConfiguration = TestScenario(Versions.Default, Planners.Default, Runtimes.Interpreted)

    def Cost2_3: TestConfiguration = TestScenario(Versions.V2_3, Planners.Cost, Runtimes.Default)

    def Cost3_1: TestConfiguration = TestScenario(Versions.V3_1, Planners.Cost, Runtimes.Default)

    def Cost3_2: TestConfiguration = TestScenario(Versions.V3_2, Planners.Cost, Runtimes.Default)

    def Cost3_3: TestConfiguration = TestScenario(Versions.V3_3, Planners.Cost, Runtimes.Default)

    def Rule2_3: TestConfiguration = TestScenario(Versions.V2_3, Planners.Rule, Runtimes.Default)

    def Rule3_1: TestConfiguration = TestScenario(Versions.V3_1, Planners.Rule, Runtimes.Default)

    def Rule3_2: TestConfiguration = TestScenario(Versions.V3_2, Planners.Rule, Runtimes.Default)

    def CurrentRulePlanner: TestConfiguration = TestScenario(Versions.latest, Planners.Rule, Runtimes.Default)

    def Version2_3: TestConfiguration = TestConfiguration(Versions.V2_3, Planners.all, Runtimes.Default)

    def Version3_1: TestConfiguration = TestConfiguration(Versions.V3_1, Planners.all, Runtimes.Default)

    def Version3_2: TestConfiguration = TestConfiguration(Versions.V3_2, Planners.all, Runtimes.Default)

    def Version3_3: TestConfiguration =
      TestConfiguration(Versions.V3_3, Planners.Cost, Runtimes(Runtimes.CompiledSource, Runtimes.CompiledBytecode)) +
        TestConfiguration(Versions.Default, Planners.Default, Runtimes(Runtimes.Interpreted, Runtimes.Slotted)) +
        TestScenario(Versions.Default, Planners.Rule, Runtimes.Default)

    def AllRulePlanners: TestConfiguration = TestConfiguration(Versions(Versions.V2_3, Versions.V3_1, Versions.V3_2, Versions.Default), Planners.Rule, Runtimes.Default)

    def Cost: TestConfiguration =
      TestConfiguration(Versions.V3_3, Planners.Cost, Runtimes(Runtimes.CompiledSource, Runtimes.CompiledBytecode)) +
        TestConfiguration(Versions(Versions.V2_3, Versions.V3_1, Versions.Default), Planners.Cost, Runtimes.Default)

    def BackwardsCompatibility: TestConfiguration = TestConfiguration(Versions.V2_3 -> Versions.V3_2, Planners.all, Runtimes.Default)

    def Procs: TestConfiguration = TestScenario(Versions.Default, Planners.Default, Runtimes.ProcedureOrSchema)

    /*
    If you are unsure what you need, this is a good start. It's not really all scenarios, but this is testing all
    interesting scenarios.
     */
    def All: TestConfiguration =
      TestConfiguration(Versions.V3_3, Planners.Cost, Runtimes(Runtimes.CompiledSource, Runtimes.CompiledBytecode)) +
        TestConfiguration(Versions.Default, Planners.Default, Runtimes(Runtimes.Interpreted, Runtimes.Slotted)) +
        TestConfiguration(Versions.V2_3 -> Versions.V3_2, Planners.all, Runtimes.Default) +
        TestScenario(Versions.Default, Planners.Rule, Runtimes.Default)

    def AllExceptSlotted: TestConfiguration =
      TestConfiguration(Versions.V3_3, Planners.Cost, Runtimes(Runtimes.CompiledSource, Runtimes.CompiledBytecode)) +
        TestConfiguration(Versions.Default, Planners.Default, Runtimes.Interpreted) +
        TestConfiguration(Versions.V2_3 -> Versions.V3_2, Planners.all, Runtimes.Default) +
        TestScenario(Versions.Default, Planners.Rule, Runtimes.Default)

    def AbsolutelyAll: TestConfiguration =
      TestConfiguration(Versions.V3_3, Planners.Cost, Runtimes(Runtimes.CompiledSource, Runtimes.CompiledBytecode)) +
        TestConfiguration(Versions.Default, Planners.Default, Runtimes(Runtimes.Interpreted, Runtimes.Slotted, Runtimes.ProcedureOrSchema)) +
        TestConfiguration(Versions.V2_3 -> Versions.V3_2, Planners.all, Runtimes.Default) +
        TestScenario(Versions.Default, Planners.Rule, Runtimes.Default)

    def Empty: TestConfiguration = TestConfiguration.empty

  }

}
