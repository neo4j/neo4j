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

import org.neo4j.cypher.NewRuntimeMonitor.{NewPlanSeen, UnableToCompileQuery}
import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan.InternalExecutionResult
import org.neo4j.cypher.internal.compiler.v3_1.{CartesianPoint => CartesianPointv3_1, GeographicPoint => GeographicPointv3_1}
import org.neo4j.cypher.internal.compiler.v3_2.{CartesianPoint => CartesianPointv3_2, GeographicPoint => GeographicPointv3_2}
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.InternalPlanDescription.Arguments.{Planner, Runtime}
import org.neo4j.cypher.internal.frontend.v3_3.helpers.Eagerly
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherTestSupport
import org.neo4j.cypher.internal.{ExecutionResult, RewindableExecutionResult}
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerMonitor, NewRuntimeMonitor}
import org.scalatest.Assertions

import scala.util.{Failure, Success, Try}

trait CypherComparisonSupport extends CypherTestSupport {
  self: ExecutionEngineFunSuite =>

  import CypherComparisonSupport._

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

  val newPlannerMonitor = NewPlannerMonitor

  val newRuntimeMonitor = new NewRuntimeMonitor

  private def extractFirstScenario(config: TestConfiguration): TestScenario = {
    val preferredScenario = Scenarios.CommunityInterpreted
    if (config.scenarios.contains(preferredScenario))
      preferredScenario
    else
      config.scenarios.head
  }

  protected def testWithUpdate(expectedSuccessFrom: TestConfiguration,
                               query: String,
                               params: (String, Any)*): InternalExecutionResult = {
    val firstScenario = extractFirstScenario(expectedSuccessFrom)

    val positiveResults = (Configs.AbsolutelyAll.scenarios - firstScenario).flatMap {
      thisScenario =>
        thisScenario.prepare()

        val tryResult = graph.rollback(Try(innerExecute(s"CYPHER ${thisScenario.preparserOptions} $query", params.toMap)))

        val expectedToSucceed = expectedSuccessFrom.scenarios.contains(thisScenario)

        if (expectedToSucceed) {
          val thisResult = tryResult.get
          thisScenario.checkStateForSuccess(query)
          thisScenario.checkResultForSuccess(query, thisResult)
          Some(thisResult -> thisScenario.name)
        } else {
          thisScenario.checkStateForFailure(query)
          thisScenario.checkResultForFailure(query, tryResult)
          None
        }

    }

    firstScenario.prepare()
    val lastResult = innerExecute(s"CYPHER ${firstScenario.preparserOptions} $query", params.toMap)
    firstScenario.checkStateForSuccess(query)
    firstScenario.checkResultForSuccess(query, lastResult)

    positiveResults.foreach {
      case (result, name) =>
        assertResultsAreSame(result, lastResult, query, s"$name returned different results than ${firstScenario.name}")
    }

    lastResult
  }

  protected def succeedWith(expectedSuccessFrom: TestConfiguration, query: String, params: (String, Any)*):
  InternalExecutionResult = {
    if (expectedSuccessFrom.scenarios.isEmpty) {
      for (thisScenario <- Configs.AbsolutelyAll.scenarios) {
        thisScenario.prepare()
        val tryResult = Try(innerExecute(s"CYPHER ${thisScenario.preparserOptions} $query", params.toMap))
        thisScenario.checkStateForFailure(query)
        thisScenario.checkResultForFailure(query, tryResult)
      }
      null
    } else {
      val firstScenario = extractFirstScenario(expectedSuccessFrom)
      firstScenario.prepare()
      val firstResult: InternalExecutionResult = innerExecute(s"CYPHER ${firstScenario.preparserOptions} $query", params.toMap)
      firstScenario.checkStateForSuccess(query)

      for (thisScenario <- Configs.AbsolutelyAll.scenarios if thisScenario != firstScenario) {
        thisScenario.prepare()
        val tryResult = Try(innerExecute(s"CYPHER ${thisScenario.preparserOptions} $query", params.toMap))

        val expectedToSucceed = expectedSuccessFrom.scenarios.contains(thisScenario)

        if (expectedToSucceed) {
          val thisResult = tryResult.get
          thisScenario.checkStateForSuccess(query)
          thisScenario.checkResultForSuccess(query, thisResult)
          assertResultsAreSame(thisResult, firstResult, query, s"${thisScenario.name} returned different results than ${firstScenario.name}", replaceNaNs = true)
        } else {
          thisScenario.checkStateForFailure(query)
          thisScenario.checkResultForFailure(query, tryResult)
        }
      }

      firstResult
    }
  }

  protected def assertResultsAreSame(result1: InternalExecutionResult, result2: InternalExecutionResult, queryText: String, errorMsg: String, replaceNaNs: Boolean = false) {
    withClue(errorMsg) {
      if (queryText.toLowerCase contains "order by") {
        result1.toComparableResultWithOptions(replaceNaNs) should contain theSameElementsInOrderAs result2.toComparableResultWithOptions(replaceNaNs)
      } else {
        result1.toComparableResultWithOptions(replaceNaNs) should contain theSameElementsAs result2.toComparableResultWithOptions(replaceNaNs)
      }
    }
  }


  private def innerExecute(queryText: String, params: Map[String, Any]): InternalExecutionResult = {
    val innerResult = eengine.execute(queryText, params, graph.transactionalContext(query = queryText -> params))
    rewindableResult(innerResult)
  }

  private def rewindableResult(result: ExecutionResult): InternalExecutionResult = {
    result match {
      case e: ClosingExecutionResult => e.inner match {
        case _: v3_3.ExecutionResultWrapper => RewindableExecutionResult(e)
        case _: v3_2.ExecutionResultWrapper => RewindableExecutionResult(e)
        case _: v3_1.ExecutionResultWrapper => RewindableExecutionResult(e)
        case _: v2_3.ExecutionResultWrapper => RewindableExecutionResult(e)
      }
    }
  }

  object Configs {
    def CompiledSource: TestConfiguration = Scenarios.CompiledSource3_3

    def CompiledByteCode: TestConfiguration = Scenarios.CompiledByteCode3_3

    def Compiled: TestConfiguration = CompiledByteCode + CompiledSource

    def CompiledSource3_2: TestConfiguration = Scenarios.CompiledSource3_2

    def CompiledByteCode3_2: TestConfiguration = Scenarios.CompiledByteCode3_2

    def Compiled3_2: TestConfiguration = CompiledSource3_2 + CompiledByteCode3_2

    def Interpreted: TestConfiguration = CommunityInterpreted + EnterpriseInterpreted

    def CommunityInterpreted: TestConfiguration = AbsolutelyAll - Compiled - Procs - EnterpriseInterpreted

    def EnterpriseInterpreted: TestConfiguration = Scenarios.EnterpriseInterpreted

    def Cost2_3: TestConfiguration = Scenarios.Compatibility2_3Cost

    def Version2_3: TestConfiguration = Scenarios.Compatibility2_3Rule + Cost2_3

    def Version3_1: TestConfiguration = Scenarios.Compatibility3_1Rule + Scenarios.Compatibility3_1Cost

    def Version3_2: TestConfiguration = Compiled3_2 + Scenarios.Compatibility3_2

    def Version3_3: TestConfiguration = Compiled + Scenarios.CommunityInterpreted + Scenarios.RulePlannerOnLatestVersion +
      EnterpriseInterpreted

    def AllRulePlanners: TestConfiguration = Scenarios.Compatibility3_1Rule + Scenarios.Compatibility2_3Rule + Scenarios
      .RulePlannerOnLatestVersion

    def Cost: TestConfiguration = Compiled + Scenarios.Compatibility3_1Cost + Scenarios.Compatibility2_3Cost + Scenarios
      .ForcedCostPlanner

    def BackwardsCompatibility: TestConfiguration = Version2_3 + Version3_1

    def Procs: TestConfiguration = Scenarios.ProcedureOrSchema

    /*
    If you are unsure what you need, this is a good start. It's not really all scenarios, but this is testing all
    interesting scenarios.
     */
    def All: TestConfiguration = AbsolutelyAll - Procs

    def AllExceptSleipnir: TestConfiguration = All - EnterpriseInterpreted

    def AbsolutelyAll: TestConfiguration = Version3_3 + BackwardsCompatibility + Procs

    def Empty: TestConfiguration = TestConfig(Set.empty)

  }

  object Scenarios {

    import CypherComparisonSupport._

    trait RuntimeScenario extends TestScenario {

      protected def argumentName: String

      override def checkResultForSuccess(query: String, internalExecutionResult: InternalExecutionResult): Unit = {
        internalExecutionResult.executionPlanDescription().arguments.collect {
          case Runtime(reportedRuntime) if reportedRuntime != argumentName =>
            fail(s"did not use the $name runtime - instead $reportedRuntime was used")
        }
      }

      override def checkResultForFailure(query: String, internalExecutionResult: Try[InternalExecutionResult]): Unit = {
        internalExecutionResult match {
          case Failure(_) => // not unexpected
          case Success(result) =>
            result.executionPlanDescription().arguments.collect {
              case Runtime(reportedRuntime) if reportedRuntime == argumentName =>
                fail(s"unexpectedly used the $name runtime for query $query")
            }
        }
      }
    }

    trait PlannerScenario extends TestScenario {

      protected def argumentName: String

      override def checkResultForSuccess(query: String, internalExecutionResult: InternalExecutionResult): Unit = {
        val description = internalExecutionResult.executionPlanDescription()
        description.arguments.collect {
          case Planner(reportedPlanner) if reportedPlanner != argumentName =>
            fail(s"did not use the $name planner - instead $reportedPlanner was used")
        }
      }

      override def checkResultForFailure(query: String, internalExecutionResult: Try[InternalExecutionResult]): Unit = {
        internalExecutionResult match {
          case Failure(_) => // not unexpected
          case Success(result) =>
            result.executionPlanDescription().arguments.collect {
              case Planner(reportedPlanner) if reportedPlanner == argumentName =>
                fail(s"unexpectedly used the $name planner for query $query")
            }
        }
      }
    }

    abstract class CompiledScenario extends RuntimeScenario {
      override protected def argumentName: String = "COMPILED"

      override def checkStateForSuccess(query: String): Unit = newRuntimeMonitor.trace.collect {
        case UnableToCompileQuery(stackTrace) => fail(s"Failed to use the compiled runtime on: $query\n$stackTrace")
      }

      override def checkStateForFailure(query: String): Unit = {
        val attempts = newRuntimeMonitor.trace.collectFirst {
          case event: NewPlanSeen => event
        }
        attempts.foreach(_ => {
          val failures = newRuntimeMonitor.trace.collectFirst {
            case failure: UnableToCompileQuery => failure
          }
          failures.orElse(fail(s"Unexpectedly used the compiled runtime on: $query"))
        })
      }
    }

    object CompiledSource3_2 extends CompiledScenario {
      override def prepare(): Unit = newRuntimeMonitor.clear()


      override def preparserOptions: String = "cypher=3.2 planner=cost runtime=compiled debug=generate_java_source"

      override def name: String = "compiled runtime through source code"

    }

    object CompiledByteCode3_2 extends CompiledScenario {
      override def prepare(): Unit = newRuntimeMonitor.clear()

      override def preparserOptions: String = "cypher=3.2 planner=cost runtime=compiled"

      override def name: String = "compiled runtime straight to bytecode"
    }

    object CompiledSource3_3 extends CompiledScenario {
      override def prepare(): Unit = newRuntimeMonitor.clear()

      override def preparserOptions: String = "planner=cost runtime=compiled debug=generate_java_source"

      override def name: String = "compiled runtime through source code"

    }

    object CompiledByteCode3_3 extends CompiledScenario {
      override def prepare(): Unit = newRuntimeMonitor.clear()

      override def preparserOptions: String = "planner=cost runtime=compiled"

      override def name: String = "compiled runtime straight to bytecode"
    }

    object EnterpriseInterpreted extends RuntimeScenario {

      override protected def argumentName: String = "ENTERPRISE-INTERPRETED"

      override def preparserOptions: String = "runtime=enterprise-interpreted"

      override def name: String = "enterprise interpreted"
    }

    object CommunityInterpreted extends RuntimeScenario {

      override protected def argumentName: String = "INTERPRETED"

      override def prepare(): Unit = newRuntimeMonitor.clear()

      override def checkStateForSuccess(query: String): Unit = newRuntimeMonitor.trace.collect {
        case UnableToCompileQuery(stackTrace) => fail(s"Failed to use the new runtime on: $query\n$stackTrace")
      }

      override def preparserOptions: String = "runtime=interpreted"

      override def name: String = "interpreted"

    }

    object ProcedureOrSchema extends RuntimeScenario {

      override protected def argumentName: String = "PROCEDURE"

      override def prepare(): Unit = newRuntimeMonitor.clear()

      override def preparserOptions: String = ""

      override def name: String = "schema or procedure"

    }

    object ForcedCostPlanner extends PlannerScenario {
      override def name: String = "cost planner without rule-fallback"

      override def preparserOptions: String = "planner=cost"

      override protected def argumentName: String = "IDP"
    }

    object RulePlannerOnLatestVersion extends TestScenario {
      override def name: String = "rule planner"

      override def preparserOptions: String = "planner=rule"
    }

    object Compatibility2_3Rule extends TestScenario {
      override def name: String = "compatibility 2.3 rule"

      override def preparserOptions: String = "2.3 planner=rule"
    }

    object Compatibility2_3Cost extends PlannerScenario {
      override def name: String = "compatibility 2.3 cost"

      override def preparserOptions: String = "2.3 planner=cost"

      override protected def argumentName: String = "IDP"
    }

    object Compatibility3_1Rule extends TestScenario {
      override def name: String = "compatibility 3.1 rule"

      override def preparserOptions: String = "3.1 planner=rule"
    }

    object Compatibility3_1Cost extends PlannerScenario {
      override def name: String = "compatibility 3.1 cost"

      override def preparserOptions: String = "3.1 planner=cost"

      override protected def argumentName: String = "IDP"
    }

    object Compatibility3_2 extends PlannerScenario {
      override def name: String = "compatibility 3.2"

      override def preparserOptions: String = "3.2"

      override protected def argumentName: String = "IDP"
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

  trait TestScenario extends Assertions with TestConfiguration {
    def name: String

//    def +(other: TestScenario): TestConfig = TestConfig(Set(this, other))

//    def +(other: TestConfig): TestConfig = other + this

    def prepare(): Unit = {}

    def checkStateForSuccess(query: String): Unit = {}

    def checkStateForFailure(query: String): Unit = {}

    def checkResultForFailure(query: String, internalExecutionResult: Try[InternalExecutionResult]): Unit = {
      if (internalExecutionResult.isSuccess)
        fail(name + " succeeded but should fail")
    }

    def checkResultForSuccess(query: String, internalExecutionResult: InternalExecutionResult): Unit = {}

    def preparserOptions: String

    override def scenarios: Set[TestScenario] = Set(this)
  }

  trait TestConfiguration {
    def scenarios: Set[TestScenario]

    def name: String

    def +(other: TestConfiguration): TestConfiguration = TestConfig(scenarios ++ other.scenarios)

    def -(other: TestConfiguration): TestConfiguration = TestConfig(scenarios -- other.scenarios)
  }

  case class TestConfig(scenarios: Set[TestScenario]) extends TestConfiguration {
    override def name: String = scenarios.map(_.name).mkString(" + ")

    override def +(other: TestConfiguration): TestConfig = TestConfig(scenarios ++ other.scenarios)

    override def -(other: TestConfiguration): TestConfig = TestConfig(scenarios -- other.scenarios)
  }
}
