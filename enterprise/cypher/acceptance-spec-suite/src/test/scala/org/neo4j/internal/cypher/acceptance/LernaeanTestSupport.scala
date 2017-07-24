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
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan.InternalExecutionResult
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{CRS, CartesianPoint, GeographicPoint}
import org.neo4j.cypher.internal.compatibility.{ClosingExecutionResult, v2_3, v3_1, v3_2, v3_3}
import org.neo4j.cypher.internal.compiler.v3_1.{CartesianPoint => CartesianPointv3_1, GeographicPoint => GeographicPointv3_1}
import org.neo4j.cypher.internal.compiler.v3_2.{CartesianPoint => CartesianPointv3_2, GeographicPoint => GeographicPointv3_2}
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.InternalPlanDescription.Arguments.{Planner, Runtime}
import org.neo4j.cypher.internal.frontend.v3_3.helpers.Eagerly
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherTestSupport
import org.neo4j.cypher.internal.{ExecutionResult, RewindableExecutionResult}
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerMonitor, NewRuntimeMonitor}
import org.scalatest.Assertions

import scala.util.{Failure, Success, Try}

trait LernaeanTestSupport extends CypherTestSupport {
  self: ExecutionEngineFunSuite =>

  import LernaeanTestSupport._

  /**
    * Get rid of Arrays and java.util.Map to make it easier to compare results by equality.
    */
  implicit class RichInternalExecutionResults(res: InternalExecutionResult) {
    def toComparableResultWithOptions(replaceNaNs: Boolean): Seq[Map[String, Any]] = res.toList.toCompararableSeq(replaceNaNs)

    def toComparableResult: Seq[Map[String, Any]] = res.toList.toCompararableSeq(replaceNaNs = false)
  }

  implicit class RichMapSeq(res: Seq[Map[String, Any]]) {

    import scala.collection.JavaConverters._

    object NanReplacement

    def toCompararableSeq(replaceNaNs: Boolean): Seq[Map[String, Any]] = {
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

  private def extractFirstScenario(config: TestConfig): TestScenario = {
    val preferredScenario = Scenarios.CommunityInterpreted
    if (config.scenarios.contains(preferredScenario))
      preferredScenario
    else
      config.scenarios.head
  }

  protected def testWith(expectedSuccessFrom: TestConfig, query: String, params: (String, Any)*): InternalExecutionResult = {
    val firstScenario = extractFirstScenario(expectedSuccessFrom)
    firstScenario.prepare()
    val firstResult: InternalExecutionResult = innerExecute(s"CYPHER ${firstScenario.preparserOptions} $query", params.toMap)
    firstScenario.checkStateForSuccess(query)
    firstScenario.checkResultForSuccess(query, firstResult)

    for (thisScenario <- Configs.AbsolutelyAll.scenarios if thisScenario != firstScenario) {
      thisScenario.prepare()
      val tryResult = Try(innerExecute(s"CYPHER ${thisScenario.preparserOptions} $query", params.toMap))

      val expectedToSucceed = expectedSuccessFrom.scenarios.contains(thisScenario)

      if (expectedToSucceed) {
        val thisResult = tryResult.get
        thisScenario.checkStateForSuccess(query)
        thisScenario.checkResultForSuccess(query, thisResult)
      } else {
        thisScenario.checkStateForFailure(query)
        thisScenario.checkResultForFailure(query, tryResult)
      }
    }

    firstResult
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
    def CompiledSource: TestConfig = TestConfig.from(Scenarios.CompiledSource3_3)

    def CompiledByteCode: TestConfig = TestConfig.from(Scenarios.CompiledByteCode3_3)

    def Compiled: TestConfig = CompiledByteCode + CompiledSource

    def CompiledSource3_2: TestConfig = TestConfig.from(Scenarios.CompiledSource3_2)

    def CompiledByteCode3_2: TestConfig = TestConfig.from(Scenarios.CompiledByteCode3_2)

    def Compiled3_2: TestConfig = CompiledSource3_2 + CompiledByteCode3_2

    def Interpreted: TestConfig = AbsolutelyAll - Compiled - Procs

    def Cost2_3: TestConfig = TestConfig.from(Scenarios.Compatibility2_3Cost)

    def Version2_3: TestConfig = Scenarios.Compatibility2_3Rule + Cost2_3

    def Version3_1: TestConfig = Scenarios.Compatibility3_1Rule + Scenarios.Compatibility3_1Cost

    def Version3_2: TestConfig = Compiled3_2 + Scenarios.Compatibility3_2

    def Version3_3: TestConfig = Compiled + Scenarios.CommunityInterpreted + Scenarios.RulePlannerOnLatestVersion

    def Cost: TestConfig = Compiled + Scenarios.Compatibility3_1Cost + Scenarios.Compatibility2_3Cost + Scenarios.ForcedCostPlanner

    def BackwardsCompatibility: TestConfig = Version2_3 + Version3_1

    def Procs: TestConfig = TestConfig.from(Scenarios.ProcedureOrSchema)

    /*
    If you are unsure what you need, this is a good start. It's not really all scenarios, but this is testing all
    interesting scenarios.
     */
    def All: TestConfig = AbsolutelyAll - Procs

    def AbsolutelyAll: TestConfig = Version3_3 + BackwardsCompatibility + Procs
  }

  object Scenarios {

    import LernaeanTestSupport._

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
object LernaeanTestSupport {

  trait TestScenario extends Assertions {
    def name: String

    def +(other: TestScenario): TestConfig = TestConfig(Set(this, other))

    def +(other: TestConfig): TestConfig = other + this

    def prepare(): Unit = {}

    def checkStateForSuccess(query: String): Unit = {}

    def checkStateForFailure(query: String): Unit = {}

    def checkResultForFailure(query: String, internalExecutionResult: Try[InternalExecutionResult]): Unit = {
      if (internalExecutionResult.isSuccess)
        fail(name + " succeeded but should fail")
    }

    def checkResultForSuccess(query: String, internalExecutionResult: InternalExecutionResult): Unit = {}

    def preparserOptions: String
  }

  object TestConfig {
    def from(scenario: TestScenario) = new TestConfig(Set(scenario))
  }

  case class TestConfig(scenarios: Set[TestScenario]) {
    def name: String = scenarios.map(_.name).mkString(" + ")

    def +(other: TestScenario): TestConfig = TestConfig(scenarios + other)

    def +(other: TestConfig): TestConfig = TestConfig(scenarios ++ other.scenarios)

    def -(other: TestScenario): TestConfig = TestConfig(scenarios - other)

    def -(other: TestConfig): TestConfig = TestConfig(scenarios -- other.scenarios)
  }
}