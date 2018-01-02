/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher

import org.neo4j.cypher.NewPlannerMonitor.{NewPlannerMonitorCall, NewQuerySeen, UnableToHandleQuery}
import org.neo4j.cypher.NewRuntimeMonitor.{NewPlanSeen, NewRuntimeMonitorCall, UnableToCompileQuery}
import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.compatibility.ExecutionResultWrapperFor2_3
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{InternalExecutionResult, NewLogicalPlanSuccessRateMonitor, NewRuntimeSuccessRateMonitor}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_3.planner.{CantCompileQueryException, CantHandleQueryException}
import org.neo4j.cypher.internal.frontend.v2_3.ast.Statement
import org.neo4j.cypher.internal.frontend.v2_3.helpers.Eagerly
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherTestSupport
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.helpers.Exceptions
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.util.{Failure, Success, Try}

object NewPlannerMonitor {

  sealed trait NewPlannerMonitorCall {
    def stackTrace: String
  }

  final case class UnableToHandleQuery(stackTrace: String) extends NewPlannerMonitorCall
  final case class NewQuerySeen(stackTrace: String) extends NewPlannerMonitorCall
}

class NewPlannerMonitor extends NewLogicalPlanSuccessRateMonitor {
  private var traceBuilder = List.newBuilder[NewPlannerMonitorCall]

  override def unableToHandleQuery(queryText: String, ast: Statement, e: CantHandleQueryException) {
    traceBuilder += UnableToHandleQuery(Exceptions.stringify(e))
  }

  override def newQuerySeen(queryText: String, ast: Statement) {
    traceBuilder += NewQuerySeen(queryText)
  }

  def trace = traceBuilder.result()

  def clear() {
    traceBuilder.clear()
  }
}

object NewRuntimeMonitor {

  sealed trait NewRuntimeMonitorCall {
    def stackTrace: String
  }

  final case class UnableToCompileQuery(stackTrace: String) extends NewRuntimeMonitorCall
  final case class NewPlanSeen(stackTrace: String) extends NewRuntimeMonitorCall
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

trait NewPlannerTestSupport extends CypherTestSupport {
  self: ExecutionEngineFunSuite =>

  //todo once the compiled runtime handles dumpToString and plan descriptions, we should enable it by default here
  //in that way we will notice when we support new queries
  override def databaseConfig(): Map[String, String] =
    Map("cypher_parser_version" -> CypherVersion.v2_3.name,
        GraphDatabaseSettings.query_non_indexed_label_warning_threshold.name() -> "10")

  val newPlannerMonitor = new NewPlannerMonitor

  val newRuntimeMonitor = new NewRuntimeMonitor

  override protected def initTest() {
    super.initTest()
    self.kernelMonitors.addMonitorListener(newPlannerMonitor)
    self.kernelMonitors.addMonitorListener(newRuntimeMonitor)
  }

  private def failedToUseNewRuntime(query: String)(trace: List[NewRuntimeMonitorCall]) {
    trace.collect {
      case UnableToCompileQuery(stackTrace) => fail(s"Failed to use the new runtime on: $query\n$stackTrace")
    }
  }

  private def unexpectedlyUsedNewRuntime(query: String)(trace: List[NewRuntimeMonitorCall]) {
    val attempts = trace.collectFirst {
      case event: NewPlanSeen => event
    }
    attempts.foreach(_ => {
      val failures = trace.collectFirst {
        case failure: UnableToCompileQuery => failure
      }
      failures.orElse(fail(s"Unexpectedly used the new runtime on: $query"))
    })
  }

    private def failedToUseNewPlanner(query: String)(trace: List[NewPlannerMonitorCall]) {
      trace.collect {
        case UnableToHandleQuery(stackTrace) => fail(s"Failed to use the new planner on: $query\n$stackTrace")
      }
    }

    private def unexpectedlyUsedNewPlanner(query: String)(trace: List[NewPlannerMonitorCall]) {
      val events = trace.collectFirst {
        case event: UnableToHandleQuery => event
      }
      events.orElse {
        fail(s"Unexpectedly used the new planner on: $query")
      }
    }

  def executeScalarWithAllPlanners[T](queryText: String, params: (String, Any)*): T = {
    val ruleResult = self.executeScalar[T](queryText, params: _*)
    val costResult = monitoringNewPlanner(self.executeScalar[T](queryText, params: _*))(failedToUseNewPlanner(queryText))(unexpectedlyUsedNewRuntime(queryText))

    assert(ruleResult === costResult, "Diverging results between rule and cost planners")

    costResult
  }

  def executeWithAllPlanners(queryText: String, params: (String, Any)*): InternalExecutionResult = {
    val ruleResult = innerExecute(s"CYPHER planner=rule $queryText", params: _*)
    val greedyResult = innerExecute(s"CYPHER planner=greedy $queryText", params: _*)
    val idpResult = executeWithCostPlannerOnly(queryText, params: _*)

    assertResultsAreSame(ruleResult, idpResult, queryText, "Diverging results between rule and cost planners")
    assertResultsAreSame(greedyResult, idpResult, queryText, "Diverging results between IDP and greedy planner")
    ruleResult.close()
    idpResult
  }

  def executeWithAllPlannersReplaceNaNs(queryText: String, params: (String, Any)*): InternalExecutionResult = {
    val ruleResult = innerExecute(s"CYPHER planner=rule $queryText", params: _*)
    val idpResult = innerExecute(s"CYPHER planner=idp $queryText", params: _*)
    val costResult = executeWithCostPlannerOnly(queryText, params: _*)

    assertResultsAreSame(ruleResult, costResult, queryText, "Diverging results between rule and cost planners", replaceNaNs = true)
    assertResultsAreSame(idpResult, costResult, queryText, "Diverging results between IDP and greedy planner", replaceNaNs = true)
    ruleResult.close()
    costResult
  }


  def executeWithCostPlannerOnly(queryText: String, params: (String, Any)*): InternalExecutionResult =
    monitoringNewPlanner(innerExecute(queryText, params: _*))(failedToUseNewPlanner(queryText))(unexpectedlyUsedNewRuntime(queryText))

  private def assertResultsAreSame(ruleResult: InternalExecutionResult, costResult: InternalExecutionResult, queryText: String, errorMsg: String, replaceNaNs: Boolean = false) {
    withClue(errorMsg) {
      if (queryText.toLowerCase contains "order by") {
        ruleResult.toComparableResultWithOptions(replaceNaNs) should contain theSameElementsInOrderAs costResult.toComparableResultWithOptions(replaceNaNs)
      } else {
        ruleResult.toComparableResultWithOptions(replaceNaNs) should contain theSameElementsAs costResult.toComparableResultWithOptions(replaceNaNs)
      }
    }
  }

  protected def innerExecute(queryText: String, params: (String, Any)*): InternalExecutionResult =
    eengine.execute(queryText, params.toMap) match {
      case e:ExecutionResultWrapperFor2_3 => RewindableExecutionResult(e)
    }

  override def execute(queryText: String, params: (String, Any)*) =
    fail("Don't use execute together with NewPlannerTestSupport")

  def withEachPlanner(body: ((String, Seq[(String, Any)]) => InternalExecutionResult) => Any) = {
    List(executeWithRulePlanner _, executeWithCostPlannerOnly _).foreach(body)
  }

  def executeWithRulePlanner(queryText: String, params: (String, Any)*) = {
    val plannerWatcher: (List[NewPlannerMonitorCall]) => Unit = unexpectedlyUsedNewPlanner(queryText)
    val runtimeWatcher: (List[NewRuntimeMonitorCall]) => Unit = unexpectedlyUsedNewRuntime(queryText)
    monitoringNewPlanner(innerExecute(queryText, params: _*))(plannerWatcher)(runtimeWatcher)
  }

  def monitoringNewPlanner[T](action: => T)(testPlanner: List[NewPlannerMonitorCall] => Unit)(testRuntime: List[NewRuntimeMonitorCall] => Unit): T = {
    newPlannerMonitor.clear()
    newRuntimeMonitor.clear()
    //if action fails we must wait to throw until after test has run
    val result = Try(action)

    //check for unexpected exceptions
    result match {
      case f@Failure(ex: CantHandleQueryException) => //do nothing
      case f@Failure(ex: CantCompileQueryException) => //do nothing
      case Failure(ex) => throw ex
      case Success(r) => //do nothing
    }

    testPlanner(newPlannerMonitor.trace)
    testRuntime(newRuntimeMonitor.trace)
    //now it is safe to throw
    result.get
  }

  /**
   * Get rid of Arrays and java.util.Map to make it easier to compare results by equality.
   */
  implicit class RichInternalExecutionResults(res: InternalExecutionResult) {
    def toComparableResultWithOptions(replaceNaNs: Boolean): Seq[Map[String, Any]] = res.toList.toCompararableSeq(replaceNaNs)
    def toComparableResult: Seq[Map[String, Any]] = res.toList.toCompararableSeq(replaceNaNs = false)
  }

  implicit class RichMapSeq(res: Seq[Map[String, Any]]) {

    import scala.collection.JavaConverters._

    def toCompararableSeq(replaceNaNs: Boolean): Seq[Map[String, Any]] = {
      def convert(v: Any): Any = v match {
        case a: Array[_] => a.toList.map(convert)
        case m: Map[_,_] =>  {
          Eagerly.immutableMapValues(m, convert)
        }
        case m: java.util.Map[_,_] =>  {
          Eagerly.immutableMapValues(m.asScala, convert)
        }
        case l: java.util.List[_] => l.asScala.map(convert)
        case d: java.lang.Double if replaceNaNs && java.lang.Double.isNaN(d) => "NaNreplacement"
        case m => m
      }


      res.map((map: Map[String, Any]) => map.map {
        case (k, v) => k -> convert(v)
      })
    }
  }

  def evaluateTo(expected: Seq[Map[String, Any]]): Matcher[InternalExecutionResult] = new Matcher[InternalExecutionResult] {
    override def apply(actual: InternalExecutionResult): MatchResult = {
      MatchResult(
        matches = actual.toComparableResult == expected.toCompararableSeq(replaceNaNs = false),
        rawFailureMessage = s"Results differ: ${actual.toComparableResult} did not equal to $expected",
        rawNegatedFailureMessage = s"Results are equal")
    }
  }
}
