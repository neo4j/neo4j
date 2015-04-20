/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.compatibility.ExecutionResultWrapperFor2_2
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.{NewLogicalPlanSuccessRateMonitor, InternalExecutionResult}
import org.neo4j.cypher.internal.compiler.v2_2.ast.Statement
import org.neo4j.cypher.internal.commons.CypherTestSupport
import org.neo4j.cypher.NewPlannerMonitor.{NewQuerySeen, UnableToHandleQuery, NewPlannerMonitorCall}
import java.io.{PrintWriter, StringWriter}
import org.neo4j.cypher.internal.compiler.v2_2.planner.CantHandleQueryException

import scala.util.Try

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
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    e.printStackTrace(pw)

    traceBuilder += UnableToHandleQuery(sw.toString)
  }

  override def newQuerySeen(queryText: String, ast: Statement) {
    traceBuilder += NewQuerySeen(queryText)
  }

  def trace = traceBuilder.result()

  def clear() {
    traceBuilder.clear()
  }
}

trait NewPlannerTestSupport extends CypherTestSupport {
  self: ExecutionEngineFunSuite =>

  override def databaseConfig(): Map[String,String] = Map("cypher_parser_version" -> CypherVersion.v2_2.name)

  val newPlannerMonitor = new NewPlannerMonitor

  override protected def initTest() {
    super.initTest()
    self.kernelMonitors.addMonitorListener(newPlannerMonitor)
  }

  def executeScalarWithAllPlanners[T](queryText: String, params: (String, Any)*): T = {
    val ruleResult = self.executeScalar[T](queryText, params: _*)
    val costResult = monitoringNewPlanner(self.executeScalar[T](queryText, params: _*))(failedToUseNewPlanner(queryText))

    assert(ruleResult === costResult, "Diverging results between rule and cost planners")

    costResult
  }

  def executeWithAllPlanners(queryText: String, params: (String, Any)*): InternalExecutionResult = {
    val ruleResult = innerExecute(s"CYPHER planner=rule $queryText", params: _*)
    val costResult = executeWithCostPlannerOnly(queryText, params: _*)

    assertResultsAreSame(ruleResult, costResult, queryText, "Diverging results between rule and cost planners")

    costResult
  }

  def executeWithCostPlannerOnly(queryText: String, params: (String, Any)*): InternalExecutionResult =
    monitoringNewPlanner(innerExecute(queryText, params: _*))(failedToUseNewPlanner(queryText))

  def executeWithRulePlannerOnly(queryText: String, params: (String, Any)*) =
    monitoringNewPlanner(innerExecute(queryText, params: _*))(unexpectedlyUsedNewPlanner(queryText))

  protected def innerExecute(queryText: String, params: (String, Any)*): InternalExecutionResult =
    eengine.execute(queryText, params.toMap) match {
      case ExecutionResultWrapperFor2_2(inner: InternalExecutionResult, _) => RewindableExecutionResult(inner)
    }

  override def execute(queryText: String, params: (String, Any)*) =
    fail("Don't use execute together with NewPlannerTestSupport")

  def monitoringNewPlanner[T](action: => T)(test: List[NewPlannerMonitorCall] => Unit): T = {
    newPlannerMonitor.clear()
    //if action fails we must wait to throw until after test has run
    val result = Try(action)
    test(newPlannerMonitor.trace)

    //now it is safe to throw
    result.get
  }

  private def assertResultsAreSame(ruleResult: InternalExecutionResult, costResult: InternalExecutionResult, queryText: String, errorMsg: String) {
    withClue(errorMsg) {
      if (queryText.toLowerCase contains "order by") {
        ruleResult.toComparableList should contain theSameElementsInOrderAs costResult.toComparableList
      } else {
        ruleResult.toComparableList should contain theSameElementsAs costResult.toComparableList
      }
    }
  }

  /**
   * Get rid of Arrays to make it easier to compare results by equality.
   */
  implicit class RichInternalExecutionResults(res: InternalExecutionResult) {
    def toComparableList: Seq[Map[String, Any]] = res.toList.withArraysAsLists
  }


  implicit class RichMapSeq(res: Seq[Map[String, Any]]) {
    def withArraysAsLists: Seq[Map[String, Any]] = res.map((map: Map[String, Any]) =>
      map.map {
        case (k, a: Array[_]) => k -> a.toList
        case m => m
      }
    )
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
}
