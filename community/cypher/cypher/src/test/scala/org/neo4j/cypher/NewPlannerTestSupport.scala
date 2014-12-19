/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

  def executeScalarWithNewPlanner[T](queryText: String, params: (String, Any)*): T =
    monitoringNewPlanner(self.executeScalar[T](queryText, params: _*)) { trace =>
      trace.collect {
        case UnableToHandleQuery(stackTrace) => fail(s"Failed to use the new planner on: $queryText\n$stackTrace")
      }
    }

  def executeWithNewPlanner(queryText: String, params: (String, Any)*): InternalExecutionResult =
    monitoringNewPlanner(innerExecute(queryText, params: _*)) { trace =>
      trace.collect {
        case UnableToHandleQuery(stackTrace) => fail(s"Failed to use the new planner on: $queryText\n$stackTrace")
      }
    }

  protected def innerExecute(queryText: String, params: (String, Any)*): InternalExecutionResult =
    eengine.execute(queryText, params.toMap) match {
      case ExecutionResultWrapperFor2_2(inner: InternalExecutionResult, _) => RewindableExecutionResult(inner)
    }

  def executeWithOlderPlanner(queryText: String, params: (String, Any)*): ExecutionResult =
    eengine.execute("cypher 2.1 " + queryText, params.toMap)

  override def execute(queryText: String, params: (String, Any)*) =
    monitoringNewPlanner(innerExecute(queryText, params: _*)) { trace =>
      val unableToHandle = trace.collectFirst {
        case event: UnableToHandleQuery => event
      }

      unableToHandle.orElse {
        fail(s"Unexpectedly used the new planner on: $queryText")
      }
    }

  def monitoringNewPlanner[T](action: => T)(test: List[NewPlannerMonitorCall] => Unit): T = {
    newPlannerMonitor.clear()
    val result = action
    test(newPlannerMonitor.trace)
    result
  }
}
