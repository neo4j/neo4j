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

import org.neo4j.cypher.internal.compiler.v2_1.executionplan.NewQueryPlanSuccessRateMonitor
import org.neo4j.cypher.internal.compiler.v2_1.ast.Statement
import org.neo4j.cypher.internal.commons.CypherTestSupport
import org.neo4j.cypher.NewPlannerMonitor.{NewQuerySeen, UnableToHandleQuery, NewPlannerMonitorCall}

object NewPlannerMonitor {

  sealed trait NewPlannerMonitorCall {
    def queryText: String
  }

  final case class UnableToHandleQuery(queryText: String) extends NewPlannerMonitorCall
  final case class NewQuerySeen(queryText: String) extends NewPlannerMonitorCall
}

class NewPlannerMonitor extends NewQueryPlanSuccessRateMonitor {
  private var traceBuilder = List.newBuilder[NewPlannerMonitorCall]

  override def unableToHandleQuery(queryText: String, ast: Statement) {
    traceBuilder += UnableToHandleQuery(queryText)
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

  val newPlannerMonitor = new NewPlannerMonitor

  override protected def initTest() {
    super.initTest()
    self.monitors.addMonitorListener(newPlannerMonitor)
  }

  def executeWithNewPlanner(queryText: String, params: (String, Any)*): ExecutionResult =
    monitoringNewPlanner(self.execute(queryText, params: _*)) { trace =>
      trace should contain(NewQuerySeen(queryText))
      trace should not contain(UnableToHandleQuery(queryText))
    }

  def monitoringNewPlanner[T](action: => T)(test: List[NewPlannerMonitorCall] => Unit): T = {
    newPlannerMonitor.clear()
    val result = action
    test(newPlannerMonitor.trace)
    result
  }
}
