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
package org.neo4j.cypher.internal.compiler.v2_2

import org.neo4j.cypher.internal.compiler.v2_2.ast.Statement
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.PipeInfo
import org.neo4j.cypher.internal.compiler.v2_2.parser.ParserMonitor
import org.neo4j.cypher.internal.compiler.v2_2.planner.PlanningMonitor
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.LogicalPlan
import org.parboiled.errors.ParseError

import scala.collection.mutable

trait StagesTimingMonitor {
  def parsingTimeElapsed(query: String, ms: Long)

  def rewritingTimeElapsed(query: String, ms: Long)

  def semanticCheckTimeElapsed(query: String, ms: Long)

  def planningTimeElapsed(query: String, ms: Long)

  def executionPlanBuilding(query: String, ms: Long)
}

class StagesTimingListener(monitors: Monitors, monitor: StagesTimingMonitor) {

  monitors.addMonitorListener(new TimingParserMonitor)
  monitors.addMonitorListener(new TimingRewriterMonitor)
  monitors.addMonitorListener(new TimingSemanticCheckMonitor)
  monitors.addMonitorListener(new TimingPlanningMonitor)

  private class TimingParserMonitor extends ParserMonitor[Statement] with TimingMonitor[String] {

    def finishParsingError(query: String, errors: Seq[ParseError]) {}

    def startParsing(query: String) {
      start(query)
    }

    def finishParsingSuccess(query: String, statement: Statement) {
      end(query, monitor.parsingTimeElapsed)
    }
  }

  private class TimingRewriterMonitor extends AstRewritingMonitor with TimingMonitor[String] {

    def startRewriting(query: String, statement: Statement) {
      start(query)
    }

    def finishRewriting(query: String, statement: Statement) {
      end(query, monitor.rewritingTimeElapsed)
    }

    def abortedRewriting(obj: AnyRef) {}
  }

  private class TimingSemanticCheckMonitor extends SemanticCheckMonitor with TimingMonitor[String] {
    def startSemanticCheck(query: String) {
      start(query)
    }

    def finishSemanticCheckSuccess(query: String) {
      end(query, monitor.semanticCheckTimeElapsed)
    }

    def finishSemanticCheckError(query: String, errors: Seq[SemanticError]) {}
  }

  private class TimingPlanningMonitor extends PlanningMonitor with TimingMonitor[String] {

    def startedPlanning(query: String) {
      start(query)
    }

    def foundPlan(query: String, p: LogicalPlan) {
      end(query, monitor.planningTimeElapsed)
      start(query)
    }

    def successfulPlanning(query: String, p: PipeInfo) {
      end(query, monitor.executionPlanBuilding)
    }
  }
}

trait TimingMonitor[T] {
  def currentTime: Long = System.currentTimeMillis()

  val timeMap = mutable.Map[T, Long]()

  def start(query: T) {
    val time = currentTime
    timeMap += (query -> time)
  }

  def end(query: T, f: (T, Long) => Unit) {
    val time = currentTime
    timeMap.remove(query).foreach {
      started => f(query, time - started)
    }
  }
}
