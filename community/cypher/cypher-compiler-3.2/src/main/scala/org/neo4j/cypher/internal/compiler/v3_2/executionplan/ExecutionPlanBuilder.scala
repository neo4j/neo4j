/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.executionplan

import org.neo4j.cypher.internal.compiler.v3_2.codegen.QueryExecutionTracer
import org.neo4j.cypher.internal.compiler.v3_2.codegen.profiling.ProfilingTracer
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.ExecutionPlanBuilder.DescriptionProvider
import org.neo4j.cypher.internal.compiler.v3_2.pipes._
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v3_2.planner.{CantCompileQueryException, CantHandleQueryException}
import org.neo4j.cypher.internal.compiler.v3_2.spi._
import org.neo4j.cypher.internal.compiler.v3_2.{ExecutionMode, ProfileMode, _}
import org.neo4j.cypher.internal.frontend.v3_2.ast.Statement

trait RunnablePlan {
  def apply(queryContext: QueryContext,
            execMode: ExecutionMode,
            descriptionProvider: DescriptionProvider,
            params: Map[String, Any],
            closer: TaskCloser): InternalExecutionResult
}

case class CompiledPlan(updating: Boolean,
                        periodicCommit: Option[PeriodicCommitInfo] = None,
                        fingerprint: Option[PlanFingerprint] = None,
                        plannerUsed: PlannerName,
                        planDescription: InternalPlanDescription,
                        columns: Seq[String],
                        executionResultBuilder: RunnablePlan )

case class PipeInfo(pipe: Pipe,
                    updating: Boolean,
                    periodicCommit: Option[PeriodicCommitInfo] = None,
                    fingerprint: Option[PlanFingerprint] = None,
                    plannerUsed: PlannerName)

case class PeriodicCommitInfo(size: Option[Long]) {
  def batchRowCount = size.getOrElse(/* defaultSize */ 1000L)
}

trait NewLogicalPlanSuccessRateMonitor {
  def newQuerySeen(queryText: String, ast:Statement)
  def unableToHandleQuery(queryText: String, ast:Statement, origin: CantHandleQueryException)
}

trait NewRuntimeSuccessRateMonitor {
  def newPlanSeen(plan: LogicalPlan)
  def unableToHandlePlan(plan: LogicalPlan, origin: CantCompileQueryException)
}

object ExecutionPlanBuilder {
  type DescriptionProvider =
        (InternalPlanDescription => (Provider[InternalPlanDescription], Option[QueryExecutionTracer]))

  def tracer( mode: ExecutionMode ) : DescriptionProvider = mode match {
    case ProfileMode =>
      val tracer = new ProfilingTracer()
      (description: InternalPlanDescription) => (new Provider[InternalPlanDescription] {

        override def get(): InternalPlanDescription = description.map {
          plan: InternalPlanDescription =>
            val data = tracer.get(plan.id)
            plan.
              addArgument(Arguments.DbHits(data.dbHits())).
              addArgument(Arguments.Rows(data.rows())).
              addArgument(Arguments.Time(data.time()))
        }
      }, Some(tracer))
    case _ => (description: InternalPlanDescription) => (new Provider[InternalPlanDescription] {
      override def get(): InternalPlanDescription = description
    }, None)
  }
}
