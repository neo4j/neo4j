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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled

import org.neo4j.cypher.internal.InternalExecutionResult
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.ExecutionPlanBuilder.DescriptionProvider
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan.{PeriodicCommitInfo, PlanFingerprint, Provider}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{CompiledRuntimeName, ExecutionMode, ProfileMode, TaskCloser}
import org.neo4j.cypher.internal.frontend.v3_3.PlannerName
import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.cypher.internal.v3_3.codegen.QueryExecutionTracer
import org.neo4j.cypher.internal.v3_3.codegen.profiling.ProfilingTracer
import org.neo4j.cypher.internal.v3_3.logical.plans.IndexUsage
import org.neo4j.values.virtual.MapValue

object ExecutionPlanBuilder {
  type DescriptionProvider =
    (InternalPlanDescription => (Provider[InternalPlanDescription], Option[QueryExecutionTracer]))

  def tracer(mode: ExecutionMode, queryContext: QueryContext): DescriptionProvider = mode match {
    case ProfileMode =>
      val tracer = new ProfilingTracer(queryContext.transactionalContext.kernelStatisticProvider)
      (description: InternalPlanDescription) =>
        (new Provider[InternalPlanDescription] {

          override def get(): InternalPlanDescription = description.map {
            plan: InternalPlanDescription =>
              val data = tracer.get(plan.id)

              plan.
                addArgument(Arguments.Runtime(CompiledRuntimeName.toTextOutput)).
                addArgument(Arguments.DbHits(data.dbHits())).
                addArgument(Arguments.PageCacheHits(data.pageCacheHits())).
                addArgument(Arguments.PageCacheMisses(data.pageCacheMisses())).
                addArgument(Arguments.Rows(data.rows())).
                addArgument(Arguments.Time(data.time()))
          }
        }, Some(tracer))
    case _ => (description: InternalPlanDescription) =>
      (new Provider[InternalPlanDescription] {
        override def get(): InternalPlanDescription = description
      }, None)
  }
}

case class CompiledPlan(updating: Boolean,
                        periodicCommit: Option[PeriodicCommitInfo] = None,
                        fingerprint: Option[PlanFingerprint] = None,
                        plannerUsed: PlannerName,
                        planDescription: InternalPlanDescription,
                        columns: Seq[String],
                        executionResultBuilder: RunnablePlan,
                        plannedIndexUsage: Seq[IndexUsage] = Seq.empty)

trait RunnablePlan {
  def apply(queryContext: QueryContext,
            execMode: ExecutionMode,
            descriptionProvider: DescriptionProvider,
            params: MapValue,
            closer: TaskCloser): InternalExecutionResult
}
