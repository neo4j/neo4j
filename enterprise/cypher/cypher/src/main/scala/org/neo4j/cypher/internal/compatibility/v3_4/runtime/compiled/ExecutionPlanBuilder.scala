/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.ExecutionPlanBuilder.DescriptionProvider
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.{PeriodicCommitInfo, PlanFingerprint, Provider}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.CompiledRuntimeName
import org.neo4j.cypher.internal.frontend.v3_4.PlannerName
import org.neo4j.cypher.internal.runtime.{ExecutionMode, InternalExecutionResult, ProfileMode, QueryContext}
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.spi.v3_4
import org.neo4j.cypher.internal.util.v3_4.TaskCloser
import org.neo4j.cypher.internal.v3_4.codegen.QueryExecutionTracer
import org.neo4j.cypher.internal.v3_4.codegen.profiling.ProfilingTracer
import org.neo4j.cypher.internal.v3_4.logical.plans.IndexUsage
import org.neo4j.values.virtual.MapValue

object ExecutionPlanBuilder {
  type DescriptionProvider =
    (Provider[InternalPlanDescription] => (Provider[InternalPlanDescription], Option[QueryExecutionTracer]))

  def tracer(mode: ExecutionMode, queryContext: QueryContext): DescriptionProvider = mode match {
    case ProfileMode =>
      val tracer = new ProfilingTracer(queryContext.transactionalContext.kernelStatisticProvider)
      (description: Provider[InternalPlanDescription]) =>
        (new Provider[InternalPlanDescription] {

          override def get(): InternalPlanDescription = description.get().map {
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
    case _ => (description: Provider[InternalPlanDescription]) => (description, None)
  }
}

case class CompiledPlan(updating: Boolean,
                        periodicCommit: Option[PeriodicCommitInfo] = None,
                        fingerprint: Option[PlanFingerprint] = None,
                        plannerUsed: PlannerName,
                        planDescription: Provider[InternalPlanDescription],
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
