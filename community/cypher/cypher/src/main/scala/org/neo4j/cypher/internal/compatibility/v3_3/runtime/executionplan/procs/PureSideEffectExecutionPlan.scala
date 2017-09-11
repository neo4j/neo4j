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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan.procs

import org.neo4j.cypher.CypherVersion
import org.neo4j.cypher.internal.InternalExecutionResult
import org.neo4j.cypher.internal.compatibility.v3_3.runtime._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan.{ExecutionPlan, InternalQueryType, SCHEMA_WRITE}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.{Id, NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.compiler.v3_3._
import org.neo4j.cypher.internal.compiler.v3_3.spi.{GraphStatistics, PlanContext}
import org.neo4j.cypher.internal.frontend.v3_3.PlannerName
import org.neo4j.cypher.internal.frontend.v3_3.notification.InternalNotification
import org.neo4j.cypher.internal.spi.v3_3.{QueryContext, UpdateCountingQueryContext}
import org.neo4j.values.virtual.MapValue

/**
  * Execution plan for performing pure side-effects, i.e. returning no data to the user.
  *
  * @param name       A name of the side-effect
  * @param queryType  The type of the query
  * @param sideEffect The actual side-effect to be performed
  */
case class PureSideEffectExecutionPlan(name: String, queryType: InternalQueryType, sideEffect: (QueryContext => Unit))
  extends ExecutionPlan {

  override def run(ctx: QueryContext, planType: ExecutionMode,
                   params: MapValue): InternalExecutionResult = {
    if (planType == ExplainMode) {
      //close all statements
      ctx.transactionalContext.close(success = true)
      ExplainExecutionResult(Array.empty, description, queryType, Set.empty)
    } else {
      if (queryType == SCHEMA_WRITE) ctx.assertSchemaWritesAllowed()

      val countingCtx = new UpdateCountingQueryContext(ctx)
      sideEffect(countingCtx)
      ctx.transactionalContext.close(success = true)
      PureSideEffectInternalExecutionResult(countingCtx, description, queryType, planType)
    }
  }

  private def description = PlanDescriptionImpl(new Id, name, NoChildren,
                                                Seq(Planner(plannerUsed.toTextOutput),
                                                    PlannerImpl(plannerUsed.name),
                                                    Runtime(runtimeUsed.toTextOutput),
                                                    RuntimeImpl(runtimeUsed.name),
                                                    Version(s"CYPHER ${CypherVersion.default.name}"))
                                                , Set.empty)

  override def runtimeUsed: RuntimeName = ProcedureRuntimeName

  override def isStale(lastTxId: () => Long, statistics: GraphStatistics): Boolean = false

  override def plannerUsed: PlannerName = ProcedurePlannerName

  override def notifications(planContext: PlanContext): Seq[InternalNotification] = Seq.empty

  override def isPeriodicCommit: Boolean = false
}
