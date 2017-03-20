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
package org.neo4j.cypher.internal.compiler.v3_2.executionplan.procs

import org.neo4j.cypher.internal.compiler.v3_2.executionplan.{ExecutionPlan, InternalExecutionResult, InternalQueryType, SCHEMA_WRITE}
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.{Id, NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.compiler.v3_2.spi.{GraphStatistics, PlanContext, QueryContext, UpdateCountingQueryContext}
import org.neo4j.cypher.internal.compiler.v3_2.{ExecutionMode, ExplainExecutionResult, ExplainMode, ProcedurePlannerName, ProcedureRuntimeName, RuntimeName}
import org.neo4j.cypher.internal.frontend.v3_2.PlannerName
import org.neo4j.cypher.internal.frontend.v3_2.notification.InternalNotification

/**
  * Execution plan for performing pure side-effects, i.e. returning no data to the user.
 *
  * @param name A name of the side-effect
  * @param queryType The type of the query
  * @param sideEffect The actual side-effect to be performed
  */
case class PureSideEffectExecutionPlan(name: String, queryType: InternalQueryType, sideEffect: (QueryContext => Unit))
  extends ExecutionPlan {

  override def run(ctx: QueryContext, planType: ExecutionMode,
                   params: Map[String, Any]): InternalExecutionResult = {
    if (planType == ExplainMode) {
      //close all statements
      ctx.transactionalContext.close(success = true)
      ExplainExecutionResult(List.empty, description, queryType, Set.empty)
    } else {
      if (queryType == SCHEMA_WRITE) ctx.assertSchemaWritesAllowed()

      val countingCtx = new UpdateCountingQueryContext(ctx)
      try {
        sideEffect(countingCtx)
        ctx.transactionalContext.close(success = true)
      }
      catch {
        case e: Throwable =>
          ctx.transactionalContext.close(success = false)
          throw e
      }
      PureSideEffectInternalExecutionResult(countingCtx, description, queryType, planType)
    }
  }

  private def description = PlanDescriptionImpl(new Id, name, NoChildren, Seq.empty, Set.empty)

  override def runtimeUsed: RuntimeName = ProcedureRuntimeName

  override def isStale(lastTxId: () => Long, statistics: GraphStatistics): Boolean = false

  override def plannerUsed: PlannerName = ProcedurePlannerName

  override def notifications(planContext: PlanContext): Seq[InternalNotification] = Seq.empty

  override def isPeriodicCommit: Boolean = false
}
