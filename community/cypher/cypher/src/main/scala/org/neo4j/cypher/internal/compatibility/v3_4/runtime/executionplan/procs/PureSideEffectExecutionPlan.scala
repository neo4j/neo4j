/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.procs

import org.neo4j.cypher.CypherVersion
import org.neo4j.cypher.internal.compatibility.v3_4.runtime._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.ExecutionPlan
import org.neo4j.cypher.internal.compiler.v3_4.{CacheCheckResult, FineToReuse}
import org.neo4j.cypher.internal.frontend.v3_4.PlannerName
import org.neo4j.cypher.internal.planner.v3_4.spi.{GraphStatistics, ProcedurePlannerName}
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.interpreted.UpdateCountingQueryContext
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.cypher.internal.runtime.planDescription.{NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
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

  private def description = PlanDescriptionImpl(Id.INVALID_ID, name, NoChildren,
                                                Seq(Planner(plannerUsed.toTextOutput),
                                                    PlannerImpl(plannerUsed.name),
                                                    PlannerVersion(plannerUsed.version),
                                                    Runtime(runtimeUsed.toTextOutput),
                                                    RuntimeImpl(runtimeUsed.name),
                                                    Version(s"CYPHER ${CypherVersion.default.name}"),
                                                    RuntimeVersion(CypherVersion.default.name))
                                                , Set.empty)

  override def runtimeUsed: RuntimeName = ProcedureRuntimeName

  override def checkPlanResusability(lastTxId: () => Long, statistics: GraphStatistics): CacheCheckResult = FineToReuse // TODO: Should this really always be reused?

  override def plannerUsed: PlannerName = ProcedurePlannerName

  override def isPeriodicCommit: Boolean = false
}
