/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders.prepare

import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{PartiallySolvedQuery, ExecutionPlanInProgress, PlanBuilder}
import org.neo4j.cypher.internal.compiler.v2_3.spi.{TokenContext, PlanContext}
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.KeyToken
import org.neo4j.cypher.internal.compiler.v2_3.pipes.PipeMonitor

class KeyTokenResolver extends PlanBuilder {

  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) = {
    val newPlan = apply(plan, ctx)
    plan != newPlan
  }

  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) = {
    val rewrittenQuery: PartiallySolvedQuery = plan.query.rewrite(resolveExpressions(_, ctx))
    plan.copy(query = rewrittenQuery)
  }

  def resolveExpressions(expr: Expression, ctx: TokenContext) = expr match {
    case (keyToken: KeyToken) => keyToken.resolve(ctx)
    case _                    => expr
  }
}
