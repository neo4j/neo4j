/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NestedPlanCollectExpression
import org.neo4j.cypher.internal.logical.plans.NestedPlanExistsExpression
import org.neo4j.cypher.internal.logical.plans.NestedPlanGetByNameExpression
import org.neo4j.cypher.internal.runtime.expressionVariableAllocation.AvailableExpressionVariables
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp

object NestedPipeExpressions {

  def build(
    pipeBuilder: PipeTreeBuilder,
    in: LogicalPlan,
    availableExpressionVariables: AvailableExpressionVariables,
    cancellationChecker: CancellationChecker
  ): LogicalPlan = {

    val buildPipeExpressions: Rewriter = new Rewriter {
      private val instance = bottomUp(Rewriter.lift {
        case expr @ NestedPlanExistsExpression(patternPlan, _) =>
          val availableForPlan = availableExpressionVariables(patternPlan.id)
          val pipe = pipeBuilder.build(patternPlan, cancellationChecker)
          NestedPipeExistsExpression(pipe, availableForPlan)(expr.position)

        case expr @ NestedPlanCollectExpression(patternPlan, expression, _) =>
          val availableForPlan = availableExpressionVariables(patternPlan.id)
          val pipe = pipeBuilder.build(patternPlan, cancellationChecker)
          NestedPipeCollectExpression(pipe, expression, availableForPlan)(expr.position)

        case expr @ NestedPlanGetByNameExpression(patternPlan, columnToGet, _) =>
          val availableForPlan = availableExpressionVariables(patternPlan.id)
          val pipe = pipeBuilder.build(patternPlan, cancellationChecker)
          NestedPipeGetByNameExpression(pipe, columnToGet.name, availableForPlan)(expr.position)
      })

      override def apply(that: AnyRef): AnyRef = instance.apply(that)
    }
    in.endoRewrite(buildPipeExpressions)
  }
}
