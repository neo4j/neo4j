/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.expressionVariableAllocation.AvailableExpressionVariables
import org.neo4j.cypher.internal.v4_0.util.Rewriter
import org.neo4j.cypher.internal.v4_0.util.bottomUp
import org.neo4j.cypher.internal.logical.plans.{LogicalPlan, NestedPlanExpression}

object NestedPipeExpressions {

  def build(pipeBuilder: PipeTreeBuilder,
            in: LogicalPlan,
            availableExpressionVariables: AvailableExpressionVariables): LogicalPlan = {

    val buildPipeExpressions: Rewriter = new Rewriter {
      private val instance = bottomUp(Rewriter.lift {
        case expr@NestedPlanExpression(patternPlan, expression) =>
          val availableForPlan = availableExpressionVariables(patternPlan.id)
          val pipe = pipeBuilder.build(patternPlan)
          val result = NestedPipeExpression(pipe, expression, availableForPlan)(expr.position)
          result
      })

      override def apply(that: AnyRef): AnyRef = instance.apply(that)
    }
    in.endoRewrite(buildPipeExpressions)
  }
}
