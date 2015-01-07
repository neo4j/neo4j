/**
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
package org.neo4j.cypher.internal.compiler.v2_0.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_0.executionplan.{PlanBuilder, ExecutionPlanInProgress}
import org.neo4j.cypher.internal.compiler.v2_0.pipes.DistinctPipe
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_0.spi.PlanContext


class DistinctBuilder extends PlanBuilder {
  def apply(p: ExecutionPlanInProgress, ctx: PlanContext) = {
    val plan = ExtractBuilder.extractIfNecessary(p, getExpressions(p))

    val expressions = getExpressions(plan)

    val pipe = new DistinctPipe(plan.pipe, expressions)

    //Mark stuff as done
    val query = plan.query.copy(
      aggregateToDo = false,
      extracted = true,
      returns = plan.query.returns.map(_.solve)
    )

    plan.copy(pipe = pipe, query = query)
  }

  private def getExpressions(plan:ExecutionPlanInProgress): Map[String, Expression] =
    plan.query.returns.flatMap(_.token.expressions(plan.pipe.symbols)).toMap

  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext) = {

      plan.query.aggregateToDo &&                  //The parser marks DISTINCT queries as aggregates. Revisit?
      plan.query.aggregation.isEmpty &&            //It's an aggregate query without aggregate expressions
      plan.query.readyToAggregate &&
      plan.query.returns.exists {
        case Unsolved(column) =>
          val symbols = plan.pipe.symbols
          column.expressions(symbols).values.forall(e => e.symbolDependenciesMet(symbols))

        case _                => false
      }
  }
}
