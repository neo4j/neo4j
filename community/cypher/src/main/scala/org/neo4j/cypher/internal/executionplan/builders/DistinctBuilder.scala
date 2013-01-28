/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.executionplan.builders

import org.neo4j.cypher.internal.executionplan.{ExecutionPlanInProgress, PlanBuilder}
import org.neo4j.cypher.internal.pipes.DistinctPipe
import org.neo4j.cypher.internal.commands.expressions.Expression


class DistinctBuilder extends PlanBuilder {
  def apply(plan: ExecutionPlanInProgress) = {

    //Extract expressions from return items
    val expressions: Map[String, Expression] =
      plan.query.returns.flatMap(_.token.expressions(plan.pipe.symbols)).toMap

    val pipe = new DistinctPipe(plan.pipe, expressions)

    //Mark stuff as done
    val query = plan.query.copy(
      aggregateQuery = Solved(true),
      extracted = true,
      returns = plan.query.returns.map(_.solve)
    )

    plan.copy(pipe = pipe, query = query)
  }

  def canWorkWith(plan: ExecutionPlanInProgress) = {

    plan.query.aggregateQuery == Unsolved(true) && //The parser marks DISTINCT queries as aggregates. Revisit?
      plan.query.aggregation.isEmpty &&            //It's an aggregate query without aggregate expressions
      plan.query.readyToAggregate &&
      plan.query.returns.exists {
        case Unsolved(column) =>
          val symbols = plan.pipe.symbols
          column.expressions(symbols).values.forall(e => e.symbolDependenciesMet(symbols))

        case _                => false
      }
  }

  def priority = PlanBuilder.Distinct
}