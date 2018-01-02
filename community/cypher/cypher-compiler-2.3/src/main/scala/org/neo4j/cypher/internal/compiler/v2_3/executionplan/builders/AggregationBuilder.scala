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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_3.pipes.{PipeMonitor, EagerAggregationPipe}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{PlanBuilder, ExecutionPlanInProgress, PartiallySolvedQuery}
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{CachedExpression, AggregationExpression, Expression}
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.compiler.v2_3.commands.ReturnItem
import java.util.UUID
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext


/*
The work done here is non-trivial, so I'll try to explain it in English.

For this return-clause:
  RETURN m, head(collect(n.x)), last(collect(n.x))

we have to take the following steps.

Step 1:
Calculate and save to the execution context the values of the keys and aggregate expressions, in this case:
Key: m
Aggregation: collect(n.x)

The aggregation result will be saved under a random key, so it can be used to sort and calculate compound expressions.

Step 2:
Rewrite the remainder of the query to not use the aggregation expression, instead now using the key to the aggregation
value.
 */

class AggregationBuilder extends PlanBuilder  {
  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) = {
    // First, calculate the key expressions and save them down to the map
    val keyExpressionsToExtract: ExtractedExpressions = getExpressions(plan)
    val planToAggregate = ExtractBuilder.extractIfNecessary(plan, keyExpressionsToExtract.keys)

    // Get the aggregate expressions to calculate, and their named key expressions
    val expressions = getExpressions(planToAggregate)
    val seq = expressions.aggregates.map(exp => "  INTERNAL_AGGREGATE" + UUID.randomUUID() -> exp).toList
    val namedAggregates = seq.toMap

    val resultPipe = new EagerAggregationPipe(planToAggregate.pipe, expressions.keys.keySet, namedAggregates)()

    // Mark return items as done if they are extracted
    val returnItems = planToAggregate.query.returns.map {
      case r@Unsolved(ReturnItem(exp, name))
        if keyExpressionsToExtract.keys.values.exists(keyExp => keyExp == exp) => r.solve
      case r                                                                   => r
    }

    //Mark aggregations as Solved.
    val resultQ = planToAggregate.query.copy(
      aggregation = planToAggregate.query.aggregation.map(_.solve),
      aggregateToDo = false,
      returns = returnItems,
      extracted = true
    )

    //Rewrite the remainder of the query to use cached expression instead of the aggregate expressions
    val rewrittenQuery = rewriteQuery(namedAggregates, planToAggregate.pipe.symbols, resultQ)

    planToAggregate.copy(query = rewrittenQuery, pipe = resultPipe)
  }

  def rewriteQuery(namedAggregates: Map[String, AggregationExpression], symbols: SymbolTable, query: PartiallySolvedQuery): PartiallySolvedQuery = {
    namedAggregates.foldLeft(query) {
      case (p, (key, aggregate)) => p.rewrite(e =>
        if (e == aggregate)
          CachedExpression(key, e.getType(symbols))
        else
          e
      )
    }
  }

  private def getExpressions(plan: ExecutionPlanInProgress): ExtractedExpressions = {
    val keys: Seq[(String, Expression)] =
      plan.query.returns.flatMap(_.token.expressions(plan.pipe.symbols)).
      filterNot(_._2.containsAggregate)

    ExtractedExpressions(keys.toMap, plan.query.aggregation.map(_.token))
  }

  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) = {
    val q = plan.query

    q.aggregateToDo && q.readyToAggregate
  }
}

case class ExtractedExpressions(keys: Map[String, Expression],
                                aggregates: Seq[AggregationExpression])  {
  lazy val namedAggregations = aggregates.map( exp => "  INTERNAL_AGGREGATE" + exp.hashCode -> exp ).toMap
}
