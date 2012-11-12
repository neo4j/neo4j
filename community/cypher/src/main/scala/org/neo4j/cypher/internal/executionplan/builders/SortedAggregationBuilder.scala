/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import org.neo4j.cypher.internal.executionplan.{PartiallySolvedQuery, PlanBuilder}
import org.neo4j.cypher.internal.pipes.{OrderedAggregationPipe, SortPipe, ExtractPipe, Pipe}
import org.neo4j.cypher.internal.commands.{AggregationExpression, SortItem, Expression}

class SortedAggregationBuilder extends PlanBuilder {
  def apply(p: Pipe, q: PartiallySolvedQuery) = {
    val sortExpressions = q.sort.filter(_.unsolved).map(_.token.expression)
    val sortItems = q.sort.filter(_.unsolved).map(_.token)
    val keyExpressions = q.returns.filter(_.unsolved).map(_.token.expression).filterNot(_.containsAggregate)
    val aggregationExpressions = q.returns.flatMap(
      _.token.expression.
        filter(_.isInstanceOf[AggregationExpression]).
        map(_.asInstanceOf[AggregationExpression])
    )

    val extractPipe = new ExtractPipe(p, keyExpressions)

    val keyColumnsNotAlreadySorted = keyExpressions.
      filterNot(sortExpressions.contains).
      map(SortItem(_, true))

    val sortPipe = new SortPipe(extractPipe, (sortItems ++ keyColumnsNotAlreadySorted).toList)
    val aggregationPipe = new OrderedAggregationPipe(sortPipe, keyExpressions, aggregationExpressions)

    (aggregationPipe, q.copy(
      aggregation = q.aggregation.map(_.solve),
      aggregateQuery = q.aggregateQuery.solve,
      sort = q.sort.map(_.solve),
      extracted = true
    ))
  }

  def isDefinedAt(p: Pipe, q: PartiallySolvedQuery) = if (!q.readyToAggregate || q.aggregation.isEmpty)
    false //If other things are still to do, let's deny
  else {
    val sortExpressions = q.sort.filter(_.unsolved).map(_.token.expression)
    val keyExpressions = q.returns.filter(_.unsolved).map(_.token.expression).filterNot(_.containsAggregate)
    sortExpressions.nonEmpty && canUseOrderedAggregation(sortExpressions, keyExpressions)
  }

  private def canUseOrderedAggregation(sortExpressions: Seq[Expression], keyExpressions: Seq[Expression]): Boolean = keyExpressions.take(sortExpressions.size) == sortExpressions

  def priority: Int = 0
}