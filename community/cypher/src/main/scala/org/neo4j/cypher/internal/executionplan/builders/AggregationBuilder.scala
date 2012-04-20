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

import org.neo4j.cypher.internal.pipes.{ExtractPipe, EagerAggregationPipe}
import org.neo4j.cypher.internal.commands.{Entity, Expression, AggregationExpression}
import org.neo4j.cypher.internal.executionplan.{ExecutionPlanInProgress, PartiallySolvedQuery, PlanBuilder}

class AggregationBuilder extends PlanBuilder {
  def apply(plan: ExecutionPlanInProgress) = {
    val q = plan.query
    val p = plan.pipe

    val keyExpressionsToExtract = q.returns.map(_.token.expression).filterNot(_.containsAggregate)

    val (extractor, psq) = ExtractBuilder.extractIfNecessary(q, p, keyExpressionsToExtract)
    val keyExpressions = psq.returns.map(_.token.expression).filterNot(_.containsAggregate)
    val aggregationExpressions: Seq[AggregationExpression] = getAggregationExpressions(psq)

    val aggregator = new EagerAggregationPipe(extractor, keyExpressions, aggregationExpressions)

    val notKeyAndNotAggregate = psq.returns.map(_.token.expression).filterNot(keyExpressions.contains)

    val resultPipe = if (notKeyAndNotAggregate.isEmpty) {
      aggregator
    } else {

      val rewritten = notKeyAndNotAggregate.map(e => {
        e.rewrite(removeAggregates)
      })

      new ExtractPipe(aggregator, rewritten)
    }

    val resultQ = psq.copy(
      aggregation = psq.aggregation.map(_.solve),
      aggregateQuery = psq.aggregateQuery.solve,
      extracted = true
    ).rewrite(removeAggregates)

    plan.copy(query = resultQ, pipe = resultPipe)
  }

  private def removeAggregates(e: Expression) = e match {
    case e: AggregationExpression => Entity(e.identifier.name)
    case x => x
  }

  private def getAggregationExpressions(psq: PartiallySolvedQuery): Seq[AggregationExpression] = {
    val eventualSortAggregation = psq.sort.filter(_.token.expression.isInstanceOf[AggregationExpression]).map(_.token.expression.asInstanceOf[AggregationExpression])
    val aggregations = psq.aggregation.map(_.token)
    (aggregations ++ eventualSortAggregation).distinct
  }

  def canWorkWith(plan: ExecutionPlanInProgress) = {
    val q = plan.query

    q.aggregateQuery.token &&
      q.aggregateQuery.unsolved &&
      q.readyToAggregate

  }

  def priority: Int = PlanBuilder.Aggregation
}