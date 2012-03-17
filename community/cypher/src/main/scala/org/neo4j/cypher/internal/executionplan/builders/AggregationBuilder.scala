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
import org.neo4j.cypher.internal.pipes.{ExtractPipe, EagerAggregationPipe, Pipe}
import org.neo4j.cypher.internal.commands.{Entity, AggregationExpression}

class AggregationBuilder extends PlanBuilder {
  def apply(p: Pipe, q: PartiallySolvedQuery) = {
    val keyExpressionsToExtract = q.returns.map(_.token.expression).filterNot(_.containsAggregate)
    val (extractor,newPsq) = ExtractBuilder.extractIfNecessary(q,p, keyExpressionsToExtract)
    val keyExpressions = newPsq.returns.map(_.token.expression).filterNot(_.containsAggregate)
    val aggregationExpressions = newPsq.aggregation.map(_.token)
    val aggregator = new EagerAggregationPipe(extractor, keyExpressions, aggregationExpressions)

    val notKeyAndNotAggregate = newPsq.returns.map(_.token.expression).filterNot(keyExpressions.contains)

    val resultPipe = if (notKeyAndNotAggregate.isEmpty) {
      aggregator
    } else {

      val rewritten = notKeyAndNotAggregate.map(e => {
        e.rewrite {
          case x: AggregationExpression => Entity(x.identifier.name)
          case x => x
        }
      })

        new ExtractPipe(aggregator, rewritten)
      }

      (resultPipe, newPsq.copy(
        aggregation = newPsq.aggregation.map(_.solve),
        aggregateQuery = newPsq.aggregateQuery.solve,
        extracted = true
      ))

    (resultPipe, q.copy(
      aggregation = q.aggregation.map(_.solve),
      aggregateQuery = q.aggregateQuery.solve,
      extracted = true
    ))
  }

  def isDefinedAt(p: Pipe, q: PartiallySolvedQuery) =
    q.aggregateQuery.token &&
      q.aggregateQuery.unsolved &&
      q.readyToAggregate

  def priority: Int = PlanBuilder.Aggregation
}