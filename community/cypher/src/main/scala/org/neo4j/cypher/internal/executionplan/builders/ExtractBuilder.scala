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

import org.neo4j.cypher.internal.pipes.{ExtractPipe, Pipe}
import org.neo4j.cypher.internal.executionplan.{PartiallySolvedQuery, PlanBuilder}
import org.neo4j.cypher.internal.commands.{CachedExpression, Expression}

class ExtractBuilder extends PlanBuilder {
  def apply(p: Pipe, q: PartiallySolvedQuery) = ExtractBuilder.extractIfNecessary(q, p, q.returns.map(_.token.expression))

  def isDefinedAt(p: Pipe, q: PartiallySolvedQuery)= !q.extracted && q.readyToAggregate && q.aggregateQuery.solved

  def priority: Int = PlanBuilder.Extraction
}

object ExtractBuilder {

  def extractIfNecessary(psq: PartiallySolvedQuery, p: Pipe, expressions: Seq[Expression]): (Pipe, PartiallySolvedQuery) = {
    val missing = p.symbols.missingExpressions(expressions)

    if (missing.nonEmpty) {
      val newPsq = expressions.foldLeft(psq)((psq, exp) => psq.rewrite(fromQueryExpression =>
        if (exp == fromQueryExpression)
          CachedExpression(fromQueryExpression.identifier.name, fromQueryExpression.identifier)
        else
          fromQueryExpression
      ))
      (new ExtractPipe(p, expressions), newPsq.copy(extracted = true))
    } else {
      (p, psq.copy(extracted = true))
    }
  }
}