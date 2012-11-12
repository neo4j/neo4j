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

import org.neo4j.cypher.internal.pipes.ExtractPipe
import org.neo4j.cypher.internal.commands.{CachedExpression, Expression}
import org.neo4j.cypher.internal.executionplan.{ExecutionPlanInProgress, PlanBuilder}

class ExtractBuilder extends PlanBuilder {
  def apply(plan: ExecutionPlanInProgress) = {
    val expressions = plan.query.returns.flatMap(_.token.expressions(plan.pipe.symbols)).distinct
    ExtractBuilder.extractIfNecessary(plan, expressions)
  }

  def canWorkWith(plan: ExecutionPlanInProgress) = {
    val q = plan.query
    !q.extracted && q.readyToAggregate && q.aggregateQuery.solved
  }

  def priority: Int = PlanBuilder.Extraction
}

object ExtractBuilder {

  def extractIfNecessary(plan: ExecutionPlanInProgress, expressions: Seq[Expression]): (ExecutionPlanInProgress) = {
    val missing = plan.pipe.symbols.missingExpressions(expressions)
    val query = plan.query
    val pipe = plan.pipe

    if (missing.nonEmpty) {
      val newPsq = expressions.foldLeft(query)((psq, exp) => psq.rewrite(fromQueryExpression =>
        if (exp == fromQueryExpression)
          CachedExpression(fromQueryExpression.identifier.name, fromQueryExpression.identifier)
        else
          fromQueryExpression
      ))

      val resultPipe = new ExtractPipe(pipe, expressions)
      val resultQuery = newPsq.copy(extracted = true)
      plan.copy(pipe = resultPipe, query = resultQuery)
    } else {
      plan.copy(query = query.copy(extracted = true))
    }
  }
}