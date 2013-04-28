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

import org.neo4j.cypher.internal.pipes.ExtractPipe
import org.neo4j.cypher.internal.executionplan.{PlanBuilder, ExecutionPlanInProgress, LegacyPlanBuilder}
import org.neo4j.cypher.internal.commands.expressions.{Identifier, CachedExpression, Expression}

class ExtractBuilder extends LegacyPlanBuilder {
  def apply(plan: ExecutionPlanInProgress) = {

    val expressions: Map[String, Expression] =
      plan.query.returns.flatMap(_.token.expressions(plan.pipe.symbols)).toMap

    ExtractBuilder.extractIfNecessary(plan, expressions)
  }

  def canWorkWith(plan: ExecutionPlanInProgress) = {
    val q = plan.query
    !q.extracted && q.readyToAggregate && !q.aggregateToDo
  }

  def priority: Int = PlanBuilder.Extraction
}

object ExtractBuilder {
  def extractIfNecessary(plan: ExecutionPlanInProgress, expressionsToExtract: Map[String, Expression]): ExecutionPlanInProgress = {

    val expressions = expressionsToExtract.filter {
      case (k, CachedExpression(_, _))      => false
      case (k1, Identifier(k2)) if k1 == k2 => false
      case _                                => true
    }

    val query = plan.query
    val pipe = plan.pipe

    if (expressions.nonEmpty) {
      val newPsq = expressions.foldLeft(query)((psq, exp) => psq.rewrite(fromQueryExpression => {
        if (exp._2 == fromQueryExpression)
          CachedExpression(exp._1, fromQueryExpression.getType(plan.pipe.symbols))
        else
          fromQueryExpression
      }
      ))

      val resultPipe = ExtractPipe(pipe, expressions)
      val resultQuery = newPsq.copy(extracted = true)
      plan.copy(pipe = resultPipe, query = resultQuery)
    } else {
      plan.copy(query = query.copy(extracted = true))
    }
  }
}