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

import org.neo4j.cypher.internal.compiler.v2_0.pipes.SortPipe
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.{PlanBuilder, ExecutionPlanInProgress}
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.{Identifier, CachedExpression, Expression}
import org.neo4j.cypher.CypherTypeException
import org.neo4j.cypher.internal.compiler.v2_0.spi.PlanContext

class SortBuilder extends PlanBuilder with SortingPreparations {
  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext) = {
    val newPlan = extractBeforeSort(plan)

    val q = newPlan.query
    val sortItems = q.sort.map(_.token)
    val resultPipe = new SortPipe(newPlan.pipe, sortItems.toList)

    val resultQ = q.copy(sort = q.sort.map(_.solve))

    plan.copy(pipe = resultPipe, query = resultQ)
  }

  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext) =
    plan.query.extracted &&
    plan.query.sort.filter(x => x.unsolved && !x.token.expression.containsAggregate).nonEmpty

  override def missingDependencies(plan: ExecutionPlanInProgress) = if (!plan.query.extracted) {
    Seq()
  } else {
    val aggregations = plan.query.sort.
      filter(_.token.expression.containsAggregate).
      map(_.token.expression.toString())

    if (aggregations.nonEmpty) {
      Seq("Aggregation expressions must be listed in the RETURN/WITH clause to be used in ORDER BY")
    } else {
      Seq()
    }
  }
}

trait SortingPreparations {
  def extractBeforeSort(plan: ExecutionPlanInProgress): ExecutionPlanInProgress = {
    val sortExpressionsToExtract: Seq[(String, Expression)] = plan.query.sort.flatMap(x => x.token.expression match {
      case _: CachedExpression => None
      case _: Identifier       => None
      case e                   => Some("  UNNAMEDS" + e.## -> e)
    })

    try {
      ExtractBuilder.extractIfNecessary(plan, sortExpressionsToExtract.toMap)
    } catch {
      case e: CypherTypeException => throw new CypherTypeException(e.getMessage + " - maybe aggregation removed it?")
    }
  }
}
