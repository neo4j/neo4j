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
package org.neo4j.cypher.internal.compiler.v2_0.executionplan.builders.prepare

import org.neo4j.cypher.internal.compiler.v2_0._
import commands._
import commands.expressions.{Identifier, AggregationExpression, Expression}
import commands.ReturnItem
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.{RandomNamer, PartiallySolvedQuery, ExecutionPlanInProgress, PlanBuilder}
import executionplan.builders.{Unsolved, QueryToken}
import spi.PlanContext

/**
 * This rewriter makes sure that aggregations are on their own in RETURN/WITH clauses, so
 * the plan builders can have an easy time
 *
 * Example:
 *
 * MATCH (n)
 * RETURN { name: n.name, count: count(*) }
 *
 * This query has a RETURN clause where the single expression contains both the aggregate key and
 * the aggregation expression. To make the job easier on the planbuilders, this rewrite will change the query to:
 *
 * MATCH (n)
 * WITH n.name AS x1, count(*) AS x2
 * RETURN { name: x1, count: x2 }
 *
 * CacheNamer is there to make this class easy to test
 */
case class AggregationPreparationRewriter(cacheNamer: Option[Expression => String] = None) extends PlanBuilder {
  val namer: Expression => String = cacheNamer.getOrElse(new RandomNamer)

  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext): ExecutionPlanInProgress = {
    val query: PartiallySolvedQuery = plan.query

    plan.copy(query = query.rewriteFromTheTail(rewrite))
  }

  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext): Boolean =
    plan.query.returns.exists(rc => returnColumnToRewrite(rc.token))

  private def returnColumnToRewrite: ReturnColumn => Boolean = {
    case ReturnItem(e, _, _) => !e.isInstanceOf[AggregationExpression] && e.containsAggregate
    case _                   => false
  }

  private def rewrite(origQuery: PartiallySolvedQuery): PartiallySolvedQuery = {
    val returnColumnsToRewrite: Seq[ReturnItem] = origQuery.returns.collect {
      case Unsolved(ri) if returnColumnToRewrite(ri) => ri.asInstanceOf[ReturnItem]
    }

    if (returnColumnsToRewrite.isEmpty) {
      origQuery
    }
    else {
      val expressionsToRewrite = returnColumnsToRewrite.map(ri => ri.expression)
      val childExpressionsToAddToWith = expressionsToRewrite.flatMap(e => e.arguments)
      val expressionsToKeep = origQuery.returns.collect {
        case QueryToken(rc@ReturnItem(e, _, _)) if !returnColumnToRewrite(rc) => e
      }

      val initialCacheExpressions: Seq[Expression] = childExpressionsToAddToWith ++ expressionsToKeep

      var sortedCacheExpressions: Seq[Expression] = Seq.empty
      for (expr <- initialCacheExpressions) {
        val len = sortedCacheExpressions.prefixLength(!_.contains(expr))
        sortedCacheExpressions = (sortedCacheExpressions.take(len) :+ expr) ++ sortedCacheExpressions.drop(len)
      }

      val cacheNameLookup = sortedCacheExpressions.reverse.map(e => e -> namer(e))

      val subExpressionsAsReturnItems: Seq[ReturnColumn] = cacheNameLookup.map {
        case (e, name) => ReturnItem(e, name, renamed = true)
      }.toSeq

      val oldTail = origQuery.tail

      val newTail = PartiallySolvedQuery().
        copy(
          slice = origQuery.slice,
          sort = origQuery.sort,
          tail = oldTail,
          returns = origQuery.returns
        )

      val newNewTail = cacheNameLookup.foldLeft(newTail) {
        case (psq, (cacheE, name)) => psq.rewrite {
          case e if e == cacheE => Identifier(name)
          case e => e
        }
      }

      val returnColumnsNotTouched: Seq[QueryToken[ReturnColumn]] = origQuery.returns.filter(!_.token
        .isInstanceOf[ReturnItem])
      val newQuery = origQuery.copy(
        slice = None,
        sort = Seq.empty,
        tail = Some(newNewTail),
        returns = subExpressionsAsReturnItems.map(Unsolved.apply) ++ returnColumnsNotTouched)

      newQuery
    }
  }
}
