/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_1.{bottomUp, AggregatingFunction, Rewriter}
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.ast.CountStar
import org.neo4j.cypher.internal.helpers.Converge.iterateUntilConverged
import org.neo4j.cypher.internal.compiler.v2_1.helpers.AggregationNameGenerator

/**
 * This rewriter makes sure that aggregations are on their own in RETURN/WITH clauses, so
 * the planner can have an easy time
 *
 * Example:
 *
 * MATCH (n)
 * RETURN { name: n.name, count: count(*) }, n.foo
 *
 * This query has a RETURN clause where the single expression contains both the aggregate key and
 * the aggregation expression. To make the job easier on the planner, this rewrite will change the query to:
 *
 * MATCH (n)
 * WITH n.name AS x1, count(*) AS x2, n.foo as X3
 * RETURN { name: x1, count: x2 }
 */
case object isolateAggregation extends Rewriter {
  def apply(in: AnyRef): Option[AnyRef] = bottomUp(instance).apply(in)

  private val instance = Rewriter.lift {
    case q@SingleQuery(clauses) =>

      val newClauses = clauses.flatMap {
        case c if !clauseNeedingWork(c) => Some(c)
        case c =>
          val originalExpressions = getExpressions(c)

          val expressionsToGoToWith: Seq[Expression] = iterateUntilConverged {
            (expressions: Seq[Expression]) => expressions.flatMap {
              case e if hasAggregateButIsNotAggregate(e) =>
                e match {
                  case ReduceExpression(_, init, _, coll, _) => Seq(init, coll)
                  case FilterExpression(_, expr, _)          => Some(expr)
                  case ExtractExpression(_, expr, _, _)      => Some(expr)
                  case ListComprehension(_, expr, _, _)      => Some(expr)
                  case _                                     => e.arguments
                }

              case e =>
                Some(e)

            }
          }(originalExpressions)

          val withReturnItems: Seq[ReturnItem] = expressionsToGoToWith.map {
            case id: Identifier => AliasedReturnItem(id, id)(id.position)
            case e              => AliasedReturnItem(e, Identifier(AggregationNameGenerator.name(e.position.offset))(e.position))(e.position)
          }
          val pos = c.position
          val withClause = With(distinct = false, ListedReturnItems(withReturnItems)(pos), None, None, None, None)(pos)

          val resultClause = c.endoRewrite(bottomUp(Rewriter.lift {
            case unalteredItem@UnaliasedReturnItem(id:Identifier, _) if originalExpressions.contains(id) =>
              unalteredItem

            case ri: UnaliasedReturnItem =>

              AliasedReturnItem(ri.expression, Identifier(ri.inputText)(ri.position))(ri.position)

            case e: Expression =>
              withReturnItems.collectFirst {
                case AliasedReturnItem(expression, identifier) if e == expression => identifier
              }.getOrElse(e)
          }))

          Seq(withClause, resultClause)
      }

      q.copy(clauses = newClauses)(q.position)
  }

  private def getExpressions(c: Clause): Seq[Expression] = c match {
    case Return(_, ListedReturnItems(returnItems), _, _, _) => returnItems.map(_.expression)
    case With(_, ListedReturnItems(returnItems), _, _, _, _) => returnItems.map(_.expression)
    case _ => Seq.empty
  }

  private def clauseNeedingWork(c: Clause): Boolean = c.exists {
    case e: Expression => hasAggregateButIsNotAggregate(e)
  }

  private def hasAggregateButIsNotAggregate(e: Expression): Boolean = e match {
    case IsAggregate(_) => false
    case e: Expression  => containsAggregate(e)
  }
}
