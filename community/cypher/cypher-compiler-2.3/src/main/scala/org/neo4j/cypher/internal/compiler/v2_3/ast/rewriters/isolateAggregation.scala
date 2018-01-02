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
package org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_3.helpers.AggregationNameGenerator
import org.neo4j.cypher.internal.compiler.v2_3.helpers.Converge.iterateUntilConverged
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.{Rewriter, bottomUp}

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
  def apply(that: AnyRef): AnyRef = instance(that)

  private val rewriter = Rewriter.lift {
    case q@SingleQuery(clauses) =>

      val newClauses = clauses.flatMap {
        case clause if !clauseNeedingWork(clause) => IndexedSeq(clause)
        case clause =>
          val (withAggregations, others) = getExpressions(clause).partition(hasAggregateButIsNotAggregate(_))

          val expressionsToIncludeInWith: Set[Expression] = others ++ extractExpressionsToInclude(withAggregations)

          val withReturnItems: Set[ReturnItem] = expressionsToIncludeInWith.map {
            e => AliasedReturnItem(e, Identifier(AggregationNameGenerator.name(e.position))(e.position))(e.position)
          }
          val pos = clause.position
          val withClause = With(distinct = false, ReturnItems(includeExisting = false, withReturnItems.toIndexedSeq)(pos), None, None, None, None)(pos)

          val expressionRewriter = createRewriterFor(withReturnItems)
          val resultClause = clause.endoRewrite(expressionRewriter)

          IndexedSeq(withClause, resultClause)
      }

      q.copy(clauses = newClauses)(q.position)
  }

  private def createRewriterFor(withReturnItems: Set[ReturnItem]): Rewriter = {
    def inner = Rewriter.lift {
      case original: Expression =>
        val rewrittenExpression = withReturnItems.collectFirst {
          case item@AliasedReturnItem(expression, variable) if original == expression =>
            item.alias.get.copyId
        }
        rewrittenExpression getOrElse original
    }

    ReturnItemSafeTopDownRewriter(inner)
  }

  private def extractExpressionsToInclude(originalExpressions: Set[Expression]): Set[Expression] = {
    val expressionsToGoToWith: Set[Expression] = iterateUntilConverged {
      (expressions: Set[Expression]) => expressions.flatMap {
        case e@ReduceExpression(_, init, coll) if hasAggregateButIsNotAggregate(e) =>
          Seq(init, coll)

        case e@FilterExpression(_, expr) if hasAggregateButIsNotAggregate(e) =>
          Seq(expr)

        case e@ExtractExpression(_, expr) if hasAggregateButIsNotAggregate(e) =>
          Seq(expr)

        case e@ListComprehension(_, expr) if hasAggregateButIsNotAggregate(e) =>
          Seq(expr)

        case e if hasAggregateButIsNotAggregate(e) =>
          e.arguments

        case e =>
          Seq(e)
      }
    }(originalExpressions).filter {
      //Constant expressions should never be isolated
      expr => IsAggregate(expr) || expr.dependencies.nonEmpty
    }
    expressionsToGoToWith
  }

  private val instance = bottomUp(rewriter)

  private def getExpressions(c: Clause): Set[Expression] = c match {
    case clause: Return => clause.returnItems.items.map(_.expression).toSet
    case clause: With => clause.returnItems.items.map(_.expression).toSet
    case _ => Set.empty
  }

  private def clauseNeedingWork(c: Clause): Boolean = c.exists {
    case e: Expression => hasAggregateButIsNotAggregate(e)
  }
}
