/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters

import org.neo4j.cypher.internal.util.v3_4.{InternalException, Rewriter, bottomUp}
import org.neo4j.cypher.internal.v3_4.expressions._
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.helpers.{AggregationNameGenerator, fixedPoint}

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
            case e => AliasedReturnItem(e, Variable(AggregationNameGenerator.name(e.position))(e.position))(e.position)
          }
          val pos = clause.position
          val withClause = With(distinct = false, ReturnItems(includeExisting = false, withReturnItems.toIndexedSeq)(pos), PassAllGraphReturnItems(pos), None, None, None, None)(pos)

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
    val expressionsToGoToWith: Set[Expression] = fixedPoint {
      (expressions: Set[Expression]) => expressions.flatMap {
        case e@ReduceExpression(_, init, coll) if hasAggregateButIsNotAggregate(e) =>
          Seq(init, coll)

        case e@FilterExpression(_, expr) if hasAggregateButIsNotAggregate(e) =>
          Seq(expr)

        case e@ExtractExpression(_, expr) if hasAggregateButIsNotAggregate(e) =>
          Seq(expr)

        case e@ListComprehension(_, expr) if hasAggregateButIsNotAggregate(e) =>
          Seq(expr)

        case e@DesugaredMapProjection(variable, items, _) if hasAggregateButIsNotAggregate(e) =>
          items.map(_.exp) :+ variable

        case e: IterablePredicateExpression  if hasAggregateButIsNotAggregate(e) =>
          val predicate: Expression = e.innerPredicate.getOrElse(throw new InternalException("Should never be empty"))
          // Weird way of doing it to make scalac happy
          Set(e.expression) ++ predicate.dependencies - e.variable

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

  private val instance = bottomUp(rewriter, _.isInstanceOf[Expression])

  private def getExpressions(c: Clause): Set[Expression] = c match {
    case clause: Return => clause.returnItems.items.map(_.expression).toSet
    case clause: With => clause.returnItems.items.map(_.expression).toSet
    case _ => Set.empty
  }

  private def clauseNeedingWork(c: Clause): Boolean = c.treeExists {
    case e: Expression => hasAggregateButIsNotAggregate(e)
  }
}
