/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.ProjectionClause
import org.neo4j.cypher.internal.ast.ReturnItem
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.expressions.DesugaredMapProjection
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IsAggregate
import org.neo4j.cypher.internal.expressions.IterablePredicateExpression
import org.neo4j.cypher.internal.expressions.ListComprehension
import org.neo4j.cypher.internal.expressions.ReduceExpression
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.rewriting.conditions.aggregationsAreIsolated
import org.neo4j.cypher.internal.rewriting.conditions.hasAggregateButIsNotAggregate
import org.neo4j.cypher.internal.util.AggregationNameGenerator
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.helpers.fixedPoint
import org.neo4j.cypher.internal.util.topDown

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
case object isolateAggregation extends StatementRewriter {

  override def instance(context: BaseContext): Rewriter = bottomUp(rewriter, _.isInstanceOf[Expression])

  override def description: String = "Makes sure that aggregations are on their own in RETURN/WITH clauses"

  override def postConditions: Set[Condition] = Set(StatementCondition(aggregationsAreIsolated))

  private val rewriter = Rewriter.lift {
    case q@SingleQuery(clauses) =>

      val newClauses = clauses.flatMap {
        case clause: ProjectionClause if clauseNeedingWork(clause) =>
          val clauseReturnItems = clause.returnItems.items
          val (withAggregations, others) = clauseReturnItems.map(_.expression).toSet.partition(hasAggregateButIsNotAggregate(_))

          val expressionsToIncludeInWith: Set[Expression] = others ++ extractExpressionsToInclude(withAggregations)

          val withReturnItems: Set[ReturnItem] = expressionsToIncludeInWith.map {
            e => AliasedReturnItem(e, Variable(AggregationNameGenerator.name(e.position))(e.position))(e.position)
          }
          val pos = clause.position
          val withClause = With(distinct = false, ReturnItems(includeExisting = false, withReturnItems.toIndexedSeq)(pos), None, None, None, None)(pos)

          val expressionRewriter = createRewriterFor(withReturnItems)
          val newReturnItems = clauseReturnItems.map {
            case ri@AliasedReturnItem(expression, _) => ri.copy(expression = expression.endoRewrite(expressionRewriter))(ri.position)
            case ri@UnaliasedReturnItem(expression, _) => ri.copy(expression = expression.endoRewrite(expressionRewriter))(ri.position)
          }
          val resultClause = clause.withReturnItems(newReturnItems)

          IndexedSeq(withClause, resultClause)

        case clause => IndexedSeq(clause)
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
    topDown(inner)
  }

  private def extractExpressionsToInclude(originalExpressions: Set[Expression]): Set[Expression] = {
    val expressionsToGoToWith: Set[Expression] = fixedPoint {
      expressions: Set[Expression] => expressions.flatMap {
        case e@ReduceExpression(_, init, coll) if hasAggregateButIsNotAggregate(e) =>
          Seq(init, coll)

        case e@ListComprehension(_, expr) if hasAggregateButIsNotAggregate(e) =>
          Seq(expr)

        case e@DesugaredMapProjection(variable, items, _) if hasAggregateButIsNotAggregate(e) =>
          items.map(_.exp) :+ variable

        case e: IterablePredicateExpression  if hasAggregateButIsNotAggregate(e) =>
          val predicate: Expression = e.innerPredicate.getOrElse(throw new IllegalStateException("Should never be empty"))
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

  private def clauseNeedingWork(c: Clause): Boolean = c.treeExists {
    case e: Expression => hasAggregateButIsNotAggregate(e)
  }
}
