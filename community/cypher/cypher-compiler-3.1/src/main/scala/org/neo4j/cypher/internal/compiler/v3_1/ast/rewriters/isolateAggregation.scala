/*
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
package org.neo4j.cypher.internal.compiler.v3_1.ast.rewriters

import org.neo4j.cypher.internal.compiler.v3_1.helpers.AggregationNameGenerator
import org.neo4j.cypher.internal.frontend.v3_1.Foldable._
import org.neo4j.cypher.internal.frontend.v3_1.Rewritable._
import org.neo4j.cypher.internal.frontend.v3_1.ast._
import org.neo4j.cypher.internal.frontend.v3_1.helpers.fixedPoint
import org.neo4j.cypher.internal.frontend.v3_1.{InternalException, Rewriter, bottomUp}

import scala.annotation.tailrec
import scala.collection.mutable

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
        case clause if !clauseNeedingWork(clause) => Some(clause)
        case clause =>
          val originalExpressions = getExpressions(clause)

          val expressionsToIncludeInWith: Set[Expression] = extractExpressionsToInclude(originalExpressions)

          val withReturnItems: Set[ReturnItem] = expressionsToIncludeInWith.map {
            case e => AliasedReturnItem(e, Variable(AggregationNameGenerator.name(e.position))(e.position))(e.position)
          }
          val pos = clause.position
          val withClause = With(distinct = false, ReturnItems(includeExisting = false, withReturnItems.toSeq)(pos), None, None, None, None)(pos)

          val expressionRewriter = createRewriterFor(withReturnItems)
          val resultClause = clause.endoRewrite(expressionRewriter)

          Seq(withClause, resultClause)
      }

      q.copy(clauses = newClauses)(q.position)
  }

  private def createRewriterFor(withReturnItems: Set[ReturnItem]): Rewriter = {
    def inner: Rewriter =
      Rewriter.lift {
        case original: Expression =>
          val rewrittenExpression = withReturnItems.collectFirst {
            case item@AliasedReturnItem(expression, variable) if original == expression =>
              item.alias.get.copyId
          }
          rewrittenExpression getOrElse original
      }

    /*
    Instead of using topDown, we do it manually here, because we don't want to rewrite the return aliases,
    only the expressions
    */
    new Rewriter {

      override def apply(that: AnyRef): AnyRef = {
        val initialStack = mutable.ArrayStack((List(that), new mutable.MutableList[AnyRef]()))
        val result = rec(initialStack)
        assert(result.size == 1)
        result.head
      }

      @tailrec
      def rec(stack: mutable.ArrayStack[(List[AnyRef], mutable.MutableList[AnyRef])]): mutable.MutableList[AnyRef] = {
        val (currentJobs, _) = stack.top
        if (currentJobs.isEmpty) {
          val (_, newChildren) = stack.pop()
          if (stack.isEmpty) {
            newChildren
          } else {
            stack.pop() match {
              case (Nil, _) => throw new InternalException("here only to stop warnings. should never happen")
              case ((returnItem@AliasedReturnItem(expression, variable)) :: jobs, doneJobs) =>
                val newExpression = newChildren.head.asInstanceOf[Expression]
                val newReturnItem = returnItem.copy(expression = newExpression)(returnItem.position)
                stack.push((jobs, doneJobs += newReturnItem))
              case (job :: jobs, doneJobs) =>
                val doneJob = job.dup(newChildren)
                stack.push((jobs, doneJobs += doneJob))
            }

            rec(stack)
          }
        } else {
          val (newJob :: jobs, doneJobs) = stack.pop()
          if (false) {
            stack.push((jobs, doneJobs += newJob))
          } else {
            val rewrittenJob = newJob.rewrite(inner)
            stack.push((rewrittenJob :: jobs, doneJobs))
            stack.push((rewrittenJob.children.toList, new mutable.MutableList()))
          }
          rec(stack)
        }
      }

    }
  }

  private def extractExpressionsToInclude(originalExpressions: Set[Expression]): Set[Expression] = {
    val expressionsToGoToWith: Set[Expression] = fixedPoint {
      (expressions: Set[Expression]) => expressions.flatMap {
        case e if hasAggregateButIsNotAggregate(e) =>
          e match {
            case ReduceExpression(_, init, coll) => Seq(init, coll)
            case FilterExpression(_, expr) => Seq(expr)
            case ExtractExpression(_, expr) => Seq(expr)
            case ListComprehension(_, expr) => Seq(expr)
            case DesugaredMapProjection(variable, items, _) => items.map(_.exp) :+ variable
            case _ => e.arguments
          }

        case e =>
          Seq(e)

      }
    }(originalExpressions).filter {
      //Constant expressions should never be isolated
      case ConstantExpression(_) => false
      case expr => true
    }
    expressionsToGoToWith
  }

  private val instance = bottomUp(rewriter, _.isInstanceOf[Expression])

  private def getExpressions(c: Clause): Set[Expression] = c match {
    case clause: Return => clause.returnItems.items.map(_.expression).toSet
    case clause: With => clause.returnItems.items.map(_.expression).toSet
    case _ => Set.empty
  }

  private def clauseNeedingWork(c: Clause): Boolean = c.exists {
    case e: Expression => hasAggregateButIsNotAggregate(e)
  }
}
