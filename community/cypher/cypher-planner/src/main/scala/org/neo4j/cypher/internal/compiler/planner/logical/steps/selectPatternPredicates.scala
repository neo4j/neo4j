/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections.containsExistsSubquery
import org.neo4j.cypher.internal.ir.ast.ExistsIRExpression
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator

case object SelectPatternPredicates extends SelectionCandidateGenerator {

  override def apply(
    lhs: LogicalPlan,
    unsolvedPredicates: Set[Expression],
    queryGraph: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Iterator[SelectionCandidate] = {
    for {
      pattern <- unsolvedPredicates.iterator.filter(containsExistsSubquery)
      if queryGraph.argumentIds.subsetOf(lhs.availableSymbols.map(_.name))
    } yield {
      val plan = pattern match {
        case p: ExistsIRExpression =>
          val rhs = rhsPlan(lhs, p, context)
          context.staticComponents.logicalPlanProducer.planSemiApply(lhs, rhs, p, context)
        case p @ Not(e: ExistsIRExpression) =>
          val rhs = rhsPlan(lhs, e, context)
          context.staticComponents.logicalPlanProducer.planAntiSemiApply(lhs, rhs, p, context)
        case o @ Ors(exprs) =>
          val (subqueryExpressions, expressions) = exprs.partition {
            case ExistsIRExpression(_, _, _)      => true
            case Not(ExistsIRExpression(_, _, _)) => true
            case _                                => false
          }
          val (plan, solvedPredicates) =
            planPredicates(lhs, subqueryExpressions, expressions, None, interestingOrderConfig, context)
          AssertMacros.checkOnlyWhenAssertionsAreEnabled(
            exprs.forall(solvedPredicates.contains),
            "planPredicates is supposed to solve all predicates in an OR clause."
          )
          context.staticComponents.logicalPlanProducer.solvePredicate(plan, o)
        case p => throw new IllegalStateException(
            s"Only ExistsIRExpression (potentially nested in Not/Ors) allowed here. Got: $p"
          )
      }
      SelectionCandidate(plan, Set(pattern))
    }
  }

  def planPredicates(
    lhs: LogicalPlan,
    subqueryExpressions: Set[Expression],
    expressions: Set[Expression],
    letExpression: Option[Expression],
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): (LogicalPlan, Set[Expression]) = {

    def planSelect(
      predicate: Expression,
      source: LogicalPlan,
      func: (LogicalPlan, LogicalPlan, Expression, LogicalPlanningContext) => LogicalPlan
    ): (LogicalPlan, Set[Expression]) = {
      val plan = func(lhs, source, onePredicate(expressions ++ letExpression.toSet), context)
      (plan, expressions + predicate)
    }

    def planSemiApply(
      predicate: Expression,
      innerExpression: Expression,
      tail: List[Expression],
      source: LogicalPlan
    ): (LogicalPlan, Set[Expression]) = {
      val (newLhs, newLetExpr) = predicate match {
        case Not(_) => createLetAntiSemiApply(lhs, source, innerExpression, expressions, letExpression, context)
        case _      => createLetSemiApply(lhs, source, innerExpression, expressions, letExpression, context)
      }
      val (plan, solvedPredicates) =
        planPredicates(newLhs, tail.toSet, Set.empty, Some(newLetExpr), interestingOrderConfig, context)
      (plan, solvedPredicates ++ Set(predicate) ++ expressions)
    }

    subqueryExpressions.toList match {
      case (p: ExistsIRExpression) :: Nil =>
        val rhs = rhsPlan(lhs, p, context)
        planSelect(p, rhs, context.staticComponents.logicalPlanProducer.planSelectOrSemiApply)

      case (p @ Not(expr: ExistsIRExpression)) :: Nil =>
        val rhs = rhsPlan(lhs, expr, context)
        planSelect(p, rhs, context.staticComponents.logicalPlanProducer.planSelectOrAntiSemiApply)

      case (p: ExistsIRExpression) :: tail =>
        val rhs = rhsPlan(lhs, p, context)
        planSemiApply(p, p, tail, rhs)

      case (p @ Not(expr: ExistsIRExpression)) :: tail =>
        val rhs = rhsPlan(lhs, expr, context)
        planSemiApply(p, expr, tail, rhs)

      case _ =>
        throw new IllegalArgumentException("There should be at least one subquery expression")
    }
  }

  private def createLetSemiApply(
    lhs: LogicalPlan,
    rhs: LogicalPlan,
    existsExpression: Expression,
    expressions: Set[Expression],
    letExpression: Option[Expression],
    context: LogicalPlanningContext
  ) = {
    val (idName, ident) = freshId(existsExpression, context.staticComponents.anonymousVariableNameGenerator)
    if (expressions.isEmpty && letExpression.isEmpty)
      (context.staticComponents.logicalPlanProducer.planLetSemiApply(lhs, rhs, idName, context), ident)
    else
      (
        context.staticComponents.logicalPlanProducer.planLetSelectOrSemiApply(
          lhs,
          rhs,
          idName,
          onePredicate(expressions ++ letExpression.toSet),
          context
        ),
        ident
      )
  }

  private def createLetAntiSemiApply(
    lhs: LogicalPlan,
    rhs: LogicalPlan,
    existsExpression: Expression,
    expressions: Set[Expression],
    letExpression: Option[Expression],
    context: LogicalPlanningContext
  ) = {
    val (idName, ident) = freshId(existsExpression, context.staticComponents.anonymousVariableNameGenerator)
    if (expressions.isEmpty && letExpression.isEmpty)
      (context.staticComponents.logicalPlanProducer.planLetAntiSemiApply(lhs, rhs, idName, context), ident)
    else
      (
        context.staticComponents.logicalPlanProducer.planLetSelectOrAntiSemiApply(
          lhs,
          rhs,
          idName,
          onePredicate(expressions ++ letExpression.toSet),
          context
        ),
        ident
      )
  }

  def rhsPlan(
    lhs: LogicalPlan,
    subquery: ExistsIRExpression,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val arguments = subquery.dependencies
    // We compute LabelInfo here instead of using plannerQueryPlanner.planSubqueryWithLabelInfo
    // This has the benefit of a smaller cache key (just the labelInfo, and not the whole plan).
    val labelInfo =
      context.staticComponents.planningAttributes.solveds.get(lhs.id).asSinglePlannerQuery.lastLabelInfo
        // We only retain the relevant label infos to get more cache hits.
        .view.filterKeys(arguments).toMap
    context.staticComponents.queryGraphSolver.planInnerOfExistsSubquery(subquery, labelInfo, context)
  }

  def onePredicate(expressions: Set[Expression]): Expression =
    if (expressions.size == 1)
      expressions.head
    else
      Ors(expressions)(expressions.head.position)

  private def freshId(existsExpression: Expression, anonymousVariableNameGenerator: AnonymousVariableNameGenerator) = {
    val name = anonymousVariableNameGenerator.nextName
    (name, Variable(name)(existsExpression.position))
  }
}
