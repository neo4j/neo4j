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
import org.neo4j.cypher.internal.compiler.planner.logical.irExpressionRewriter
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.plannerQueryPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.SelectPatternPredicates.planPredicates
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.HasMappableExpressions
import org.neo4j.cypher.internal.expressions.ListSlice
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.ScopeExpression
import org.neo4j.cypher.internal.expressions.functions.Coalesce
import org.neo4j.cypher.internal.expressions.functions.Exists
import org.neo4j.cypher.internal.expressions.functions.Head
import org.neo4j.cypher.internal.ir.Selections.containsExistsSubquery
import org.neo4j.cypher.internal.ir.ast.CountIRExpression
import org.neo4j.cypher.internal.ir.ast.ExistsIRExpression
import org.neo4j.cypher.internal.ir.ast.ForAllRepetitions
import org.neo4j.cypher.internal.ir.ast.IRExpression
import org.neo4j.cypher.internal.ir.ast.ListIRExpression
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.topDown

import scala.collection.mutable

/**
 * Prepares expressions containing ListIRExpressions by solving them in a sub-query through RollUpApply and replacing
 * the original expression with an identifier, or preferably GetDegree when possible.
 * 
 * A query such as:
 * MATCH (n) RETURN (n)-->()
 * 
 * Would be solved with a plan such as
 * 
 * +Rollup (creates the collection with all the produced paths from RHS)
 * | \
 * | +(RHS) Projection (of path)
 * | |
 * | +Expand( (n)-->() )
 * | |
 * | +Argument
 * |
 * +(LHS) AllNodesScan(n)
 * 
 * ListIRExpressions that are in positions where we cannot use RollUpApply, as well as other IRExpressions are rewritten to a NestedPlanExpression.
 */
object SubqueryExpressionSolver {

  /**
   * Get a Solver to solve multiple expressions and finally return a rewritten plan of the given source.
   *
   * The usage pattern is like this:
   *
   * {{{
   * val solver = SubqueryExpressionSolver.solverFor(source, context)
   * val rewrittenExpression = solver.solve(someExpressionForANewPlan)
   * val rewrittenSource = solver.rewrittenPlan()
   * // Proceed to plan a new operator using rewrittenExpression instead of someExpressionForANewPlan, and rewrittenSource instead of source
   * }}}
   *
   * @param source the LogicalPlan that a new operator will be put on top of.
   */
  def solverFor(source: LogicalPlan, context: LogicalPlanningContext): SolverForInnerPlan =
    new SolverForInnerPlan(source, context)

  /**
   * Get a Solver to solve multiple expressions and finally rewrite a planned leaf plan.
   *
   * The usage pattern is like this:
   *
   * {{{
   * val solver = SubqueryExpressionSolver.solverForLeafPlan(argumentIds, context)
   * val rewrittenExpression = solver.solve(someExpressionForANewPlan)
   * val newArguments = solver.newArguments
   * val plan = // plan leaf plan using `argumentIds ++ newArguments`
   * val rewrittenPlan = solver.rewriteLeafPlan(plan)
   * }}}
   *
   * @param argumentIds the argument IDs of the leaf plan that is about to be planned
   */
  def solverForLeafPlan(argumentIds: Set[LogicalVariable], context: LogicalPlanningContext): SolverForLeafPlan =
    new SolverForLeafPlan(argumentIds, context)

  abstract class Solver(initialPlan: LogicalPlan, context: LogicalPlanningContext) {
    protected var resultPlan: LogicalPlan = initialPlan
    protected var arguments: mutable.Builder[LogicalVariable, Set[LogicalVariable]] = Set.newBuilder[LogicalVariable]

    def solve(expression: Expression, maybeKey: Option[LogicalVariable] = None): Expression = {
      if (resultPlan == null) {
        throw new IllegalArgumentException("You cannot solve more expressions after obtaining the rewritten plan.")
      }
      if (qualifiesForRewriting(expression, context)) {
        val RewriteResult(plan, solvedExp, introducedVariables) = expression match {
          case expression: ListIRExpression =>
            val (newPlan, newVar) =
              solveUsingRollUpApply(resultPlan, expression, maybeKey, context)
            RewriteResult(newPlan, newVar, Set(newVar))

          case expression: CountIRExpression =>
            val (newPlan, newVar) =
              solveUsingApply(resultPlan, expression, maybeKey, context)
            RewriteResult(newPlan, newVar, Set(newVar))

          case expression: ExistsIRExpression =>
            val (newPlan, newVar) =
              solveUsingLetSemiApply(resultPlan, expression, maybeKey, context)
            RewriteResult(newPlan, newVar, Set(newVar))

          case Not(expression: ExistsIRExpression) =>
            val (newPlan, newVar) =
              solveUsingLetAntiSemiApply(resultPlan, expression, maybeKey, context)
            RewriteResult(newPlan, newVar, Set(newVar))

          case inExpression =>
            // Any remaining IRExpressions are rewritten with RollupApply/Apply/LetSemiApply/... or NestedPlanExpression
            rewriteInnerExpressions(resultPlan, inExpression, context)
        }
        resultPlan = plan
        arguments ++= introducedVariables
        solvedExp
      } else {
        expression
      }
    }

  }

  class SolverForInnerPlan(source: LogicalPlan, context: LogicalPlanningContext)
      extends Solver(source, context) {

    def rewrittenPlan(): LogicalPlan = {
      val result = this.resultPlan
      this.resultPlan = null
      result
    }
  }

  class SolverForLeafPlan(argumentIds: Set[LogicalVariable], context: LogicalPlanningContext)
      extends Solver(
        context.staticComponents.logicalPlanProducer.ForSubqueryExpressionSolver.planArgument(
          argumentIds,
          context
        ), // When we have a leaf plan, we start with a single row on the LHS of the RollUpApply

        context
      ) {

    def newArguments: Set[LogicalVariable] = {
      arguments.result()
    }

    def rewriteLeafPlan(leafPlan: LogicalPlan): LogicalPlan = {
      val lhsOfApply = this.resultPlan
      this.resultPlan = null
      lhsOfApply match {
        case _: Argument =>
          // We did not change anything. No need to wrap the leaf plan in an apply.
          leafPlan
        case _ =>
          context.staticComponents.logicalPlanProducer.ForSubqueryExpressionSolver.planApply(
            lhsOfApply,
            leafPlan,
            context
          )
      }
    }
  }

  private case class RewriteResult(
    currentPlan: LogicalPlan,
    currentExpression: Expression,
    introducedVariables: Set[LogicalVariable]
  )

  /**
   * Solve a ListIRExpression by planning it recursively and attach it to the given plan with [[org.neo4j.cypher.internal.logical.plans.RollUpApply]]
   *
   * @param source   the current plan
   * @param expr     the ListIRExpression with the subquery
   * @param maybeKey optionally, a variable name for `expr`
   * @return A tuple of (the combined logical plan, the variable name for `expr`)
   */
  private def solveUsingRollUpApply(
    source: LogicalPlan,
    expr: ListIRExpression,
    maybeKey: Option[LogicalVariable],
    context: LogicalPlanningContext
  ): (LogicalPlan, LogicalVariable) = {
    val collection = maybeKey.getOrElse(expr.collection)
    val subQueryPlan = plannerQueryPlanner.planSubqueryWithLabelInfo(source, expr, context)
    val producedPlan = context.staticComponents.logicalPlanProducer.ForSubqueryExpressionSolver.planRollup(
      source,
      subQueryPlan,
      collection,
      expr.variableToCollect,
      context
    )

    (producedPlan, collection)
  }

  private def solveUsingApply(
    source: LogicalPlan,
    expr: CountIRExpression,
    maybeKey: Option[LogicalVariable],
    context: LogicalPlanningContext
  ): (LogicalPlan, LogicalVariable) = {
    val countVariable = maybeKey.getOrElse(expr.countVariable)
    val subQueryPlan = {
      val exprToPlan = maybeKey.fold(expr)(expr.renameCountVariable)
      plannerQueryPlanner.planSubqueryWithLabelInfo(source, exprToPlan, context)
    }
    val producedPlan =
      context.staticComponents.logicalPlanProducer.ForSubqueryExpressionSolver.planCountExpressionApply(
        source,
        subQueryPlan,
        context
      )

    (producedPlan, countVariable)
  }

  private def solveUsingLetSemiApplyVariant(
    source: LogicalPlan,
    expr: ExistsIRExpression,
    maybeKey: Option[LogicalVariable],
    fn: (LogicalPlan, LogicalPlan, LogicalVariable, LogicalPlanningContext) => LogicalPlan,
    context: LogicalPlanningContext
  ): (LogicalPlan, LogicalVariable) = {
    val variable = maybeKey.getOrElse(expr.existsVariable)
    val subQueryPlan = plannerQueryPlanner.planSubqueryWithLabelInfo(source, expr, context)
    val producedPlan = fn(source, subQueryPlan, variable, context)

    (producedPlan, variable)
  }

  private def solveUsingLetSemiApply(
    source: LogicalPlan,
    expr: ExistsIRExpression,
    maybeKey: Option[LogicalVariable],
    context: LogicalPlanningContext
  ): (LogicalPlan, LogicalVariable) =
    solveUsingLetSemiApplyVariant(
      source,
      expr,
      maybeKey,
      context.staticComponents.logicalPlanProducer.planLetSemiApply,
      context
    )

  private def solveUsingLetAntiSemiApply(
    source: LogicalPlan,
    expr: ExistsIRExpression,
    maybeKey: Option[LogicalVariable],
    context: LogicalPlanningContext
  ): (LogicalPlan, LogicalVariable) =
    solveUsingLetSemiApplyVariant(
      source,
      expr,
      maybeKey,
      context.staticComponents.logicalPlanProducer.planLetAntiSemiApply,
      context
    )

  private def solveUsingLetSelectOrSemiApply(
    source: LogicalPlan,
    expr: ExistsIRExpression,
    maybeKey: Option[LogicalVariable],
    orExpression: Expression,
    context: LogicalPlanningContext
  ): (LogicalPlan, LogicalVariable) =
    solveUsingLetSemiApplyVariant(
      source,
      expr,
      maybeKey,
      context.staticComponents.logicalPlanProducer.planLetSelectOrSemiApply(_, _, _, orExpression, _),
      context
    )

  private def solveUsingLetSelectOrAntiSemiApply(
    source: LogicalPlan,
    expr: ExistsIRExpression,
    maybeKey: Option[LogicalVariable],
    orExpression: Expression,
    context: LogicalPlanningContext
  ): (LogicalPlan, LogicalVariable) =
    solveUsingLetSemiApplyVariant(
      source,
      expr,
      maybeKey,
      context.staticComponents.logicalPlanProducer.planLetSelectOrAntiSemiApply(_, _, _, orExpression, _),
      context
    )

  /**
   * Rewrite any [[IRExpression]] inside `expression`. If RollupApply/Apply/LetSemiApply/... is not possible,
   * it will use the [[irExpressionRewriter]] to generate [[org.neo4j.cypher.internal.logical.plans.NestedPlanExpression]]s.
   * 
   * @param plan the current plan
   * @param expression the expression to rewrite
   * @return A tuple of RewriteResult(the new plan, the rewritten expression, introduced variables)
   */
  private def rewriteInnerExpressions(
    plan: LogicalPlan,
    expression: Expression,
    context: LogicalPlanningContext
  ): RewriteResult = {
    val subqueryExpressions: Seq[IRExpression] =
      expression.folder(context.staticComponents.cancellationChecker).findAllByClass[IRExpression]

    // First rewrite all IR expressions with RollupApply/Apply/LetSemiApply/..., where it is possible.
    val RewriteResult(finalPlan, expressionAfterRollupApply, finalIntroducedVariables) = {
      subqueryExpressions.foldLeft(RewriteResult(plan, expression, Set.empty)) {
        case (RewriteResult(currentPlan, currentExpression, introducedVariables), irExpression) =>
          var newPlan: LogicalPlan = null
          var newVariable: LogicalVariable = null
          def updateVars(tuple: (LogicalPlan, LogicalVariable)): LogicalVariable = {
            val (plan, variable) = tuple
            newPlan = plan
            newVariable = variable
            variable
          }

          def solveOrsWithExists(
            exprs: ListSet[Expression],
            negated: Boolean
          ): Expression = {
            val existsIRExpression = irExpression.asInstanceOf[ExistsIRExpression]
            val excludeExpr = if (negated) Not(irExpression)(InputPosition.NONE) else irExpression

            val (otherExistsExpressions, otherExpressions) = (exprs - excludeExpr).partition {
              case ExistsIRExpression(_, _, _)      => true
              case Not(ExistsIRExpression(_, _, _)) => true
              case _                                => false
            }
            if (otherExpressions.isEmpty) {
              val (p, v) =
                if (negated) solveUsingLetAntiSemiApply(currentPlan, existsIRExpression, None, context)
                else solveUsingLetSemiApply(currentPlan, existsIRExpression, None, context)
              newPlan = p
              newVariable = v
              SelectPatternPredicates.onePredicate(otherExistsExpressions + v)
            } else {
              val orExpr = SelectPatternPredicates.onePredicate(otherExpressions)
              val (p, v) =
                if (negated) solveUsingLetSelectOrAntiSemiApply(currentPlan, existsIRExpression, None, orExpr, context)
                else solveUsingLetSelectOrSemiApply(currentPlan, existsIRExpression, None, orExpr, context)
              newPlan = p
              newVariable = v
              SelectPatternPredicates.onePredicate(otherExistsExpressions + v)
            }
          }

          val inner = Rewriter.lift {
            case listIRExpression: ListIRExpression if listIRExpression == irExpression =>
              updateVars(solveUsingRollUpApply(currentPlan, listIRExpression, None, context))
            case countIRExpression: CountIRExpression if countIRExpression == irExpression =>
              updateVars(solveUsingApply(currentPlan, countIRExpression, None, context))
            case existsIRExpression: ExistsIRExpression if existsIRExpression == irExpression =>
              updateVars(solveUsingLetSemiApply(currentPlan, existsIRExpression, None, context))
            case Not(existsIRExpression: ExistsIRExpression) if existsIRExpression == irExpression =>
              updateVars(solveUsingLetAntiSemiApply(currentPlan, existsIRExpression, None, context))
            case Ors(exprs)
              if irExpression.isInstanceOf[ExistsIRExpression] &&
                exprs.contains(irExpression) =>
              solveOrsWithExists(exprs, negated = false)
            case Ors(exprs)
              if irExpression.isInstanceOf[ExistsIRExpression] &&
                exprs.contains(Not(irExpression)(InputPosition.NONE)) =>
              solveOrsWithExists(exprs, negated = true)
          }
          /*
           * It's important to not go use RollUpApply if the expression we are working with is:
           *
           * a) inside a loop. If that is not honored, it will produce the wrong results by not having the correct scope.
           * b) inside a conditional expression. Otherwise it can be executed even when not strictly needed.
           * c) inside an expression that accessed only part of the list. Otherwise we do too much work. To avoid that we inject a Limit into the
           * NestedPlanExpression.
           */
          val rewriter = topDown(
            rewriter = inner,
            stopper = {
              case _: ListIRExpression   => false
              case _: CountIRExpression  => false
              case _: ExistsIRExpression => false
              // Loops
              case _: ScopeExpression => true
              // Conditionals & List accesses
              case _: ContainerIndex     => true
              case _: ListSlice          => true
              case f: FunctionInvocation => f.function == Exists || f.function == Coalesce || f.function == Head
              // Rewritten at a later stage, requires additional processing
              case _: ForAllRepetitions => true

              case _ => false
            },
            cancellation = context.staticComponents.cancellationChecker
          )
          val rewrittenExpression = currentExpression.endoRewrite(rewriter)

          if (rewrittenExpression == currentExpression) {
            RewriteResult(
              currentPlan,
              currentExpression,
              introducedVariables
            )
          } else {
            RewriteResult(newPlan, rewrittenExpression, introducedVariables + newVariable)
          }
      }
    }

    // Second, rewrite all remaining IR expressions to NestedPlanExpressions
    val finalExpression = expressionAfterRollupApply.endoRewrite(irExpressionRewriter(finalPlan, context))
    RewriteResult(
      finalPlan,
      finalExpression,
      finalIntroducedVariables
    )
  }

  private def qualifiesForRewriting(exp: AnyRef, context: LogicalPlanningContext): Boolean =
    exp.folder(context.staticComponents.cancellationChecker).treeExists {
      case _: IRExpression => true
    }

  case class ForMappable[T]() {

    def solve(
      inner: LogicalPlan,
      mappable: HasMappableExpressions[T],
      context: LogicalPlanningContext
    ): (T, LogicalPlan) = {
      val solver = SubqueryExpressionSolver.solverFor(inner, context)
      val rewrittenExpression = mappable.mapExpressions(solver.solve(_))
      val rewrittenInner = solver.rewrittenPlan()
      (rewrittenExpression, rewrittenInner)
    }
  }

  object ForMulti {

    def solve(
      inner: LogicalPlan,
      expressions: Seq[Expression],
      context: LogicalPlanningContext
    ): (Seq[Expression], LogicalPlan) = {
      if (expressions.isEmpty) {
        (expressions, inner)
      } else {
        val solver = SubqueryExpressionSolver.solverFor(inner, context)
        val rewrittenExpressions: Seq[Expression] = expressions.map(solver.solve(_))
        val rewrittenInner = solver.rewrittenPlan()
        (rewrittenExpressions, rewrittenInner)
      }
    }
  }

  object ForSingle {

    def solve(
      inner: LogicalPlan,
      expression: Expression,
      context: LogicalPlanningContext
    ): (Expression, LogicalPlan) = {
      val solver = SubqueryExpressionSolver.solverFor(inner, context)
      val rewrittenExpression = solver.solve(expression)
      val rewrittenInner = solver.rewrittenPlan()
      (rewrittenExpression, rewrittenInner)
    }
  }

  object ForExistentialSubquery {

    def solve(
      lhs: LogicalPlan,
      unsolvedPredicates: Seq[Expression],
      interestingOrderConfig: InterestingOrderConfig,
      context: LogicalPlanningContext
    ): (Seq[Expression], LogicalPlan) = {
      unsolvedPredicates.filter(containsExistsSubquery).foldLeft((Seq.empty[Expression], lhs)) {
        case ((solvedExprs, plan), p: ExistsIRExpression) =>
          val rhs = SelectPatternPredicates.rhsPlan(plan, p, context)
          val solvedPlan = context.staticComponents.logicalPlanProducer.planSemiApplyInHorizon(plan, rhs, p, context)
          (solvedExprs :+ p, solvedPlan)
        case ((solvedExprs, plan), not @ Not(e: ExistsIRExpression)) =>
          val rhs = SelectPatternPredicates.rhsPlan(plan, e, context)
          val solvedPlan =
            context.staticComponents.logicalPlanProducer.planAntiSemiApplyInHorizon(plan, rhs, not, context)
          (solvedExprs :+ not, solvedPlan)
        case ((solvedExprs, plan), o @ Ors(exprs)) =>
          val (existsExpressions, expressions) = exprs.partition {
            case ExistsIRExpression(_, _, _)      => true
            case Not(ExistsIRExpression(_, _, _)) => true
            case _                                => false
          }
          // Only plan if the OR contains an EXISTS.
          if (existsExpressions.nonEmpty) {
            val (planWithPredicates, solvedPredicates) =
              planPredicates(plan, existsExpressions, expressions, None, interestingOrderConfig, context)
            AssertMacros.checkOnlyWhenAssertionsAreEnabled(
              exprs.forall(solvedPredicates.contains),
              "planPredicates is supposed to solve all predicates in an OR clause."
            )
            val solvedPlan = context.staticComponents.logicalPlanProducer.solvePredicateInHorizon(planWithPredicates, o)
            (solvedExprs :+ o, solvedPlan)
          } else (solvedExprs, plan)
        case (acc, _) => acc
      }
    }
  }
}
