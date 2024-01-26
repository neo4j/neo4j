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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.projection
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IsAggregate
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.phases.Namespacer
import org.neo4j.cypher.internal.ir
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Asc
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Desc
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder.FullSatisfaction
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder.NoSatisfaction
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder.Satisfaction
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.ColumnOrder
import org.neo4j.cypher.internal.logical.plans.Descending
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

object SortPlanner {

  /**
   * Given a plan and an interesting order, try to return a plan that satisfies the interesting order.
   *
   * If the interesting order is empty, return None.
   * If the interesting order is non-empty, and the given plan already satisfies the interesting order, return the same plan.
   * If the interesting order is non-empty, and the given plan does not already satisfy the interesting order, try to plan a Sort/PartialSort
   * to satisfy the interesting order. If that is possible, return the new plan, otherwise None.
   *
   * @param isPushDownSort `true` if this attempts to plan the sort earlier than written in the original query.
   */
  def maybeSortedPlan(
    plan: LogicalPlan,
    interestingOrderConfig: InterestingOrderConfig,
    isPushDownSort: Boolean,
    context: LogicalPlanningContext,
    updateSolved: Boolean
  ): Option[LogicalPlan] = {
    if (interestingOrderConfig.orderToSolve.requiredOrderCandidate.nonEmpty) {
      orderSatisfaction(interestingOrderConfig, context, plan) match {
        case FullSatisfaction() =>
          Some(plan)
        case NoSatisfaction() =>
          planSort(plan, Seq.empty, interestingOrderConfig, isPushDownSort, context, updateSolved)
        case Satisfaction(satisfiedPrefix, _) =>
          planSort(plan, satisfiedPrefix, interestingOrderConfig, isPushDownSort, context, updateSolved)
      }
    } else {
      None
    }
  }

  /**
   * Given a plan and an interesting order, try to return a plan that satisfies the interesting order for the current available symbols.
   *
   * Tries to sort with `maybeSortedPlan`. If plan after sorting is:
   * - Fully sorted: return plan
   * - No sorted columns: return None
   * - Partially sorted, but the non-sorted expressions aren't in scope yet: return plan
   */
  def planIfAsSortedAsPossible(
    plan: LogicalPlan,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Option[LogicalPlan] = {
    // This plan will be fully sorted if possible, but even otherwise it might be as sorted as currently possible.
    val newPlan =
      maybeSortedPlan(plan, interestingOrderConfig, isPushDownSort = true, context, updateSolved = true).getOrElse(plan)
    val asSortedAsPossible = SatisfiedForPlan(plan)
    orderSatisfaction(interestingOrderConfig, context, newPlan) match {
      case asSortedAsPossible() => Some(newPlan)
      case _                    => None
    }
  }

  /**
   * Given a plan and an interesting order, plan a Sort if necessary.
   * Update the Solveds attribute if no Sort planning was necessary.
   * Throw an exception if no plan that can solve the interesting order could be found.
   */
  def ensureSortedPlanWithSolved(
    plan: LogicalPlan,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext,
    updateSolved: Boolean
  ): LogicalPlan =
    maybeSortedPlan(plan, interestingOrderConfig, isPushDownSort = false, context, updateSolved = updateSolved) match {
      case Some(sortedPlan) =>
        if (sortedPlan == plan) context.staticComponents.logicalPlanProducer.updateSolvedForSortedItems(
          sortedPlan,
          interestingOrderConfig.orderToReport,
          context
        )
        else sortedPlan
      case _ if interestingOrderConfig.orderToSolve.requiredOrderCandidate.nonEmpty =>
        throw new AssertionError(s"Expected a sorted plan but got\n$plan")
      case _ => plan
    }

  /**
   * Given an interesting order and a plan with a provided order, return the Satisfaction of the interesting order given that provided order.
   */
  def orderSatisfaction(
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext,
    plan: LogicalPlan
  ): Satisfaction =
    context.staticComponents.planningAttributes.providedOrders.get(plan.id).satisfies(
      interestingOrderConfig.orderToSolve
    )

  case class SatisfiedForPlan(plan: LogicalPlan) {

    def unapply(arg: Satisfaction): Boolean = {
      arg.satisfiedPrefix.nonEmpty && (arg.missingSuffix.isEmpty || {
        val dependenciesOfMissingSuffix = for {
          columnOrder <- arg.missingSuffix.toSet[ir.ordering.ColumnOrder]
          dependency <- columnOrder.dependencies
        } yield dependency

        // If all dependencies of the not sorted suffix are available,
        // we could be sorted by all columns (instead of just by the satisfied prefix).
        // If only some of those dependencies are available, we could theoretically be sorted
        // by a larger prefix, but we don't currently inject PartialSort plans in the middle
        // of IDP so that will never happen.
        !dependenciesOfMissingSuffix.subsetOf(plan.availableSymbols)
      })
    }
  }

  /**
   * @param plan the plan to plan Sort on top of.
   * @param satisfiedPrefix the prefix of the order to solve that is already satisfied.
   * @param interestingOrderConfig the order to solve.
   * @param isPushDownSort `true` if this attempts to plan the sort earlier than written in the original query.
   * @param updateSolved `true` if the solved attribute should be updated. 
   * @return `plan` with Sort on top, if 
   *         * there was an order to solve,
   *         * it was possible to solve it now, and
   *         * it was OK to plan it now.
   *         `None` otherwise.
   */
  private def planSort(
    plan: LogicalPlan,
    satisfiedPrefix: Seq[ir.ordering.ColumnOrder],
    interestingOrderConfig: InterestingOrderConfig,
    isPushDownSort: Boolean,
    context: LogicalPlanningContext,
    updateSolved: Boolean
  ): Option[LogicalPlan] = {

    def idFrom(expression: Expression, projection: Map[LogicalVariable, Expression]): LogicalVariable = {
      projection
        .collectFirst { case (key, e) if e == expression => key }
        .getOrElse(
          varFor(
            Namespacer.genName(
              context.staticComponents.anonymousVariableNameGenerator,
              ExpressionStringifier.pretty(_ =>
                context.staticComponents.anonymousVariableNameGenerator.nextName
              ).apply(expression)
            )
          )
        )
    }

    def projected(
      plan: LogicalPlan,
      projections: Map[LogicalVariable, Expression],
      updateSolved: Boolean
    ): LogicalPlan = {
      val projectionDeps = projections.flatMap(e => e._2.dependencies)
      val projectionsToMarkSolved = projections.filter(_._2 match {
        case IsAggregate(_) => false
        case _              => true
      })
      if (projectionsToMarkSolved.nonEmpty && projectionDeps.forall(e => plan.availableSymbols.contains(e))) {
        val keepAllColumns = if (updateSolved) Some(projectionsToMarkSolved) else None
        projection(plan, projectionsToMarkSolved, keepAllColumns, context)
      } else
        plan
    }

    case class SortColumnsWithProjections(
      columnOrder: ColumnOrder,
      providedOrderColumn: ir.ordering.ColumnOrder,
      unaliasedProjections: Option[(LogicalVariable, Expression)]
    )

    val sortItems: Seq[SortColumnsWithProjections] =
      interestingOrderConfig.orderToSolve.requiredOrderCandidate.order.map {
        // Aliased sort expressions
        case asc @ Asc(v: Variable, _) =>
          SortColumnsWithProjections(Ascending(v), asc, None)
        case desc @ Desc(v: Variable, _) =>
          SortColumnsWithProjections(Descending(v), desc, None)

        // Unaliased sort expressions
        case asc @ Asc(expression, projections) =>
          val columnId = idFrom(expression, projections)
          SortColumnsWithProjections(Ascending(columnId), asc, Some(columnId -> expression))
        case desc @ Desc(expression, projections) =>
          val columnId = idFrom(expression, projections)
          SortColumnsWithProjections(Descending(columnId), desc, Some(columnId -> expression))
      }

    // Project all variables needed for sort in two steps
    // First the ones that are part of projection list and may introduce variables that are needed for the second projection
    val projections =
      sortItems.foldLeft(Map.empty[LogicalVariable, Expression])((acc, i) => acc ++ i.providedOrderColumn.projections)
    val projected1 = projected(plan, projections, updateSolved = updateSolved)
    // And then all the ones from unaliased sort items that may refer to newly introduced variables
    val unaliasedProjections =
      sortItems.foldLeft(Map.empty[LogicalVariable, Expression])((acc, i) => acc ++ i.unaliasedProjections)
    val projected2 = projected(projected1, unaliasedProjections, updateSolved = false)

    val sortColumns: Seq[ColumnOrder] = sortItems.map(_.columnOrder)
    val providedOrderColumns = sortItems.map(_.providedOrderColumn)

    val sortSymbolsAvailable = sortColumns.forall(column => projected2.availableSymbols.contains(column.id))
    def deterministicSortExpressionsOnly = providedOrderColumns.forall(_.expression.isDeterministic)
    def okToSortNow = !isPushDownSort || deterministicSortExpressionsOnly

    if (sortSymbolsAvailable && okToSortNow) {
      // Pipelined runtime does currently not support PartialSort
      if (satisfiedPrefix.isEmpty || !context.settings.executionModel.providedOrderPreserving) {
        // Full sort required
        Some(context.staticComponents.logicalPlanProducer.planSort(
          projected2,
          sortColumns,
          providedOrderColumns,
          interestingOrderConfig.orderToReport,
          context
        ))
      } else {
        // Partial sort suffices
        val (prefixColumnOrders, suffixColumnOrders) = sortColumns.splitAt(satisfiedPrefix.length)
        Some(context.staticComponents.logicalPlanProducer.planPartialSort(
          projected2,
          prefixColumnOrders,
          suffixColumnOrders,
          providedOrderColumns,
          interestingOrderConfig.orderToReport,
          context
        ))
      }
    } else {
      None
    }
  }
}
