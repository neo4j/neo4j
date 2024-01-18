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
package org.neo4j.cypher.internal.logical.plans.ordering

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Asc
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Desc
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.projectExpression
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder.FullSatisfaction
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder.Satisfaction
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.AtMostOneRow
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.PartialSort
import org.neo4j.cypher.internal.logical.plans.PartialTop
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.logical.plans.ordering
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrder.OrderOrigin
import org.neo4j.cypher.internal.util.NonEmptyList

import scala.annotation.tailrec

object ProvidedOrder {

  /**
   * The origin of a provided order.
   * If a plan introduced the ordering itself: [[Self]].
   * If it kept or modified a provided order from the left or right, [[Left]] or [[Right]].
   */
  sealed trait OrderOrigin
  final case object Self extends OrderOrigin
  final case object Left extends OrderOrigin
  final case object Right extends OrderOrigin
  final case object Both extends OrderOrigin

  private[ordering] def apply(columns: Seq[ColumnOrder], orderOrigin: OrderOrigin): ProvidedOrder = {
    if (columns.isEmpty) NoProvidedOrder
    else NonEmptyProvidedOrder(NonEmptyList.from(columns), orderOrigin)
  }

  def unapply(arg: ProvidedOrder): Option[Seq[ColumnOrder]] = arg match {
    case NoProvidedOrder                      => Some(Seq.empty)
    case NonEmptyProvidedOrder(allColumns, _) => Some(allColumns.toIndexedSeq)
  }

  val empty: ProvidedOrder = NoProvidedOrder

  private[ordering] def asc(
    expression: Expression,
    projections: Map[LogicalVariable, Expression] = Map.empty
  ): NonEmptyProvidedOrder =
    ordering.NonEmptyProvidedOrder(NonEmptyList(Asc(expression, projections)), Self)

  private[ordering] def desc(
    expression: Expression,
    projections: Map[LogicalVariable, Expression] = Map.empty
  ): NonEmptyProvidedOrder =
    ordering.NonEmptyProvidedOrder(NonEmptyList(Desc(expression, projections)), Self)
}

/**
 * A factory to create ProvidedOrders.
 * Outside of this package, all ProvidedOrders can only be created through a factory.
 */
sealed trait ProvidedOrderFactory {

  /**
   * A providedOrder for the given plan.
   */
  def providedOrder(providedOrder: ProvidedOrder, plan: Option[LogicalPlan]): ProvidedOrder

  /**
   * A providedOrder with the given columns and origin for the given plan.
   */
  def providedOrder(columns: Seq[ColumnOrder], orderOrigin: OrderOrigin, plan: Option[LogicalPlan]): ProvidedOrder

  /**
   * A providedOrder with a single ascending column.
   */
  def asc(expression: Expression, projections: Map[LogicalVariable, Expression] = Map.empty): ProvidedOrder

  /**
   * A providedOrder with a single descending column.
   */
  def desc(expression: Expression, projections: Map[LogicalVariable, Expression] = Map.empty): ProvidedOrder
}

case object DefaultProvidedOrderFactory extends ProvidedOrderFactory {

  override def providedOrder(providedOrder: ProvidedOrder, plan: Option[LogicalPlan]): ProvidedOrder = providedOrder

  override def providedOrder(
    columns: Seq[ColumnOrder],
    orderOrigin: OrderOrigin,
    plan: Option[LogicalPlan]
  ): ProvidedOrder =
    ProvidedOrder.apply(columns, orderOrigin)

  override def asc(
    expression: Expression,
    projections: Map[LogicalVariable, Expression] = Map.empty
  ): ProvidedOrder =
    ProvidedOrder.asc(expression, projections)

  override def desc(
    expression: Expression,
    projections: Map[LogicalVariable, Expression] = Map.empty
  ): ProvidedOrder =
    ProvidedOrder.desc(expression, projections)
}

case object ParallelExecutionProvidedOrderFactory extends ProvidedOrderFactory {

  /**
   * These plans are known to not break ordering in the parallel runtime.
   */
  object OrderMaintainingAllowlistedPlan {

    def unapply(plan: LogicalPlan): Boolean = plan match {
      // Provides order
      case _: Sort => true
      case _: Top  => true

      // Provides order AND propagates order
      case _: PartialSort => true
      case _: PartialTop  => true

      // Only 1 row, so ordered by definition
      case p if p.distinctness == AtMostOneRow => true

      // Propagates order
      case _: Skip               => true
      case _: Limit              => true
      case _: Selection          => true
      case _: Projection         => true
      case _: ProduceResult      => true
      case Apply(_, _: Argument) => true
      case _                     => false
    }
  }

  override def providedOrder(providedOrder: ProvidedOrder, plan: Option[LogicalPlan]): ProvidedOrder = {
    plan match {
      case Some(OrderMaintainingAllowlistedPlan()) => providedOrder
      case _                                       => ProvidedOrder.empty
    }
  }

  override def providedOrder(
    columns: Seq[ColumnOrder],
    orderOrigin: OrderOrigin,
    plan: Option[LogicalPlan]
  ): ProvidedOrder = providedOrder(ProvidedOrder.apply(columns, orderOrigin), plan)

  override def asc(expression: Expression, projections: Map[LogicalVariable, Expression] = Map.empty): ProvidedOrder =
    ProvidedOrder.empty

  override def desc(expression: Expression, projections: Map[LogicalVariable, Expression] = Map.empty): ProvidedOrder =
    ProvidedOrder.empty
}

/**
 * A LogicalPlan can guarantee to provide its results in a particular order. This trait
 * is used for the purpose of conveying the information of which order the results are in,
 * if they are in any defined order.
 */
sealed trait ProvidedOrder {

  /**
   * @return sequence of columns with sort direction
   */
  def columns: Seq[ColumnOrder]

  /**
   * @return whether this ProvidedOrder is empty
   */
  def isEmpty: Boolean

  /**
   * @return the origin of the order, or None, if this is empty.
   */
  def orderOrigin: Option[OrderOrigin]

  /**
   * Add an ascending column to this ProvidedOrder
   */
  def asc(expression: Expression, projections: Map[LogicalVariable, Expression] = Map.empty)(
    implicit factory: ProvidedOrderFactory,
    plan: Option[LogicalPlan]
  ): ProvidedOrder

  /**
   * Add an descending column to this ProvidedOrder
   */
  def desc(expression: Expression, projections: Map[LogicalVariable, Expression] = Map.empty)(
    implicit factory: ProvidedOrderFactory,
    plan: Option[LogicalPlan]
  ): ProvidedOrder

  /**
   * Returns a new provided order where the order columns of this are concatenated with
   * the order columns of the other provided order. Example:
   * [n.foo ASC, n.bar DESC].followedBy([n.baz ASC]) = [n.foo ASC, n.bar DESC, n.baz ASC]
   *
   * If this is empty, then the returned provided order will also be empty, regardless of the
   * given nextOrder.
   */
  def followedBy(nextOrder: ProvidedOrder)(
    implicit factory: ProvidedOrderFactory,
    plan: Option[LogicalPlan]
  ): ProvidedOrder

  /**
   * Trim provided order up until a sort column that matches any of the given args.
   */
  def upToExcluding(args: Set[LogicalVariable])(
    implicit factory: ProvidedOrderFactory,
    plan: Option[LogicalPlan]
  ): ProvidedOrder

  /**
   * Returns the common prefix between this and another provided order.
   */
  def commonPrefixWith(otherOrder: ProvidedOrder)(
    implicit factory: ProvidedOrderFactory,
    plan: Option[LogicalPlan]
  ): ProvidedOrder

  /**
   * The same order columns, but with OrderOrigin = [[Left]]
   */
  def fromLeft(implicit factory: ProvidedOrderFactory, plan: Option[LogicalPlan]): ProvidedOrder

  /**
   * The same order columns, but with OrderOrigin = [[Right]]
   */
  def fromRight(implicit factory: ProvidedOrderFactory, plan: Option[LogicalPlan]): ProvidedOrder

  /**
   * The same order columns, but with OrderOrigin = [[Both]]
   */
  def fromBoth(implicit factory: ProvidedOrderFactory, plan: Option[LogicalPlan]): ProvidedOrder

  /**
   * Checks if a RequiredOrder is satisfied by a ProvidedOrder
   */
  def satisfies(interestingOrder: InterestingOrder): Satisfaction = {
    @tailrec
    def satisfied(
      providedOrder: Expression,
      requiredOrder: Expression,
      projections: Map[LogicalVariable, Expression]
    ): Boolean = {
      val projected = projectExpression(requiredOrder, projections)
      if (providedOrder == requiredOrder || providedOrder == projected) {
        true
      } else if (projected != requiredOrder) {
        satisfied(providedOrder, projected, projections)
      } else {
        false
      }
    }

    interestingOrder.requiredOrderCandidate.order.zipAll(this.columns, null, null).foldLeft(
      Satisfaction(Seq.empty, Seq.empty)
    ) {
      case (s, (null, _)) => s // no required order left
      case (s @ FullSatisfaction(), (satisfiedColumn @ Asc(requiredExp, projections), Asc(providedExpr, _)))
        if satisfied(providedExpr, requiredExp, projections) => s.withSatisfied(satisfiedColumn)
      case (s @ FullSatisfaction(), (satisfiedColumn @ Desc(requiredExp, projections), Desc(providedExpr, _)))
        if satisfied(providedExpr, requiredExp, projections) => s.withSatisfied(satisfiedColumn)
      case (s, (unsatisfiedColumn, _)) =>
        s.withMissing(
          unsatisfiedColumn
        ) // required order left but no provided or provided not matching or previous column not matching
    }
  }

}

private case object NoProvidedOrder extends ProvidedOrder {
  override def columns: Seq[ColumnOrder] = Seq.empty
  override def isEmpty: Boolean = true
  override def orderOrigin: Option[OrderOrigin] = None

  override def asc(expression: Expression, projections: Map[LogicalVariable, Expression])(
    implicit factory: ProvidedOrderFactory,
    plan: Option[LogicalPlan]
  ): ProvidedOrder = this

  override def desc(expression: Expression, projections: Map[LogicalVariable, Expression])(
    implicit factory: ProvidedOrderFactory,
    plan: Option[LogicalPlan]
  ): ProvidedOrder = this

  override def followedBy(nextOrder: ProvidedOrder)(
    implicit factory: ProvidedOrderFactory,
    plan: Option[LogicalPlan]
  ): ProvidedOrder = this

  override def upToExcluding(args: Set[LogicalVariable])(
    implicit factory: ProvidedOrderFactory,
    plan: Option[LogicalPlan]
  ): ProvidedOrder = this

  override def commonPrefixWith(otherOrder: ProvidedOrder)(
    implicit factory: ProvidedOrderFactory,
    plan: Option[LogicalPlan]
  ): ProvidedOrder = this

  override def fromLeft(implicit factory: ProvidedOrderFactory, plan: Option[LogicalPlan]): ProvidedOrder = this
  override def fromRight(implicit factory: ProvidedOrderFactory, plan: Option[LogicalPlan]): ProvidedOrder = this
  override def fromBoth(implicit factory: ProvidedOrderFactory, plan: Option[LogicalPlan]): ProvidedOrder = this
}

private case class NonEmptyProvidedOrder(allColumns: NonEmptyList[ColumnOrder], theOrderOrigin: OrderOrigin)
    extends ProvidedOrder {

  override def columns: Seq[ColumnOrder] = allColumns.toIndexedSeq

  override def isEmpty: Boolean = false

  override def orderOrigin: Option[OrderOrigin] = Some(theOrderOrigin)

  def asc(expression: Expression, projections: Map[LogicalVariable, Expression] = Map.empty)(
    implicit factory: ProvidedOrderFactory,
    plan: Option[LogicalPlan]
  ): ProvidedOrder =
    factory.providedOrder(NonEmptyProvidedOrder(allColumns :+ Asc(expression, projections), theOrderOrigin), plan)

  def desc(expression: Expression, projections: Map[LogicalVariable, Expression] = Map.empty)(
    implicit factory: ProvidedOrderFactory,
    plan: Option[LogicalPlan]
  ): ProvidedOrder =
    factory.providedOrder(NonEmptyProvidedOrder(allColumns :+ Desc(expression, projections), theOrderOrigin), plan)

  override def fromLeft(implicit factory: ProvidedOrderFactory, plan: Option[LogicalPlan]): ProvidedOrder =
    factory.providedOrder(copy(theOrderOrigin = ProvidedOrder.Left), plan)

  override def fromRight(implicit factory: ProvidedOrderFactory, plan: Option[LogicalPlan]): ProvidedOrder =
    factory.providedOrder(copy(theOrderOrigin = ProvidedOrder.Right), plan)

  override def fromBoth(implicit factory: ProvidedOrderFactory, plan: Option[LogicalPlan]): ProvidedOrder =
    factory.providedOrder(copy(theOrderOrigin = ProvidedOrder.Both), plan)

  override def followedBy(nextOrder: ProvidedOrder)(
    implicit factory: ProvidedOrderFactory,
    plan: Option[LogicalPlan]
  ): ProvidedOrder = {
    factory.providedOrder(NonEmptyProvidedOrder(allColumns :++ nextOrder.columns, theOrderOrigin), plan)
  }

  override def upToExcluding(args: Set[LogicalVariable])(
    implicit factory: ProvidedOrderFactory,
    plan: Option[LogicalPlan]
  ): ProvidedOrder = {
    val (_, trimmed) = columns.foldLeft((false, Seq.empty[ColumnOrder])) {
      case (acc, _) if acc._1                                      => acc
      case (acc, col) if args.intersect(col.dependencies).nonEmpty => (true, acc._2)
      case (acc, col)                                              => (acc._1, acc._2 :+ col)
    }
    if (trimmed.isEmpty) {
      NoProvidedOrder
    } else {
      factory.providedOrder(NonEmptyProvidedOrder(NonEmptyList.from(trimmed), theOrderOrigin), plan)
    }
  }

  override def commonPrefixWith(otherOrder: ProvidedOrder)(
    implicit factory: ProvidedOrderFactory,
    plan: Option[LogicalPlan]
  ): ProvidedOrder =
    otherOrder match {
      case NoProvidedOrder => NoProvidedOrder
      case other: NonEmptyProvidedOrder =>
        val newColumns = columns.zip(other.columns).takeWhile { case (a, b) => a == b }.map(_._1)
        if (newColumns.isEmpty) NoProvidedOrder
        else factory.providedOrder(copy(allColumns = NonEmptyList.from(newColumns)), plan)
    }
}
