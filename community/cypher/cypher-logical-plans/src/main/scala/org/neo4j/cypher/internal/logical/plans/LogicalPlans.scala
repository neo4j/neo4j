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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.util.CancellationChecker

import scala.collection.mutable

object LogicalPlans {

  trait Mapper[T] {
    def onLeaf(plan: LogicalPlan): T
    def onOneChildPlan(plan: LogicalPlan, source: T): T
    def onTwoChildPlan(plan: LogicalPlan, lhs: T, rhs: T): T
  }

  /**
   * Traverses the logical plan tree structure and maps the tree in a bottom up fashion.
   *
   * Given a logical plan such as:
   *
   *         a
   *        / \
   *       b   c
   *      /   / \
   *     d   e   f
   *
   * the mapper will be called in the following sequence:
   *
   *   F = mapLeaf(f)
   *   E = mapLeaf(e)
   *   C = mapTwoChildPlan(c, E, F)
   *   D = mapLeaf(d)
   *   B = mapOneChildPlan(b, D)
   *   A = mapTwoChildPlan(a, B, C)
   */
  def map[T](plan: LogicalPlan, mapper: Mapper[T]): T = {
    val planStack = new mutable.Stack[LogicalPlan]()
    val resultStack = new mutable.Stack[T]()
    var comingFrom = plan
    def populate(plan: LogicalPlan): Unit = {
      var current = plan
      while (!current.isLeaf) {
        planStack.push(current)
        (current.lhs, current.rhs) match {
          case (Some(_), Some(right)) =>
            current = right

          case (Some(left), None) =>
            current = left
          case _ => throw new IllegalStateException("This must not be!")
        }
      }
      comingFrom = current
      planStack.push(current)
    }

    populate(plan)

    while (planStack.nonEmpty) {
      val current = planStack.pop()

      (current.lhs, current.rhs) match {
        case (None, None) =>
          val result = mapper.onLeaf(current)
          resultStack.push(result)

        case (Some(_), None) =>
          val source = resultStack.pop()
          val result = mapper.onOneChildPlan(current, source)
          resultStack.push(result)

        case (Some(left), Some(right)) if right eq left =>
          throw new IllegalStateException(
            s"Tried to map bad logical plan. LHS and RHS must never be the same: op: $current\nfull plan: $plan"
          )

        case (Some(left), Some(_)) if comingFrom eq left =>
          val arg1 = resultStack.pop()
          val arg2 = resultStack.pop()
          val result = mapper.onTwoChildPlan(current, arg1, arg2)
          resultStack.push(result)

        case (Some(left), Some(right)) if comingFrom eq right =>
          planStack.push(current)
          populate(left)

        case (Some(_), Some(_)) =>
          throw new IllegalStateException(
            s"Tried to map bad logical plan. In a two child plan, we must come from either RHS or LHS: op: $current\nfull plan: $plan"
          )

        case (None, Some(_)) =>
          throw new IllegalStateException(
            s"Tried to map bad logical plan. We can not have a RHS without having a LHS: op: $current\nfull plan: $plan"
          )
      }

      comingFrom = current

    }

    val result = resultStack.pop()
    assert(resultStack.isEmpty, "Should have emptied the stack of pipes by now!")
    result
  }

  /**
   * Fold over this logical plan tree in execution order.
   *
   * In this fold, the plan tree is visited in execution order, starting from
   * the leftmost leaf, and moving towards the root. Unlike a fold over a linear
   * structure, the plan is a binary tree, and therefore we need an additional
   * function for combining the left and right sides of some operators.
   *
   * NOTE: To avoid unpleasant surprises it is important that ACC is immutable,
   *       unless you really know what you're doing. The same ACC instance might
   *       be passed into several callback with the expectation of it being unchanged.
   *
   * @param f                   maps (currentAcc, plan) => acc for plan
   * @param combineLeftAndRight combines the lhsAcc and rhsAcc of plan
   * @param mapArguments        maps an accumulator before giving it to the LHS leaves of ApplyPlans.
   *                            Invoked with the accumulator of LHS children of ApplyPlans and the ApplyPlan.
   */
  def foldPlan[ACC](initialAcc: ACC)(
    root: LogicalPlan,
    f: (ACC, LogicalPlan) => ACC,
    combineLeftAndRight: (ACC, ACC, LogicalBinaryPlan) => ACC,
    mapArguments: (ACC, LogicalPlan) => ACC = (acc: ACC, _: LogicalPlan) => acc
  )(cancellation: CancellationChecker): ACC = {
    var stack: List[LogicalPlan] = root :: Nil

    /**
     * @param argumentAcc this is the ACC captured right after processing the LHS of an ApplyPlan.
     *                    It is used as the the first argument to `combineLeftAndRight` after processing the RHS
     * @param mappedArgumentAcc this is `mapArguments(argumentAcc, applyPlan)`. It is used while on the RHS of the ApplyPlan.
     */
    case class ArgumentStackEntry(argumentAcc: ACC, mappedArgumentAcc: ACC)

    // For each ApplyPlan that we are currently nested under there is one element in this stack.
    var argumentStack: List[ArgumentStackEntry] = ArgumentStackEntry(initialAcc, initialAcc) :: Nil
    var lhsStack: List[ACC] = Nil
    var acc: ACC = initialAcc
    var comingFrom: LogicalPlan = null

    def populate(): Unit = {
      while (!stack.head.isLeaf) {
        stack = stack.head.lhs.get :: stack
      }
    }

    populate()

    while (stack != Nil) {
      cancellation.throwIfCancelled()
      val current = stack.head
      val newStack = stack.tail
      stack = newStack

      (current.lhs, current.rhs) match {
        case (None, None) =>
          acc = f(acc, current)
        case (Some(_), None) =>
          acc = f(acc, current)
        case (Some(lhs), Some(rhs)) if comingFrom eq lhs =>
          if (current.isInstanceOf[ApplyPlan]) {
            val mappedArgumentAcc = mapArguments(acc, current)
            argumentStack = ArgumentStackEntry(acc, mappedArgumentAcc) :: argumentStack
            acc = mappedArgumentAcc
          } else {
            lhsStack = acc :: lhsStack
            acc = argumentStack.head.mappedArgumentAcc
          }
          stack = rhs :: current :: stack
          populate()
        case (Some(_), Some(rhs)) if comingFrom eq rhs =>
          if (current.isInstanceOf[ApplyPlan]) {
            val ArgumentStackEntry(lhsAcc, _) = argumentStack.head
            argumentStack = argumentStack.tail
            acc = combineLeftAndRight(lhsAcc, acc, current.asInstanceOf[LogicalBinaryPlan])
          } else {
            val lhsAcc = lhsStack.head
            lhsStack = lhsStack.tail
            acc = combineLeftAndRight(lhsAcc, acc, current.asInstanceOf[LogicalBinaryPlan])
          }
        case (Some(_), Some(_)) =>
          throw new IllegalStateException(
            s"Tried to map bad logical plan. In a two child plan, we must come from either RHS or LHS: op: $current"
          )

        case (None, Some(_)) =>
          throw new IllegalStateException(
            s"Tried to map bad logical plan. We can not have a RHS without having a LHS: op: $current"
          )
      }

      comingFrom = current
    }
    acc
  }

  /**
   * Fold over this logical plan tree in execution order.
   *
   * In this fold, the plan tree is visited in execution order, starting from
   * the leftmost leaf, and moving towards the root.
   *
   * Unlike [[foldPlan()]], this does not treat ApplyPlans and other binary plans differently.
   * Instead, a single accumulator gets passed through.
   *
   * NOTE: To avoid unpleasant surprises it is important that ACC is immutable,
   * unless you really know what you're doing. The same ACC instance might
   * be passed into several callback with the expectation of it being unchanged.
   *
   * @param f maps (currentAcc, plan) => acc for plan
   */
  def simpleFoldPlan[ACC](initialAcc: ACC)(
    root: LogicalPlan,
    f: (ACC, LogicalPlan) => ACC
  )(cancellation: CancellationChecker): ACC = {
    var stack: List[LogicalPlan] = root :: Nil
    var acc: ACC = initialAcc
    var comingFrom: LogicalPlan = null

    def populate(): Unit = {
      while (!stack.head.isLeaf) {
        stack = stack.head.lhs.get :: stack
      }
    }

    populate()

    while (stack != Nil) {
      cancellation.throwIfCancelled()
      val current = stack.head
      val newStack = stack.tail
      stack = newStack

      (current.lhs, current.rhs) match {
        case (None, None) =>
          acc = f(acc, current)
        case (Some(_), None) =>
          acc = f(acc, current)
        case (Some(lhs), Some(rhs)) if comingFrom eq lhs =>
          stack = rhs :: current :: stack
          populate()
        case (Some(_), Some(rhs)) if comingFrom eq rhs =>
          acc = f(acc, current)
        case (Some(_), Some(_)) =>
          throw new IllegalStateException(
            s"Tried to map bad logical plan. In a two child plan, we must come from either RHS or LHS: op: $current"
          )
        case (None, Some(_)) =>
          throw new IllegalStateException(
            s"Tried to map bad logical plan. We can not have a RHS without having a LHS: op: $current"
          )
      }

      comingFrom = current
    }
    acc
  }

}
