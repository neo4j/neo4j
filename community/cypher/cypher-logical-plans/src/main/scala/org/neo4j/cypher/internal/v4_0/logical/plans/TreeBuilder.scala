/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.v4_0.logical.plans

import org.neo4j.cypher.internal.v4_0.util.InternalException

import scala.collection.mutable

trait TreeBuilder[T] {

/*
 * Traverses the logical plan tree structure and builds up the corresponding output structure.
 * The traversal order is a kind of depth-first combined in/post-order (left first), in that
 * we visit (1) the left subtree, (2) the root, (3) the right subtree, and then (4) revisit the root.
 *
 * It also has a specific mechanism that allows you to return an optional output at step (2) that
 * will be passed on as an optional input to step (3). This mechanism uses the same output stack
 * and that is the only reason why the type T is wrapped in an Option.
 * This can be used to flatten the output structure, e.g. an Apply plan `a` can be turned into:
 *   a              a
 *  / \    ===>    /
 * b   c          c
 *               /
 *              b
 *
 * Given a logical plan such as:
 *
 *         a
 *        / \
 *       b   c
 *      / \   \
 *     d   e   f
 *
 * The virtual method callbacks will be called in the following sequence:
 *
 * D      = onLeaf(d, None)
 * maybeX = onTwoChildPlanComingFromLeft(b, D)
 * E      = onLeaf(e, maybeX)
 * B      = onTwoChildPlanComingFromRight(b, D, E)
 * maybeY = onTwoChildPlanComingFromLeft(a, B)
 * F      = onLeaf(f, maybeY)
 * C      = onOneChildPlan(c, F)
 * A      = onTwoChildPlanComingFromRight(a, B, C)
 */

  protected def onLeaf(plan: LogicalPlan, source: Option[T]): T
  protected def onOneChildPlan(plan: LogicalPlan, source: T): T
  protected def onTwoChildPlanComingFromLeft(plan: LogicalPlan, lhs: T): Option[T]
  protected def onTwoChildPlanComingFromRight(plan: LogicalPlan, lhs: T, rhs: T): T

  def create(plan: LogicalPlan): T = {

    val planStack = new mutable.Stack[LogicalPlan]()
    val outputStack = new mutable.Stack[Option[T]]()
    var comingFrom = plan

    /**
      * Eagerly populate the stack using all the lhs children.
      */
    def populate(plan: LogicalPlan) = {
      var current = plan
      while (!current.isLeaf) {
        planStack.push(current)
        current = current.lhs.getOrElse(throw new InternalException("This must not be!"))
      }
      comingFrom = current
      planStack.push(current)
    }

    populate(plan)
    outputStack.push(None) // Start the first lhs leaf without a source

    while (planStack.nonEmpty) {
      val current = planStack.pop()

      (current.lhs, current.rhs) match {
        case (None, None) =>
          val maybeSource = outputStack.pop() // From onTwoChildPlanComingFromLeft()
          val output = onLeaf(current, maybeSource)
          outputStack.push(Some(output))

        case (Some(_), None) =>
          val source = outputStack.pop().getOrElse(throw new InternalException("One child plan must have a source"))
          val output = onOneChildPlan(current, source)
          outputStack.push(Some(output))

        case (Some(left), Some(right)) if right == left =>
          throw new InternalException(s"Tried to map bad logical plan. LHS and RHS must never be the same: op: $current\nfull plan: $plan")

        case (Some(left), Some(right)) if comingFrom eq left =>
          val leftOutput = outputStack.top.getOrElse(throw new InternalException("Two child plan must have a lhs source"))
          val maybeSourceToTheRhsLeaf = onTwoChildPlanComingFromLeft(current, leftOutput)
          outputStack.push(maybeSourceToTheRhsLeaf)
          planStack.push(current)
          populate(right)

        case (Some(left), Some(right)) if comingFrom eq right =>
          val rightOutput = outputStack.pop().getOrElse(throw new InternalException("Two child plan must have a rhs source"))
          val leftOutput = outputStack.pop().getOrElse(throw new InternalException("Two child plan must have a lhs source"))
          val output = onTwoChildPlanComingFromRight(current, leftOutput, rightOutput)
          outputStack.push(Some(output))
      }

      comingFrom = current
    }

    val result = outputStack.pop().getOrElse(throw new InternalException("We should always have a result"))
    assert(outputStack.isEmpty, "Should have emptied the stack of output by now!")

    result
  }
}
