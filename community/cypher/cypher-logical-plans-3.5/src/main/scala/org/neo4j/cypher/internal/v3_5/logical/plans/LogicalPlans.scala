/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.v3_5.logical.plans

import scala.collection.mutable

object LogicalPlans {

  trait Mapper[T] {
    def onLeaf(plan: LogicalPlan): T
    def onOneChildPlan(plan: LogicalPlan, source: T): T
    def onTwoChildPlan(plan: LogicalPlan, lhs: T, rhs: T): T
  }

  /**
    * Traverses the logical plan tree structure and maps the tree in a bottom but fashion.
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
    *   B = mapOneChingPlan(b, D)
    *   A = mapTwoChildPlan(a, B, C)
    */
  def map[T](plan: LogicalPlan, mapper: Mapper[T]): T = {
    val planStack = new mutable.Stack[LogicalPlan]()
    val resultStack = new mutable.Stack[T]()
    var comingFrom = plan
    def populate(plan: LogicalPlan) = {
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
          throw new IllegalStateException(s"Tried to map bad logical plan. LHS and RHS must never be the same: op: $current\nfull plan: $plan")

        case (Some(left), Some(_)) if comingFrom eq left =>
          val arg1 = resultStack.pop()
          val arg2 = resultStack.pop()
          val result = mapper.onTwoChildPlan(current, arg1, arg2)
          resultStack.push(result)

        case (Some(left), Some(right)) if comingFrom eq right =>
          planStack.push(current)
          populate(left)
      }

      comingFrom = current

    }

    val result = resultStack.pop()
    assert(resultStack.isEmpty, "Should have emptied the stack of pipes by now!")
    result
  }
}
