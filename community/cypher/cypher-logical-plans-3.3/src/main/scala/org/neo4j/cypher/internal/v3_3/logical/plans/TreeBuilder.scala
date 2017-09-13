/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.v3_3.logical.plans

import org.neo4j.cypher.internal.frontend.v3_3.InternalException

import scala.collection.mutable

trait TreeBuilder[TO] {

  /*
Traverses the logical plan tree structure and builds up the corresponding output structure. Given a logical plan such as:

        a
       / \
      b   c
     /   / \
    d   e   f

 populate(a) starts the session, and eagerly adds [a, c, f] to the plan stack. We then immediately pop 'f' from the
 plan stack, we build an output for it add it to the output stack, and pop 'c' from the plan stack. Since we are coming from
 'f', we add [c, e] to the stack and then pop 'e' out again. This is a leaf, so we build an output for it and add it to the
 output stack. We now pop 'c' from the plan stack again. This time we are coming from 'e', and so we know we can use
 two outputs from the output stack to use when building 'c'. We add this output to the output stack and pop 'a' from the plan
 stack. Since we are coming from 'a's RHS, we add [a,b,d] to the stack. Next step is to pop 'd' out, and build an output
 for it, storing it in the output stack. Pop ut 'b' from the plan stack, one output from the output stack, and build an output for 'b'.
 Next we pop out 'a', and this time we are coming from the LHS, and we can now pop two outputs from the output stack to
 build the output for 'a'. Thanks for reading this far - I didn't think we would make it!
 */
  def create(plan: LogicalPlan): TO = {

    val planStack = new mutable.Stack[LogicalPlan]()
    val outputStack = new mutable.Stack[TO]()
    var comingFrom = plan

    def populate(plan: LogicalPlan) = {
      var current = plan
      while (!current.isLeaf) {
        planStack.push(current)
        (current.lhs, current.rhs) match {
          case (_, Some(right)) =>
            current = right

          case (Some(left), None) =>
            current = left

          case _ => throw new InternalException("This must not be!")
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
          val output = build(current)
          outputStack.push(output)

        case (Some(_), None) =>
          val source = outputStack.pop()
          val output = build(current, source)
          outputStack.push(output)

        case (Some(left), Some(_)) if comingFrom eq left =>
          val arg1 = outputStack.pop()
          val arg2 = outputStack.pop()
          val output = build(current, arg1, arg2)

          outputStack.push(output)

        case (Some(left), Some(right)) if comingFrom eq right =>
          planStack.push(current)
          populate(left)
      }

      comingFrom = current
    }

    val result = outputStack.pop()
    assert(outputStack.isEmpty, "Should have emptied the stack of output by now!")

    result
  }

  protected def build(plan: LogicalPlan): TO

  protected def build(plan: LogicalPlan, source: TO): TO

  protected def build(plan: LogicalPlan, lhs: TO, rhs: TO): TO
}
