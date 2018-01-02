/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_4.runtime

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.SlotConfiguration.Size
import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.ir.v3_4.IdName
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions._
import org.neo4j.cypher.internal.v3_4.logical.plans._

class SlotAllocationArgumentsTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  private val x = IdName("x")
  private val y = IdName("y")
  private val z = IdName("z")
  private val LABEL = LabelName("label")(pos)
  private val r = IdName("r")
  private val semanticTable = SemanticTable()

  test("zero size argument for single all nodes scan") {
    // given
    val plan = AllNodesScan(x, Set.empty)(solved)
    plan.assignIds()

    // when
    val arguments = SlotAllocation.allocateSlots(plan, semanticTable).argumentSizes

    // then
    arguments should have size 1
    arguments(plan.assignedId) should equal(Size(0, 0))
  }

  test("zero size argument for only leaf operator") {
    // given
    val leaf = AllNodesScan(x, Set.empty)(solved)
    val expand = Expand(leaf, x, SemanticDirection.INCOMING, Seq.empty, z, r, ExpandAll)(solved)
    expand.assignIds()

    // when
    val arguments = SlotAllocation.allocateSlots(expand, semanticTable).argumentSizes

    // then
    arguments should have size 1
    arguments(leaf.assignedId) should equal(Size(0, 0))
  }

  test("zero size argument for argument operator") {
    val argument = Argument(Set.empty)(solved)
    argument.assignIds()

    // when
    val arguments = SlotAllocation.allocateSlots(argument, semanticTable).argumentSizes

    // then
    arguments should have size 1
    arguments(argument.assignedId) should equal(Size(0, 0))
  }

  test("correct long size argument for rhs leaf") {
    // given
    val lhs = leaf()
    val rhs = leaf()
    val plan = applyRight(pipe(lhs, 1, 0), rhs)
    plan.assignIds()

    // when
    val arguments = SlotAllocation.allocateSlots(plan, semanticTable).argumentSizes

    // then
    arguments should have size 2
    arguments(lhs.assignedId) should equal(Size(0, 0))
    arguments(rhs.assignedId) should equal(Size(1, 0))
  }

  test("correct ref size argument for rhs leaf") {
    // given
    val lhs = leaf()
    val rhs = leaf()
    val plan = applyRight(pipe(lhs, 0, 1), rhs)
    plan.assignIds()

    // when
    val arguments = SlotAllocation.allocateSlots(plan, semanticTable).argumentSizes

    // then
    arguments should have size 2
    arguments(lhs.assignedId) should equal(Size(0, 0))
    arguments(rhs.assignedId) should equal(Size(0, 1))
  }

  test("correct size argument for more slots") {
    // given
    val lhs = leaf()
    val rhs = leaf()
    val plan = applyRight(pipe(lhs, 17, 11), rhs)
    plan.assignIds()

    // when
    val arguments = SlotAllocation.allocateSlots(plan, semanticTable).argumentSizes

    // then
    arguments should have size 2
    arguments(lhs.assignedId) should equal(Size(0, 0))
    arguments(rhs.assignedId) should equal(Size(17, 11))
  }

  test("apply right keeps rhs slots") {
    // given
    //        applyRight
    //         \        \
    //    applyRight    leaf3
    //    /        \
    // +1 long   +1 ref
    //    |         |
    //  leaf1     leaf2

    val leaf1 = leaf()
    val leaf2 = leaf()
    val leaf3 = leaf()
    val plan = applyRight(applyRight(pipe(leaf1, 1, 0), pipe(leaf2, 0, 1)), leaf3)
    plan.assignIds()

    // when
    val arguments = SlotAllocation.allocateSlots(plan, semanticTable).argumentSizes

    // then
    arguments should have size 3
    arguments(leaf1.assignedId) should equal(Size(0, 0))
    arguments(leaf2.assignedId) should equal(Size(1, 0))
    arguments(leaf3.assignedId) should equal(Size(1, 1))
  }

  test("apply left ignores rhs slots") {
    // given
    //          applyRight
    //         /          \
    //     applyLeft     leaf3
    //    /        \
    // +1 long   +1 ref
    //    |         |
    //  leaf1     leaf2

    val leaf1 = leaf()
    val leaf2 = leaf()
    val leaf3 = leaf()
    val plan = applyRight(applyLeft(pipe(leaf1, 1, 0), pipe(leaf2, 0, 1)), leaf3)
    plan.assignIds()

    // when
    val arguments = SlotAllocation.allocateSlots(plan, semanticTable).argumentSizes

    // then
    arguments should have size 3
    arguments(leaf1.assignedId) should equal(Size(0, 0))
    arguments(leaf2.assignedId) should equal(Size(1, 0))
    arguments(leaf3.assignedId) should equal(Size(1, 0))
  }

  test("apply left argument does not leak downstream slots") {
    // given
    //       +1 ref
    //         /
    //     applyLeft
    //    /        \
    // +1 long   leaf2
    //    |
    //  leaf1

    val leaf1 = leaf()
    val leaf2 = leaf()
    val plan = pipe(applyLeft(pipe(leaf1, 1, 0), leaf2), 0, 1)
    plan.assignIds()

    // when
    val arguments = SlotAllocation.allocateSlots(plan, semanticTable).argumentSizes

    // then
    arguments should have size 2
    arguments(leaf1.assignedId) should equal(Size(0, 0))
    arguments(leaf2.assignedId) should equal(Size(1, 0))
  }

  test("argument is passed over pipeline break") {
    // given
    //     applyRight
    //    /        \
    // +1 long   +1 ref
    //    |       --|--
    //  leaf1     breaker
    //              |
    //            leaf2

    val leaf1 = leaf()
    val leaf2 = leaf()
    val plan = applyRight(pipe(leaf1, 1, 0), pipe(break(leaf2), 0, 1))
    plan.assignIds()

    // when
    val arguments = SlotAllocation.allocateSlots(plan, semanticTable).argumentSizes

    // then
    arguments should have size 2
    arguments(leaf1.assignedId) should equal(Size(0, 0))
    arguments(leaf2.assignedId) should equal(Size(1, 0))
  }

  test("Optional should record argument size") {
    // given
    //     applyRight
    //    /        \
    // +1 long   optional
    //    |         |
    //  leaf1     leaf2

    val leaf1 = leaf()
    val leaf2 = leaf()
    val optional = Optional(leaf2, Set.empty)(solved)
    val plan = applyRight(pipe(leaf1, 1, 0), optional)
    plan.assignIds()

    // when
    val arguments = SlotAllocation.allocateSlots(plan, semanticTable).argumentSizes

    // then
    arguments should have size 3
    arguments(leaf1.assignedId) should equal(Size(0, 0))
    arguments(leaf2.assignedId) should equal(Size(1, 0))
    arguments(optional.assignedId) should equal(Size(1, 0))
  }

  private def leaf() = Argument(Set.empty)(solved)
  private def applyRight(lhs:LogicalPlan, rhs:LogicalPlan) = Apply(lhs, rhs)(solved)
  private def applyLeft(lhs:LogicalPlan, rhs:LogicalPlan) = SemiApply(lhs, rhs)(solved)
  private def break(source:LogicalPlan) = Eager(source)(solved)
  private def pipe(source:LogicalPlan, nLongs:Int, nRefs:Int) = {
    var curr = source
    for ( i <- 0 until nLongs ) {
      curr = CreateNode(curr, IdName("long"+i), Nil, None)(solved)
    }
    for ( i <- 0 until nRefs ) {
      curr = UnwindCollection(curr, IdName("ref"+i), listOf(literalInt(1)))(solved)
    }
    curr
  }
}
