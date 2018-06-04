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
package org.neo4j.cypher.internal.compatibility.v3_5.runtime

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration.Size
import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.ir.v3_5.CreateNode
import org.opencypher.v9_0.ast.semantics.SemanticTable
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.opencypher.v9_0.expressions._
import org.neo4j.cypher.internal.v3_5.logical.plans._

class SlotAllocationArgumentsTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  private val x = "x"
  private val z = "z"
  private val r = "r"
  private val semanticTable = SemanticTable()

  test("zero size argument for single all nodes scan") {
    // given
    val plan = AllNodesScan(x, Set.empty)

    // when
    val arguments = SlotAllocation.allocateSlots(plan, semanticTable).argumentSizes

    // then
    arguments should have size 1
    arguments(plan.id) should equal(Size(0, 0))
  }

  test("zero size argument for only leaf operator") {
    // given
    val leaf = AllNodesScan(x, Set.empty)
    val expand = Expand(leaf, x, SemanticDirection.INCOMING, Seq.empty, z, r, ExpandAll)

    // when
    val arguments = SlotAllocation.allocateSlots(expand, semanticTable).argumentSizes

    // then
    arguments should have size 1
    arguments(leaf.id) should equal(Size(0, 0))
  }

  test("zero size argument for argument operator") {
    val argument = Argument(Set.empty)

    // when
    val arguments = SlotAllocation.allocateSlots(argument, semanticTable).argumentSizes

    // then
    arguments should have size 1
    arguments(argument.id) should equal(Size(0, 0))
  }

  test("correct long size argument for rhs leaf") {
    // given
    val lhs = leaf()
    val rhs = leaf()
    val plan = applyRight(pipe(lhs, 1, 0), rhs)

    // when
    val arguments = SlotAllocation.allocateSlots(plan, semanticTable).argumentSizes

    // then
    arguments should have size 2
    arguments(lhs.id) should equal(Size(0, 0))
    arguments(rhs.id) should equal(Size(1, 0))
  }

  test("correct ref size argument for rhs leaf") {
    // given
    val lhs = leaf()
    val rhs = leaf()
    val plan = applyRight(pipe(lhs, 0, 1), rhs)

    // when
    val arguments = SlotAllocation.allocateSlots(plan, semanticTable).argumentSizes

    // then
    arguments should have size 2
    arguments(lhs.id) should equal(Size(0, 0))
    arguments(rhs.id) should equal(Size(0, 1))
  }

  test("correct size argument for more slots") {
    // given
    val lhs = leaf()
    val rhs = leaf()
    val plan = applyRight(pipe(lhs, 17, 11), rhs)

    // when
    val arguments = SlotAllocation.allocateSlots(plan, semanticTable).argumentSizes

    // then
    arguments should have size 2
    arguments(lhs.id) should equal(Size(0, 0))
    arguments(rhs.id) should equal(Size(17, 11))
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

    // when
    val arguments = SlotAllocation.allocateSlots(plan, semanticTable).argumentSizes

    // then
    arguments should have size 3
    arguments(leaf1.id) should equal(Size(0, 0))
    arguments(leaf2.id) should equal(Size(1, 0))
    arguments(leaf3.id) should equal(Size(1, 1))
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

    // when
    val arguments = SlotAllocation.allocateSlots(plan, semanticTable).argumentSizes

    // then
    arguments should have size 3
    arguments(leaf1.id) should equal(Size(0, 0))
    arguments(leaf2.id) should equal(Size(1, 0))
    arguments(leaf3.id) should equal(Size(1, 0))
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

    // when
    val arguments = SlotAllocation.allocateSlots(plan, semanticTable).argumentSizes

    // then
    arguments should have size 2
    arguments(leaf1.id) should equal(Size(0, 0))
    arguments(leaf2.id) should equal(Size(1, 0))
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

    // when
    val arguments = SlotAllocation.allocateSlots(plan, semanticTable).argumentSizes

    // then
    arguments should have size 2
    arguments(leaf1.id) should equal(Size(0, 0))
    arguments(leaf2.id) should equal(Size(1, 0))
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
    val optional = Optional(leaf2, Set.empty)
    val plan = applyRight(pipe(leaf1, 1, 0), optional)

    // when
    val arguments = SlotAllocation.allocateSlots(plan, semanticTable).argumentSizes

    // then
    arguments should have size 3
    arguments(leaf1.id) should equal(Size(0, 0))
    arguments(leaf2.id) should equal(Size(1, 0))
    arguments(optional.id) should equal(Size(1, 0))
  }

  private def leaf() = Argument(Set.empty)
  private def applyRight(lhs:LogicalPlan, rhs:LogicalPlan) = Apply(lhs, rhs)
  private def applyLeft(lhs:LogicalPlan, rhs:LogicalPlan) = SemiApply(lhs, rhs)
  private def break(source:LogicalPlan) = Eager(source)
  private def pipe(source:LogicalPlan, nLongs:Int, nRefs:Int) = {
    var curr: LogicalPlan =
      Create(
        source,
        (0 until nLongs).map(i => CreateNode("long"+i, Nil, None)),
        Nil
      )

    for ( i <- 0 until nRefs ) {
      curr = UnwindCollection(curr, "ref"+i, listOf(literalInt(1)))
    }
    curr
  }
}
