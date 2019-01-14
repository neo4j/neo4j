/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.compatibility.v3_4.runtime

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.SlotConfiguration.Size
import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions._
import org.neo4j.cypher.internal.v3_4.logical.plans._

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
    var curr = source
    for ( i <- 0 until nLongs ) {
      curr = CreateNode(curr, "long"+i, Nil, None)
    }
    for ( i <- 0 until nRefs ) {
      curr = UnwindCollection(curr, "ref"+i, listOf(literalInt(1)))
    }
    curr
  }
}
