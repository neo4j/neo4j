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
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Create
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.SemiApply
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.LiveVariables
import org.neo4j.cypher.internal.physicalplanning.SlotAllocation.SlotMetaData
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.Size
import org.neo4j.cypher.internal.runtime.CypherRuntimeConfiguration
import org.neo4j.cypher.internal.runtime.expressionVariableAllocation.AvailableExpressionVariables
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

//noinspection NameBooleanParameters
class SlotAllocationArgumentsTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  private val semanticTable = SemanticTable()
  private val NO_EXPR_VARS = new AvailableExpressionVariables()
  private val config = CypherRuntimeConfiguration.defaultConfiguration

  test("zero size argument for single all nodes scan") {
    // given
    val plan = AllNodesScan(varFor("x"), Set.empty)

    // when
    val arguments = allocateSlots(
      plan,
      semanticTable,
      BREAK_FOR_LEAFS,
      NO_EXPR_VARS,
      config,
      new AnonymousVariableNameGenerator()
    ).argumentSizes

    // then
    arguments should have size 1
    arguments(plan.id) should equal(Size(0, 0))
  }

  test("zero size argument for only leaf operator") {
    // given
    val leaf = AllNodesScan(varFor("x"), Set.empty)
    val expand = Expand(leaf, varFor("x"), INCOMING, Seq.empty, varFor("z"), varFor("r"), ExpandAll)

    // when
    val arguments = allocateSlots(
      expand,
      semanticTable,
      BREAK_FOR_LEAFS,
      NO_EXPR_VARS,
      config,
      new AnonymousVariableNameGenerator()
    ).argumentSizes

    // then
    arguments should have size 1
    arguments(leaf.id) should equal(Size(0, 0))
  }

  test("zero size argument for argument operator") {
    val argument = Argument(Set.empty)

    // when
    val arguments = allocateSlots(
      argument,
      semanticTable,
      BREAK_FOR_LEAFS,
      NO_EXPR_VARS,
      config,
      new AnonymousVariableNameGenerator()
    ).argumentSizes

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
    val arguments = allocateSlots(
      plan,
      semanticTable,
      BREAK_FOR_LEAFS,
      NO_EXPR_VARS,
      config,
      new AnonymousVariableNameGenerator()
    ).argumentSizes

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
    val arguments = allocateSlots(
      plan,
      semanticTable,
      BREAK_FOR_LEAFS,
      NO_EXPR_VARS,
      config,
      new AnonymousVariableNameGenerator()
    ).argumentSizes

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
    val arguments = allocateSlots(
      plan,
      semanticTable,
      BREAK_FOR_LEAFS,
      NO_EXPR_VARS,
      config,
      new AnonymousVariableNameGenerator()
    ).argumentSizes

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
    val arguments = allocateSlots(
      plan,
      semanticTable,
      BREAK_FOR_LEAFS,
      NO_EXPR_VARS,
      config,
      new AnonymousVariableNameGenerator()
    ).argumentSizes

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
    val arguments = allocateSlots(
      plan,
      semanticTable,
      BREAK_FOR_LEAFS,
      NO_EXPR_VARS,
      config,
      new AnonymousVariableNameGenerator()
    ).argumentSizes

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
    val arguments = allocateSlots(
      plan,
      semanticTable,
      BREAK_FOR_LEAFS,
      NO_EXPR_VARS,
      config,
      new AnonymousVariableNameGenerator()
    ).argumentSizes

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
    val arguments = allocateSlots(
      plan,
      semanticTable,
      BREAK_FOR_LEAFS,
      NO_EXPR_VARS,
      config,
      new AnonymousVariableNameGenerator()
    ).argumentSizes

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
    val arguments = allocateSlots(
      plan,
      semanticTable,
      BREAK_FOR_LEAFS,
      NO_EXPR_VARS,
      config,
      new AnonymousVariableNameGenerator()
    ).argumentSizes

    // then
    arguments should have size 3
    arguments(leaf1.id) should equal(Size(0, 0))
    arguments(leaf2.id) should equal(Size(1, 0))
    arguments(optional.id) should equal(Size(1, 0))
  }

  test("Distinct should retain argument slots") {
    // given
    //     applyRight
    //    /        \
    // +1 long   distinct
    // +2 ref       |
    //    |       +2 long
    //  leaf1     +2 ref
    //              |
    //            leaf2

    val leaf1 = leaf()
    val lhs = pipe(leaf1, 1, 2, "lhsLong", "lhsRef")
    val leaf2 = leaf()
    val rhs = pipe(leaf2, 2, 2, "rhsLong", "rhsRef")
    val distinct = Distinct(rhs, Map(varFor("rhsLong0") -> varFor("rhsLong0"), varFor("rhsRef1") -> varFor("rhsRef1")))
    val plan = applyRight(lhs, distinct)

    // when
    val slots = allocateSlots(
      plan,
      semanticTable,
      BREAK_FOR_LEAFS,
      NO_EXPR_VARS,
      config,
      new AnonymousVariableNameGenerator()
    ).slotConfigurations

    // then
    slots(distinct.id) should be(SlotConfiguration.empty
      .newLong("lhsLong0", false, CTNode)
      .newLong("rhsLong0", false, CTNode)
      .newLong("rhsLong1", false, CTNode) // kept because we don't break pipeline on distinct
      .newReference("lhsRef0", false, CTAny)
      .newReference("lhsRef1", false, CTAny)
      .newReference("rhsRef0", false, CTAny) // kept because we don't break pipeline on distinct
      .newReference("rhsRef1", false, CTAny))
  }

  test("Aggregation should retain argument slots") {
    // given
    //     applyRight
    //    /        \
    // +1 long   aggregation
    // +2 ref       |
    //    |       +2 long
    //  leaf1     +2 ref
    //              |
    //            leaf2

    val leaf1 = leaf()
    val lhs = pipe(leaf1, 1, 2, "lhsLong", "lhsRef")
    val leaf2 = leaf()
    val rhs = pipe(leaf2, 2, 2, "rhsLong", "rhsRef")
    val aggregation =
      Aggregation(rhs, Map(varFor("rhsLong0") -> varFor("rhsLong0")), Map(varFor("rhsRef1") -> varFor("rhsRef1")), None)
    val plan = applyRight(lhs, aggregation)

    // when
    val slots = allocateSlots(
      plan,
      semanticTable,
      PipelineBreakingPolicy.breakFor(leaf1, leaf2, aggregation),
      NO_EXPR_VARS,
      config,
      new AnonymousVariableNameGenerator()
    ).slotConfigurations

    // then
    slots(aggregation.id) should be(SlotConfiguration.empty
      .newLong("lhsLong0", false, CTNode)
      .newLong("rhsLong0", false, CTNode)
      .newReference("lhsRef0", false, CTAny)
      .newReference("lhsRef1", false, CTAny)
      .newReference("rhsRef1", true, CTAny))
  }

  private def leaf() = Argument(Set.empty)
  private def applyRight(lhs: LogicalPlan, rhs: LogicalPlan) = Apply(lhs, rhs)
  private def applyLeft(lhs: LogicalPlan, rhs: LogicalPlan) = SemiApply(lhs, rhs)
  private def break(source: LogicalPlan) = Eager(source)

  private def pipe(
    source: LogicalPlan,
    nLongs: Int,
    nRefs: Int,
    longPrefix: String = "long",
    refPrefix: String = "ref"
  ) = {
    var curr: LogicalPlan =
      Create(
        source,
        (0 until nLongs).map(i => createNode(longPrefix + i))
      )

    for (i <- 0 until nRefs) {
      curr = UnwindCollection(curr, varFor(refPrefix + i), listOfInt(1))
    }
    curr
  }

  private def allocateSlots(
    lp: LogicalPlan,
    semanticTable: SemanticTable,
    breakingPolicy: PipelineBreakingPolicy,
    availableExpressionVariables: AvailableExpressionVariables,
    config: CypherRuntimeConfiguration,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator
  ): SlotMetaData = SlotAllocation.allocateSlots(
    lp,
    semanticTable,
    breakingPolicy,
    availableExpressionVariables,
    config,
    anonymousVariableNameGenerator,
    new LiveVariables()
  )
}
