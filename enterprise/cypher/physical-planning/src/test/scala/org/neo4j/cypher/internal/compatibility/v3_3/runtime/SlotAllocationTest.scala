/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime

import org.neo4j.cypher.internal.compiler.v3_3.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_3.{LabelId, SemanticDirection}
import org.neo4j.cypher.internal.ir.v3_3.{CardinalityEstimation, IdName, PlannerQuery, VarPatternLength}
import org.neo4j.cypher.internal.v3_3.logical.plans.{Ascending, _}
import org.neo4j.cypher.internal.v3_3.logical.{plans => logicalPlans}

class SlotAllocationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  private val x = IdName("x")
  private val y = IdName("y")
  private val z = IdName("z")
  private val LABEL = LabelName("label")(pos)
  private val r = IdName("r")

  test("only single allnodes scan") {
    // given
    val plan = AllNodesScan(x, Set.empty)(solved)
    plan.assignIds()

    // when
    val allocations = SlotAllocation.allocateSlots(plan)

    // then
    allocations should have size 1
    allocations(plan.assignedId) should equal(
      PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode, "x")), 1, 0))
  }

  test("limit should not introduce slots") {
    // given
    val plan = logicalPlans.Limit(AllNodesScan(x, Set.empty)(solved), literalInt(1), DoNotIncludeTies)(solved)
    plan.assignIds()

    // when
    val allocations = SlotAllocation.allocateSlots(plan)

    // then
    allocations should have size 2
    allocations(plan.assignedId) should equal(
      PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode, "x")), 1, 0))
  }

  test("single labelscan scan") {
    // given
    val plan = NodeByLabelScan(x, LABEL, Set.empty)(solved)
    plan.assignIds()

    // when
    val allocations = SlotAllocation.allocateSlots(plan)

    // then
    allocations should have size 1
    allocations(plan.assignedId) should equal(PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode, "x")), 1, 0))
  }

  test("labelscan with filtering") {
    // given
    val leaf = NodeByLabelScan(x, LABEL, Set.empty)(solved)
    val filter = Selection(Seq(True()(pos)), leaf)(solved)
    filter.assignIds()

    // when
    val allocations = SlotAllocation.allocateSlots(filter)

    // then
    allocations should have size 2
    allocations(leaf.assignedId) should equal(PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode, "x")), 1, 0))
    allocations(filter.assignedId) shouldBe theSameInstanceAs(allocations(leaf.assignedId))
  }

  test("single node with expand") {
    // given
    val allNodesScan = AllNodesScan(x, Set.empty)(solved)
    val expand = Expand(allNodesScan, x, SemanticDirection.INCOMING, Seq.empty, z, r, ExpandAll)(solved)
    expand.assignIds()

    // when
    val allocations = SlotAllocation.allocateSlots(expand)

    // then we'll end up with two pipelines
    allocations should have size 2
    val labelScanAllocations = allocations(allNodesScan.assignedId)
    labelScanAllocations should equal(
      PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode, "x")), numberOfLongs = 1, numberOfReferences = 0))

    val expandAllocations = allocations(expand.assignedId)
    expandAllocations should equal(
      PipelineInformation(Map(
        "x" -> LongSlot(0, nullable = false, CTNode, "x"),
        "r" -> LongSlot(1, nullable = false, CTRelationship, "r"),
        "z" -> LongSlot(2, nullable = false, CTNode, "z")), numberOfLongs = 3, numberOfReferences = 0))
  }

  test("single node with expand into") {
    // given
    val allNodesScan = AllNodesScan(x, Set.empty)(solved)
    val expand = Expand(allNodesScan, x, SemanticDirection.INCOMING, Seq.empty, x, r, ExpandInto)(solved)
    expand.assignIds()

    // when
    val allocations = SlotAllocation.allocateSlots(expand)

    // then we'll end up with two pipelines
    allocations should have size 2
    val labelScanAllocations = allocations(allNodesScan.assignedId)
    labelScanAllocations should equal(
      PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode, "x")), numberOfLongs = 1, numberOfReferences = 0))

    val expandAllocations = allocations(expand.assignedId)
    expandAllocations should equal(
      PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode, "x"), "r" -> LongSlot(1, nullable = false,
        CTRelationship, "r")), numberOfLongs = 2, numberOfReferences = 0))
  }

  test("optional node") {
    // given
    val leaf = AllNodesScan(x, Set.empty)(solved)
    val plan = Optional(leaf)(solved)
    plan.assignIds()

    // when
    val allocations = SlotAllocation.allocateSlots(plan)

    // then
    allocations should have size 2
    allocations(plan.assignedId) should equal(PipelineInformation(Map("x" -> LongSlot(0, nullable = true, CTNode, "x")), 1, 0))
  }

  test("single node with optionalExpand ExpandAll") {
    // given
    val allNodesScan = AllNodesScan(x, Set.empty)(solved)
    val expand = OptionalExpand(allNodesScan, x, SemanticDirection.INCOMING, Seq.empty, z, r, ExpandAll)(solved)
    expand.assignIds()

    // when
    val allocations = SlotAllocation.allocateSlots(expand)

    // then we'll end up with two pipelines
    allocations should have size 2
    val labelScanAllocations = allocations(allNodesScan.assignedId)
    labelScanAllocations should equal(
      PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode, "x")), numberOfLongs = 1, numberOfReferences =
        0))

    val expandAllocations = allocations(expand.assignedId)
    expandAllocations should equal(
      PipelineInformation(Map(
        "x" -> LongSlot(0, nullable = false, CTNode, "x"),
        "r" -> LongSlot(1, nullable = true, CTRelationship, "r"),
        "z" -> LongSlot(2, nullable = true, CTNode, "z")
      ), numberOfLongs = 3, numberOfReferences = 0))
  }

  test("single node with optionalExpand ExpandInto") {
    // given
    val allNodesScan = AllNodesScan(x, Set.empty)(solved)
    val expand = OptionalExpand(allNodesScan, x, SemanticDirection.INCOMING, Seq.empty, x, r, ExpandInto)(solved)
    expand.assignIds()

    // when
    val allocations = SlotAllocation.allocateSlots(expand)

    // then we'll end up with two pipelines
    allocations should have size 2
    val labelScanAllocations = allocations(allNodesScan.assignedId)
    labelScanAllocations should equal(
      PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode, "x")), numberOfLongs = 1, numberOfReferences = 0))

    val expandAllocations = allocations(expand.assignedId)
    expandAllocations should equal(
      PipelineInformation(Map(
        "x" -> LongSlot(0, nullable = false, CTNode, "x"),
        "r" -> LongSlot(1, nullable = true, CTRelationship, "r")
      ), numberOfLongs = 2, numberOfReferences = 0))
  }

  test("single node with var length expand") {
    // given
    val allNodesScan = AllNodesScan(x, Set.empty)(solved)
    val varLength = VarPatternLength(1, Some(15))
    val tempNode = IdName("r_NODES")
    val tempEdge = IdName("r_EDGES")
    val expand = VarExpand(allNodesScan, x, SemanticDirection.INCOMING, SemanticDirection.INCOMING, Seq.empty, z, r,
      varLength, ExpandAll, tempNode, tempEdge, True()(pos), True()(pos), Seq.empty)(solved)
    expand.assignIds()

    // when
    val allocations = SlotAllocation.allocateSlots(expand)

    // then we'll end up with two pipelines
    allocations should have size 2
    val labelScanAllocations = allocations(allNodesScan.assignedId)
    labelScanAllocations should equal(
      PipelineInformation(Map(
        "x" -> LongSlot(0, nullable = false, CTNode, "x"),
        "r_NODES" -> LongSlot(1, nullable = false, CTNode, "r_NODES"),
        "r_EDGES" -> LongSlot(2, nullable = false, CTRelationship, "r_EDGES")),
        numberOfLongs = 3, numberOfReferences = 0))

    val expandAllocations = allocations(expand.assignedId)
    expandAllocations should equal(
      PipelineInformation(Map(
        "x" -> LongSlot(0, nullable = false, CTNode, "x"),
        "r" -> RefSlot(0, nullable = false, CTList(CTRelationship), "r"),
        "z" -> LongSlot(1, nullable = false, CTNode, "z")), numberOfLongs = 2, numberOfReferences = 1))
  }

  test("let's skip this one") {
    // given
    val allNodesScan = AllNodesScan(x, Set.empty)(solved)
    val skip = logicalPlans.Skip(allNodesScan, literalInt(42))(solved)
    skip.assignIds()

    // when
    val allocations = SlotAllocation.allocateSlots(skip)

    // then
    allocations should have size 2
    val labelScanAllocations = allocations(allNodesScan.assignedId)
    labelScanAllocations should equal(
      PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode, "x")), numberOfLongs = 1, numberOfReferences = 0))

    val expandAllocations = allocations(skip.assignedId)
    expandAllocations shouldBe theSameInstanceAs(labelScanAllocations)
  }

  test("all we need is to apply ourselves") {
    // given
    val lhs = NodeByLabelScan(x, LABEL, Set.empty)(solved)
    val label = LabelToken("label2", LabelId(0))
    val seekExpression = SingleQueryExpression(literalInt(42))
    val rhs = NodeIndexSeek(z, label, Seq.empty, seekExpression, Set(x))(solved)
    val apply = Apply(lhs, rhs)(solved)
    apply.assignIds()

    // when
    val allocations = SlotAllocation.allocateSlots(apply)

    // then
    allocations should have size 3
    allocations(lhs.assignedId) should equal(
      PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode, "x")), numberOfLongs = 1, numberOfReferences = 0))

    val rhsPipeline = allocations(rhs.assignedId)

    rhsPipeline should equal(
      PipelineInformation(Map(
        "x" -> LongSlot(0, nullable = false, CTNode, "x"),
        "z" -> LongSlot(1, nullable = false, CTNode, "z")
      ), numberOfLongs = 2, numberOfReferences = 0))

    allocations(apply.assignedId) shouldBe theSameInstanceAs(rhsPipeline)
  }

  test("aggregation used for distinct") {
    // given
    val leaf = NodeByLabelScan(x, LABEL, Set.empty)(solved)
    val distinct = Aggregation(leaf, Map("x" -> varFor("x")), Map.empty)(solved)
    distinct.assignIds()

    // when
    val allocations = SlotAllocation.allocateSlots(distinct)

    // then
    allocations should have size 2
    allocations(leaf.assignedId) should equal(
      PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode, "x")), numberOfLongs = 1, numberOfReferences = 0))

    allocations(distinct.assignedId) should equal(
      PipelineInformation(Map("x" -> RefSlot(0, nullable = false, CTNode, "x")), numberOfLongs = 0, numberOfReferences = 1))
  }

  test("optional travels through aggregation used for distinct") {
    // given OPTIONAL MATCH (x) RETURN DISTINCT x, x.propertyKey
    val leaf = NodeByLabelScan(x, LABEL, Set.empty)(solved)
    val optional = Optional(leaf)(solved)
    val distinct = Distinct(optional, Map("x" -> varFor("x"), "x.propertyKey" -> prop("x", "propertyKey")))(solved)
    distinct.assignIds()

    // when
    val allocations = SlotAllocation.allocateSlots(distinct)

    // then
    allocations should have size 3
    allocations(leaf.assignedId) should equal(
      PipelineInformation(Map("x" -> LongSlot(0, nullable = true, CTNode, "x")), numberOfLongs = 1, numberOfReferences = 0))

    allocations(optional.assignedId) should be theSameInstanceAs allocations(leaf.assignedId)
    allocations(distinct.assignedId) should equal(PipelineInformation(numberOfLongs = 0, numberOfReferences = 2, slots = Map(
      "x" -> RefSlot(0, nullable = true, CTNode, "x"),
      "x.propertyKey" -> RefSlot(1, nullable = true, CTAny, "x.propertyKey")
    )))
  }

  test("optional travels through aggregation") {
    // given OPTIONAL MATCH (x) RETURN x, x.propertyKey, count(*)
    val leaf = NodeByLabelScan(x, LABEL, Set.empty)(solved)
    val optional = Optional(leaf)(solved)
    val countStar = Aggregation(optional,
      groupingExpressions = Map("x" -> varFor("x"),
        "x.propertyKey" -> prop("x", "propertyKey")),
      aggregationExpression = Map("count(*)" -> CountStar()(pos)))(solved)
    countStar.assignIds()

    // when
    val allocations = SlotAllocation.allocateSlots(countStar)

    // then
    allocations should have size 3
    allocations(leaf.assignedId) should equal(
      PipelineInformation(Map("x" -> LongSlot(0, nullable = true, CTNode, "x")), numberOfLongs = 1, numberOfReferences = 0))

    allocations(optional.assignedId) should be theSameInstanceAs allocations(leaf.assignedId)
    allocations(countStar.assignedId) should equal(
      PipelineInformation(numberOfLongs = 0, numberOfReferences = 3, slots = Map(
        "x" -> RefSlot(0, nullable = true, CTNode, "x"),
        "x.propertyKey" -> RefSlot(1, nullable = true, CTAny, "x.propertyKey"),
        "count(*)" -> RefSlot(2, nullable = true, CTAny, "count(*)")
      )))
  }

  test("labelscan with projection") {
    // given
    val leaf = NodeByLabelScan(x, LABEL, Set.empty)(solved)
    val projection = Projection(leaf, Map("x" -> varFor("x"), "x.propertyKey" -> prop("x", "propertyKey")))(solved)
    projection.assignIds()

    // when
    val allocations = SlotAllocation.allocateSlots(projection)

    // then
    allocations should have size 2
    allocations(leaf.assignedId) should equal(PipelineInformation(numberOfLongs = 1, numberOfReferences = 1, slots = Map(
      "x" -> LongSlot(0, nullable = false, CTNode, "x"),
      "x.propertyKey" -> RefSlot(0, nullable = true, CTAny, "x.propertyKey")
    )))
    allocations(projection.assignedId) shouldBe theSameInstanceAs(allocations(leaf.assignedId))
  }

  test("cartesian product") {
    // given
    val lhs = NodeByLabelScan(x, LabelName("label1")(pos), Set.empty)(solved)
    val rhs = NodeByLabelScan(y, LabelName("label2")(pos), Set.empty)(solved)
    val Xproduct = CartesianProduct(lhs, rhs)(solved)
    Xproduct.assignIds()

    // when
    val allocations = SlotAllocation.allocateSlots(Xproduct)

    // then
    allocations should have size 3
    allocations(lhs.assignedId) should equal(PipelineInformation(numberOfLongs = 1, numberOfReferences = 0, slots = Map(
      "x" -> LongSlot(0, nullable = false, CTNode, "x")
    )))
    allocations(rhs.assignedId) should equal(PipelineInformation(numberOfLongs = 1, numberOfReferences = 0, slots = Map(
      "y" -> LongSlot(0, nullable = false, CTNode, "y")
    )))
    allocations(Xproduct.assignedId) should equal(PipelineInformation(numberOfLongs = 2, numberOfReferences = 0, slots = Map(
      "x" -> LongSlot(0, nullable = false, CTNode, "x"),
      "y" -> LongSlot(1, nullable = false, CTNode, "y")
    )))
  }

  test("that argument does not apply here") {
    // given MATCH (x) MATCH (x)<-[r]-(y)
    val lhs = NodeByLabelScan(x, LABEL, Set.empty)(solved)
    val arg = Argument(Set(x))(solved)()
    val rhs = Expand(arg, x, SemanticDirection.INCOMING, Seq.empty, y, r, ExpandAll)(solved)

    val apply = Apply(lhs, rhs)(solved)
    apply.assignIds()

    // when
    val allocations = SlotAllocation.allocateSlots(apply)

    // then
    val lhsPipeline = PipelineInformation(Map(
      "x" -> LongSlot(0, nullable = false, CTNode, "x")),
      numberOfLongs = 1, numberOfReferences = 0)

    val rhsPipeline = PipelineInformation(Map(
      "x" -> LongSlot(0, nullable = false, CTNode, "x"),
      "y" -> LongSlot(2, nullable = false, CTNode, "y"),
      "r" -> LongSlot(1, nullable = false, CTRelationship, "r")
    ), numberOfLongs = 3, numberOfReferences = 0)

    allocations should have size 4
    allocations(arg.assignedId) should equal(lhsPipeline)
    allocations(lhs.assignedId) should equal(lhsPipeline)
    allocations(apply.assignedId) should equal(rhsPipeline)
    allocations(rhs.assignedId) should equal(rhsPipeline)
  }

  test("unwind and project") {
    // given UNWIND [1,2,3] as x RETURN x
    val leaf = SingleRow()(solved)
    val unwind = UnwindCollection(leaf, IdName("x"), listOf(literalInt(1), literalInt(2), literalInt(3)))(solved)
    val produceResult = ProduceResult(Seq("x"), unwind)
    produceResult.assignIds()

    // when
    val allocations = SlotAllocation.allocateSlots(produceResult)

    // then
    allocations should have size 3
    allocations(leaf.assignedId) should equal(PipelineInformation(Map.empty, 0, 0))


    allocations(unwind.assignedId) should equal(PipelineInformation(numberOfLongs = 0, numberOfReferences = 1, slots = Map(
      "x" -> RefSlot(0, nullable = true, CTAny, "x")
    )))
    allocations(produceResult.assignedId) shouldBe theSameInstanceAs(allocations(unwind.assignedId))
  }

  test("unwind and project and sort") {
    // given UNWIND [1,2,3] as x RETURN x ORDER BY x
    val xVar = varFor("x")
    val xVarName = IdName.fromVariable(xVar)
    val leaf = SingleRow()(solved)
    val unwind = UnwindCollection(leaf, xVarName, listOf(literalInt(1), literalInt(2), literalInt(3)))(solved)
    val sort = Sort(unwind, List(Ascending(xVarName)))(solved)
    val produceResult = ProduceResult(Seq("x"), sort)
    produceResult.assignIds()

    // when
    val allocations = SlotAllocation.allocateSlots(produceResult)

    // then
    allocations should have size 4
    allocations(leaf.assignedId) should equal(PipelineInformation(Map.empty, 0, 0))


    val expectedPipeline = PipelineInformation(numberOfLongs = 0, numberOfReferences = 1, slots = Map(
      "x" -> RefSlot(0, nullable = true, CTAny, "x")
    ))
    allocations(unwind.assignedId) should equal(expectedPipeline)
    allocations(sort.assignedId) shouldBe theSameInstanceAs(allocations(unwind.assignedId))
    allocations(produceResult.assignedId) shouldBe theSameInstanceAs(allocations(unwind.assignedId))
  }

  test("semi apply") {
    // MATCH (x) WHERE (x) -[:r]-> (y) ....
    testSemiApply(SemiApply(_, _))
  }

  test("anti semi apply") {
    // MATCH (x) WHERE NOT (x) -[:r]-> (y) ....
    testSemiApply(AntiSemiApply(_, _))
  }

  def testSemiApply(
                     semiApplyBuilder: (LogicalPlan, LogicalPlan) =>
                       PlannerQuery with CardinalityEstimation => AbstractSemiApply
                   ): Unit = {
    val lhs = NodeByLabelScan(x, LABEL, Set.empty)(solved)
    val arg = Argument(Set(x))(solved)()
    val rhs = Expand(arg, x, SemanticDirection.INCOMING, Seq.empty, y, r, ExpandAll)(solved)
    val semiApply = semiApplyBuilder(lhs, rhs)(solved)
    semiApply.assignIds()
    val allocations = SlotAllocation.allocateSlots(semiApply)

    val lhsPipeline = PipelineInformation(Map(
      "x" -> LongSlot(0, nullable = false, CTNode, "x")),
      numberOfLongs = 1, numberOfReferences = 0)

    val argumentSide = lhsPipeline

    val rhsPipeline = PipelineInformation(Map(
      "x" -> LongSlot(0, nullable = false, CTNode, "x"),
      "y" -> LongSlot(2, nullable = false, CTNode, "y"),
      "r" -> LongSlot(1, nullable = false, CTRelationship, "r")
    ), numberOfLongs = 3, numberOfReferences = 0)

    allocations should have size 4
    allocations(semiApply.assignedId) should equal(lhsPipeline)
    allocations(lhs.assignedId) should equal(lhsPipeline)
    allocations(rhs.assignedId) should equal(rhsPipeline)
    allocations(arg.assignedId) should equal(argumentSide)
  }

  test("singlerow on two sides of Apply") {
    val sr1 = SingleRow()(solved)
    val sr2 = SingleRow()(solved)
    val pr1 = Projection(sr1, Map("x" -> literalInt(42)))(solved)
    val pr2 = Projection(sr2, Map("y" -> literalInt(666)))(solved)
    val apply = Apply(pr1, pr2)(solved)
    apply.assignIds()

    // when
    val allocations = SlotAllocation.allocateSlots(apply)

    // then
    allocations should have size 5
    val lhsPipeline = PipelineInformation(Map("x" -> RefSlot(0, nullable = true, CTAny, "x")), 0, 1)
    val rhsPipeline = PipelineInformation(Map("x" -> RefSlot(0, nullable = true, CTAny, "x"), "y" -> RefSlot(1, nullable = true, CTAny, "y")), 0, 2)
    allocations(sr1.assignedId) should equal(lhsPipeline)
    allocations(pr1.assignedId) should equal(lhsPipeline)
    allocations(sr2.assignedId) should equal(rhsPipeline)
    allocations(pr2.assignedId) should equal(rhsPipeline)
    allocations(apply.assignedId) should equal(rhsPipeline)
  }

  test("should allocate aggregation") {
    // Given MATCH (x)-[r:R]->(y) RETURN x, x.prop, count(r.prop)
    val labelScan = NodeByLabelScan(x, LABEL, Set.empty)(solved)
    val expand = Expand(labelScan, x, SemanticDirection.INCOMING, Seq.empty, y, r, ExpandAll)(solved)

    val grouping = Map(
      "x" -> varFor("x"),
      "x.prop" -> prop("x", "prop")
    )
    val aggregations = Map(
      "count(r.prop)" -> FunctionInvocation(FunctionName("count")(pos), prop("r", "prop"))(pos)
    )
    val aggregation = Aggregation(expand, grouping, aggregations)(solved)
    aggregation.assignIds()

    // when
    val allocations = SlotAllocation.allocateSlots(aggregation)

    allocations should have size 3
    allocations(expand.assignedId) should equal(
      PipelineInformation(Map(
        "x" -> LongSlot(0, nullable = false, CTNode, "x"),
        "r" -> LongSlot(1, nullable = false, CTRelationship, "r"),
        "y" -> LongSlot(2, nullable = false, CTNode, "y")
      ), numberOfLongs = 3, numberOfReferences = 0)
    )
    allocations(aggregation.assignedId) should equal(
      PipelineInformation(Map(
        "x" -> RefSlot(0, nullable = false, CTNode, "x"),
        "x.prop" -> RefSlot(1, nullable = true, CTAny, "x.prop"),
        "count(r.prop)" -> RefSlot(2, nullable = true, CTAny, "count(r.prop)")
      ), numberOfLongs = 0, numberOfReferences = 3)
    )
  }
}
