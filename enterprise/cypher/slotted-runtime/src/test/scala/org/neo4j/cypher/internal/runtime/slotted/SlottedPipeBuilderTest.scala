/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher.internal.runtime.slotted

import java.time.Clock

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.SlotAllocation.PhysicalPlan
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.SlotConfiguration.Size
import org.neo4j.cypher.internal.compatibility.v3_4.runtime._
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.Metrics
import org.neo4j.cypher.internal.compiler.v3_4.planner.{HardcodedGraphStatistics, LogicalPlanningTestSupport2}
import org.neo4j.cypher.internal.frontend.v3_4.phases.Monitors
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.ir.v3_4.VarPatternLength
import org.neo4j.cypher.internal.planner.v3_4.spi.{IDPPlannerName, PlanContext}
import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Literal, Property, Variable}
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.KeyToken
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.slotted.expressions.{NodeProperty, RelationshipProperty, SlottedExpressionConverters}
import org.neo4j.cypher.internal.runtime.slotted.pipes
import org.neo4j.cypher.internal.runtime.slotted.pipes._
import org.neo4j.cypher.internal.util.v3_4.symbols.{CTAny, CTList, CTNode, CTRelationship}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.v3_4.{LabelId, PropertyKeyId}
import org.neo4j.cypher.internal.v3_4.expressions._
import org.neo4j.cypher.internal.v3_4.logical.plans
import org.neo4j.cypher.internal.v3_4.logical.plans._

//noinspection NameBooleanParameters
class SlottedPipeBuilderTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  implicit private val table = SemanticTable()

  private def build(beforeRewrite: LogicalPlan): Pipe = {
    val planContext = mock[PlanContext]
    when(planContext.statistics).thenReturn(HardcodedGraphStatistics)
    when(planContext.getOptPropertyKeyId("propertyKey")).thenReturn(Some(0))
    val physicalPlan: PhysicalPlan = SlotAllocation.allocateSlots(beforeRewrite, table)
    val slottedRewriter = new SlottedRewriter(planContext)
    val logicalPlan = slottedRewriter(beforeRewrite, physicalPlan.slotConfigurations)
    val converters = new ExpressionConverters(CommunityExpressionConverter, SlottedExpressionConverters)
    val executionPlanBuilder = new PipeExecutionPlanBuilder( mock[Clock], mock[Monitors],
      expressionConverters = converters, pipeBuilderFactory = SlottedPipeBuilder.Factory(physicalPlan))
    val context = PipeExecutionBuilderContext(mock[Metrics.CardinalityModel], table, IDPPlannerName, new StubReadOnlies, new StubCardinalities)
    executionPlanBuilder.build(None, logicalPlan)(context, planContext).pipe
  }

  private val x = "x"
  private val z = "z"
  private val r = "r"
  private val r2 = "r2"
  private val LABEL = LabelName("label1")(pos)
  private val X_NODE_SLOTS = SlotConfiguration.empty.newLong("x", false, CTNode)

  test("only single allnodes scan") {
    // given
    val plan: AllNodesScan = AllNodesScan(x, Set.empty)

    // when
    val pipe = build(plan)

    // then
    pipe should equal(
      AllNodesScanSlottedPipe("x", X_NODE_SLOTS, Size.zero)()
    )
  }

  test("single all nodes scan with limit") {
    // given
    val plan = plans.Limit(AllNodesScan(x, Set.empty), literalInt(1), DoNotIncludeTies)

    // when
    val pipe = build(plan)

    // then
    pipe should equal(
      LimitPipe(
        AllNodesScanSlottedPipe("x", X_NODE_SLOTS, Size.zero)(),
        Literal(1)
      )()
    )
  }

  test("eagerize before create node") {
    // given
    val label = LabelName("label")(pos)
    val allNodeScan: AllNodesScan = AllNodesScan(x, Set.empty)
    val eager = Eager(allNodeScan)
    val createNode = CreateNode(eager, z, Seq(label), None)

    // when
    val pipe = build(createNode)

    // then
    val beforeEagerSlots = SlotConfiguration.empty
      .newLong("x", false, CTNode)

    val afterEagerSlots = SlotConfiguration.empty
      .newLong("x", false, CTNode)
      .newLong("z", false, CTNode)

    pipe should equal(
      CreateNodeSlottedPipe(
        EagerSlottedPipe(
          AllNodesScanSlottedPipe("x", beforeEagerSlots, Size.zero)(),
          afterEagerSlots)(),
        "z", afterEagerSlots, Seq(LazyLabel(label)), None)()
    )
  }

  test("create node") {
    // given
    val label = LabelName("label")(pos)
    val argument = Argument()
    val createNode = CreateNode(argument, z, Seq(label), None)

    // when
    val pipe = build(createNode)

    // then
    pipe should equal(
      CreateNodeSlottedPipe(
        ArgumentSlottedPipe(SlotConfiguration.empty.newLong("z", false, CTNode), Size.zero)(),
        "z",
        SlotConfiguration(Map("z" -> LongSlot(0, nullable = false, CTNode)), 1, 0),
        Seq(LazyLabel(label)),
        None
      )()
    )
  }

  test("single label scan") {
    // given
    val label = LabelName("label")(pos)
    val plan = NodeByLabelScan(x, label, Set.empty)

    // when
    val pipe = build(plan)

    // then
    pipe should equal(
      NodesByLabelScanSlottedPipe("x", LazyLabel(label), X_NODE_SLOTS, Size.zero)()
    )
  }

  test("label scan with filtering") {
    // given
    val label = LabelName("label")(pos)
    val leaf = NodeByLabelScan(x, label, Set.empty)
    val filter = Selection(Seq(True()(pos)), leaf)

    // when
    val pipe = build(filter)

    // then
    pipe should equal(
      FilterPipe(
        NodesByLabelScanSlottedPipe("x", LazyLabel(label), X_NODE_SLOTS, Size.zero)(),
        predicates.True()
      )()
    )
    pipe.asInstanceOf[FilterPipe].predicate.owningPipe should equal(pipe)
  }

  test("single node with expand") {
    // given
    val allNodesScan = AllNodesScan(x, Set.empty)
    val expand = Expand(allNodesScan, x, SemanticDirection.INCOMING, Seq.empty, z, r, ExpandAll)

    // when
    val pipe = build(expand)

    // then
    val xNodeSlot = LongSlot(0, nullable = false, CTNode)
    val rRelSlot = LongSlot(1, nullable = false, CTRelationship)
    val zNodeSlot = LongSlot(2, nullable = false, CTNode)
    pipe should equal(ExpandAllSlottedPipe(
      AllNodesScanSlottedPipe("x", X_NODE_SLOTS, Size.zero)(),
      xNodeSlot, rRelSlot.offset, zNodeSlot.offset,
      SemanticDirection.INCOMING,
      LazyTypes.empty,
      SlotConfiguration(Map(
        "x" -> xNodeSlot,
        "r" -> rRelSlot,
        "z" -> zNodeSlot), numberOfLongs = 3, numberOfReferences = 0)
    )())
  }

  test("single node with expand into") {
    // given
    val allNodesScan = AllNodesScan(x, Set.empty)
    val expand = Expand(allNodesScan, x, SemanticDirection.INCOMING, Seq.empty, x, r, ExpandInto)

    // when
    val pipe = build(expand)

    // then
    val nodeSlot = LongSlot(0, nullable = false, CTNode)
    val relSlot = LongSlot(1, nullable = false, CTRelationship)
    pipe should equal(ExpandIntoSlottedPipe(
      AllNodesScanSlottedPipe("x", X_NODE_SLOTS, Size.zero)(),
      nodeSlot, relSlot.offset, nodeSlot, SemanticDirection.INCOMING, LazyTypes.empty,
      SlotConfiguration(Map("x" -> nodeSlot, "r" -> relSlot), numberOfLongs = 2, numberOfReferences = 0)
    )())
  }

  test("single optional node with expand") {
    // given
    val allNodesScan = AllNodesScan("x", Set.empty)
    val optional = Optional(allNodesScan)
    val expand = Expand(optional, "x", SemanticDirection.INCOMING, Seq.empty, "z", "r", ExpandAll)

    // when
    val pipe = build(expand)

    // then
    val xNodeSlot = LongSlot(0, nullable = true, CTNode)
    val rRelSlot = LongSlot(1, nullable = false, CTRelationship)
    val zNodeSlot = LongSlot(2, nullable = false, CTNode)
    val allNodeScanSlots = SlotConfiguration(Map("x" -> xNodeSlot), numberOfLongs = 1, numberOfReferences = 0)
    val expandSlots = SlotConfiguration(Map(
      "x" -> xNodeSlot,
      "r" -> rRelSlot,
      "z" -> zNodeSlot), numberOfLongs = 3, numberOfReferences = 0)

    pipe should equal(
      ExpandAllSlottedPipe(
        OptionalSlottedPipe(
          AllNodesScanSlottedPipe("x", allNodeScanSlots, Size.zero)(),
          Array(xNodeSlot),
          allNodeScanSlots,
          Size.zero
        )(),
        xNodeSlot, rRelSlot.offset, zNodeSlot.offset,
        SemanticDirection.INCOMING,
        LazyTypes.empty,
        expandSlots
      )())
  }

  test("single optional node with expand into") {
    // given
    val allNodesScan = AllNodesScan("x", Set.empty)
    val optional = Optional(allNodesScan)
    val expand = Expand(optional, "x", SemanticDirection.INCOMING, Seq.empty, "x", "r", ExpandInto)

    // when
    val pipe = build(expand)

    // then
    val nodeSlot = LongSlot(0, nullable = true, CTNode)
    val relSlot = LongSlot(1, nullable = false, CTRelationship)
    val allNodeScanSlots = SlotConfiguration(Map("x" -> nodeSlot), numberOfLongs = 1, numberOfReferences = 0)
    val expandSlots = SlotConfiguration(Map("x" -> nodeSlot, "r" -> relSlot), numberOfLongs = 2, numberOfReferences = 0)

    pipe should equal(
      ExpandIntoSlottedPipe(
        OptionalSlottedPipe(
          AllNodesScanSlottedPipe("x", allNodeScanSlots, Size.zero)(),
          Array(nodeSlot),
          allNodeScanSlots,
          Size.zero
        )(),
        nodeSlot, relSlot.offset, nodeSlot, SemanticDirection.INCOMING, LazyTypes.empty,
        expandSlots)()
    )
  }

  test("optional node") {
    // given
    val leaf = AllNodesScan(x, Set.empty)
    val plan = Optional(leaf)

    // when
    val pipe = build(plan)
    val nodeSlot = LongSlot(0, nullable = true, CTNode)

    // then
    val expectedSlots = SlotConfiguration(Map("x" -> nodeSlot), numberOfLongs = 1, numberOfReferences = 0)
    pipe should equal(OptionalSlottedPipe(
      AllNodesScanSlottedPipe("x", expectedSlots, Size.zero)(),
      Array(nodeSlot),
      expectedSlots,
      Size.zero
    )())
  }

  test("optional refslot value") {
    // given
    val arg = Argument(Set.empty)
    val project = Projection(arg, Map("x" -> literalInt(1)))
    val plan = Optional(project)

    // when
    val pipe = build(plan)
    val refSlot = RefSlot(0, nullable = true, CTAny)

    // then
    val expectedSlots = SlotConfiguration(Map("x" -> refSlot), numberOfLongs = 0, numberOfReferences = 1)
    pipe should equal(OptionalSlottedPipe(
      ProjectionSlottedPipe(
        ArgumentSlottedPipe(expectedSlots, Size.zero)(),
        Map(0 -> Literal(1))
      )(),
      Array(refSlot),
      expectedSlots,
      Size.zero
    )())
  }

  test("single node with optionalExpand ExpandAll") {
    // given
    val allNodesScan = AllNodesScan(x, Set.empty)
    val expand = OptionalExpand(allNodesScan, x, SemanticDirection.INCOMING, Seq.empty, z, r, ExpandAll)

    // when
    val pipe = build(expand)

    // then
    pipe should equal(OptionalExpandAllSlottedPipe(
      AllNodesScanSlottedPipe("x", X_NODE_SLOTS, Size.zero)(),
      X_NODE_SLOTS("x"), 1, 2, SemanticDirection.INCOMING, LazyTypes.empty, predicates.True(),
      SlotConfiguration(Map(
        "x" -> LongSlot(0, nullable = false, CTNode),
        "r" -> LongSlot(1, nullable = true, CTRelationship),
        "z" -> LongSlot(2, nullable = true, CTNode)), numberOfLongs = 3, numberOfReferences = 0)
    )())
    pipe.asInstanceOf[OptionalExpandAllSlottedPipe].predicate.owningPipe should equal(pipe)
  }

  test("single node with optionalExpand ExpandInto") {
    // given
    val allNodesScan = AllNodesScan(x, Set.empty)
    val expand = OptionalExpand(allNodesScan, x, SemanticDirection.INCOMING, Seq.empty, x, r, ExpandInto)

    // when
    val pipe = build(expand)

    // then
    pipe should equal(OptionalExpandIntoSlottedPipe(
      AllNodesScanSlottedPipe("x", X_NODE_SLOTS, Size.zero)(),
      X_NODE_SLOTS("x"), 1, X_NODE_SLOTS("x"), SemanticDirection.INCOMING, LazyTypes.empty, predicates.True(),
      SlotConfiguration(Map(
        "x" -> LongSlot(0, nullable = false, CTNode),
        "r" -> LongSlot(1, nullable = true, CTRelationship)), numberOfLongs = 2, numberOfReferences = 0)
    )())
    pipe.asInstanceOf[OptionalExpandIntoSlottedPipe].predicate.owningPipe should equal(pipe)
  }

  test("single node with varlength expand") {
    // given
    val allNodesScan = AllNodesScan(x, Set.empty)
    val varLength = VarPatternLength(1, Some(15))
    val tempNode = "r_NODES"
    val tempEdge = "r_EDGES"
    val expand = VarExpand(allNodesScan, x, SemanticDirection.INCOMING, SemanticDirection.INCOMING, Seq.empty,
      z, r, varLength, ExpandAll, tempNode, tempEdge, True()(pos), True()(pos), Seq())

    // when
    val pipe = build(expand)

    // then
    val xNodeSlot = LongSlot(0, nullable = false, CTNode)
    val tempNodeSlot = LongSlot(1, nullable = false, CTNode)
    val tempRelSlot = LongSlot(2, nullable = false, CTRelationship)
    val zNodeSlot = LongSlot(1, nullable = false, CTNode)
    val rRelSlot = RefSlot(0, nullable = false, CTList(CTRelationship))

    val allNodeScanSlots = SlotConfiguration(Map(
      "x" -> xNodeSlot,
      "r_NODES" -> tempNodeSlot,
      "r_EDGES" -> tempRelSlot), numberOfLongs = 3, numberOfReferences = 0)

    val varExpandSlots = SlotConfiguration(Map(
      "x" -> xNodeSlot,
      "r" -> rRelSlot,
      "z" -> zNodeSlot), numberOfLongs = 2, numberOfReferences = 1)

    pipe should equal(VarLengthExpandSlottedPipe(
      AllNodesScanSlottedPipe("x", allNodeScanSlots, Size.zero)(),
      xNodeSlot, rRelSlot.offset, zNodeSlot,
      SemanticDirection.INCOMING, SemanticDirection.INCOMING,
      LazyTypes.empty, varLength.min, varLength.max, shouldExpandAll = true,
      varExpandSlots,
      tempNodeSlot.offset, tempRelSlot.offset,
      commands.predicates.True(), commands.predicates.True(), Size(1, 0)
    )())
    pipe.asInstanceOf[VarLengthExpandSlottedPipe].nodePredicate.owningPipe should equal(pipe)
    pipe.asInstanceOf[VarLengthExpandSlottedPipe].edgePredicate.owningPipe should equal(pipe)
  }

  test("single node with varlength expand into") {
    // given
    val allNodesScan = AllNodesScan(x, Set.empty)
    val expand = Expand(allNodesScan, x, SemanticDirection.OUTGOING, Seq.empty, z, r, ExpandAll)
    val varLength = VarPatternLength(1, Some(15))
    val tempNode = "r_NODES"
    val tempEdge = "r_EDGES"
    val varExpand = VarExpand(expand, x, SemanticDirection.INCOMING, SemanticDirection.INCOMING, Seq.empty,
      z, r2, varLength, ExpandInto, tempNode, tempEdge, True()(pos), True()(pos), Seq())

    // when
    val pipe = build(varExpand)

    // then
    val xNodeSlot = LongSlot(0, nullable = false, CTNode)
    val rRelSlot = LongSlot(1, nullable = false, CTRelationship)
    val zNodeSlot = LongSlot(2, nullable = false, CTNode)
    val tempNodeSlot = LongSlot(3, nullable = false, CTNode)
    val tempRelSlot = LongSlot(4, nullable = false, CTRelationship)
    val r2RelSlot = RefSlot(0, nullable = false, CTList(CTRelationship))

    val allNodeScanSlots =
      SlotConfiguration(Map(
        "x" -> LongSlot(0, nullable = false, CTNode)),
        numberOfLongs = 1, numberOfReferences = 0)

    val expandSlots =
      SlotConfiguration(Map(
        "x" -> LongSlot(0, nullable = false, CTNode),
        "r" -> LongSlot(1, nullable = false, CTRelationship),
        "z" -> LongSlot(2, nullable = false, CTNode),
        "r_NODES" -> LongSlot(3, nullable = false, CTNode),
        "r_EDGES" -> LongSlot(4, nullable = false, CTRelationship)),
        numberOfLongs = 5, numberOfReferences = 0)

    val varExpandSlots =
      SlotConfiguration(Map(
      "x" -> LongSlot(0, nullable = false, CTNode),
      "r" -> LongSlot(1, nullable = false, CTRelationship),
      "z" -> LongSlot(2, nullable = false, CTNode),
      "r2" -> RefSlot(0, nullable = false, CTList(CTRelationship))),
      numberOfLongs = 3, numberOfReferences = 1)

    pipe should equal(
      VarLengthExpandSlottedPipe(
        ExpandAllSlottedPipe(
          AllNodesScanSlottedPipe("x", allNodeScanSlots, Size.zero)(),
          xNodeSlot, rRelSlot.offset, zNodeSlot.offset,
          SemanticDirection.OUTGOING,
          LazyTypes.empty,
          expandSlots)(),
        xNodeSlot, r2RelSlot.offset, zNodeSlot,
        SemanticDirection.INCOMING, SemanticDirection.INCOMING,
        LazyTypes.empty, varLength.min, varLength.max, shouldExpandAll = false,
        varExpandSlots,
        tempNodeSlot.offset, tempRelSlot.offset,
        commands.predicates.True(), commands.predicates.True(), Size(3, 0))()
    )
    pipe.asInstanceOf[VarLengthExpandSlottedPipe].nodePredicate.owningPipe should equal(pipe)
    pipe.asInstanceOf[VarLengthExpandSlottedPipe].edgePredicate.owningPipe should equal(pipe)
  }

  test("single node with varlength expand and a predicate") {
    // given
    val allNodesScan = AllNodesScan(x, Set.empty)
    val varLength = VarPatternLength(1, Some(15))
    val tempNode = "r_NODES"
    val tempEdge = "r_EDGES"
    val nodePredicate = Equals(prop(tempNode, "propertyKey"), literalInt(4))(pos)
    val edgePredicate = Not(LessThan(prop(tempEdge, "propertyKey"), literalInt(4))(pos))(pos)

    val expand = VarExpand(allNodesScan, x, SemanticDirection.INCOMING, SemanticDirection.INCOMING, Seq.empty,
      z, r, varLength, ExpandAll, tempNode, tempEdge, nodePredicate, edgePredicate, Seq())

    // when
    val pipe = build(expand)

    // then
    val xNodeSlot = LongSlot(0, nullable = false, CTNode)
    val tempNodeSlot = LongSlot(1, nullable = false, CTNode)
    val tempRelSlot = LongSlot(2, nullable = false, CTRelationship)
    val zNodeSlot = LongSlot(1, nullable = false, CTNode)
    val rRelSlot = RefSlot(0, nullable = false, CTList(CTRelationship))

    val allNodeScanSlots = SlotConfiguration(Map(
      "x" -> xNodeSlot,
      "r_NODES" -> tempNodeSlot,
      "r_EDGES" -> tempRelSlot), numberOfLongs = 3, numberOfReferences = 0)

    val varExpandSlots = SlotConfiguration(Map(
      "x" -> xNodeSlot,
      "r" -> rRelSlot,
      "z" -> zNodeSlot), numberOfLongs = 2, numberOfReferences = 1)

    pipe should equal(VarLengthExpandSlottedPipe(
      AllNodesScanSlottedPipe("x", allNodeScanSlots, Size.zero)(),
      xNodeSlot, rRelSlot.offset, zNodeSlot,
      SemanticDirection.INCOMING, SemanticDirection.INCOMING,
      LazyTypes.empty, varLength.min, varLength.max, shouldExpandAll = true,
      varExpandSlots,
      tempNodeSlot.offset, tempRelSlot.offset,
      commands.predicates.Equals(NodeProperty(1, 0), Literal(4)),
      commands.predicates.Not(
        commands.predicates.LessThan(RelationshipProperty(2, 0), Literal(4))
      ),
      Size(1, 0)
    )())
    pipe.asInstanceOf[VarLengthExpandSlottedPipe].nodePredicate.owningPipe should equal(pipe)
    pipe.asInstanceOf[VarLengthExpandSlottedPipe].edgePredicate.owningPipe should equal(pipe)
  }

  test("let's skip this one") {
    // given
    val allNodesScan = AllNodesScan(x, Set.empty)
    val skip = plans.Skip(allNodesScan, literalInt(42))

    // when
    val pipe = build(skip)

    // then
    pipe should equal(SkipPipe(
      AllNodesScanSlottedPipe("x", X_NODE_SLOTS, Size.zero)(),
      commands.expressions.Literal(42)
    )())
  }

  test("all we need is to apply ourselves") {
    // given
    val lhs = NodeByLabelScan(x, LabelName("label")(pos), Set.empty)
    val label = LabelToken("label2", LabelId(0))
    val seekExpression = SingleQueryExpression(literalInt(42))
    val rhs = NodeIndexSeek(z, label, Seq.empty, seekExpression, Set(x))
    val apply = Apply(lhs, rhs)

    // when
    val pipe = build(apply)

    // then
    pipe should equal(ApplySlottedPipe(
      NodesByLabelScanSlottedPipe("x", LazyLabel("label"), X_NODE_SLOTS, Size.zero)(),
      NodeIndexSeekSlottedPipe("z", label, Seq.empty, SingleQueryExpression(commands.expressions.Literal(42)), IndexSeek,
        SlotConfiguration.empty
          .newLong("x", false, CTNode)
          .newLong("z", false, CTNode),
        Size(1, 0))()
    )())
  }

  ignore("aggregation used for distinct") {
    // given
    val leaf = NodeByLabelScan(x, LabelName("label")(pos), Set.empty)
    val distinct = Aggregation(leaf, Map("x" -> varFor("x")), Map.empty)

    // when
    val pipe = build(distinct)

    // then
    pipe should equal(DistinctPipe(
      NodesByLabelScanSlottedPipe("x", LazyLabel("label"), X_NODE_SLOTS, Size.zero)(),
      Map("x" -> commands.expressions.Variable("x"))
    )())
  }

  ignore("optional travels through aggregation used for distinct") {
    // given OPTIONAL MATCH (x) RETURN DISTINCT x, x.propertyKey
    val leaf = NodeByLabelScan(x, LabelName("label")(pos), Set.empty)
    val optional = Optional(leaf)
    val distinct = Aggregation(optional,
      groupingExpressions = Map("x" -> varFor("x"), "x.propertyKey" -> prop("x", "propertyKey")),
      aggregationExpression = Map.empty)

    // when
    val pipe = build(distinct)

    // then
    val labelScan = NodesByLabelScanSlottedPipe("x", LazyLabel("label"), X_NODE_SLOTS, Size.zero)()
    val optionalPipe = OptionalSlottedPipe(labelScan, Array(X_NODE_SLOTS("x")), X_NODE_SLOTS, Size.zero)()
    pipe should equal(DistinctPipe(
      optionalPipe,
      Map("x" -> Variable("x"), "x.propertyKey" -> Property(Variable("x"), KeyToken.Resolved("propertyKey", 0, PropertyKey)))
    )())
  }

  ignore("optional travels through aggregation") {
    // given OPTIONAL MATCH (x) RETURN x, x.propertyKey, count(*)
    val leaf = NodeByLabelScan(x, LabelName("label")(pos), Set.empty)
    val optional = Optional(leaf)
    val distinct = Aggregation(optional,
      groupingExpressions = Map("x" -> varFor("x"), "x.propertyKey" -> prop("x", "propertyKey")),
      aggregationExpression = Map("count" -> CountStar()(pos)))

    // when
    val pipe = build(distinct)

    // then
    val nodeByLabelScan = NodesByLabelScanSlottedPipe("x", LazyLabel("label"), X_NODE_SLOTS, Size.zero)()
    val grouping = Map(
      "x" -> Variable("x"),
      "x.propertyKey" -> Property(Variable("x"), KeyToken.Resolved("propertyKey", 0, PropertyKey))
    )
    pipe should equal(EagerAggregationPipe(
      OptionalSlottedPipe(nodeByLabelScan, Array(X_NODE_SLOTS("x")), X_NODE_SLOTS, Size.zero)(),
      keyExpressions = grouping,
      aggregations = Map("count" -> commands.expressions.CountStar())
    )())
  }

  test("labelscan with projection") {
    // given
    val leaf = NodeByLabelScan(x, LabelName("label")(pos), Set.empty)
    val projection = Projection(leaf, Map("x" -> varFor("x"), "x.propertyKey" -> prop("x", "propertyKey")))

    // when
    val pipe = build(projection)

    // then
    val slots = SlotConfiguration.empty
      .newLong("x", false, CTNode)
      .newReference("x.propertyKey", true, CTAny)

    pipe should equal(ProjectionSlottedPipe(
      NodesByLabelScanSlottedPipe("x", LazyLabel("label"), slots, Size.zero)(),
      Map(0 -> NodeProperty(slots("x.propertyKey").offset, 0))
    )())
  }

  test("labelscan with projection and alias") {
    // given
    val leaf = NodeByLabelScan(x, LabelName("label")(pos), Set.empty)
    val projection = Projection(leaf, Map("A" -> varFor("x"), "x.propertyKey" -> prop("x", "propertyKey")))

    // when
    val pipe = build(projection)

    // then
    val slots = SlotConfiguration.empty
      .newLong("x", false, CTNode)
      .addAlias("A", "x")
      .newReference("x.propertyKey", true, CTAny)

    pipe should equal(ProjectionSlottedPipe(
      NodesByLabelScanSlottedPipe("x", LazyLabel("label"), slots, Size.zero)(),
      Map(0 -> NodeProperty(slots("x.propertyKey").offset, 0))
    )())
  }

  test("cartesian product") {
    // given
    val lhs = NodeByLabelScan(x, LABEL, Set.empty)
    val rhs = NodeByLabelScan("y", LabelName("label2")(pos), Set.empty)
    val Xproduct = CartesianProduct(lhs, rhs)

    // when
    val pipe = build(Xproduct)

    val lhsSlots = SlotConfiguration.empty.newLong("x", nullable = false, CTNode)
    val rhsSlots = SlotConfiguration.empty.newLong("y", nullable = false, CTNode)
    val xProdSlots = SlotConfiguration.empty
      .newLong("x", nullable = false, CTNode)
      .newLong("y", nullable = false, CTNode)

    // then
    pipe should equal(CartesianProductSlottedPipe(
      NodesByLabelScanSlottedPipe("x", LazyLabel("label1"), lhsSlots, Size.zero)(),
      NodesByLabelScanSlottedPipe("y", LazyLabel("label2"), rhsSlots, Size.zero)(),
      lhsLongCount = 1, lhsRefCount = 0, xProdSlots, argumentSize = Size.zero)()
    )
  }

  test("foreach") {
    // given
    val lhs = NodeByLabelScan(x, LABEL, Set.empty)
    val rhs = NodeByLabelScan("y", LabelName("label2")(pos), Set.empty) // This would be meaningless as a real-world example
    val foreach = ForeachApply(lhs, rhs, "z", literalIntList(1, 2))

    // when
    val pipe = build(foreach)

    // then
    val lhsSlots = SlotConfiguration.empty
      .newLong("x", nullable = false, CTNode)
      .newReference("z", nullable = true, CTAny)

    val rhsSlots = SlotConfiguration.empty
      .newLong("x", nullable = false, CTNode)
      .newReference("z", nullable = true, CTAny)
      .newLong("y", nullable = false, CTNode)

    val foreachSlots = lhsSlots

    pipe should equal(ForeachSlottedPipe(
      NodesByLabelScanSlottedPipe("x", LazyLabel("label1"), lhsSlots, Size.zero)(),
      NodesByLabelScanSlottedPipe("y", LazyLabel("label2"), rhsSlots, Size(nLongs = 1, nReferences = 1))(),
      lhsSlots("z"),
      commands.expressions.ListLiteral(commands.expressions.Literal(1), commands.expressions.Literal(2)))()
    )
    pipe.asInstanceOf[ForeachSlottedPipe].expression.owningPipe should equal(pipe)
  }

  test("that argument does not apply here") {
    // given MATCH (x) MATCH (x)<-[r]-(y)
    val lhs = NodeByLabelScan(x, LABEL, Set.empty)
    val arg = Argument(Set(x))
    val rhs = Expand(arg, x, SemanticDirection.INCOMING, Seq.empty, z, r, ExpandAll)

    val apply = Apply(lhs, rhs)

    // when
    val pipe = build(apply)

    // then
    val lhsSlots = SlotConfiguration.empty.newLong("x", false, CTNode)

    val rhsSlots = SlotConfiguration.empty
      .newLong("x", false, CTNode)
      .newLong("r", false, CTRelationship)
      .newLong("z", false, CTNode)

    pipe should equal(ApplySlottedPipe(
      NodesByLabelScanSlottedPipe("x", LazyLabel(LABEL), lhsSlots, Size.zero)(),
      ExpandAllSlottedPipe(
        ArgumentSlottedPipe(lhsSlots, Size(1, 0))(),
        rhsSlots("x"), 1, 2, SemanticDirection.INCOMING, LazyTypes.empty, rhsSlots
      )()
    )())
  }

  test("NodeIndexScan should yield a NodeIndexScanSlottedPipe") {
    // given
    val leaf = NodeIndexScan(
      "n",
      LabelToken("Awesome", LabelId(0)),
      PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)),
      Set.empty)

    // when
    val pipe = build(leaf)

    // then

    pipe should equal(
      NodeIndexScanSlottedPipe(
        "n",
        LabelToken("Awesome", LabelId(0)),
        PropertyKeyToken("prop", PropertyKeyId(0)),
        SlotConfiguration.empty.newLong("n", false, CTNode), Size.zero)())
  }

  test("Should use NodeIndexUniqueSeek") {
    // given
    val label = LabelToken("label2", LabelId(0))
    val seekExpression = SingleQueryExpression(literalInt(42))
    val seek = NodeUniqueIndexSeek(z, label, Seq.empty, seekExpression, Set(x))

    // when
    val pipe = build(seek)

    // then
    pipe should equal(
      NodeIndexSeekSlottedPipe("z", label, Seq.empty, SingleQueryExpression(commands.expressions.Literal(42)), UniqueIndexSeek,
        SlotConfiguration.empty.newLong("z", false, CTNode), Size.zero)()
    )
  }

  test("unwind and sort") {
    // given UNWIND [1,2,3] as x RETURN x ORDER BY x
    val xVar = varFor("x")
    val xVarName = xVar.name
    val leaf = Argument()
    val unwind = UnwindCollection(leaf, xVarName, listOf(literalInt(1), literalInt(2), literalInt(3)))
    val sort = Sort(unwind, List(plans.Ascending(xVarName)))

    // when
    val pipe = build(sort)

    // then
    val expectedSlots1 = SlotConfiguration.empty
    val xSlot = RefSlot(0, nullable = true, CTAny)
    val expectedSlots2 =
      SlotConfiguration(numberOfLongs = 0, numberOfReferences = 1, slots = Map("x" -> xSlot))

    pipe should equal(
      SortSlottedPipe(orderBy = Seq(pipes.Ascending(xSlot)), slots = expectedSlots2,
        source = UnwindSlottedPipe(collection = commands.expressions.ListLiteral(commands.expressions.Literal(1),
          commands.expressions.Literal(2), commands.expressions.Literal(3)), offset = 0, slots = expectedSlots2,
          source = ArgumentSlottedPipe(expectedSlots1, Size.zero)()
        )()
      )()
    )
  }

}
