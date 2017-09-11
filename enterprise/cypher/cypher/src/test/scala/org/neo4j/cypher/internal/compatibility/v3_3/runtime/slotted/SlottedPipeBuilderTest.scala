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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted

import org.mockito.Mockito._
import org.neo4j.cypher.internal.BuildEnterpriseInterpretedExecutionPlan.EnterprisePipeBuilderFactory
import org.neo4j.cypher.internal.compatibility.v3_3.runtime._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.{Literal, Property, Variable}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.predicates
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.values.KeyToken
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.expressions._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.pipes._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.{pipes => slottedPipes}
import org.neo4j.cypher.internal.compiled_runtime.v3_3.codegen.CompiledRuntimeContextHelper
import org.neo4j.cypher.internal.compiler.v3_3.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v3_3.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v3_3.{HardcodedGraphStatistics, IDPPlannerName}
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.symbols.{CTAny, CTList, CTNode, CTRelationship}
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_3.{LabelId, PropertyKeyId, SemanticDirection, SemanticTable, ast}
import org.neo4j.cypher.internal.ir.v3_3.{IdName, VarPatternLength}
import org.neo4j.cypher.internal.v3_3.logical.plans
import org.neo4j.cypher.internal.v3_3.logical.plans._

class SlottedPipeBuilderTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  implicit private val table = SemanticTable()

  private def build(beforeRewrite: LogicalPlan): Pipe = {
    beforeRewrite.assignIds()
    val planContext = mock[PlanContext]
    when(planContext.statistics).thenReturn(HardcodedGraphStatistics)
    when(planContext.getOptPropertyKeyId("propertyKey")).thenReturn(Some(0))
    val context: EnterpriseRuntimeContext = CompiledRuntimeContextHelper.create(planContext = planContext)
    val pipelines: Map[LogicalPlanId, PipelineInformation] = SlotAllocation.allocateSlots(beforeRewrite)
    val slottedRewriter = new SlottedRewriter(context.planContext)
    val logicalPlan = slottedRewriter(beforeRewrite, pipelines)
    val converters = new ExpressionConverters(CommunityExpressionConverter, SlottedExpressionConverters)
    val executionPlanBuilder = new PipeExecutionPlanBuilder(context.clock, context.monitors,
      expressionConverters = converters, pipeBuilderFactory = EnterprisePipeBuilderFactory(pipelines))
    val pipeBuildContext = PipeExecutionBuilderContext(context.metrics.cardinality, table, IDPPlannerName)
    executionPlanBuilder.build(None, logicalPlan)(pipeBuildContext, context.planContext).pipe
  }

  private val x = IdName("x")
  private val z = IdName("z")
  private val r = IdName("r")
  private val LABEL = LabelName("label1")(pos)

  test("only single allnodes scan") {
    // given
    val plan: AllNodesScan = AllNodesScan(x, Set.empty)(solved)

    // when
    val pipe = build(plan)

    // then
    pipe should equal(
      AllNodesScanSlottedPipe("x", PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode, "x")), 1, 0))()
    )
  }

  test("single allnodes scan with limit") {
    // given
    val plan = plans.Limit(AllNodesScan(x, Set.empty)(solved), literalInt(1), DoNotIncludeTies)(solved)

    // when
    val pipe = build(plan)

    // then
    pipe should equal(
      LimitPipe(
        AllNodesScanSlottedPipe("x", PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode, "x")), 1, 0))(),
        Literal(1)
      )()
    )
  }

  test("eagerize before create node") {
    // given
    val label = LabelName("label")(pos)
    val allNodeScan: AllNodesScan = AllNodesScan(x, Set.empty)(solved)
    val eager = Eager(allNodeScan)(solved)
    val createNode = CreateNode(eager, z, Seq(label), None)(solved)

    // when
    val pipe = build(createNode)

    // then
    val beforeEagerPipelineInformation = PipelineInformation.empty
      .newLong("x", false, CTNode)

    val afterEagerPipelineInformation = PipelineInformation.empty
      .newLong("x", false, CTNode)
      .newLong("z", false, CTNode)

    pipe should equal(
      CreateNodeSlottedPipe(
        EagerSlottedPipe(
          AllNodesScanSlottedPipe("x", beforeEagerPipelineInformation)(),
          afterEagerPipelineInformation)(),
        "z", afterEagerPipelineInformation, Seq(LazyLabel(label)), None)()
    )
  }

  test("create node") {
    // given
    val label = LabelName("label")(pos)
    val singleRow = SingleRow()(solved)
    val createNode = CreateNode(singleRow, z, Seq(label), None)(solved)

    // when
    val pipe = build(createNode)

    // then
    pipe should equal(
      CreateNodeSlottedPipe(
        SingleRowSlottedPipe(PipelineInformation(Map("z" -> LongSlot(0, nullable = false, CTNode, "z")), 1, 0))(),
        "z",
        PipelineInformation(Map("z" -> LongSlot(0, nullable = false, CTNode, "z")), 1, 0),
        Seq(LazyLabel(label)),
        None
      )()
    )
  }

  test("single labelscan scan") {
    // given
    val label = LabelName("label")(pos)
    val plan = NodeByLabelScan(x, label, Set.empty)(solved)

    // when
    val pipe = build(plan)

    // then
    pipe should equal(
      NodesByLabelScanSlottedPipe("x", LazyLabel(label), PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode, "x")), 1, 0))()
    )
  }

  test("labelscan with filtering") {
    // given
    val label = LabelName("label")(pos)
    val leaf = NodeByLabelScan(x, label, Set.empty)(solved)
    val filter = Selection(Seq(ast.True()(pos)), leaf)(solved)

    // when
    val pipe = build(filter)

    // then
    pipe should equal(
      FilterPipe(
        NodesByLabelScanSlottedPipe("x", LazyLabel(label), PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode, "x")), 1, 0))(),
        predicates.True()
      )()
    )
  }

  test("single node with expand") {
    // given
    val allNodesScan = AllNodesScan(x, Set.empty)(solved)
    val expand = Expand(allNodesScan, x, SemanticDirection.INCOMING, Seq.empty, z, r, ExpandAll)(solved)

    // when
    val pipe = build(expand)

    // then
    val xNodeSlot = LongSlot(0, nullable = false, CTNode, "x")
    val rRelSlot = LongSlot(1, nullable = false, CTRelationship, "r")
    val zNodeSlot = LongSlot(2, nullable = false, CTNode, "z")
    pipe should equal(ExpandAllSlottedPipe(
      AllNodesScanSlottedPipe("x", PipelineInformation(Map("x" -> xNodeSlot), numberOfLongs = 1, numberOfReferences = 0))(),
      xNodeSlot.offset, rRelSlot.offset, zNodeSlot.offset,
      SemanticDirection.INCOMING,
      LazyTypes.empty,
      PipelineInformation(Map(
        "x" -> xNodeSlot,
        "r" -> rRelSlot,
        "z" -> zNodeSlot), numberOfLongs = 3, numberOfReferences = 0)
    )())
  }

  test("single node with expand into") {
    // given
    val allNodesScan = AllNodesScan(x, Set.empty)(solved)
    val expand = Expand(allNodesScan, x, SemanticDirection.INCOMING, Seq.empty, x, r, ExpandInto)(solved)

    // when
    val pipe = build(expand)

    // then
    val nodeSlot = LongSlot(0, nullable = false, CTNode, "x")
    val relSlot = LongSlot(1, nullable = false, CTRelationship, "r")
    pipe should equal(ExpandIntoSlottedPipe(
      AllNodesScanSlottedPipe("x", PipelineInformation(Map("x" -> nodeSlot), numberOfLongs = 1, numberOfReferences = 0))(),
      nodeSlot.offset, relSlot.offset, nodeSlot.offset, SemanticDirection.INCOMING, LazyTypes.empty,
      PipelineInformation(Map("x" -> nodeSlot, "r" -> relSlot), numberOfLongs = 2, numberOfReferences = 0)
    )())
  }

  test("single optional node with expand") {
    // given
    val allNodesScan = AllNodesScan(IdName("x"), Set.empty)(solved)
    val optional = Optional(allNodesScan)(solved)
    val expand = Expand(optional, IdName("x"), SemanticDirection.INCOMING, Seq.empty, IdName("z"), IdName("r"), ExpandAll)(solved)

    // when
    val pipe = build(expand)

    // then
    val xNodeSlot = LongSlot(0, nullable = true, CTNode, "x")
    val rRelSlot = LongSlot(1, nullable = false, CTRelationship, "r")
    val zNodeSlot = LongSlot(2, nullable = false, CTNode, "z")
    val allNodeScanPipeline = PipelineInformation(Map("x" -> xNodeSlot), numberOfLongs = 1, numberOfReferences = 0)
    val expandPipeline = PipelineInformation(Map(
      "x" -> xNodeSlot,
      "r" -> rRelSlot,
      "z" -> zNodeSlot), numberOfLongs = 3, numberOfReferences = 0)

    pipe should equal(
      ExpandAllSlottedPipe(
        OptionalSlottedPipe(
          AllNodesScanSlottedPipe("x", allNodeScanPipeline)(),
          Seq(0),
          allNodeScanPipeline
        )(),
        xNodeSlot.offset, rRelSlot.offset, zNodeSlot.offset,
        SemanticDirection.INCOMING,
        LazyTypes.empty,
        expandPipeline
      )())
  }

  test("single optional node with expand into") {
    // given
    val allNodesScan = AllNodesScan(IdName("x"), Set.empty)(solved)
    val optional = Optional(allNodesScan)(solved)
    val expand = Expand(optional, IdName("x"), SemanticDirection.INCOMING, Seq.empty, IdName("x"), IdName("r"), ExpandInto)(solved)

    // when
    val pipe = build(expand)

    // then
    val nodeSlot = LongSlot(0, nullable = true, CTNode, "x")
    val relSlot = LongSlot(1, nullable = false, CTRelationship, "r")
    val allNodeScanPipeline = PipelineInformation(Map("x" -> nodeSlot), numberOfLongs = 1, numberOfReferences = 0)
    val expandPipeline = PipelineInformation(Map("x" -> nodeSlot, "r" -> relSlot), numberOfLongs = 2, numberOfReferences = 0)

    pipe should equal(
      ExpandIntoSlottedPipe(
        OptionalSlottedPipe(
          AllNodesScanSlottedPipe("x", allNodeScanPipeline)(),
          Seq(0),
          allNodeScanPipeline
        )(),
        nodeSlot.offset, relSlot.offset, nodeSlot.offset, SemanticDirection.INCOMING, LazyTypes.empty,
        expandPipeline)()
    )
  }

  test("optional node") {
    // given
    val leaf = AllNodesScan(x, Set.empty)(solved)
    val plan = Optional(leaf)(solved)

    // when
    val pipe = build(plan)

    // then
    val expectedPipeLineInfo = PipelineInformation(Map("x" -> LongSlot(0, nullable = true, CTNode, "x")), numberOfLongs = 1, numberOfReferences = 0)
    pipe should equal(OptionalSlottedPipe(
      AllNodesScanSlottedPipe("x", expectedPipeLineInfo)(),
      Seq(0),
      expectedPipeLineInfo
    )())
  }

  test("single node with optionalExpand ExpandAll") {
    // given
    val allNodesScan = AllNodesScan(x, Set.empty)(solved)
    val expand = OptionalExpand(allNodesScan, x, SemanticDirection.INCOMING, Seq.empty, z, r, ExpandAll)(solved)

    // when
    val pipe = build(expand)

    // then
    pipe should equal(OptionalExpandAllSlottedPipe(
      AllNodesScanSlottedPipe("x",
        PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode, "x")), numberOfLongs = 1, numberOfReferences = 0))(),
      0, 1, 2, SemanticDirection.INCOMING, LazyTypes.empty, predicates.True(),
      PipelineInformation(Map(
        "x" -> LongSlot(0, nullable = false, CTNode, "x"),
        "r" -> LongSlot(1, nullable = true, CTRelationship, "r"),
        "z" -> LongSlot(2, nullable = true, CTNode, "z")), numberOfLongs = 3, numberOfReferences = 0)
    )())
  }

  test("single node with optionalExpand ExpandInto") {
    // given
    val allNodesScan = AllNodesScan(x, Set.empty)(solved)
    val expand = OptionalExpand(allNodesScan, x, SemanticDirection.INCOMING, Seq.empty, x, r, ExpandInto)(solved)

    // when
    val pipe = build(expand)

    // then
    pipe should equal(OptionalExpandIntoSlottedPipe(
      AllNodesScanSlottedPipe("x",
        PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode, "x")), numberOfLongs = 1, numberOfReferences = 0))(),
      0, 1, 0, SemanticDirection.INCOMING, LazyTypes.empty, predicates.True(),
      PipelineInformation(Map(
        "x" -> LongSlot(0, nullable = false, CTNode, "x"),
        "r" -> LongSlot(1, nullable = true, CTRelationship, "r")), numberOfLongs = 2, numberOfReferences = 0)
    )())
  }

  test("single node with varlength expand") {
    // given
    val allNodesScan = AllNodesScan(x, Set.empty)(solved)
    val varLength = VarPatternLength(1, Some(15))
    val tempNode = IdName("r_NODES")
    val tempEdge = IdName("r_EDGES")
    val expand = VarExpand(allNodesScan, x, SemanticDirection.INCOMING, SemanticDirection.INCOMING, Seq.empty,
      z, r, varLength, ExpandAll, tempNode, tempEdge, True()(pos), True()(pos), Seq())(solved)

    // when
    val pipe = build(expand)

    // then
    val xNodeSlot = LongSlot(0, nullable = false, CTNode, "x")
    val tempNodeSlot = LongSlot(1, nullable = false, CTNode, "r_NODES")
    val tempRelSlot = LongSlot(2, nullable = false, CTRelationship, "r_EDGES")
    val zNodeSlot = LongSlot(1, nullable = false, CTNode, "z")
    val rRelSlot = RefSlot(0, nullable = false, CTList(CTRelationship), "r")

    val allNodeScanPipelineInformation = PipelineInformation(Map(
      "x" -> xNodeSlot,
      "r_NODES" -> tempNodeSlot,
      "r_EDGES" -> tempRelSlot), numberOfLongs = 3, numberOfReferences = 0)

    val varExpandPipelineInformation = PipelineInformation(Map(
      "x" -> xNodeSlot,
      "r" -> rRelSlot,
      "z" -> zNodeSlot), numberOfLongs = 2, numberOfReferences = 1)

    pipe should equal(VarLengthExpandSlottedPipe(
      AllNodesScanSlottedPipe("x", allNodeScanPipelineInformation)(),
      xNodeSlot.offset, rRelSlot.offset, zNodeSlot.offset,
      SemanticDirection.INCOMING, SemanticDirection.INCOMING,
      LazyTypes.empty, varLength.min, varLength.max, shouldExpandAll = true,
      varExpandPipelineInformation,
      tempNodeSlot.offset, tempRelSlot.offset,
      commands.predicates.True(), commands.predicates.True(), 1
    )())
  }

  test("single node with varlength expand and a predicate") {
    // given
    val allNodesScan = AllNodesScan(x, Set.empty)(solved)
    val varLength = VarPatternLength(1, Some(15))
    val tempNode = IdName("r_NODES")
    val tempEdge = IdName("r_EDGES")
    val nodePredicate = Equals(prop(tempNode.name, "propertyKey"), literalInt(4))(pos)
    val edgePredicate = Not(LessThan(prop(tempEdge.name, "propertyKey"), literalInt(4))(pos))(pos)

    val expand = VarExpand(allNodesScan, x, SemanticDirection.INCOMING, SemanticDirection.INCOMING, Seq.empty,
      z, r, varLength, ExpandAll, tempNode, tempEdge, nodePredicate, edgePredicate, Seq())(solved)

    // when
    val pipe = build(expand)

    // then
    val xNodeSlot = LongSlot(0, nullable = false, CTNode, "x")
    val tempNodeSlot = LongSlot(1, nullable = false, CTNode, "r_NODES")
    val tempRelSlot = LongSlot(2, nullable = false, CTRelationship, "r_EDGES")
    val zNodeSlot = LongSlot(1, nullable = false, CTNode, "z")
    val rRelSlot = RefSlot(0, nullable = false, CTList(CTRelationship), "r")

    val allNodeScanPipelineInformation = PipelineInformation(Map(
      "x" -> xNodeSlot,
      "r_NODES" -> tempNodeSlot,
      "r_EDGES" -> tempRelSlot), numberOfLongs = 3, numberOfReferences = 0)

    val varExpandPipelineInformation = PipelineInformation(Map(
      "x" -> xNodeSlot,
      "r" -> rRelSlot,
      "z" -> zNodeSlot), numberOfLongs = 2, numberOfReferences = 1)

    pipe should equal(VarLengthExpandSlottedPipe(
      AllNodesScanSlottedPipe("x", allNodeScanPipelineInformation)(),
      xNodeSlot.offset, rRelSlot.offset, zNodeSlot.offset,
      SemanticDirection.INCOMING, SemanticDirection.INCOMING,
      LazyTypes.empty, varLength.min, varLength.max, shouldExpandAll = true,
      varExpandPipelineInformation,
      tempNodeSlot.offset, tempRelSlot.offset,
      commands.predicates.Equals(NodeProperty(1, 0), Literal(4)),
      commands.predicates.Not(
        commands.predicates.LessThan(RelationshipProperty(2, 0), Literal(4))), 1
    )())
  }

  test("let's skip this one") {
    // given
    val allNodesScan = AllNodesScan(x, Set.empty)(solved)
    val skip = plans.Skip(allNodesScan, literalInt(42))(solved)

    // when
    val pipe = build(skip)

    // then
    pipe should equal(SkipPipe(
      AllNodesScanSlottedPipe("x",
        PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode, "x")), numberOfLongs = 1, numberOfReferences = 0))(),
      commands.expressions.Literal(42)
    )())
  }

  test("all we need is to apply ourselves") {
    // given
    val lhs = NodeByLabelScan(x, LabelName("label")(pos), Set.empty)(solved)
    val label = LabelToken("label2", LabelId(0))
    val seekExpression = SingleQueryExpression(literalInt(42))
    val rhs = NodeIndexSeek(z, label, Seq.empty, seekExpression, Set(x))(solved)
    val apply = Apply(lhs, rhs)(solved)

    // when
    val pipe = build(apply)

    // then
    pipe should equal(ApplySlottedPipe(
      NodesByLabelScanSlottedPipe("x", LazyLabel("label"),
        PipelineInformation(Map(
          "x" -> LongSlot(0, nullable = false, CTNode, "x")),
          numberOfLongs = 1, numberOfReferences = 0))(),
      NodeIndexSeekSlottedPipe("z", label, Seq.empty, SingleQueryExpression(commands.expressions.Literal(42)), IndexSeek,
        PipelineInformation(Map(
          "x" -> LongSlot(0, nullable = false, CTNode, "x"),
          "z" -> LongSlot(1, nullable = false, CTNode, "z")
        ), numberOfLongs = 2, numberOfReferences = 0))()
    )())
  }

  ignore("aggregation used for distinct") {
    // given
    val leaf = NodeByLabelScan(x, LabelName("label")(pos), Set.empty)(solved)
    val distinct = Aggregation(leaf, Map("x" -> varFor("x")), Map.empty)(solved)

    // when
    val pipe = build(distinct)

    // then
    pipe should equal(DistinctPipe(
      NodesByLabelScanSlottedPipe("x", LazyLabel("label"),
        PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode, "x")), numberOfLongs = 1, numberOfReferences = 0))(),
      Map("x" -> commands.expressions.Variable("x"))
    )())
  }

  ignore("optional travels through aggregation used for distinct") {
    // given OPTIONAL MATCH (x) RETURN DISTINCT x, x.propertyKey
    val leaf = NodeByLabelScan(x, LabelName("label")(pos), Set.empty)(solved)
    val optional = Optional(leaf)(solved)
    val distinct = Aggregation(optional,
      groupingExpressions = Map("x" -> varFor("x"), "x.propertyKey" -> prop("x", "propertyKey")),
      aggregationExpression = Map.empty)(solved)

    // when
    val pipe = build(distinct)

    val pipelineInformation = PipelineInformation(Map("x" -> LongSlot(0, nullable = true, CTNode, "x")), numberOfLongs = 1, numberOfReferences = 0)
    // then
    val labelScan = NodesByLabelScanSlottedPipe("x", LazyLabel("label"),
      pipelineInformation)()
    val optionalPipe = OptionalSlottedPipe(labelScan, Seq(0), pipelineInformation)()
    pipe should equal(DistinctPipe(
      optionalPipe,
      Map("x" -> Variable("x"), "x.propertyKey" -> Property(Variable("x"), KeyToken.Resolved("propertyKey", 0, PropertyKey)))
    )())
  }

  ignore("optional travels through aggregation") {
    // given OPTIONAL MATCH (x) RETURN x, x.propertyKey, count(*)
    val leaf = NodeByLabelScan(x, LabelName("label")(pos), Set.empty)(solved)
    val optional = Optional(leaf)(solved)
    val distinct = Aggregation(optional,
      groupingExpressions = Map("x" -> varFor("x"), "x.propertyKey" -> prop("x", "propertyKey")),
      aggregationExpression = Map("count" -> CountStar()(pos)))(solved)

    // when
    val pipe = build(distinct)

    // then
    val pipelineInfo = PipelineInformation(Map("x" -> LongSlot(0, nullable = true, CTNode, "x")), numberOfLongs = 1, numberOfReferences = 0)
    val nodeByLabelScan = NodesByLabelScanSlottedPipe("x", LazyLabel("label"), pipelineInfo)()
    val grouping = Map(
      "x" -> Variable("x"),
      "x.propertyKey" -> Property(Variable("x"), KeyToken.Resolved("propertyKey", 0, PropertyKey))
    )
    pipe should equal(EagerAggregationPipe(
      OptionalSlottedPipe(nodeByLabelScan, Seq(0), pipelineInfo)(),
      keyExpressions = grouping,
      aggregations = Map("count" -> commands.expressions.CountStar())
    )())
  }

  test("labelscan with projection") {
    // given
    val leaf = NodeByLabelScan(x, LabelName("label")(pos), Set.empty)(solved)
    val projection = Projection(leaf, Map("x" -> varFor("x"), "x.propertyKey" -> prop("x", "propertyKey")))(solved)

    // when
    val pipe = build(projection)

    // then
    val pipeline = PipelineInformation(numberOfLongs = 1, numberOfReferences = 1,
      slots = Map("x" -> LongSlot(0, nullable = false, CTNode, "x"), "x.propertyKey" -> RefSlot(0, nullable = true, CTAny, "x.propertyKey")))
    pipe should equal(ProjectionSlottedPipe(
      NodesByLabelScanSlottedPipe("x", LazyLabel("label"),
        pipeline)(),
      Map(pipeline("x") -> NodeFromSlot(pipeline("x").offset),
          pipeline("x.propertyKey") -> NodeProperty(pipeline("x.propertyKey").offset, 0))
    )())
  }

  //TODO add this test when cartesian product works
  ignore("cartesian product") {
    // given
    val lhs = NodeByLabelScan(x, LABEL, Set.empty)(solved)
    val rhs = NodeByLabelScan(IdName("y"), LabelName("label2")(pos), Set.empty)(solved)
    val Xproduct = CartesianProduct(lhs, rhs)(solved)

    // when
    val pipe = build(Xproduct)

    val lhsPipeInfo = PipelineInformation.empty.newLong("x", nullable = false, CTNode)
    val rhsPipeInfo = PipelineInformation.empty.newLong("y", nullable = false, CTNode)
    val xProdPipeInfo = PipelineInformation.empty
      .newLong("x", nullable = false, CTNode)
      .newLong("y", nullable = false, CTNode)

    // then
    pipe should equal(CartesianProductSlottedPipe(
      NodesByLabelScanSlottedPipe("x", LazyLabel("label1"), lhsPipeInfo)(),
      NodesByLabelScanSlottedPipe("y", LazyLabel("label2"), rhsPipeInfo)(),
      lhsLongCount = 1, lhsRefCount = 0, xProdPipeInfo)()
    )
  }

  test("that argument does not apply here") {
    // given MATCH (x) MATCH (x)<-[r]-(y)
    val lhs = NodeByLabelScan(x, LABEL, Set.empty)(solved)
    val arg = Argument(Set(x))(solved)()
    val rhs = Expand(arg, x, SemanticDirection.INCOMING, Seq.empty, z, r, ExpandAll)(solved)

    val apply = Apply(lhs, rhs)(solved)

    // when
    val pipe = build(apply)

    // then
    val lhsPipeline = PipelineInformation(Map(
      "x" -> LongSlot(0, nullable = false, CTNode, "x")),
      numberOfLongs = 1, numberOfReferences = 0)

    val rhsPipeline = PipelineInformation(Map(
      "x" -> LongSlot(0, nullable = false, CTNode, "x"),
      "z" -> LongSlot(2, nullable = false, CTNode, "z"),
      "r" -> LongSlot(1, nullable = false, CTRelationship, "r")
    ), numberOfLongs = 3, numberOfReferences = 0)


    pipe should equal(ApplySlottedPipe(
      NodesByLabelScanSlottedPipe("x", LazyLabel(LABEL), lhsPipeline)(),
      ExpandAllSlottedPipe(
        ArgumentSlottedPipe(lhsPipeline)(), 0, 1, 2, SemanticDirection.INCOMING, LazyTypes.empty, rhsPipeline)())())
  }

  test("NodeIndexScan should yield a NodeIndexScanSlottedPipe") {
    // given
    val leaf = NodeIndexScan(
      "n",
      LabelToken("Awesome", LabelId(0)),
      PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)),
      Set.empty)(solved)

    // when
    val pipe = build(leaf)

    // then

    pipe should equal(
      NodeIndexScanSlottedPipe(
        "n",
        LabelToken("Awesome", LabelId(0)),
        PropertyKeyToken("prop", PropertyKeyId(0)),
        PipelineInformation(Map("n" -> LongSlot(0, nullable = false, CTNode, "n")), 1, 0))())
  }

  test("Should use NodeIndexUniqeSeek") {
    // given
    val label = LabelToken("label2", LabelId(0))
    val seekExpression = SingleQueryExpression(literalInt(42))
    val seek = NodeUniqueIndexSeek(z, label, Seq.empty, seekExpression, Set(x))(solved)

    // when
    val pipe = build(seek)

    // then
    pipe should equal(
      NodeIndexSeekSlottedPipe("z", label, Seq.empty, SingleQueryExpression(commands.expressions.Literal(42)), UniqueIndexSeek,
        PipelineInformation(Map(
          "z" -> LongSlot(0, nullable = false, CTNode, "z")
        ), numberOfLongs = 1, numberOfReferences = 0))()
    )
  }

  test("unwind and sort") {
    // given UNWIND [1,2,3] as x RETURN x ORDER BY x
    val xVar = varFor("x")
    val xVarName = IdName.fromVariable(xVar)
    val leaf = SingleRow()(solved)
    val unwind = UnwindCollection(leaf, xVarName, listOf(literalInt(1), literalInt(2), literalInt(3)))(solved)
    val sort = Sort(unwind, List(plans.Ascending(xVarName)))(solved)

    // when
    val pipe = build(sort)

    // then
    val expectedPipeline1 = PipelineInformation(Map.empty, 0, 0)
    val xSlot = RefSlot(0, nullable = true, CTAny, "x")
    val expectedPipeline2 = PipelineInformation(numberOfLongs = 0, numberOfReferences = 1, slots = Map(
      "x" -> xSlot
    ))

    pipe should equal(
      SortSlottedPipe(orderBy = Seq(slottedPipes.Ascending(xSlot)), pipelineInformation = expectedPipeline2,
        source = UnwindSlottedPipe(collection = commands.expressions.ListLiteral(commands.expressions.Literal(1), commands.expressions.Literal(2), commands.expressions.Literal(3)), offset = 0, pipeline = expectedPipeline2,
          source = SingleRowSlottedPipe(expectedPipeline1)()
        )()
      )()
    )
  }

}
