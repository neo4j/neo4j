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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted

import org.mockito.Mockito._
import org.neo4j.cypher.internal.BuildEnterpriseInterpretedExecutionPlan.RegisteredPipeBuilderFactory
import org.neo4j.cypher.internal.compatibility.v3_3.runtime._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.{Property, Variable}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.predicates
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.expressions.EnterpriseExpressionConverters
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.pipes.{AllNodesScanRegisterPipe, ExpandAllRegisterPipe, NodesByLabelScanRegisterPipe}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes._
import org.neo4j.cypher.internal.compiled_runtime.v3_3.codegen.CompiledRuntimeContextHelper
import org.neo4j.cypher.internal.compiler.v3_3.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.{LogicalPlanIdentificationBuilder, plans}
import org.neo4j.cypher.internal.compiler.v3_3.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v3_3.{HardcodedGraphStatistics, IDPPlannerName}
import org.neo4j.cypher.internal.frontend.v3_3.ast.{CountStar, LabelName, LabelToken}
import org.neo4j.cypher.internal.frontend.v3_3.symbols.{CTAny, CTNode, CTRelationship}
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_3.{LabelId, SemanticDirection, SemanticTable, ast}
import org.neo4j.cypher.internal.ir.v3_3.IdName

class RegisteredPipeBuilderTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  implicit val pipeMonitor = mock[PipeMonitor]
  implicit val table = SemanticTable()

  private def build(beforeRewrite: LogicalPlan): Pipe = {
    val planContext = mock[PlanContext]
    when(planContext.statistics).thenReturn(HardcodedGraphStatistics)
    val context: EnterpriseRuntimeContext = CompiledRuntimeContextHelper.create(planContext = planContext)
    val beforePipelines: Map[LogicalPlan, PipelineInformation] = RegisterAllocation.allocateRegisters(beforeRewrite)
    val registeredRewriter = new RegisteredRewriter(context.planContext)
    val (logicalPlan, pipelines) = registeredRewriter(beforeRewrite, beforePipelines)
    val idMap = LogicalPlanIdentificationBuilder(logicalPlan)
    val converters = new ExpressionConverters(CommunityExpressionConverter, EnterpriseExpressionConverters)
    val executionPlanBuilder = new PipeExecutionPlanBuilder(context.clock, context.monitors,
      expressionConverters = converters, pipeBuilderFactory = RegisteredPipeBuilderFactory(pipelines))
    val pipeBuildContext = PipeExecutionBuilderContext(context.metrics.cardinality, table, IDPPlannerName)
    executionPlanBuilder.build(None, logicalPlan, idMap)(pipeBuildContext, context.planContext).pipe
  }

  test("only single allnodes scan") {
    // given
    val plan: AllNodesScan = AllNodesScan(IdName("x"), Set.empty)(solved)

    // when
    val pipe = build(plan)

    // then
    pipe should equal(
      AllNodesScanRegisterPipe("x", PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode)), 1, 0))()
    )
  }

  test("single labelscan scan") {
    // given
    val label = LabelName("label")(pos)
    val plan = NodeByLabelScan(IdName("x"), label, Set.empty)(solved)

    // when
    val pipe = build(plan)

    // then
    pipe should equal(
      NodesByLabelScanRegisterPipe("x", LazyLabel(label), PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode)), 1, 0))()
    )
  }

  test("labelscan with filtering") {
    // given
    val label = LabelName("label")(pos)
    val leaf = NodeByLabelScan(IdName("x"), label, Set.empty)(solved)
    val filter = Selection(Seq(ast.True()(pos)), leaf)(solved)

    // when
    val pipe = build(filter)

    // then
    pipe should equal(
      FilterPipe(
        NodesByLabelScanRegisterPipe("x", LazyLabel(label), PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode)), 1, 0))(),
        predicates.True()
      )()
    )
  }

  test("single node with expand") {
    // given
    val allNodesScan = AllNodesScan(IdName("x"), Set.empty)(solved)
    val expand = Expand(allNodesScan, IdName("x"), SemanticDirection.INCOMING, Seq.empty, IdName("z"), IdName("r"), ExpandAll)(solved)

    // when
    val pipe = build(expand)

    // then
    pipe should equal(ExpandAllRegisterPipe(
      AllNodesScanRegisterPipe("x", PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode)), numberOfLongs = 1, numberOfReferences = 0))(),
      0, 1, 2, SemanticDirection.INCOMING, LazyTypes.empty,
      PipelineInformation(Map(
        "x" -> LongSlot(0, nullable = false, CTNode),
        "r" -> LongSlot(1, nullable = false, CTRelationship),
        "z" -> LongSlot(2, nullable = false, CTNode)), numberOfLongs = 3, numberOfReferences = 0)
    )())
  }

  test("single node with expand into") {
    // given
    val allNodesScan = AllNodesScan(IdName("x"), Set.empty)(solved)
    val expand = Expand(allNodesScan, IdName("x"), SemanticDirection.INCOMING, Seq.empty, IdName("x"), IdName("r"), ExpandInto)(solved)

    // when
    val pipe = build(expand)

    // then
    pipe should equal(ExpandIntoPipe(
      AllNodesScanRegisterPipe("x", PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode)), numberOfLongs = 1, numberOfReferences = 0))(),
      "x", "r", "x", SemanticDirection.INCOMING, LazyTypes.empty
    )())
  }

  test("optional node") {
    // given
    val leaf = AllNodesScan(IdName("x"), Set.empty)(solved)
    val plan = Optional(leaf)(solved)

    // when
    val pipe = build(plan)

    // then
    pipe should equal(OptionalPipe(
      Set("x"),
      AllNodesScanRegisterPipe("x", PipelineInformation(Map("x" -> LongSlot(0, nullable = true, CTNode)), 1, 0))()
    )())
  }

  test("single node with optionalExpand ExpandAll") {
    // given
    val allNodesScan = AllNodesScan(IdName("x"), Set.empty)(solved)
    val expand = OptionalExpand(allNodesScan, IdName("x"), SemanticDirection.INCOMING, Seq.empty, IdName("z"), IdName("r"), ExpandAll)(solved)

    // when
    val pipe = build(expand)

    // then
    pipe should equal(OptionalExpandAllPipe(
      AllNodesScanRegisterPipe("x",
        PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode)), numberOfLongs = 1, numberOfReferences = 0))(),
      "x", "r", "z", SemanticDirection.INCOMING, LazyTypes.empty, predicates.True()
    )())
  }

  test("single node with optionalExpand ExpandInto") {
    // given
    val allNodesScan = AllNodesScan(IdName("x"), Set.empty)(solved)
    val expand = OptionalExpand(allNodesScan, IdName("x"), SemanticDirection.INCOMING, Seq.empty, IdName("x"), IdName("r"), ExpandInto)(solved)

    // when
    val pipe = build(expand)

    // then
    pipe should equal(OptionalExpandIntoPipe(
      AllNodesScanRegisterPipe("x",
        PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode)), numberOfLongs = 1, numberOfReferences = 0))(),
      "x", "r", "x", SemanticDirection.INCOMING, LazyTypes.empty, predicates.True()
    )())
  }

  test("let's skip this one") {
    // given
    val allNodesScan = AllNodesScan(IdName("x"), Set.empty)(solved)
    val skip = plans.Skip(allNodesScan, literalInt(42))(solved)

    // when
    val pipe = build(skip)

    // then
    pipe should equal(SkipPipe(
      AllNodesScanRegisterPipe("x",
        PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode)), numberOfLongs = 1, numberOfReferences = 0))(),
      commands.expressions.Literal(42)
    )())
  }

  test("all we need is to apply ourselves") {
    // given
    val lhs = NodeByLabelScan(IdName("x"), LabelName("label")(pos), Set.empty)(solved)
    val label = LabelToken("label2", LabelId(0))
    val seekExpression = SingleQueryExpression(literalInt(42))
    val rhs = NodeIndexSeek(IdName("z"), label, Seq.empty, seekExpression, Set(IdName("x")))(solved)
    val apply = Apply(lhs, rhs)(solved)

    // when
    val pipe = build(apply)

    // then
    pipe should equal(ApplyPipe(
      NodesByLabelScanRegisterPipe("x", LazyLabel("label"),
        PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode)), numberOfLongs = 1, numberOfReferences = 0))(),
      NodeIndexSeekPipe("z", label, Seq.empty, SingleQueryExpression(commands.expressions.Literal(42)), IndexSeek)()
    )())
  }

  test("aggregation used for distinct") {
    // given
    val leaf = NodeByLabelScan(IdName("x"), LabelName("label")(pos), Set.empty)(solved)
    val distinct = Aggregation(leaf, Map("x" -> varFor("x")), Map.empty)(solved)

    // when
    val pipe = build(distinct)

    // then
    pipe should equal(DistinctPipe(
      NodesByLabelScanRegisterPipe("x", LazyLabel("label"),
        PipelineInformation(Map("x" -> LongSlot(0, nullable = false, CTNode)), numberOfLongs = 1, numberOfReferences = 0))(),
      Map("x" -> commands.expressions.Variable("x"))
    )())
  }

  //TODO fix: it fails on build()
  test("optional travels through aggregation used for distinct") {
    // given OPTIONAL MATCH (x) RETURN DISTINCT x, x.propertyKey
    val leaf = NodeByLabelScan(IdName("x"), LabelName("label")(pos), Set.empty)(solved)
    val optional = Optional(leaf)(solved)
    val distinct = Aggregation(optional,
      groupingExpressions = Map("x" -> varFor("x"), "x.propertyKey" -> prop("x", "propertyKey")),
      //TODO the error has something to do with this empty map
      aggregationExpression = Map.empty)(solved)

    // when
    val pipe = build(distinct)

    // then
    pipe should equal(DistinctPipe(
      OptionalPipe(Set.empty,
        NodesByLabelScanRegisterPipe("x", LazyLabel("labe"),
          PipelineInformation(Map("x" -> LongSlot(0, nullable = true, CTNode)), numberOfLongs = 1, numberOfReferences = 0)
        )())(),
      Map("x" -> Variable("x"), "x.propertyKey" -> Property(Variable("x"), PropertyKey("propertyKey")))
    )())
  }

  test("optional travels through aggregation") {
    // given OPTIONAL MATCH (x) RETURN DISTINCT x, x.propertyKey
    val leaf = NodeByLabelScan(IdName("x"), LabelName("label")(pos), Set.empty)(solved)
    val optional = Optional(leaf)(solved)
    val distinct = Aggregation(optional,
      groupingExpressions = Map("x" -> varFor("x"), "x.propertyKey" -> prop("x", "propertyKey")),
      aggregationExpression = Map("count" -> CountStar()(pos)))(solved)

    // when
    val pipe = build(distinct)

    // then
    pipe should equal(EagerAggregationPipe(
      OptionalPipe(
        Set("x"),
        NodesByLabelScanRegisterPipe("x", LazyLabel("label"),
          PipelineInformation(Map("x" -> LongSlot(0, nullable = true, CTNode)), numberOfLongs = 1, numberOfReferences = 0))())(),
      keyExpressions = Set("x", "x.propertyKey"),
      aggregations = Map("count" -> commands.expressions.CountStar())
    )())
  }

  //TODO fix: fails on build()
  test("labelscan with projection") {
    // given
    val leaf = NodeByLabelScan(IdName("x"), LabelName("label")(pos), Set.empty)(solved)
    val projection = Projection(leaf, Map("x" -> varFor("x"), "x.propertyKey" -> prop("x", "propertyKey")))(solved)

    // when
    val pipe = build(projection)

    // then
    pipe should equal(ProjectionPipe(
      NodesByLabelScanRegisterPipe("x", LazyLabel("label"),
        PipelineInformation(numberOfLongs = 1, numberOfReferences = 1, slots = Map("x" -> LongSlot(0, nullable = false, CTNode), "x.propertyKey" -> RefSlot(0, nullable = true, CTAny))))(),
      expressions = Map("x" -> commands.expressions.Variable("x"))
    )())
  }

  test("cartesian product") {
    // given
    val lhs = NodeByLabelScan(IdName("x"), LabelName("label1")(pos), Set.empty)(solved)
    val rhs = NodeByLabelScan(IdName("y"), LabelName("label2")(pos), Set.empty)(solved)
    val Xproduct = CartesianProduct(lhs, rhs)(solved)

    // when
    val pipe = build(Xproduct)

    // then
    pipe should equal(CartesianProductPipe(
      NodesByLabelScanRegisterPipe("x", LazyLabel("label1"),
        PipelineInformation(numberOfLongs = 1, numberOfReferences = 0, slots = Map("x" -> LongSlot(0, nullable = false, CTNode))))(),
      NodesByLabelScanRegisterPipe("y", LazyLabel("label2"),
        PipelineInformation(numberOfLongs = 1, numberOfReferences = 0, slots = Map("y" -> LongSlot(0, nullable = false, CTNode))))()
    )())
  }
}
