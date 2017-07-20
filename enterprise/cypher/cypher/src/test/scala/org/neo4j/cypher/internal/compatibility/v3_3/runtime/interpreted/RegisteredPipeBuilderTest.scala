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

import org.neo4j.cypher.internal.BuildEnterpriseInterpretedExecutionPlan.RegisteredPipeBuilderFactory
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.expressions.EnterpriseExpressionConverters
import org.neo4j.cypher.internal.compatibility.v3_3.runtime._
import org.neo4j.cypher.internal.compiled_runtime.v3_3.codegen.CompiledRuntimeContextHelper
import org.neo4j.cypher.internal.compiler.v3_3.{HardcodedGraphStatistics, IDPPlannerName}
import org.neo4j.cypher.internal.compiler.v3_3.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.LogicalPlanIdentificationBuilder
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_3.{SemanticDirection, SemanticTable, ast}
import org.neo4j.cypher.internal.frontend.v3_3.ast.LabelName
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.predicates
import org.neo4j.cypher.internal.frontend.v3_3.symbols.{CTNode, CTRelationship}
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_3.IdName
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.pipes.{AllNodesScanRegisterPipe, ExpandAllRegisterPipe, NodesByLabelScanRegisterPipe}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes._

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

}
