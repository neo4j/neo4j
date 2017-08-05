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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime

import java.time.Clock

import org.mockito.Mockito.{atLeastOnce, verify, when}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.Literal
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.predicates.True
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.values.KeyToken.Resolved
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.values.TokenType
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.{expressions => legacy}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan.PipeInfo
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.FakeIdMap
import org.neo4j.cypher.internal.compiler.v3_3.planner._
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.Metrics
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_3.{RelTypeId, SemanticDirection, SemanticTable}
import org.neo4j.cypher.internal.ir.v3_3._

import scala.collection.mutable

class PipeExecutionPlanBuilderIT extends CypherFunSuite with LogicalPlanningTestSupport {

  implicit val planContext: PlanContext = newMockedPlanContext
  implicit val LogicalPlanningContext = newMockedLogicalPlanningContext(planContext)
  implicit val pipeBuildContext = newMockedPipeExecutionPlanBuilderContext
  val patternRel = PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
  val converters = new ExpressionConverters(CommunityExpressionConverter)

  private val planBuilder = {
    val converters = new ExpressionConverters(CommunityExpressionConverter)
    new PipeExecutionPlanBuilder(Clock.systemUTC(), monitors, expressionConverters = converters, pipeBuilderFactory = CommunityPipeBuilderFactory)
  }

  def build(f: PlannerQuery with CardinalityEstimation => LogicalPlan): PipeInfo = {
    val logicalPlan = f(solved)
    planBuilder.build(None, logicalPlan, new FakeIdMap)
  }

  test("projection only query") {
    val logicalPlan = Projection(
      SingleRow()(solvedWithEstimation(1.0)), Map("42" -> SignedDecimalIntegerLiteral("42")(pos)))_
    val pipeInfo = build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(ProjectionPipe(SingleRowPipe()(), Map("42" -> legacy.Literal(42)))())
  }

  test("simple pattern query") {
    val logicalPlan = AllNodesScan(IdName("n"), Set.empty)_
    val pipeInfo: PipeInfo = build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(AllNodesScanPipe("n")())
  }

  test("simple label scan query") {
    val logicalPlan = NodeByLabelScan(IdName("n"), lblName("Foo"), Set.empty)_
    val pipeInfo = build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(NodeByLabelScanPipe("n", LazyLabel("Foo"))())
  }

  test("simple node by id seek query") {
    val astLiteral: Expression = ListLiteral(Seq(SignedDecimalIntegerLiteral("42")_))_
    val logicalPlan = NodeByIdSeek(IdName("n"), ManySeekableArgs(astLiteral), Set.empty)_
    val pipeInfo = build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(NodeByIdSeekPipe("n", SingleSeekArg(Literal(42)))())
  }

  test("simple node by id seek query with multiple values") {
    val astCollection: ListLiteral = ListLiteral(
      Seq(SignedDecimalIntegerLiteral("42")_, SignedDecimalIntegerLiteral("43")_, SignedDecimalIntegerLiteral("43")_)
    )_
    val logicalPlan = NodeByIdSeek(IdName("n"), ManySeekableArgs(astCollection), Set.empty)_
    val pipeInfo = build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(NodeByIdSeekPipe("n", ManySeekArgs(converters.toCommandExpression(astCollection)))())
  }

  test("simple relationship by id seek query") {
    val astLiteral: Expression = ListLiteral(Seq(SignedDecimalIntegerLiteral("42")_))_
    val fromNode = "from"
    val toNode = "to"
    val logicalPlan = DirectedRelationshipByIdSeek(IdName("r"), ManySeekableArgs(astLiteral), IdName(fromNode), IdName(toNode), Set.empty)_
    val pipeInfo = build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(DirectedRelationshipByIdSeekPipe("r", SingleSeekArg(Literal(42)), toNode, fromNode)())
  }

  test("simple relationship by id seek query with multiple values") {
    val astCollection: Expression =
      ListLiteral(Seq(SignedDecimalIntegerLiteral("42")_, SignedDecimalIntegerLiteral("43")_, SignedDecimalIntegerLiteral("43")_))_

    val fromNode = "from"
    val toNode = "to"
    val logicalPlan = DirectedRelationshipByIdSeek(IdName("r"), ManySeekableArgs(astCollection), IdName(fromNode), IdName(toNode), Set.empty)_
    val pipeInfo = build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(DirectedRelationshipByIdSeekPipe("r", ManySeekArgs(converters.toCommandExpression(astCollection)), toNode, fromNode)())
  }

  test("simple undirected relationship by id seek query with multiple values") {
    val astCollection: Expression =
      ListLiteral(Seq(SignedDecimalIntegerLiteral("42")_, SignedDecimalIntegerLiteral("43")_, SignedDecimalIntegerLiteral("43")_))_

    val fromNode = "from"
    val toNode = "to"
    val logicalPlan = UndirectedRelationshipByIdSeek(IdName("r"), ManySeekableArgs(astCollection), IdName(fromNode), IdName(toNode), Set.empty)_
    val pipeInfo = build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(UndirectedRelationshipByIdSeekPipe("r", ManySeekArgs(converters.toCommandExpression(astCollection)), toNode, fromNode)())
  }

  test("simple cartesian product") {
    val lhs = AllNodesScan(IdName("n"), Set.empty)(solved)
    val rhs = AllNodesScan(IdName("m"), Set.empty)(solved)
    val logicalPlan = CartesianProduct(lhs, rhs)_
    val pipeInfo = build(logicalPlan)

    pipeInfo.pipe should equal(CartesianProductPipe(AllNodesScanPipe("n")(), AllNodesScanPipe("m")())())
  }

  test("simple expand") {
    val logicalPlan = Expand(AllNodesScan("a", Set.empty)(solved), "a", SemanticDirection.INCOMING, Seq(), "b", "r1")_
    val pipeInfo = build(logicalPlan)

    pipeInfo.pipe should equal(ExpandAllPipe( AllNodesScanPipe("a")(), "a", "r1", "b", SemanticDirection.INCOMING, LazyTypes.empty)())
  }

  test("simple expand into existing variable MATCH a-[r]->a ") {
    val logicalPlan = Expand(
      AllNodesScan("a", Set.empty)(solved), "a", SemanticDirection.INCOMING, Seq(), "a", "r", ExpandInto)_
    val pipeInfo = build(logicalPlan)

    val inner: Pipe = ExpandIntoPipe( AllNodesScanPipe("a")(), "a", "r", "a", SemanticDirection.INCOMING, LazyTypes.empty)()

    pipeInfo.pipe should equal(inner)
  }

  test("optional expand into existing variable MATCH a OPTIONAL MATCH a-[r]->a ") {
    val logicalPlan = OptionalExpand(
      AllNodesScan("a", Set.empty)(solved), "a", SemanticDirection.INCOMING, Seq(), "a", "r", ExpandInto)_
    val pipeInfo = build(logicalPlan)

    pipeInfo.pipe should equal(
      OptionalExpandIntoPipe(AllNodesScanPipe("a")(), "a", "r", "a", SemanticDirection.INCOMING, LazyTypes.empty, True())())
  }

  test("simple computeHash join") {
    val logicalPlan =
      NodeHashJoin(
        Set(IdName("b")),
        Expand(AllNodesScan("a", Set.empty)(solved), "a", SemanticDirection.INCOMING, Seq(), "b", "r1")(solved),
        Expand(AllNodesScan("c", Set.empty)(solved), "c", SemanticDirection.INCOMING, Seq(), "b", "r2")(solved)
      )_
    val pipeInfo = build(logicalPlan)

    pipeInfo.pipe should equal(NodeHashJoinPipe(
      Set("b"),
      ExpandAllPipe( AllNodesScanPipe("a")(), "a", "r1", "b", SemanticDirection.INCOMING, LazyTypes.empty)(),
      ExpandAllPipe( AllNodesScanPipe("c")(), "c", "r2", "b", SemanticDirection.INCOMING, LazyTypes.empty)()
    )())
  }

  test("Aggregation on top of Projection => DistinctPipe with resolved expressions") {
    // GIVEN
    val token = 42
    when(planContext.getOptPropertyKeyId("prop")).thenReturn(Some(token))
    val allNodesScan = AllNodesScan("n", Set.empty)(solved)
    val expressions = Map("n.prop" -> Property(Variable("n")(pos), PropertyKeyName("prop")(pos))(pos))
    val projection = Projection(allNodesScan, expressions)(solved)
    val aggregation = Aggregation(projection, expressions, Map.empty) _

    // WHEN
    val pipe = build(aggregation).pipe

    // THEN
    verify(planContext, atLeastOnce()).getOptPropertyKeyId("prop")
    pipe should equal(
      DistinctPipe(
        AllNodesScanPipe("n")(),
        Map("n.prop" -> legacy.Property(legacy.Variable("n"),
          Resolved("prop", token, TokenType.PropertyKey))))())
  }

  def newMockedPipeExecutionPlanBuilderContext: PipeExecutionBuilderContext = {
    val context = mock[PipeExecutionBuilderContext]
    val cardinality = new Metrics.CardinalityModel {
      def apply(pq: PlannerQuery, ignored: QueryGraphSolverInput, ignoredAsWell: SemanticTable) = pq match {
        case PlannerQuery.empty => Cardinality(1)
        case _ => Cardinality(104999.99999)
      }
    }
    when(context.cardinality).thenReturn(cardinality)
    val semanticTable = new SemanticTable(resolvedRelTypeNames = mutable.Map("existing1" -> RelTypeId(1), "existing2" -> RelTypeId(2), "existing3" -> RelTypeId(3)))

    when(context.semanticTable).thenReturn(semanticTable)

    context
  }
}
