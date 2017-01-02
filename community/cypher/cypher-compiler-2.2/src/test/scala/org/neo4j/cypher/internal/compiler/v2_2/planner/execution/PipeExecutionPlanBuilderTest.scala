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
package org.neo4j.cypher.internal.compiler.v2_2.planner.execution

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.commands.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_2.ast.{Collection, SignedDecimalIntegerLiteral, SignedIntegerLiteral}
import org.neo4j.cypher.internal.compiler.v2_2.commands.{True, expressions => legacy}
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.PipeInfo
import org.neo4j.cypher.internal.compiler.v2_2.pipes.{EntityByIdExprs => PipeEntityByIdExprs, _}
import org.neo4j.cypher.internal.compiler.v2_2.planner._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Cardinality
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{EntityByIdExprs => PlanEntityByIdExprs, _}
import org.neo4j.graphdb.Direction
import org.neo4j.helpers.Clock

class PipeExecutionPlanBuilderTest extends CypherFunSuite with LogicalPlanningTestSupport {

  implicit val planContext = newMockedPlanContext
  implicit val pipeMonitor = mock[PipeMonitor]
  implicit val LogicalPlanningContext = newMockedLogicalPlanningContext(planContext)
  implicit val pipeBuildContext = newMockedPipeExecutionPlanBuilderContext
  val patternRel = PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)

  val planBuilder = new PipeExecutionPlanBuilder(Clock.SYSTEM_CLOCK, monitors)

  def build(f: PlannerQuery with CardinalityEstimation => LogicalPlan): PipeInfo =
    planBuilder.build(f(solved))

  test("projection only query") {
    val logicalPlan = Projection(
      SingleRow()(solvedWithEstimation(1.0)), Map("42" -> SignedDecimalIntegerLiteral("42")(pos)))_
    val pipeInfo = build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(ProjectionNewPipe(SingleRowPipe(), Map("42" -> legacy.Literal(42)))())
  }

  test("simple pattern query") {
    val logicalPlan = AllNodesScan(IdName("n"), Set.empty)_
    val pipeInfo: PipeInfo = build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(AllNodesScanPipe("n")())
  }

  test("simple label scan query") {
    val logicalPlan = NodeByLabelScan(IdName("n"), LazyLabel("Foo"), Set.empty)_
    val pipeInfo = build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(NodeByLabelScanPipe("n", LazyLabel("Foo"))())
  }

  test("simple node by id seek query") {
    val astLiteral: SignedIntegerLiteral = SignedDecimalIntegerLiteral("42")_
    val logicalPlan = NodeByIdSeek(IdName("n"), PlanEntityByIdExprs(Seq(astLiteral)), Set.empty)_
    val pipeInfo = build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(NodeByIdSeekPipe("n", PipeEntityByIdExprs(Seq(astLiteral.asCommandExpression)))())
  }

  test("simple node by id seek query with multiple values") {
    val astCollection: Collection = Collection(
      Seq(SignedDecimalIntegerLiteral("42")_, SignedDecimalIntegerLiteral("43")_, SignedDecimalIntegerLiteral("43")_)
    )_
    val logicalPlan = NodeByIdSeek(IdName("n"), PlanEntityByIdExprs(Seq(astCollection)), Set.empty)_
    val pipeInfo = build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(NodeByIdSeekPipe("n", PipeEntityByIdExprs(Seq(astCollection.asCommandExpression)))())
  }

  test("simple relationship by id seek query") {
    val astLiteral: SignedIntegerLiteral = SignedDecimalIntegerLiteral("42")_
    val fromNode = "from"
    val toNode = "to"
    val logicalPlan = DirectedRelationshipByIdSeek(IdName("r"), PlanEntityByIdExprs(Seq(astLiteral)), IdName(fromNode), IdName(toNode), Set.empty)_
    val pipeInfo = build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(DirectedRelationshipByIdSeekPipe("r", PipeEntityByIdExprs(Seq(astLiteral.asCommandExpression)), toNode, fromNode)())
  }

  test("simple relationship by id seek query with multiple values") {
    val astCollection: Seq[SignedIntegerLiteral] =
      Seq(SignedDecimalIntegerLiteral("42")_, SignedDecimalIntegerLiteral("43")_, SignedDecimalIntegerLiteral("43")_)

    val fromNode = "from"
    val toNode = "to"
    val logicalPlan = DirectedRelationshipByIdSeek(IdName("r"), PlanEntityByIdExprs(astCollection), IdName(fromNode), IdName(toNode), Set.empty)_
    val pipeInfo = build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(DirectedRelationshipByIdSeekPipe("r", PipeEntityByIdExprs(astCollection.map(_.asCommandExpression)), toNode, fromNode)())
  }

  test("simple undirected relationship by id seek query with multiple values") {
    val astCollection: Seq[SignedIntegerLiteral] =
      Seq(SignedDecimalIntegerLiteral("42")_, SignedDecimalIntegerLiteral("43")_, SignedDecimalIntegerLiteral("43")_)

    val fromNode = "from"
    val toNode = "to"
    val logicalPlan = UndirectedRelationshipByIdSeek(IdName("r"), PlanEntityByIdExprs(astCollection), IdName(fromNode), IdName(toNode), Set.empty)_
    val pipeInfo = build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(UndirectedRelationshipByIdSeekPipe("r", PipeEntityByIdExprs(astCollection.map(_.asCommandExpression)), toNode, fromNode)())
  }

  test("simple cartesian product") {
    val lhs = AllNodesScan(IdName("n"), Set.empty)(solved)
    val rhs = AllNodesScan(IdName("m"), Set.empty)(solved)
    val logicalPlan = CartesianProduct(lhs, rhs)_
    val pipeInfo = build(logicalPlan)

    pipeInfo.pipe should equal(CartesianProductPipe(AllNodesScanPipe("n")(), AllNodesScanPipe("m")())())
  }

  test("simple expand") {
    val logicalPlan = Expand(AllNodesScan("a", Set.empty)(solved), "a", Direction.INCOMING, Seq(), "b", "r1")_
    val pipeInfo = build(logicalPlan)

    pipeInfo.pipe should equal(ExpandAllPipe( AllNodesScanPipe("a")(), "a", "r1", "b", Direction.INCOMING, LazyTypes.empty)())
  }

  test("simple expand into existing identifier MATCH a-[r]->a ") {
    val logicalPlan = Expand(
      AllNodesScan("a", Set.empty)(solved), "a", Direction.INCOMING, Seq(), "a", "r", ExpandInto)_
    val pipeInfo = build(logicalPlan)

    val inner: Pipe = ExpandIntoPipe( AllNodesScanPipe("a")(), "a", "r", "a", Direction.INCOMING, LazyTypes.empty)()

    pipeInfo.pipe should equal(inner)
  }

  test("optional expand into existing identifier MATCH a OPTIONAL MATCH a-[r]->a ") {
    val logicalPlan = OptionalExpand(
      AllNodesScan("a", Set.empty)(solved), "a", Direction.INCOMING, Seq(), "a", "r", ExpandInto)_
    val pipeInfo = build(logicalPlan)

    pipeInfo.pipe should equal(
      OptionalExpandIntoPipe(AllNodesScanPipe("a")(), "a", "r", "a", Direction.INCOMING, LazyTypes.empty, True())())
  }

  test("simple hash join") {
    val logicalPlan =
      NodeHashJoin(
        Set(IdName("b")),
        Expand(AllNodesScan("a", Set.empty)(solved), "a", Direction.INCOMING, Seq(), "b", "r1")(solved),
        Expand(AllNodesScan("c", Set.empty)(solved), "c", Direction.INCOMING, Seq(), "b", "r2")(solved)
      )_
    val pipeInfo = build(logicalPlan)

    pipeInfo.pipe should equal(NodeHashJoinPipe(
      Set("b"),
      ExpandAllPipe( AllNodesScanPipe("a")(), "a", "r1", "b", Direction.INCOMING, LazyTypes.empty)(),
      ExpandAllPipe( AllNodesScanPipe("c")(), "c", "r2", "b", Direction.INCOMING, LazyTypes.empty)()
    )())
  }
}
