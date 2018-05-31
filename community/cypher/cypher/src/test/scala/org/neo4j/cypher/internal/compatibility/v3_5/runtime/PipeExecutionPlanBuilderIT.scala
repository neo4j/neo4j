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

import org.mockito.Mockito.{atLeastOnce, verify, when}
import org.neo4j.cypher.internal.compiler.v3_5.planner._
import org.neo4j.cypher.internal.ir.v3_5._
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanContext
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.True
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.KeyToken.Resolved
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.TokenType
import org.neo4j.cypher.internal.runtime.interpreted.commands.{expressions => legacy}
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.opencypher.v9_0.ast.semantics.SemanticTable
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.RelTypeId
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

import scala.collection.mutable

class PipeExecutionPlanBuilderIT extends CypherFunSuite with LogicalPlanningTestSupport {

  val pipeBuildContext = pipeExecutionPlanBuilderContext
  val planContext: PlanContext = newMockedPlanContext

  val patternRel = PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
  val converters = new ExpressionConverters(CommunityExpressionConverter)

  private val planBuilder = {
    val converters = new ExpressionConverters(CommunityExpressionConverter)
    new PipeExecutionPlanBuilder(expressionConverters = converters, pipeBuilderFactory = InterpretedPipeBuilderFactory)
  }

  private def build(logicalPlan: LogicalPlan): Pipe = {
    planBuilder.build(logicalPlan)(pipeBuildContext, planContext)
  }

  test("projection only query") {
    val logicalPlan = Projection(
      Argument(), Map("42" -> SignedDecimalIntegerLiteral("42")(pos)))
    val pipe = build(logicalPlan)

    pipe should equal(ProjectionPipe(ArgumentPipe()(), Map("42" -> legacy.Literal(42)))())
  }

  test("simple pattern query") {
    val logicalPlan = AllNodesScan("n", Set.empty)
    val pipe = build(logicalPlan)

    pipe should equal(AllNodesScanPipe("n")())
  }

  test("simple label scan query") {
    val logicalPlan = NodeByLabelScan("n", lblName("Foo"), Set.empty)
    val pipe = build(logicalPlan)

    pipe should equal(NodeByLabelScanPipe("n", LazyLabel("Foo"))())
  }

  test("simple node by id seek query") {
    val astLiteral: Expression = ListLiteral(Seq(SignedDecimalIntegerLiteral("42")_))_
    val logicalPlan = NodeByIdSeek("n", ManySeekableArgs(astLiteral), Set.empty)
    val pipe = build(logicalPlan)

    pipe should equal(NodeByIdSeekPipe("n", SingleSeekArg(Literal(42)))())
  }

  test("simple node by id seek query with multiple values") {
    val astCollection: ListLiteral = ListLiteral(
      Seq(SignedDecimalIntegerLiteral("42")_, SignedDecimalIntegerLiteral("43")_, SignedDecimalIntegerLiteral("43")_)
    )_
    val logicalPlan = NodeByIdSeek("n", ManySeekableArgs(astCollection), Set.empty)
    val pipe = build(logicalPlan)

    pipe should equal(NodeByIdSeekPipe("n", ManySeekArgs(converters.toCommandExpression(astCollection)))())
  }

  test("simple relationship by id seek query") {
    val astLiteral: Expression = ListLiteral(Seq(SignedDecimalIntegerLiteral("42")_))_
    val fromNode = "from"
    val toNode = "to"
    val logicalPlan = DirectedRelationshipByIdSeek("r", ManySeekableArgs(astLiteral), fromNode, toNode, Set.empty)
    val pipe = build(logicalPlan)

    pipe should equal(DirectedRelationshipByIdSeekPipe("r", SingleSeekArg(Literal(42)), toNode, fromNode)())
  }

  test("simple relationship by id seek query with multiple values") {
    val astCollection: Expression =
      ListLiteral(Seq(SignedDecimalIntegerLiteral("42")_, SignedDecimalIntegerLiteral("43")_, SignedDecimalIntegerLiteral("43")_))_

    val fromNode = "from"
    val toNode = "to"
    val logicalPlan = DirectedRelationshipByIdSeek("r", ManySeekableArgs(astCollection), fromNode, toNode, Set.empty)
    val pipe = build(logicalPlan)

    pipe should equal(DirectedRelationshipByIdSeekPipe("r", ManySeekArgs(converters.toCommandExpression(astCollection)), toNode, fromNode)())
  }

  test("simple undirected relationship by id seek query with multiple values") {
    val astCollection: Expression =
      ListLiteral(Seq(SignedDecimalIntegerLiteral("42")_, SignedDecimalIntegerLiteral("43")_, SignedDecimalIntegerLiteral("43")_))_

    val fromNode = "from"
    val toNode = "to"
    val logicalPlan = UndirectedRelationshipByIdSeek("r", ManySeekableArgs(astCollection), fromNode, toNode, Set.empty)
    val pipe = build(logicalPlan)

    pipe should equal(UndirectedRelationshipByIdSeekPipe("r", ManySeekArgs(converters.toCommandExpression(astCollection)), toNode, fromNode)())
  }

  test("simple cartesian product") {
    val lhs = AllNodesScan("n", Set.empty)
    val rhs = AllNodesScan("m", Set.empty)
    val logicalPlan = CartesianProduct(lhs, rhs)
    val pipe = build(logicalPlan)

    pipe should equal(CartesianProductPipe(AllNodesScanPipe("n")(), AllNodesScanPipe("m")())())
  }

  test("simple expand") {
    val logicalPlan = Expand(AllNodesScan("a", Set.empty), "a", SemanticDirection.INCOMING, Seq(), "b", "r1")(idGen)
    val pipe = build(logicalPlan)

    pipe should equal(ExpandAllPipe( AllNodesScanPipe("a")(), "a", "r1", "b", SemanticDirection.INCOMING, LazyTypes.empty)())
  }

  test("simple expand into existing variable MATCH a-[r]->a ") {
    val logicalPlan = Expand(AllNodesScan("a", Set.empty), "a", SemanticDirection.INCOMING, Seq(), "a", "r", ExpandInto)(idGen)
    val pipe = build(logicalPlan)

    val inner: Pipe = ExpandIntoPipe( AllNodesScanPipe("a")(), "a", "r", "a", SemanticDirection.INCOMING, LazyTypes.empty)()

    pipe should equal(inner)
  }

  test("optional expand into existing variable MATCH a OPTIONAL MATCH a-[r]->a ") {
    val logicalPlan = OptionalExpand(AllNodesScan("a", Set.empty), "a", SemanticDirection.INCOMING, Seq(), "a", "r", ExpandInto)(idGen)
    val pipe = build(logicalPlan)

    pipe should equal(
      OptionalExpandIntoPipe(AllNodesScanPipe("a")(), "a", "r", "a", SemanticDirection.INCOMING, LazyTypes.empty, True())())
  }

  test("simple hash join") {
    val logicalPlan =
      NodeHashJoin(
        Set("b"),
        Expand(AllNodesScan("a", Set.empty), "a", SemanticDirection.INCOMING, Seq(), "b", "r1"),
        Expand(AllNodesScan("c", Set.empty), "c", SemanticDirection.INCOMING, Seq(), "b", "r2")
      )
    val pipe = build(logicalPlan)

    pipe should equal(NodeHashJoinPipe(
      Set("b"),
      ExpandAllPipe( AllNodesScanPipe("a")(), "a", "r1", "b", SemanticDirection.INCOMING, LazyTypes.empty)(),
      ExpandAllPipe( AllNodesScanPipe("c")(), "c", "r2", "b", SemanticDirection.INCOMING, LazyTypes.empty)()
    )())
  }

  test("Aggregation on top of Projection => DistinctPipe with resolved expressions") {
    // GIVEN
    val token = 42
    when(planContext.getOptPropertyKeyId("prop")).thenReturn(Some(token))
    val allNodesScan = AllNodesScan("n", Set.empty)
    val expressions = Map("n.prop" -> Property(Variable("n")(pos), PropertyKeyName("prop")(pos))(pos))
    val projection = Projection(allNodesScan, expressions)
    val aggregation = Aggregation(projection, expressions, Map.empty)

    // WHEN
    val pipe = build(aggregation)

    // THEN
    verify(planContext, atLeastOnce()).getOptPropertyKeyId("prop")
    pipe should equal(
      DistinctPipe(
        AllNodesScanPipe("n")(),
        Map("n.prop" -> legacy.Property(legacy.Variable("n"),
          Resolved("prop", token, TokenType.PropertyKey))))())
  }

  def pipeExecutionPlanBuilderContext: PipeExecutionBuilderContext = {
    val semanticTable = new SemanticTable(resolvedRelTypeNames =
                                            mutable.Map("existing1" -> RelTypeId(1),
                                                        "existing2" -> RelTypeId(2),
                                                        "existing3" -> RelTypeId(3)))
    PipeExecutionBuilderContext(semanticTable, readOnly = true, mock[Cardinalities])
  }
}
