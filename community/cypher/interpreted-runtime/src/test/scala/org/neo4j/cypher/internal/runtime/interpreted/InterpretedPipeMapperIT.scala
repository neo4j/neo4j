/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.interpreted

import org.mockito.Mockito.{atLeastOnce, verify, when}
import org.neo4j.cypher.internal.ir.{PatternRelationship, SimplePatternLength}
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.planner.spi.{PlanContext, TokenContext}
import org.neo4j.cypher.internal.runtime.QueryIndexRegistrator
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.KeyToken.Resolved
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.TokenType
import org.neo4j.cypher.internal.runtime.interpreted.commands.{expressions => legacy}
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.v4_0.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.cypher.internal.v4_0.util.RelTypeId
import org.neo4j.cypher.internal.v4_0.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

import scala.collection.mutable

class InterpretedPipeMapperIT extends CypherFunSuite with AstConstructionTestSupport  {
  private implicit val idGen: SequentialIdGen = new SequentialIdGen()

  val planContext: PlanContext = mock[PlanContext]
  val semanticTable = new SemanticTable(resolvedRelTypeNames =
    mutable.Map("existing1" -> RelTypeId(1),
      "existing2" -> RelTypeId(2),
      "existing3" -> RelTypeId(3)))

  val patternRel = PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
  val converters = new ExpressionConverters(CommunityExpressionConverter(TokenContext.EMPTY))

  private val pipeMapper =
    InterpretedPipeMapper(readOnly = true, converters, planContext, mock[QueryIndexRegistrator])(semanticTable)

  private def build(logicalPlan: LogicalPlan): Pipe =
    PipeTreeBuilder(pipeMapper).build(logicalPlan)

  test("projection only query") {
    val logicalPlan = Projection(
      Argument(), Map("42" -> literalInt(42)))
    val pipe = build(logicalPlan)

    pipe should equal(ProjectionPipe(ArgumentPipe()(), Map("42" -> legacy.Literal(42))))
  }

  test("simple pattern query") {
    val logicalPlan = AllNodesScan("n", Set.empty)
    val pipe = build(logicalPlan)

    pipe should equal(AllNodesScanPipe("n")())
  }

  test("simple label scan query") {
    val logicalPlan = NodeByLabelScan("n", labelName("Foo"), Set.empty)
    val pipe = build(logicalPlan)

    pipe should equal(NodeByLabelScanPipe("n", LazyLabel("Foo"))())
  }

  test("simple node by id seek query") {
    val astLiteral = listOfInt(42)
    val logicalPlan = NodeByIdSeek("n", ManySeekableArgs(astLiteral), Set.empty)
    val pipe = build(logicalPlan)

    pipe should equal(NodeByIdSeekPipe("n", SingleSeekArg(legacy.Literal(42)))())
  }

  test("simple node by id seek query with multiple values") {
    val astCollection = listOfInt(42, 43, 43)
    val logicalPlan = NodeByIdSeek("n", ManySeekableArgs(astCollection), Set.empty)
    val pipe = build(logicalPlan)

    pipe should equal(NodeByIdSeekPipe("n", ManySeekArgs(converters.toCommandExpression(logicalPlan.id, astCollection)))())
  }

  test("simple relationship by id seek query") {
    val astLiteral = listOfInt(42)
    val fromNode = "from"
    val toNode = "to"
    val logicalPlan = DirectedRelationshipByIdSeek("r", ManySeekableArgs(astLiteral), fromNode, toNode, Set.empty)
    val pipe = build(logicalPlan)

    pipe should equal(DirectedRelationshipByIdSeekPipe("r", SingleSeekArg(legacy.Literal(42)), toNode, fromNode)())
  }

  test("simple relationship by id seek query with multiple values") {
    val astCollection = listOfInt(42, 43, 43)

    val fromNode = "from"
    val toNode = "to"
    val logicalPlan = DirectedRelationshipByIdSeek("r", ManySeekableArgs(astCollection), fromNode, toNode, Set.empty)
    val pipe = build(logicalPlan)

    pipe should equal(DirectedRelationshipByIdSeekPipe("r", ManySeekArgs(converters.toCommandExpression(logicalPlan.id, astCollection)), toNode, fromNode)())
  }

  test("simple undirected relationship by id seek query with multiple values") {
    val astCollection = listOfInt(42, 43, 43)

    val fromNode = "from"
    val toNode = "to"
    val logicalPlan = UndirectedRelationshipByIdSeek("r", ManySeekableArgs(astCollection), fromNode, toNode, Set.empty)
    val pipe = build(logicalPlan)

    pipe should equal(UndirectedRelationshipByIdSeekPipe("r", ManySeekArgs(converters.toCommandExpression(logicalPlan.id, astCollection)), toNode, fromNode)())
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

    pipe should equal(ExpandAllPipe(AllNodesScanPipe("a")(), "a", "r1", "b", SemanticDirection.INCOMING, RelationshipTypes.empty)())
  }

  test("simple expand into existing variable MATCH a-[r]->a ") {
    val logicalPlan = Expand(AllNodesScan("a", Set.empty), "a", SemanticDirection.INCOMING, Seq(), "a", "r", ExpandInto)(idGen)
    val pipe = build(logicalPlan)

    val inner: Pipe = ExpandIntoPipe(AllNodesScanPipe("a")(), "a", "r", "a", SemanticDirection.INCOMING, RelationshipTypes.empty)()

    pipe should equal(inner)
  }

  test("optional expand into existing variable MATCH a OPTIONAL MATCH a-[r]->a ") {
    val logicalPlan = OptionalExpand(AllNodesScan("a", Set.empty), "a", SemanticDirection.INCOMING, Seq(), "a", "r", ExpandInto)(idGen)
    val pipe = build(logicalPlan)

    pipe should equal(
      OptionalExpandIntoPipe(AllNodesScanPipe("a")(), "a", "r", "a", SemanticDirection.INCOMING, RelationshipTypes.empty, None)())
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
      ExpandAllPipe(AllNodesScanPipe("a")(), "a", "r1", "b", SemanticDirection.INCOMING, RelationshipTypes.empty)(),
      ExpandAllPipe(AllNodesScanPipe("c")(), "c", "r2", "b", SemanticDirection.INCOMING, RelationshipTypes.empty)()
      )())
  }

  test("Aggregation with no aggregating columns => DistinctPipe with resolved expressions") {
    // GIVEN
    val token = 42
    when(planContext.getOptPropertyKeyId("prop")).thenReturn(Some(token))
    val allNodesScan = AllNodesScan("n", Set.empty)
    val expressions = Map("n.prop" -> prop("n", "prop"))
    val aggregation = Aggregation(allNodesScan, expressions, Map.empty)

    // WHEN
    val pipe = build(aggregation)

    // THEN
    verify(planContext, atLeastOnce()).getOptPropertyKeyId("prop")
    pipe should equal(
      DistinctPipe(
        AllNodesScanPipe("n")(),
        Array(DistinctPipe.GroupingCol("n.prop", legacy.Property(legacy.Variable("n"),
          Resolved("prop", token, TokenType.PropertyKey)))))())
  }
}
