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
package org.neo4j.cypher.internal.compiler.planner.logical.plans

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.planner._
import org.neo4j.cypher.internal.compiler.planner.logical.ExpressionEvaluator
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.planner.logical.steps.idSeekLeafPlanner
import org.neo4j.cypher.internal.ir._
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.ast.semantics.{ExpressionTypeInfo, SemanticTable}
import org.neo4j.cypher.internal.v4_0.expressions.{PatternExpression, RelTypeName, SemanticDirection}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v4_0.util.{Cost, RelTypeId, symbols}

class IdSeekLeafPlannerTest extends CypherFunSuite  with LogicalPlanningTestSupport {

  private val statistics = hardcodedStatistics
  // NOTE: rewriters make sure that all EQUALS will be rewritten to IN so here only the latter should be tested

  private val evaluator = mock[ExpressionEvaluator]
  test("simple node by id seek with a collection of node ids") {
    // given
    val expr = in(id(varFor("n")), listOfInt(42, 43, 43))
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set("n"), expr))),
      patternNodes = Set("n")
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(config)).thenReturn((plan: LogicalPlan, _: QueryGraphSolverInput, _: Cardinalities) => plan match {
      case _: NodeByIdSeek => Cost(1)
      case _               => Cost(Double.MaxValue)
    })
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = factory.newMetrics(statistics, evaluator, config))
    when(context.semanticTable.isNode(varFor("n"))).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(qg, InterestingOrder.empty, context)

    // then
    resultPlans should equal(
      Seq(NodeByIdSeek("n", ManySeekableArgs(listOfInt(42, 43, 43)), Set.empty))
    )
  }

  test("node by id seek with a collection of node ids via previous variable") {
    // given
    val expr = in(id(varFor("n")), varFor("arr"))
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set("n"), expr))),
      patternNodes = Set("n"),
      argumentIds = Set("arr")
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(config)).thenReturn((plan: LogicalPlan, _: QueryGraphSolverInput, _: Cardinalities) => plan match {
      case _: NodeByIdSeek => Cost(1)
      case _               => Cost(Double.MaxValue)
    })
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = factory.newMetrics(statistics, evaluator, config))
    when(context.semanticTable.isNode(varFor("n"))).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(qg, InterestingOrder.empty, context)

    // then
    resultPlans should equal(
      Seq(NodeByIdSeek("n", ManySeekableArgs(varFor("arr")), Set("arr")))
    )
  }

  test("node by id seek should not be produced when the argument expression is an unbound variable") {
    // given match (n) where id(n) in arr
    val expr = in(id(varFor("n")), varFor("arr"))
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set("n"), expr))),
      patternNodes = Set("n"),
      argumentIds = Set()
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(config)).thenReturn((plan: LogicalPlan, _: QueryGraphSolverInput, _: Cardinalities) => plan match {
      case _: NodeByIdSeek => Cost(1)
      case _               => Cost(Double.MaxValue)
    })
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = factory.newMetrics(statistics, evaluator, config))
    when(context.semanticTable.isNode(varFor("n"))).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(qg, InterestingOrder.empty, context)

    // then
    resultPlans should equal(Seq.empty)
  }

  test("node by id seek should not be produced when the node variable is an argument") {
    // given match (n) where id(n) in arr
    val expr = in(id(varFor("n")), varFor("arr"))
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set("n"), expr))),
      patternNodes = Set("n"),
      argumentIds = Set("arr", "n")
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(config)).thenReturn((plan: LogicalPlan, _: QueryGraphSolverInput, _: Cardinalities) => plan match {
      case _: NodeByIdSeek => Cost(1)
      case _               => Cost(Double.MaxValue)
    })
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = factory.newMetrics(statistics, evaluator, config))
    when(context.semanticTable.isNode(varFor("n"))).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(qg, InterestingOrder.empty, context)

    // then
    resultPlans should equal(Seq.empty)
  }

  test("simple directed relationship by id seek with a collection of relationship ids") {
    // given
    val expr = in(id(varFor("r")), listOfInt(42, 43, 43))
    val from = "from"
    val end = "to"
    val patternRel = PatternRelationship("r", (from, end), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set("r"), expr))),
      patternNodes = Set(from, end),
      patternRelationships = Set(patternRel)
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(config)).thenReturn((plan: LogicalPlan, _: QueryGraphSolverInput, _: Cardinalities) => plan match {
      case _: DirectedRelationshipByIdSeek => Cost(1)
      case _                               => Cost(Double.MaxValue)
    })
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = factory.newMetrics(statistics, evaluator, config))
    when(context.semanticTable.isRelationship(varFor("r"))).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(qg, InterestingOrder.empty, context)

    // then
    resultPlans should equal(
      Seq(DirectedRelationshipByIdSeek("r", ManySeekableArgs(listOfInt(42, 43, 43)), from, end, Set.empty))
    )
  }

  test("simple undirected relationship by id seek with a collection of relationship ids") {
    // given
    val expr = in(id(varFor("r")), listOfInt(42, 43, 43))
    val from = "from"
    val end = "to"
    val patternRel = PatternRelationship("r", (from, end), SemanticDirection.BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set("r"), expr))),
      patternNodes = Set(from, end),
      patternRelationships = Set(patternRel))

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(config)).thenReturn((plan: LogicalPlan, _: QueryGraphSolverInput, _: Cardinalities) => plan match {
      case _: UndirectedRelationshipByIdSeek => Cost(2)
      case _                                 => Cost(Double.MaxValue)
    })
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = factory.newMetrics(statistics, evaluator, config))
    when(context.semanticTable.isRelationship(varFor("r"))).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(qg, InterestingOrder.empty, context)

    // then
    resultPlans should equal(
      Seq(UndirectedRelationshipByIdSeek("r", ManySeekableArgs(listOfInt(42, 43, 43)), from, end, Set.empty))
    )
  }

  test("simple undirected typed relationship by id seek with a collection of relationship ids") {
    // given
    val expr = in(id(varFor("r")), listOfInt(42))
    val from = "from"
    val end = "to"
    val relTypeX = RelTypeName("X")(pos)

    val semanticTable =
      new SemanticTable(ASTAnnotationMap(
        varFor("r") -> ExpressionTypeInfo(symbols.CTRelationship)
      ))
    semanticTable.resolvedRelTypeNames += "X" -> RelTypeId(1)

    val patternRel = PatternRelationship(
      "r", (from, end), SemanticDirection.BOTH,
      Seq(relTypeX),
      SimplePatternLength
    )
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set("r"), expr))),
      patternNodes = Set(from, end),
      patternRelationships = Set(patternRel))

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(config)).thenReturn((plan: LogicalPlan, _: QueryGraphSolverInput, _: Cardinalities) => plan match {
      case _: UndirectedRelationshipByIdSeek => Cost(2)
      case _                                 => Cost(Double.MaxValue)
    })
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = factory.newMetrics(statistics, evaluator, config), semanticTable = semanticTable)

    // when
    val resultPlans = idSeekLeafPlanner(qg, InterestingOrder.empty, context)

    // then
    resultPlans should equal(
      Seq(Selection(
        ands(equals(function("type", varFor("r")), literalString("X"))),
        UndirectedRelationshipByIdSeek("r", ManySeekableArgs(listOfInt(42)), from, end, Set.empty)
      ))
    )
  }

  test("simple undirected multi-typed relationship by id seek with  a collection of relationship ids") {
    // given
    val expr = in(id(varFor("r")), listOfInt(42))
    val from = "from"
    val end = "to"
    val relTypeX = RelTypeName("X") _
    val relTypeY = RelTypeName("Y") _

    val semanticTable =
      new SemanticTable(ASTAnnotationMap(
        varFor("r") -> ExpressionTypeInfo(symbols.CTRelationship)
      ))
    semanticTable.resolvedRelTypeNames += "X" -> RelTypeId(1)
    semanticTable.resolvedRelTypeNames += "Y" -> RelTypeId(2)

    val patternRel = PatternRelationship(
      "r", (from, end), SemanticDirection.BOTH,
      Seq[RelTypeName](relTypeX, relTypeY),
      SimplePatternLength
    )
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set("r"), expr))),
      patternNodes = Set(from, end),
      patternRelationships = Set(patternRel))

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(config)).thenReturn((plan: LogicalPlan, _: QueryGraphSolverInput, _: Cardinalities) => plan match {
      case _: UndirectedRelationshipByIdSeek => Cost(2)
      case _                                 => Cost(Double.MaxValue)
    })
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext(), metrics = factory.newMetrics(statistics, evaluator, config), semanticTable = semanticTable)

    // when
    val resultPlans = idSeekLeafPlanner(qg, InterestingOrder.empty, context)

    // then
    resultPlans should equal(
      Seq(Selection(
        ands(
          ors(
            equals(function("type", varFor("r")), literalString("X")),
            equals(function("type", varFor("r")), literalString("Y"))
          )
        ),
        UndirectedRelationshipByIdSeek("r", ManySeekableArgs(listOfInt(42)), from, end, Set.empty)
    )))
  }
}
