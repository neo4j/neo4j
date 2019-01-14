/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.plans

import org.mockito.Mockito._
import org.neo4j.cypher.internal.util.v3_4.{Cost, RelTypeId, symbols}
import org.neo4j.cypher.internal.compiler.v3_4.planner._
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.ExpressionEvaluator
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.steps.idSeekLeafPlanner
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_4.semantics.{ExpressionTypeInfo, SemanticTable}
import org.neo4j.cypher.internal.ir.v3_4._
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.v3_4.logical.plans._
import org.neo4j.cypher.internal.v3_4.expressions._

import scala.collection.mutable

class IdSeekLeafPlannerTest extends CypherFunSuite  with LogicalPlanningTestSupport {

  private val statistics = hardcodedStatistics
  private implicit val subQueryLookupTable = Map.empty[PatternExpression, QueryGraph]

  // NOTE: the ronja rewriters make sure that all EQUALS will be rewritten to IN so here only the latter should be tested

  private val evaluator = mock[ExpressionEvaluator]
  test("simple node by id seek with a collection of node ids") {
    // given
    val variable: Variable = Variable("n")_
    val expr = In(
      FunctionInvocation(FunctionName("id")_, distinct = false, Array(variable))_,
      ListLiteral(
        Seq(SignedDecimalIntegerLiteral("42")_, SignedDecimalIntegerLiteral("43")_, SignedDecimalIntegerLiteral("43")_)
      )_
    )_
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set("n"), expr))),
      patternNodes = Set("n")
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(config)).thenReturn((plan: LogicalPlan, input: QueryGraphSolverInput, _: Cardinalities) => plan match {
      case _: NodeByIdSeek => Cost(1)
      case _               => Cost(Double.MaxValue)
    })
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext,
      metrics = factory.newMetrics(statistics, evaluator, config)
    )
    when(context.semanticTable.isNode(variable)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(qg, context, solveds, cardinalities)

    // then
    resultPlans should equal(
      Seq(NodeByIdSeek("n", ManySeekableArgs(ListLiteral(Seq(
        SignedDecimalIntegerLiteral("42")_, SignedDecimalIntegerLiteral("43")_, SignedDecimalIntegerLiteral("43")_
      ))_), Set.empty))
    )
  }

  test("node by id seek with a collection of node ids via previous variable") {
    // given
    val variable: Variable = Variable("n")_
    val expr = In(
      FunctionInvocation(FunctionName("id")_, variable)_,
      Variable("arr")_
    )_
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set("n"), expr))),
      patternNodes = Set("n"),
      argumentIds = Set("arr")
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(config)).thenReturn((plan: LogicalPlan, input: QueryGraphSolverInput, _: Cardinalities) => plan match {
      case _: NodeByIdSeek => Cost(1)
      case _               => Cost(Double.MaxValue)
    })
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext,
      metrics = factory.newMetrics(statistics, evaluator, config)
    )
    when(context.semanticTable.isNode(variable)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(qg, context, solveds, cardinalities)

    // then
    resultPlans should equal(
      Seq(NodeByIdSeek("n", ManySeekableArgs(Variable("arr")_), Set("arr")))
    )
  }

  test("node by id seek should not be produced when the argument expression is an unbound variable") {
    // given match (n) where id(n) in arr
    val variable: Variable = Variable("n")_
    val expr = In(
      FunctionInvocation(FunctionName("id")_, variable)_,
      Variable("arr")_
    )_
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set("n"), expr))),
      patternNodes = Set("n"),
      argumentIds = Set()
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(config)).thenReturn((plan: LogicalPlan, input: QueryGraphSolverInput, _: Cardinalities) => plan match {
      case _: NodeByIdSeek => Cost(1)
      case _               => Cost(Double.MaxValue)
    })
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext,
      metrics = factory.newMetrics(statistics, evaluator, config)
    )
    when(context.semanticTable.isNode(variable)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(qg, context, solveds, cardinalities)

    // then
    resultPlans should equal(Seq.empty)
  }

  test("node by id seek should not be produced when the node variable is an argument") {
    // given match (n) where id(n) in arr
    val variable: Variable = Variable("n")_
    val expr = In(
      FunctionInvocation(FunctionName("id")_, variable)_,
      Variable("arr")_
    )_
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set("n"), expr))),
      patternNodes = Set("n"),
      argumentIds = Set("arr", "n")
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(config)).thenReturn((plan: LogicalPlan, input: QueryGraphSolverInput, _: Cardinalities) => plan match {
      case _: NodeByIdSeek => Cost(1)
      case _               => Cost(Double.MaxValue)
    })
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext,
      metrics = factory.newMetrics(statistics, evaluator, config)
    )
    when(context.semanticTable.isNode(variable)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(qg, context, solveds, cardinalities)

    // then
    resultPlans should equal(Seq.empty)
  }

  test("simple directed relationship by id seek with a collection of relationship ids") {
    // given
    val rIdent: Variable = Variable("r")_
    val expr = In(
      FunctionInvocation(FunctionName("id")_, distinct = false, Array(rIdent))_,
      ListLiteral(
        Seq(SignedDecimalIntegerLiteral("42")_, SignedDecimalIntegerLiteral("43")_, SignedDecimalIntegerLiteral("43")_)
      )_
    )_
    val from = "from"
    val end = "to"
    val patternRel = PatternRelationship("r", (from, end), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set("r"), expr))),
      patternNodes = Set(from, end),
      patternRelationships = Set(patternRel)
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(config)).thenReturn((plan: LogicalPlan, input: QueryGraphSolverInput, _: Cardinalities) => plan match {
      case _: DirectedRelationshipByIdSeek => Cost(1)
      case _                               => Cost(Double.MaxValue)
    })
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext,
      metrics = factory.newMetrics(statistics, evaluator, config)
    )
    when(context.semanticTable.isRelationship(rIdent)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(qg, context, solveds, cardinalities)

    // then
    resultPlans should equal(Seq(DirectedRelationshipByIdSeek("r", ManySeekableArgs(ListLiteral(Seq(
      SignedDecimalIntegerLiteral("42")_, SignedDecimalIntegerLiteral("43")_, SignedDecimalIntegerLiteral("43")_
    ))_), from, end, Set.empty)))
  }

  test("simple undirected relationship by id seek with a collection of relationship ids") {
    // given
    val rIdent: Variable = Variable("r")_
    val expr = In(
      FunctionInvocation(FunctionName("id")_, distinct = false, Array(rIdent))_,
      ListLiteral(
        Seq(SignedDecimalIntegerLiteral("42")_, SignedDecimalIntegerLiteral("43")_, SignedDecimalIntegerLiteral("43")_)
      )_
    )_
    val from = "from"
    val end = "to"
    val patternRel = PatternRelationship("r", (from, end), SemanticDirection.BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set("r"), expr))),
      patternNodes = Set(from, end),
      patternRelationships = Set(patternRel))

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(config)).thenReturn((plan: LogicalPlan, input: QueryGraphSolverInput, _: Cardinalities) => plan match {
      case _: UndirectedRelationshipByIdSeek => Cost(2)
      case _                                 => Cost(Double.MaxValue)
    })
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext,
      metrics = factory.newMetrics(statistics, evaluator, config)
    )
    when(context.semanticTable.isRelationship(rIdent)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(qg, context, solveds, cardinalities)

    // then
    resultPlans should equal(Seq(UndirectedRelationshipByIdSeek("r", ManySeekableArgs(ListLiteral(Seq(
      SignedDecimalIntegerLiteral("42")_, SignedDecimalIntegerLiteral("43")_, SignedDecimalIntegerLiteral("43")_
    ))_), from, end, Set.empty)))
  }

  test("simple undirected typed relationship by id seek with a collection of relationship ids") {
    // given
    val rIdent: Variable = Variable("r")_
    val expr = In(
      FunctionInvocation(FunctionName("id")_, distinct = false, Array(rIdent))_,
      ListLiteral(Seq(SignedDecimalIntegerLiteral("42")_))_
    )_
    val from = "from"
    val end = "to"
    val relTypeX = RelTypeName("X")(pos)

    val semanticTable =
      new SemanticTable(ASTAnnotationMap(
        rIdent -> ExpressionTypeInfo(symbols.CTRelationship)
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
    when(factory.newCostModel(config)).thenReturn((plan: LogicalPlan, input: QueryGraphSolverInput, _: Cardinalities) => plan match {
      case _: UndirectedRelationshipByIdSeek => Cost(2)
      case _                                 => Cost(Double.MaxValue)
    })
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext,
      metrics = factory.newMetrics(statistics, evaluator, config),
      semanticTable = semanticTable
    )

    // when
    val resultPlans = idSeekLeafPlanner(qg, context, solveds, cardinalities)

    // then
    resultPlans should equal(
      Seq(Selection(
        Seq(Equals(FunctionInvocation(FunctionName("type")_, rIdent)_, StringLiteral("X")_)_),
        UndirectedRelationshipByIdSeek("r", ManySeekableArgs(ListLiteral(Seq(SignedDecimalIntegerLiteral("42")_))_), from, end, Set.empty)
      ))
    )
  }

  test("simple undirected multi-typed relationship by id seek with  a collection of relationship ids") {
    // given
    val rIdent: Variable = Variable("r")_
    val expr = In(
      FunctionInvocation(FunctionName("id")_, distinct = false, Array(rIdent))_,
      ListLiteral(Seq(SignedDecimalIntegerLiteral("42")_))_
    )_
    val from = "from"
    val end = "to"
    val relTypeX = RelTypeName("X") _
    val relTypeY = RelTypeName("Y") _

    val semanticTable =
      new SemanticTable(ASTAnnotationMap(
        rIdent -> ExpressionTypeInfo(symbols.CTRelationship)
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
    when(factory.newCostModel(config)).thenReturn((plan: LogicalPlan, input: QueryGraphSolverInput, _: Cardinalities) => plan match {
      case _: UndirectedRelationshipByIdSeek => Cost(2)
      case _                                 => Cost(Double.MaxValue)
    })
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext,
      metrics = factory.newMetrics(statistics, evaluator, config),
      semanticTable = semanticTable
    )

    // when
    val resultPlans = idSeekLeafPlanner(qg, context, solveds, cardinalities)

    // then
    resultPlans should equal(
      Seq(Selection(
        Seq(
          Ors(Set(
            Equals(FunctionInvocation(FunctionName("type")_, rIdent)_, StringLiteral("X")_)(pos),
            Equals(FunctionInvocation(FunctionName("type")_, rIdent)_, StringLiteral("Y")_)(pos)
          ))_
        ),
        UndirectedRelationshipByIdSeek("r", ManySeekableArgs(ListLiteral(Seq(SignedDecimalIntegerLiteral("42")_))_), from, end, Set.empty)
    )))
  }
}
