/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_3.planner._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Cost
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps.idSeekLeafPlanner
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{RelTypeId, SemanticDirection}

import scala.collection.mutable

class IdSeekLeafPlannerTest extends CypherFunSuite  with LogicalPlanningTestSupport {

  private val statistics = hardcodedStatistics
  private implicit val subQueryLookupTable = Map.empty[PatternExpression, QueryGraph]

  // NOTE: the ronja rewriters make sure that all EQUALS will be rewritten to IN so here only the latter should be tested

  test("simple node by id seek with a collection of node ids") {
    // given
    val identifier: Identifier = Identifier("n")_
    val expr = In(
      FunctionInvocation(FunctionName("id")_, distinct = false, Array(identifier))_,
      Collection(
        Seq(SignedDecimalIntegerLiteral("42")_, SignedDecimalIntegerLiteral("43")_, SignedDecimalIntegerLiteral("43")_)
      )_
    )_
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set(IdName("n")), expr))),
      patternNodes = Set(IdName("n"))
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel()).thenReturn((plan: LogicalPlan, input: QueryGraphSolverInput) => plan match {
      case _: NodeByIdSeek => Cost(1)
      case _               => Cost(Double.MaxValue)
    })
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext,
      metrics = factory.newMetrics(statistics)
    )
    when(context.semanticTable.isNode(identifier)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(qg)

    // then
    resultPlans should equal(
      Seq(NodeByIdSeek(IdName("n"), ManySeekableArgs(Collection(Seq(
        SignedDecimalIntegerLiteral("42")_, SignedDecimalIntegerLiteral("43")_, SignedDecimalIntegerLiteral("43")_
      ))_), Set.empty)(solved))
    )
  }

  test("node by id seek with a collection of node ids via previous identifier") {
    // given
    val identifier: Identifier = Identifier("n")_
    val expr = In(
      FunctionInvocation(FunctionName("id")_, identifier)_,
      Identifier("arr")_
    )_
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set(IdName("n")), expr))),
      patternNodes = Set(IdName("n")),
      argumentIds = Set(IdName("arr"))
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel()).thenReturn((plan: LogicalPlan, input: QueryGraphSolverInput) => plan match {
      case _: NodeByIdSeek => Cost(1)
      case _               => Cost(Double.MaxValue)
    })
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext,
      metrics = factory.newMetrics(statistics)
    )
    when(context.semanticTable.isNode(identifier)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(qg)

    // then
    resultPlans should equal(
      Seq(NodeByIdSeek(IdName("n"), ManySeekableArgs(Identifier("arr")_), Set("arr"))(solved))
    )
  }

  test("node by id seek should not be produced when the argument expression is an unbound identifier") {
    // given match n where id(n) in arr
    val identifier: Identifier = Identifier("n")_
    val expr = In(
      FunctionInvocation(FunctionName("id")_, identifier)_,
      Identifier("arr")_
    )_
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set(IdName("n")), expr))),
      patternNodes = Set(IdName("n")),
      argumentIds = Set()
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel()).thenReturn((plan: LogicalPlan, input: QueryGraphSolverInput) => plan match {
      case _: NodeByIdSeek => Cost(1)
      case _               => Cost(Double.MaxValue)
    })
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext,
      metrics = factory.newMetrics(statistics)
    )
    when(context.semanticTable.isNode(identifier)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(qg)

    // then
    resultPlans should equal(Seq.empty)
  }

  test("node by id seek should not be produced when the node identifier is an argument") {
    // given match n where id(n) in arr
    val identifier: Identifier = Identifier("n")_
    val expr = In(
      FunctionInvocation(FunctionName("id")_, identifier)_,
      Identifier("arr")_
    )_
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set(IdName("n")), expr))),
      patternNodes = Set(IdName("n")),
      argumentIds = Set(IdName("arr"), IdName("n"))
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel()).thenReturn((plan: LogicalPlan, input: QueryGraphSolverInput) => plan match {
      case _: NodeByIdSeek => Cost(1)
      case _               => Cost(Double.MaxValue)
    })
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext,
      metrics = factory.newMetrics(statistics)
    )
    when(context.semanticTable.isNode(identifier)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(qg)

    // then
    resultPlans should equal(Seq.empty)
  }

  test("simple directed relationship by id seek with a collection of relationship ids") {
    // given
    val rIdent: Identifier = Identifier("r")_
    val expr = In(
      FunctionInvocation(FunctionName("id")_, distinct = false, Array(rIdent))_,
      Collection(
        Seq(SignedDecimalIntegerLiteral("42")_, SignedDecimalIntegerLiteral("43")_, SignedDecimalIntegerLiteral("43")_)
      )_
    )_
    val from = IdName("from")
    val end = IdName("to")
    val patternRel = PatternRelationship(IdName("r"), (from, end), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set(IdName("r")), expr))),
      patternNodes = Set(from, end),
      patternRelationships = Set(patternRel)
    )

    val factory = newMockedMetricsFactory
    when(factory.newCostModel()).thenReturn((plan: LogicalPlan, input: QueryGraphSolverInput) => plan match {
      case _: DirectedRelationshipByIdSeek => Cost(1)
      case _                               => Cost(Double.MaxValue)
    })
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext,
      metrics = factory.newMetrics(statistics)
    )
    when(context.semanticTable.isRelationship(rIdent)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(qg)

    // then
    resultPlans should equal(Seq(DirectedRelationshipByIdSeek(IdName("r"), ManySeekableArgs(Collection(Seq(
      SignedDecimalIntegerLiteral("42")_, SignedDecimalIntegerLiteral("43")_, SignedDecimalIntegerLiteral("43")_
    ))_), from, end, Set.empty)(solved)))
  }

  test("simple undirected relationship by id seek with a collection of relationship ids") {
    // given
    val rIdent: Identifier = Identifier("r")_
    val expr = In(
      FunctionInvocation(FunctionName("id")_, distinct = false, Array(rIdent))_,
      Collection(
        Seq(SignedDecimalIntegerLiteral("42")_, SignedDecimalIntegerLiteral("43")_, SignedDecimalIntegerLiteral("43")_)
      )_
    )_
    val from = IdName("from")
    val end = IdName("to")
    val patternRel = PatternRelationship(IdName("r"), (from, end), SemanticDirection.BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set(IdName("r")), expr))),
      patternNodes = Set(from, end),
      patternRelationships = Set(patternRel))

    val factory = newMockedMetricsFactory
    when(factory.newCostModel()).thenReturn((plan: LogicalPlan, input: QueryGraphSolverInput) => plan match {
      case _: UndirectedRelationshipByIdSeek => Cost(2)
      case _                                 => Cost(Double.MaxValue)
    })
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext,
      metrics = factory.newMetrics(statistics)
    )
    when(context.semanticTable.isRelationship(rIdent)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(qg)

    // then
    resultPlans should equal(Seq(UndirectedRelationshipByIdSeek(IdName("r"), ManySeekableArgs(Collection(Seq(
      SignedDecimalIntegerLiteral("42")_, SignedDecimalIntegerLiteral("43")_, SignedDecimalIntegerLiteral("43")_
    ))_), from, end, Set.empty)(solved)))
  }

  test("simple undirected typed relationship by id seek with a collection of relationship ids") {
    // given
    val rIdent: Identifier = Identifier("r")_
    val expr = In(
      FunctionInvocation(FunctionName("id")_, distinct = false, Array(rIdent))_,
      Collection(Seq(SignedDecimalIntegerLiteral("42")_))_
    )_
    val from = IdName("from")
    val end = IdName("to")

    val semanticTable = newMockedSemanticTable
    when(semanticTable.resolvedRelTypeNames).thenReturn(mutable.Map("X" -> RelTypeId(1)))

    val patternRel = PatternRelationship(
      IdName("r"), (from, end), SemanticDirection.BOTH,
      Seq(RelTypeName("X")_),
      SimplePatternLength
    )
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set(IdName("r")), expr))),
      patternNodes = Set(from, end),
      patternRelationships = Set(patternRel))

    val factory = newMockedMetricsFactory
    when(factory.newCostModel()).thenReturn((plan: LogicalPlan, input: QueryGraphSolverInput) => plan match {
      case _: UndirectedRelationshipByIdSeek => Cost(2)
      case _                                 => Cost(Double.MaxValue)
    })
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext,
      metrics = factory.newMetrics(statistics)
    )
    when(context.semanticTable.isRelationship(rIdent)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(qg)

    // then
    resultPlans should equal(
      Seq(Selection(
        Seq(Equals(FunctionInvocation(FunctionName("type")_, rIdent)_, StringLiteral("X")_)_),
        UndirectedRelationshipByIdSeek(IdName("r"), ManySeekableArgs(Collection(Seq(SignedDecimalIntegerLiteral("42")_))_), from, end, Set.empty)(solved)
      )(solved))
    )
  }

  test("simple undirected multi-typed relationship by id seek with  a collection of relationship ids") {
    // given
    val rIdent: Identifier = Identifier("r")_
    val expr = In(
      FunctionInvocation(FunctionName("id")_, distinct = false, Array(rIdent))_,
      Collection(Seq(SignedDecimalIntegerLiteral("42")_))_
    )_
    val from = IdName("from")
    val end = IdName("to")


    val semanticTable = newMockedSemanticTable
    when(semanticTable.resolvedRelTypeNames).thenReturn(mutable.Map("X" -> RelTypeId(1), "Y" -> RelTypeId(2)))

    val patternRel = PatternRelationship(
      IdName("r"), (from, end), SemanticDirection.BOTH,
      Seq[RelTypeName](RelTypeName("X")_, RelTypeName("Y")_),
      SimplePatternLength
    )
    val qg = QueryGraph(
      selections = Selections(Set(Predicate(Set(IdName("r")), expr))),
      patternNodes = Set(from, end),
      patternRelationships = Set(patternRel))

    val factory = newMockedMetricsFactory
    when(factory.newCostModel()).thenReturn((plan: LogicalPlan, input: QueryGraphSolverInput) => plan match {
      case _: UndirectedRelationshipByIdSeek => Cost(2)
      case _                                 => Cost(Double.MaxValue)
    })
    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext,
      metrics = factory.newMetrics(statistics)
    )
    when(context.semanticTable.isRelationship(rIdent)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(qg)

    // then
    resultPlans should equal(
      Seq(Selection(
        Seq(
          Ors(Set(
            Equals(FunctionInvocation(FunctionName("type")_, rIdent)_, StringLiteral("X")_)(pos),
            Equals(FunctionInvocation(FunctionName("type")_, rIdent)_, StringLiteral("Y")_)(pos)
          ))_
        ),
        UndirectedRelationshipByIdSeek(IdName("r"), ManySeekableArgs(Collection(Seq(SignedDecimalIntegerLiteral("42")_))_), from, end, Set.empty)(solved)
    )(solved)))
  }
}
