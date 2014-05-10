/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans

import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps.idSeekLeafPlanner
import org.neo4j.cypher.internal.compiler.v2_1.RelTypeId
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.Candidates
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps.QueryPlanProducer._

import org.mockito.Matchers._
import org.mockito.Mockito._
import scala.collection.mutable

class IdSeekLeafPlannerTest extends CypherFunSuite  with LogicalPlanningTestSupport {

  private val statistics = newMockedStatistics

  test("simple node by id seek with a node id expression") {
    // given
    val identifier: Identifier = Identifier("n")_
    val projections: Map[String, Expression] = Map("n" -> identifier)
    val expr = Equals(
      FunctionInvocation(FunctionName("id")_, distinct = false, Array(identifier))_,
      SignedIntegerLiteral("42")_
    )_
    val qg = QueryGraph(
      projection = QueryProjection(projections = projections),
      selections = Selections(Set(Predicate(Set(IdName("n")), expr))),
      patternNodes = Set(IdName("n")))

    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: NodeByIdSeek => 1
      case _               => Double.MaxValue
    })
    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = qg,
      metrics = factory.newMetrics(statistics, newMockedSemanticTable)
    )
    when(context.semanticTable.isNode(identifier)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(qg)

    // then
    resultPlans should equal(Candidates(
      planNodeByIdSeek(IdName("n"), Seq(SignedIntegerLiteral("42")_), Seq(expr))
    ))
  }

  test("simple node by id seek with a collection of node ids") {
    // given
    val identifier: Identifier = Identifier("n")_
    val projections: Map[String, Expression] = Map("n" -> identifier)
    val expr = In(
      FunctionInvocation(FunctionName("id")_, distinct = false, Array(identifier))_,
      Collection(
        Seq(SignedIntegerLiteral("42")_, SignedIntegerLiteral("43")_, SignedIntegerLiteral("43")_)
      )_
    )_
    val qg = QueryGraph(
      projection = QueryProjection(projections = projections),
      selections = Selections(Set(Predicate(Set(IdName("n")), expr))),
      patternNodes = Set(IdName("n")))

    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: NodeByIdSeek => 1
      case _               => Double.MaxValue
    })
    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = qg,
      metrics = factory.newMetrics(statistics, newMockedSemanticTable)
    )
    when(context.semanticTable.isNode(identifier)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(qg)

    // then
    resultPlans should equal(Candidates(
      planNodeByIdSeek(IdName("n"), Seq(
        SignedIntegerLiteral("42")_, SignedIntegerLiteral("43")_, SignedIntegerLiteral("43")_
      ), Seq(expr))
    ))
  }

  test("simple directed relationship by id seek with a rel id expression") {
    // given
    val rIdent: Identifier = Identifier("r")_
    val fromIdent: Identifier = Identifier("from")_
    val toIdent: Identifier = Identifier("to")_
    val projections: Map[String, Expression] = Map("r" -> rIdent, "from" -> fromIdent, "to" -> toIdent)
    val expr = Equals(
      FunctionInvocation(FunctionName("id")_, distinct = false, Array(rIdent))_,
      SignedIntegerLiteral("42")_
    )_
    val from = IdName("from")
    val end = IdName("to")
    val patternRel = PatternRelationship(IdName("r"), (from, end), Direction.OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      projection = QueryProjection(projections = projections),
      selections = Selections(Set(Predicate(Set(IdName("r")), expr))),
      patternNodes = Set(from, end),
      patternRelationships = Set(patternRel))

    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: DirectedRelationshipByIdSeek => 1
      case _                               => Double.MaxValue
    })
    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = qg,
      metrics = factory.newMetrics(statistics, newMockedSemanticTable)
    )
    when(context.semanticTable.isRelationship(rIdent)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(qg)

    // then
    resultPlans should equal(Candidates(planDirectedRelationshipByIdSeek(IdName("r"), Seq(SignedIntegerLiteral("42")_), from, end, patternRel, Seq(expr))))
  }

  test("simple undirected relationship by id seek with a rel id expression") {
    // given
    val rIdent: Identifier = Identifier("r")_
    val fromIdent: Identifier = Identifier("from")_
    val toIdent: Identifier = Identifier("to")_
    val projections: Map[String, Expression] = Map("r" -> rIdent, "from" -> fromIdent, "to" -> toIdent)
    val expr = Equals(
      FunctionInvocation(FunctionName("id")_, distinct = false, Array(rIdent))_,
      SignedIntegerLiteral("42")_
    )_
    val from = IdName("from")
    val end = IdName("to")
    val patternRel = PatternRelationship(IdName("r"), (from, end), Direction.BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      projection = QueryProjection(projections = projections),
      selections = Selections(Set(Predicate(Set(IdName("r")), expr))),
      patternNodes = Set(from, end),
      patternRelationships = Set(patternRel))

    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: UndirectedRelationshipByIdSeek => 2
      case _                                 => Double.MaxValue
    })
    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = qg,
      metrics = factory.newMetrics(statistics, newMockedSemanticTable)
    )

    when(context.semanticTable.isRelationship(rIdent)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(qg)

    // then
    resultPlans should equal(Candidates(planUndirectedRelationshipByIdSeek(IdName("r"), Seq(SignedIntegerLiteral("42")_), from, end, patternRel, Seq(expr))))
  }

  test("simple directed relationship by id seek with a collection of relationship ids") {
    // given
    val rIdent: Identifier = Identifier("r")_
    val fromIdent: Identifier = Identifier("from")_
    val toIdent: Identifier = Identifier("to")_
    val projections: Map[String, Expression] = Map("r" -> rIdent, "from" -> fromIdent, "to" -> toIdent)
    val expr = In(
      FunctionInvocation(FunctionName("id")_, distinct = false, Array(rIdent))_,
      Collection(
        Seq(SignedIntegerLiteral("42")_, SignedIntegerLiteral("43")_, SignedIntegerLiteral("43")_)
      )_
    )_
    val from = IdName("from")
    val end = IdName("to")
    val patternRel = PatternRelationship(IdName("r"), (from, end), Direction.OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      projection = QueryProjection(projections = projections),
      selections = Selections(Set(Predicate(Set(IdName("r")), expr))),
      patternNodes = Set(from, end),
      patternRelationships = Set(patternRel))

    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: DirectedRelationshipByIdSeek => 1
      case _                               => Double.MaxValue
    })
    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = qg,
      metrics = factory.newMetrics(statistics, newMockedSemanticTable)
    )
    when(context.semanticTable.isRelationship(rIdent)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(qg)

    // then
    resultPlans should equal(Candidates(planDirectedRelationshipByIdSeek(IdName("r"), Seq(
      SignedIntegerLiteral("42")_, SignedIntegerLiteral("43")_, SignedIntegerLiteral("43")_
    ), from, end, patternRel, Seq(expr))))
  }

  test("simple undirected relationship by id seek with a collection of relationship ids") {
    // given
    val rIdent: Identifier = Identifier("r")_
    val fromIdent: Identifier = Identifier("from")_
    val toIdent: Identifier = Identifier("to")_
    val projections: Map[String, Expression] = Map("r" -> rIdent, "from" -> fromIdent, "to" -> toIdent)
    val expr = In(
      FunctionInvocation(FunctionName("id")_, distinct = false, Array(rIdent))_,
      Collection(
        Seq(SignedIntegerLiteral("42")_, SignedIntegerLiteral("43")_, SignedIntegerLiteral("43")_)
      )_
    )_
    val from = IdName("from")
    val end = IdName("to")
    val patternRel = PatternRelationship(IdName("r"), (from, end), Direction.BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      projection = QueryProjection(projections = projections),
      selections = Selections(Set(Predicate(Set(IdName("r")), expr))),
      patternNodes = Set(from, end),
      patternRelationships = Set(patternRel))

    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: UndirectedRelationshipByIdSeek => 2
      case _                                 => Double.MaxValue
    })
    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = qg,
      metrics = factory.newMetrics(statistics, newMockedSemanticTable)
    )
    when(context.semanticTable.isRelationship(rIdent)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(qg)

    // then
    resultPlans should equal(Candidates(planUndirectedRelationshipByIdSeek(IdName("r"), Seq(
      SignedIntegerLiteral("42")_, SignedIntegerLiteral("43")_, SignedIntegerLiteral("43")_
    ), from, end, patternRel, Seq(expr))))
  }

  test("simple undirected typed relationship by id seek with a rel id expression") {
    // given
    val rIdent: Identifier = Identifier("r")_
    val fromIdent: Identifier = Identifier("from")_
    val toIdent: Identifier = Identifier("to")_
    val projections: Map[String, Expression] = Map("r" -> rIdent, "from" -> fromIdent, "to" -> toIdent)
    val expr = Equals(
      FunctionInvocation(FunctionName("id")_, distinct = false, Array(rIdent))_,
      SignedIntegerLiteral("42")_
    )_
    val from = IdName("from")
    val end = IdName("to")

    val semanticTable = newMockedSemanticTable
    when(semanticTable.resolvedRelTypeNames).thenReturn(mutable.Map("X" -> RelTypeId(1)))

    val patternRel = PatternRelationship(
      IdName("r"), (from, end), Direction.BOTH,
      Seq(RelTypeName("X")_),
      SimplePatternLength
    )
    val qg = QueryGraph(
      projection = QueryProjection(projections = projections),
      selections = Selections(Set(Predicate(Set(IdName("r")), expr))),
      patternNodes = Set(from, end),
      patternRelationships = Set(patternRel))

    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: UndirectedRelationshipByIdSeek => 2
      case _                                 => Double.MaxValue
    })
    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = qg,
      metrics = factory.newMetrics(statistics, semanticTable)
    )
    when(context.semanticTable.isRelationship(rIdent)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(qg)

    // then
    resultPlans should equal(Candidates(
      planHiddenSelection(
        Seq(Equals(FunctionInvocation(FunctionName("type")_, rIdent)_, StringLiteral("X")_)_),
        planUndirectedRelationshipByIdSeek(IdName("r"), Seq(SignedIntegerLiteral("42")_), from, end, patternRel, Seq(expr))
      )
    ))
  }

  test("simple undirected multi-typed relationship by id seek with a rel id expression") {
    // given
    val rIdent: Identifier = Identifier("r")_
    val fromIdent: Identifier = Identifier("from")_
    val toIdent: Identifier = Identifier("to")_
    val projections: Map[String, Expression] = Map("r" -> rIdent, "from" -> fromIdent, "to" -> toIdent)
    val expr = Equals(
      FunctionInvocation(FunctionName("id")_, distinct = false, Array(rIdent))_,
      SignedIntegerLiteral("42")_
    )_
    val from = IdName("from")
    val end = IdName("to")


    val semanticTable = newMockedSemanticTable
    when(semanticTable.resolvedRelTypeNames).thenReturn(mutable.Map("X" -> RelTypeId(1), "Y" -> RelTypeId(2)))

    val patternRel = PatternRelationship(
      IdName("r"), (from, end), Direction.BOTH,
      Seq[RelTypeName](RelTypeName("X")_, RelTypeName("Y")_),
      SimplePatternLength
    )
    val qg = QueryGraph(
      projection = QueryProjection(projections = projections),
      selections = Selections(Set(Predicate(Set(IdName("r")), expr))),
      patternNodes = Set(from, end),
      patternRelationships = Set(patternRel))

    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: UndirectedRelationshipByIdSeek => 2
      case _                                 => Double.MaxValue
    })
    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = qg,
      metrics = factory.newMetrics(statistics, semanticTable)
    )
    when(context.semanticTable.isRelationship(rIdent)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(qg)

    // then
    resultPlans should equal(Candidates(
      planHiddenSelection(
        Seq(
          Ors(List(
            Equals(FunctionInvocation(FunctionName("type")_, rIdent)_, StringLiteral("X")_)(pos),
            Equals(FunctionInvocation(FunctionName("type")_, rIdent)_, StringLiteral("Y")_)(pos)
          ))_
        ),
        planUndirectedRelationshipByIdSeek(IdName("r"), Seq(SignedIntegerLiteral("42")_), from, end, patternRel, Seq(expr))
    )))
  }
}
