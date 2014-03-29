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

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.RelTypeId
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.idSeekLeafPlanner
import org.mockito.Mockito._
import org.neo4j.graphdb.Direction

class IdSeekLeafPlannerTest extends CypherFunSuite  with LogicalPlanningTestSupport {

  test("simple node by id seek with a node id expression") {
    // given
    val identifier: Identifier = Identifier("n")_
    val projections: Map[String, Expression] = Map("n" -> identifier)
    val expr = Equals(
      FunctionInvocation(FunctionName("id")_, distinct = false, Array(identifier))_,
      SignedIntegerLiteral("42")_
    )_
    val qg = QueryGraph(projections, Selections(Seq(Set(IdName("n")) -> expr)), Set(IdName("n")), Set.empty)

    implicit val context = newMockedLogicalPlanContext(
      queryGraph = qg,
      metrics = newMetricsFactory.replaceCardinalityEstimator {
        case _: NodeByIdSeek => 1
      }.newMetrics
    )
    when(context.semanticTable.isNode(identifier)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(Seq(expr))()

    // then
    resultPlans should equal(Seq(NodeByIdSeek(IdName("n"), Seq(SignedIntegerLiteral("42")_))()))
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
    val qg = QueryGraph(projections, Selections(Seq(Set(IdName("n")) -> expr)), Set(IdName("n")), Set.empty)

    implicit val context = newMockedLogicalPlanContext(
      queryGraph = qg,
      metrics = newMetricsFactory.replaceCardinalityEstimator {
        case _: NodeByIdSeek => 1
      }.newMetrics
    )
    when(context.semanticTable.isNode(identifier)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(Seq(expr))()

    // then
    resultPlans should equal(Seq(NodeByIdSeek(IdName("n"), Seq(
      SignedIntegerLiteral("42")_, SignedIntegerLiteral("43")_, SignedIntegerLiteral("43")_
    ))()))
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
    val patternRel = PatternRelationship(IdName("r"), (from, end), Direction.OUTGOING, Seq.empty)
    val qg = QueryGraph(projections, Selections(Seq(Set(IdName("r")) -> expr)), Set(from, end), Set(patternRel))

    implicit val context = newMockedLogicalPlanContext(
      queryGraph = qg,
      metrics = newMetricsFactory.replaceCardinalityEstimator {
        case _: DirectedRelationshipByIdSeek => 1
      }.newMetrics
    )
    when(context.semanticTable.isRelationship(rIdent)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(Seq(expr))()

    // then
    resultPlans should equal(Seq(DirectedRelationshipByIdSeek(IdName("r"), Seq(SignedIntegerLiteral("42")_), from, end)()))
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
    val patternRel = PatternRelationship(IdName("r"), (from, end), Direction.BOTH, Seq.empty)
    val qg = QueryGraph(projections, Selections(Seq(Set(IdName("r")) -> expr)), Set(from, end), Set(patternRel))

    implicit val context = newMockedLogicalPlanContext(
      queryGraph = qg,
      metrics = newMetricsFactory.replaceCardinalityEstimator {
        case _: UndirectedRelationshipByIdSeek => 2
      }.newMetrics
    )

    when(context.semanticTable.isRelationship(rIdent)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(Seq(expr))()

    // then
    resultPlans should equal(Seq(UndirectedRelationshipByIdSeek(IdName("r"), Seq(SignedIntegerLiteral("42")_), from, end)()))
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
    val patternRel = PatternRelationship(IdName("r"), (from, end), Direction.OUTGOING, Seq.empty)
    val qg = QueryGraph(projections, Selections(Seq(Set(IdName("r")) -> expr)), Set(from, end), Set(patternRel))

    implicit val context = newMockedLogicalPlanContext(
      queryGraph = qg,
      metrics = newMetricsFactory.replaceCardinalityEstimator {
        case _: DirectedRelationshipByIdSeek => 1
      }.newMetrics
    )
    when(context.semanticTable.isRelationship(rIdent)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(Seq(expr))()

    // then
    resultPlans should equal(Seq(DirectedRelationshipByIdSeek(IdName("r"), Seq(
      SignedIntegerLiteral("42")_, SignedIntegerLiteral("43")_, SignedIntegerLiteral("43")_
    ), from, end)()))
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
    val patternRel = PatternRelationship(IdName("r"), (from, end), Direction.BOTH, Seq.empty)
    val qg = QueryGraph(projections, Selections(Seq(Set(IdName("r")) -> expr)), Set(from, end), Set(patternRel))

    implicit val context = newMockedLogicalPlanContext(
      queryGraph = qg,
      metrics = newMetricsFactory.replaceCardinalityEstimator {
        case _: UndirectedRelationshipByIdSeek => 2
      }.newMetrics
    )
    when(context.semanticTable.isRelationship(rIdent)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(Seq(expr))()

    // then
    resultPlans should equal(Seq(UndirectedRelationshipByIdSeek(IdName("r"), Seq(
      SignedIntegerLiteral("42")_, SignedIntegerLiteral("43")_, SignedIntegerLiteral("43")_
    ), from, end)()))
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
    val patternRel = PatternRelationship(
      IdName("r"), (from, end), Direction.BOTH,
      Seq(RelTypeName("X")(Some(RelTypeId(1)))_)
    )
    val qg = QueryGraph(projections, Selections(Seq(Set(IdName("r")) -> expr)), Set(from, end), Set(patternRel))

    implicit val context = newMockedLogicalPlanContext(
      queryGraph = qg,
      metrics = newMetricsFactory.replaceCardinalityEstimator {
        case _: UndirectedRelationshipByIdSeek => 2
      }.newMetrics
    )
    when(context.semanticTable.isRelationship(rIdent)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(Seq(expr))()

    // then
    resultPlans should equal(Seq(
      Selection(
        Seq(Equals(FunctionInvocation(FunctionName("type")_, rIdent)_, StringLiteral("X")_)_),
        UndirectedRelationshipByIdSeek(IdName("r"), Seq(SignedIntegerLiteral("42")_), from, end)()
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
    val patternRel = PatternRelationship(
      IdName("r"), (from, end), Direction.BOTH,
      Seq(
        RelTypeName("X")(Some(RelTypeId(1)))_,
        RelTypeName("Y")(Some(RelTypeId(2)))_
      )
    )
    val qg = QueryGraph(projections, Selections(Seq(Set(IdName("r")) -> expr)), Set(from, end), Set(patternRel))

    implicit val context = newMockedLogicalPlanContext(
      queryGraph = qg,
      metrics = newMetricsFactory.replaceCardinalityEstimator {
        case _: UndirectedRelationshipByIdSeek => 2
      }.newMetrics
    )
    when(context.semanticTable.isRelationship(rIdent)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(Seq(expr))()

    // then
    resultPlans should equal(Seq(
      Selection(
        Seq[Or](
          Or(
            Equals(FunctionInvocation(FunctionName("type")_, rIdent)_, StringLiteral("X")_)_,
            Equals(FunctionInvocation(FunctionName("type")_, rIdent)_, StringLiteral("Y")_)_
          )_
        ),
        UndirectedRelationshipByIdSeek(IdName("r"), Seq(SignedIntegerLiteral("42")_), from, end)()
      )
    ))
  }
}
