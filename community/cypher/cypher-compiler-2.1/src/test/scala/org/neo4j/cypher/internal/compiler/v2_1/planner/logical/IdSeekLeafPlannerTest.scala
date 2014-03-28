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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.{InputPosition, RelTypeId, DummyPosition}
import org.mockito.Mockito._
import org.neo4j.graphdb.Direction

class IdSeekLeafPlannerTest extends CypherFunSuite  with LogicalPlanningTestSupport {

  val pos = DummyPosition(0)

  test("simple node by id seek with a node id expression") {
    // given
    val identifier = Identifier("n")(pos)
    val projections = Map("n" -> identifier)
    val expr = Equals(
      FunctionInvocation(FunctionName("id")(pos), distinct = false, Array(identifier))(pos),
      SignedIntegerLiteral("42")(pos)
    )(pos)
    val qg = QueryGraph(projections, Selections(Seq(Set(IdName("n")) -> expr)), Set(IdName("n")), Set.empty)

    implicit val context = newMockedLogicalPlanContext(
      queryGraph = qg,
      estimator = CardinalityEstimator.lift {
        case _: NodeByIdSeek => 1
      }
    )
    when(context.semanticTable.isNode(identifier)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(Seq(expr))()

    // then
    resultPlans should equal(Seq(NodeByIdSeek(IdName("n"), Seq(SignedIntegerLiteral("42")(pos)))()))
  }

  test("simple node by id seek with a collection of node ids") {
    // given
    val identifier = Identifier("n")(pos)
    val projections = Map("n" -> identifier)
    val expr = In(
      FunctionInvocation(FunctionName("id")(pos), distinct = false, Array(identifier))(pos),
      Collection(
        Seq(SignedIntegerLiteral("42")(pos), SignedIntegerLiteral("43")(pos), SignedIntegerLiteral("43")(pos))
      )(pos)
    )(pos)
    val qg = QueryGraph(projections, Selections(Seq(Set(IdName("n")) -> expr)), Set(IdName("n")), Set.empty)

    implicit val context = newMockedLogicalPlanContext(
      queryGraph = qg,
      estimator = CardinalityEstimator.lift {
        case _: NodeByIdSeek => 1
      }
    )
    when(context.semanticTable.isNode(identifier)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(Seq(expr))()

    // then
    resultPlans should equal(Seq(NodeByIdSeek(IdName("n"), Seq(
      SignedIntegerLiteral("42")(pos), SignedIntegerLiteral("43")(pos), SignedIntegerLiteral("43")(pos)
    ))()))
  }

  test("simple directed relationship by id seek with a rel id expression") {
    // given
    val rIdent = Identifier("r")(pos)
    val fromIdent = Identifier("from")(pos)
    val toIdent = Identifier("to")(pos)
    val projections = Map("r" -> rIdent, "from" -> fromIdent, "to" -> toIdent)
    val expr = Equals(
      FunctionInvocation(FunctionName("id")(pos), distinct = false, Array(rIdent))(pos),
      SignedIntegerLiteral("42")(pos)
    )(pos)
    val from = IdName("from")
    val end = IdName("to")
    val patternRel = PatternRelationship(IdName("r"), (from, end), Direction.OUTGOING, Seq.empty)
    val qg = QueryGraph(projections, Selections(Seq(Set(IdName("r")) -> expr)), Set(from, end), Set(patternRel))

    implicit val context = newMockedLogicalPlanContext(
      queryGraph = qg,
      estimator = CardinalityEstimator.lift {
        case _: DirectedRelationshipByIdSeek => 1
      }
    )
    when(context.semanticTable.isRelationship(rIdent)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(Seq(expr))()

    // then
    resultPlans should equal(Seq(DirectedRelationshipByIdSeek(IdName("r"), Seq(SignedIntegerLiteral("42")(pos)), from, end)()))
  }

  test("simple undirected relationship by id seek with a rel id expression") {
    // given
    val rIdent = Identifier("r")(pos)
    val fromIdent = Identifier("from")(pos)
    val toIdent = Identifier("to")(pos)
    val projections = Map("r" -> rIdent, "from" -> fromIdent, "to" -> toIdent)
    val expr = Equals(
      FunctionInvocation(FunctionName("id")(pos), distinct = false, Array(rIdent))(pos),
      SignedIntegerLiteral("42")(pos)
    )(pos)
    val from = IdName("from")
    val end = IdName("to")
    val patternRel = PatternRelationship(IdName("r"), (from, end), Direction.BOTH, Seq.empty)
    val qg = QueryGraph(projections, Selections(Seq(Set(IdName("r")) -> expr)), Set(from, end), Set(patternRel))

    implicit val context = newMockedLogicalPlanContext(
      queryGraph = qg,
      estimator = CardinalityEstimator.lift {
        case _: UndirectedRelationshipByIdSeek => 2
      }
    )

    when(context.semanticTable.isRelationship(rIdent)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(Seq(expr))()

    // then
    resultPlans should equal(Seq(UndirectedRelationshipByIdSeek(IdName("r"), Seq(SignedIntegerLiteral("42")(pos)), from, end)()))
  }

  test("simple directed relationship by id seek with a collection of relationship ids") {
    // given
    val rIdent = Identifier("r")(pos)
    val fromIdent = Identifier("from")(pos)
    val toIdent = Identifier("to")(pos)
    val projections = Map("r" -> rIdent, "from" -> fromIdent, "to" -> toIdent)
    val expr = In(
      FunctionInvocation(FunctionName("id")(pos), distinct = false, Array(rIdent))(pos),
      Collection(
        Seq(SignedIntegerLiteral("42")(pos), SignedIntegerLiteral("43")(pos), SignedIntegerLiteral("43")(pos))
      )(pos)
    )(pos)
    val from = IdName("from")
    val end = IdName("to")
    val patternRel = PatternRelationship(IdName("r"), (from, end), Direction.OUTGOING, Seq.empty)
    val qg = QueryGraph(projections, Selections(Seq(Set(IdName("r")) -> expr)), Set(from, end), Set(patternRel))

    implicit val context = newMockedLogicalPlanContext(
      queryGraph = qg,
      estimator = CardinalityEstimator.lift {
        case _: DirectedRelationshipByIdSeek => 1
      }
    )
    when(context.semanticTable.isRelationship(rIdent)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(Seq(expr))()

    // then
    resultPlans should equal(Seq(DirectedRelationshipByIdSeek(IdName("r"), Seq(
      SignedIntegerLiteral("42")(pos), SignedIntegerLiteral("43")(pos), SignedIntegerLiteral("43")(pos)
    ), from, end)()))
  }

  test("simple undirected relationship by id seek with a collection of relationship ids") {
    // given
    val rIdent = Identifier("r")(pos)
    val fromIdent = Identifier("from")(pos)
    val toIdent = Identifier("to")(pos)
    val projections = Map("r" -> rIdent, "from" -> fromIdent, "to" -> toIdent)
    val expr = In(
      FunctionInvocation(FunctionName("id")(pos), distinct = false, Array(rIdent))(pos),
      Collection(
        Seq(SignedIntegerLiteral("42")(pos), SignedIntegerLiteral("43")(pos), SignedIntegerLiteral("43")(pos))
      )(pos)
    )(pos)
    val from = IdName("from")
    val end = IdName("to")
    val patternRel = PatternRelationship(IdName("r"), (from, end), Direction.BOTH, Seq.empty)
    val qg = QueryGraph(projections, Selections(Seq(Set(IdName("r")) -> expr)), Set(from, end), Set(patternRel))

    implicit val context = newMockedLogicalPlanContext(
      queryGraph = qg,
      estimator = CardinalityEstimator.lift {
        case _: UndirectedRelationshipByIdSeek => 2
      }
    )
    when(context.semanticTable.isRelationship(rIdent)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(Seq(expr))()

    // then
    resultPlans should equal(Seq(UndirectedRelationshipByIdSeek(IdName("r"), Seq(
      SignedIntegerLiteral("42")(pos), SignedIntegerLiteral("43")(pos), SignedIntegerLiteral("43")(pos)
    ), from, end)()))
  }

  test("simple undirected typed relationship by id seek with a rel id expression") {
    // given
    val rIdent = Identifier("r")(pos)
    val fromIdent = Identifier("from")(pos)
    val toIdent = Identifier("to")(pos)
    val projections = Map("r" -> rIdent, "from" -> fromIdent, "to" -> toIdent)
    val expr = Equals(
      FunctionInvocation(FunctionName("id")(pos), distinct = false, Array(rIdent))(pos),
      SignedIntegerLiteral("42")(pos)
    )(pos)
    val from = IdName("from")
    val end = IdName("to")
    val patternRel = PatternRelationship(
      IdName("r"), (from, end), Direction.BOTH,
      Seq(RelTypeName("X")(Some(RelTypeId(1)))(pos))
    )
    val qg = QueryGraph(projections, Selections(Seq(Set(IdName("r")) -> expr)), Set(from, end), Set(patternRel))

    implicit val context = newMockedLogicalPlanContext(
      queryGraph = qg,
      estimator = CardinalityEstimator.lift {
        case _: UndirectedRelationshipByIdSeek => 2
      }
    )
    when(context.semanticTable.isRelationship(rIdent)).thenReturn(true)

    // when
    val resultPlans = idSeekLeafPlanner(Seq(expr))()

    // then
    resultPlans should equal(Seq(
      Selection(
        Seq(Equals(FunctionInvocation(FunctionName("type")(pos), rIdent)(pos), StringLiteral("X")(pos))(pos)),
        UndirectedRelationshipByIdSeek(IdName("r"), Seq(SignedIntegerLiteral("42")(pos)), from, end)()
      )
    ))
  }

  test("simple undirected multi-typed relationship by id seek with a rel id expression") {
    // given
    val rIdent = Identifier("r")(pos)
    val fromIdent = Identifier("from")(pos)
    val toIdent = Identifier("to")(pos)
    val projections = Map("r" -> rIdent, "from" -> fromIdent, "to" -> toIdent)
    val expr = Equals(
      FunctionInvocation(FunctionName("id")(pos), distinct = false, Array(rIdent))(pos),
      SignedIntegerLiteral("42")(pos)
    )(pos)
    val from = IdName("from")
    val end = IdName("to")
    val patternRel = PatternRelationship(
      IdName("r"), (from, end), Direction.BOTH,
      Seq(
        RelTypeName("X")(Some(RelTypeId(1)))(pos),
        RelTypeName("Y")(Some(RelTypeId(2)))(pos)
      )
    )
    val qg = QueryGraph(projections, Selections(Seq(Set(IdName("r")) -> expr)), Set(from, end), Set(patternRel))

    implicit val context = newMockedLogicalPlanContext(
      queryGraph = qg,
      estimator = CardinalityEstimator.lift {
        case _: UndirectedRelationshipByIdSeek => 2
      }
    )
    when(context.semanticTable.isRelationship(rIdent)).thenReturn(true)

    implicit def withPos[T](expr: InputPosition => T): T = expr(pos)

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
