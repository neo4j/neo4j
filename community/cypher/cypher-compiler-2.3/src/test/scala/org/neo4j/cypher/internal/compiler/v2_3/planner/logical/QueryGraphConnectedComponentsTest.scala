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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical

import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{IdName, PatternRelationship, ShortestPathPattern, SimplePatternLength}
import org.neo4j.cypher.internal.compiler.v2_3.planner.{LogicalPlanningTestSupport, QueryGraph, Selections}
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class QueryGraphConnectedComponentsTest
  extends CypherFunSuite with AstConstructionTestSupport with LogicalPlanningTestSupport {

  private val labelA = LabelName("A")(pos)
  private val prop = ident("prop")
  private val propKeyName = PropertyKeyName(prop.name)(pos)
  private val A = IdName("a")
  private val B = IdName("b")
  private val C = IdName("c")
  private val D = IdName("d")
  private val X = IdName("x")
  private val Y = IdName("y")
  private val A_to_B = PatternRelationship(IdName("r1"), (A, B), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
  private val B_to_A = PatternRelationship(IdName("r3"), (B, A), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
  private val C_to_X = PatternRelationship(IdName("r7"), (C, X), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
  private val B_to_X = PatternRelationship(IdName("r12"), (B, X), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
  private val D_to_Y = PatternRelationship(IdName("r12"), (D, Y), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
  private val identA = ident(A.name)
  private val identB = ident(B.name)

  test("empty query graph returns no connected querygraphs") {
    QueryGraph().connectedComponents shouldBe empty
  }

  test("single node") {
    val singleNodeQG = QueryGraph(patternNodes = Set(A))

    singleNodeQG.connectedComponents shouldBe Seq(singleNodeQG)
  }

  test("two disconnected nodes") {
    QueryGraph(patternNodes = Set(A, B)).connectedComponents should equal(Seq(
      QueryGraph(patternNodes = Set(A)),
      QueryGraph(patternNodes = Set(B))
    ))
  }

  test("two nodes connected through one rel") {
    val graph = QueryGraph(
      patternNodes = Set(A, B),
      patternRelationships = Set(A_to_B)
    )

    graph.connectedComponents should equal(Seq(graph))
  }

  test("two disconnected relationships") {
    val graph = QueryGraph(
      patternNodes = Set(A, B, C, X),
      patternRelationships = Set(A_to_B, C_to_X)
    )

    graph.connectedComponents should equal(Seq(
      QueryGraph(patternNodes = Set(A, B), patternRelationships = Set(A_to_B)),
      QueryGraph(patternNodes = Set(C, X), patternRelationships = Set(C_to_X))
    ))
  }

  test("two disconnected relationships with one argument") {
    val graph = QueryGraph(
      patternNodes = Set(A, B, C, X),
      patternRelationships = Set(A_to_B, C_to_X),
      argumentIds = Set(A)
    )

    graph.connectedComponents should equal(Seq(
      QueryGraph(patternNodes = Set(A, B), patternRelationships = Set(A_to_B), argumentIds = Set(A)),
      QueryGraph(patternNodes = Set(C, X), patternRelationships = Set(C_to_X), argumentIds = Set(A))
    ))
  }

  test("two disconnected relationships with each one argument") {
    val graph = QueryGraph(
      patternNodes = Set(A, B, C, X),
      patternRelationships = Set(A_to_B, C_to_X),
      argumentIds = Set(A, C)
    )

    graph.connectedComponents should equal(Seq(graph))
  }

  test("two nodes connected through an optional QG") {
    val graph = QueryGraph(
      patternNodes = Set(A, B),
      optionalMatches = Seq(
        QueryGraph(patternNodes = Set(A, B), argumentIds = Set(A, B), patternRelationships = Set(A_to_B))
      )
    )

    graph.connectedComponents should equal(Seq(
      QueryGraph(patternNodes = Set(A)),
      QueryGraph(patternNodes = Set(B))
    ))
  }

  test("two disconnected pattern nodes with each one predicate") {
    val graph = QueryGraph(
      patternNodes = Set(A, B),
      selections = Selections.from(
        identHasLabel("a", "Label"),
        identHasLabel("b", "Label"))
    )

    graph.connectedComponents should equal(Seq(
      QueryGraph(patternNodes = Set(A), selections = Selections.from(identHasLabel("a", "Label"))),
      QueryGraph(patternNodes = Set(B), selections = Selections.from(identHasLabel("b", "Label")))
    ))
  }

  test("two disconnected pattern nodes with one predicate connecting them") {
    val propA = Property(identA, propKeyName)(pos)
    val propB = Property(identB, propKeyName)(pos)
    val predicate = Equals(propA, propB)(pos)

    val graph = QueryGraph(patternNodes = Set(A, B), selections = Selections.from(predicate))

    graph.connectedComponents should equal(Seq(
      QueryGraph(patternNodes = Set(A)),
      QueryGraph(patternNodes = Set(B))
    ))
  }

  test("two disconnected relationships with each predicate on one of the relationships") {
    val propA = Property(ident(A_to_B.name.name), propKeyName)(pos)
    val predicate = Equals(propA, StringLiteral("something")(pos))(pos)

    val graph = QueryGraph(
      patternNodes = Set(A, B, C, X),
      patternRelationships = Set(A_to_B, C_to_X),
      selections = Selections.from(predicate)
    )

    graph.connectedComponents should equal(Seq(
      QueryGraph(patternNodes = Set(A, B), patternRelationships = Set(A_to_B), selections = Selections.from(predicate)),
      QueryGraph(patternNodes = Set(C, X), patternRelationships = Set(C_to_X))
    ))
  }

  test("two disconnected pattern relationships with hints on one side") {
    val graph = QueryGraph(patternNodes = Set(A, B), hints = Set(UsingScanHint(identA, labelA)(pos)))

    graph.connectedComponents should equal(Seq(
      QueryGraph(patternNodes = Set(A), hints = Set(UsingScanHint(identA, labelA)(pos))),
      QueryGraph(patternNodes = Set(B))
    ))
  }

  test("two disconnected pattern nodes with one shortest path between them") {
    val shortestPath: ShortestPathPattern = ShortestPathPattern(Some(IdName("r")), A_to_B, single = true)(null)

    val graph = QueryGraph(patternNodes = Set(A, B), shortestPathPatterns = Set(shortestPath))

    graph.connectedComponents should equal(Seq(
      QueryGraph(patternNodes = Set(A)),
      QueryGraph(patternNodes = Set(B))
    ))
  }

  test("a connected pattern that has a shortest path in it") {
    val shortestPath: ShortestPathPattern = ShortestPathPattern(Some(IdName("r")), A_to_B, single = true)(null)

    val graph = QueryGraph(
      patternNodes = Set(A, B),
      patternRelationships = Set(B_to_A),
      shortestPathPatterns = Set(shortestPath))

    graph.connectedComponents should equal(Seq(graph))
  }

  test("two disconnected pattern relationships with arguments") {
    val graph = QueryGraph(patternNodes = Set(A, B), argumentIds = Set(C))

    graph.connectedComponents should equal(Seq(
      QueryGraph(patternNodes = Set(A), argumentIds = Set(C)),
      QueryGraph(patternNodes = Set(B), argumentIds = Set(C))
    ))
  }

  test("a pattern node with a hint") {
    val graph = QueryGraph.empty.
      addPatternNodes(A).
      addHints(Set(NodeByIdentifiedIndex(ident("a"), "index", "key", mock[Expression])(pos)))

    graph.connectedComponents should equal(Seq(graph))
  }

  test("nodes solved by argument should be in the same component") {
    val graph = QueryGraph.empty.
    addPatternNodes(A, B).
    addArgumentIds(Seq(A, B))

    graph.connectedComponents should equal(Seq(graph))
  }

  test("one node and a relationship connected through an optional QG") {
    // MATCH (a), (b)-->(x)
    val graph = QueryGraph(
      argumentIds = Set(X),
      patternNodes = Set(A, B, X),
      patternRelationships = Set(B_to_X),
      optionalMatches = Seq(
        QueryGraph(patternNodes = Set(A, B), argumentIds = Set(A, B), patternRelationships = Set(A_to_B))
      )
    )

    val components = graph.connectedComponents
    components should equal(Seq(
      QueryGraph(patternNodes = Set(A), argumentIds = Set(X)),
      QueryGraph(patternNodes = Set(B, X), patternRelationships = Set(B_to_X), argumentIds = Set(X))
    ))
  }

  test("should pick the predicates correctly when they depend on arguments") {
    //  UNWIND [0] as x match (a)-[r]->(b) where id(r) = x
    val graph = QueryGraph(
      argumentIds = Set(X),
      patternNodes = Set(A, B),
      patternRelationships = Set(A_to_B),
      selections = Selections.from(Equals(Identifier(A_to_B.name.name)(pos), Identifier(X.name.name)(pos))(pos))
    )

    val components = graph.connectedComponents
    components should equal(Seq(graph))
  }

  test("two pattern with same rel name should be in the same connected component") {
    // MATCH (d)-[r]->(y), (b)-[r]->(x)
    val graph = QueryGraph(
      patternNodes = Set(B, X, D, Y),
      patternRelationships = Set(B_to_X, D_to_Y)
    )

    graph.connectedComponents should equal(Seq(graph))
  }
}
