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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{IdName, PatternRelationship, ShortestPathPattern, SimplePatternLength}
import org.neo4j.cypher.internal.compiler.v2_2.planner.{LogicalPlanningTestSupport, QueryGraph, Selections}
import org.neo4j.graphdb.Direction

class QueryGraphConnectedComponentsTest
  extends CypherFunSuite with AstConstructionTestSupport with LogicalPlanningTestSupport {

  private val labelA = LabelName("A")(pos)
  private val prop = ident("prop")
  private val propKeyName = PropertyKeyName(prop.name)(pos)
  private val A = IdName("a")
  private val B = IdName("b")
  private val C = IdName("c")
  private val X = IdName("x")
  private val R1 = PatternRelationship(IdName("r1"), (A, B), Direction.OUTGOING, Seq.empty, SimplePatternLength)
  private val R2 = PatternRelationship(IdName("r3"), (B, A), Direction.OUTGOING, Seq.empty, SimplePatternLength)
  private val R3 = PatternRelationship(IdName("r7"), (C, X), Direction.OUTGOING, Seq.empty, SimplePatternLength)
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
      patternRelationships = Set(R1)
    )

    graph.connectedComponents should equal(Seq(graph))
  }

  test("two disconnected relationships") {
    val graph = QueryGraph(
      patternNodes = Set(A, B, C, X),
      patternRelationships = Set(R1, R3)
    )

    graph.connectedComponents should equal(Seq(
      QueryGraph(patternNodes = Set(A, B), patternRelationships = Set(R1)),
      QueryGraph(patternNodes = Set(C, X), patternRelationships = Set(R3))
    ))
  }

  test("two disconnected relationships with one argument") {
    val graph = QueryGraph(
      patternNodes = Set(A, B, C, X),
      patternRelationships = Set(R1, R3),
      argumentIds = Set(A)
    )

    graph.connectedComponents should equal(Seq(
      QueryGraph(patternNodes = Set(A, B), patternRelationships = Set(R1), argumentIds = Set(A)),
      QueryGraph(patternNodes = Set(C, X), patternRelationships = Set(R3), argumentIds = Set(A))
    ))
  }

  test("two disconnected relationships with each one argument") {
    val graph = QueryGraph(
      patternNodes = Set(A, B, C, X),
      patternRelationships = Set(R1, R3),
      argumentIds = Set(A, C)
    )

    graph.connectedComponents should equal(Seq(graph))
  }

  test("two nodes connected through an optional QG") {
    val graph = QueryGraph(
      patternNodes = Set(A, B),
      optionalMatches = Seq(
        QueryGraph(patternNodes = Set(A, B), argumentIds = Set(A, B), patternRelationships = Set(R1))
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
    val propA = Property(ident(R1.name.name), propKeyName)(pos)
    val predicate = Equals(propA, StringLiteral("something")(pos))(pos)

    val graph = QueryGraph(
      patternNodes = Set(A, B, C, X),
      patternRelationships = Set(R1, R3),
      selections = Selections.from(predicate)
    )

    graph.connectedComponents should equal(Seq(
      QueryGraph(patternNodes = Set(A, B), patternRelationships = Set(R1), selections = Selections.from(predicate)),
      QueryGraph(patternNodes = Set(C, X), patternRelationships = Set(R3))
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
    val shortestPath: ShortestPathPattern = ShortestPathPattern(Some(IdName("r")), R1, single = true)(null)

    val graph = QueryGraph(patternNodes = Set(A, B), shortestPathPatterns = Set(shortestPath))

    graph.connectedComponents should equal(Seq(
      QueryGraph(patternNodes = Set(A)),
      QueryGraph(patternNodes = Set(B))
    ))
  }

  test("a connected pattern that has a shortest path in it") {
    val shortestPath: ShortestPathPattern = ShortestPathPattern(Some(IdName("r")), R1, single = true)(null)

    val graph = QueryGraph(
      patternNodes = Set(A, B),
      patternRelationships = Set(R2),
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
      addHints(Set(NodeByIdentifiedIndex(ident("a"), ident("index"), ident("key"), mock[Expression])(pos)))

    graph.connectedComponents should equal(Seq(graph))
  }

  test("nodes solved by argument should be in the same component") {
    val graph = QueryGraph.empty.
    addPatternNodes(A, B).
    addArgumentIds(Seq(A, B))

    graph.connectedComponents should equal(Seq(graph))
  }
}
