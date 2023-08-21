/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.UsingScanHint
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.ExhaustivePathPattern
import org.neo4j.cypher.internal.ir.NodeBinding
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SelectivePathPattern
import org.neo4j.cypher.internal.ir.ShortestRelationshipPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VariableGrouping
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.UpperBound.Unlimited
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class QueryGraphConnectedComponentsTest
    extends CypherFunSuite with LogicalPlanningTestSupport {

  private val A = "a"
  private val B = "b"
  private val C = "c"
  private val D = "d"
  private val X = "x"
  private val A_to_B = PatternRelationship("r1", (A, B), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
  private val C_to_X = PatternRelationship("r7", (C, X), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
  private val B_to_X = PatternRelationship("r12", (B, X), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

  private def rel(from: String, to: String, rel: String): PatternRelationship =
    PatternRelationship(rel, (from, to), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

  private def rel(from: String, to: String): PatternRelationship = {
    rel(from, to, s"rel$from$to")
  }

  private def qpp(from: String, to: String): QuantifiedPathPattern =
    QuantifiedPathPattern(
      leftBinding = NodeBinding(s"${from}_inner_singleton", from),
      rightBinding = NodeBinding(s"${to}_inner_singleton", to),
      patternRelationships = List(rel(s"${from}_inner_singleton", s"${to}_inner_singleton", "r")),
      repetition = Repetition(0, Unlimited),
      nodeVariableGroupings = Set(
        VariableGrouping(s"${from}_inner_singleton", s"${from}_inner_group"),
        VariableGrouping(s"${to}_inner_singleton", s"${to}_inner_group")
      ),
      relationshipVariableGroupings = Set(VariableGrouping("r", "r_group"))
    )

  private def spp(from: String, to: String) =
    SelectivePathPattern(
      ExhaustivePathPattern.NodeConnections(NonEmptyList(qpp(from, to))),
      Selections.empty,
      SelectivePathPattern.Selector.Any(1)
    )

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
      optionalMatches = Vector(
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
      selections = Selections.from(Seq(
        hasLabels(A, "Label"),
        hasLabels(B, "Label")
      ))
    )

    graph.connectedComponents should equal(Seq(
      QueryGraph(patternNodes = Set(A), selections = Selections.from(hasLabels(A, "Label"))),
      QueryGraph(patternNodes = Set(B), selections = Selections.from(hasLabels(B, "Label")))
    ))
  }

  test("two disconnected pattern nodes with one predicate connecting them") {
    val predicate = equals(prop(A, "prop"), prop(B, "prop"))

    val graph = QueryGraph(patternNodes = Set(A, B), selections = Selections.from(predicate))

    graph.connectedComponents should equal(Seq(
      QueryGraph(patternNodes = Set(A)),
      QueryGraph(patternNodes = Set(B))
    ))
  }

  test("two disconnected relationships with each predicate on one of the relationships") {
    val predicate = equals(prop(A_to_B.name, "prop"), literalString("something"))

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
    val usingScanHint = UsingScanHint(varFor(A), labelOrRelTypeName("A"))(pos)
    val graph = QueryGraph(patternNodes = Set(A, B), hints = Set(usingScanHint))

    graph.connectedComponents should equal(Seq(
      QueryGraph(patternNodes = Set(A), hints = Set(usingScanHint)),
      QueryGraph(patternNodes = Set(B))
    ))
  }

  test("two disconnected pattern nodes with one shortest path between them") {
    val shortestPath: ShortestRelationshipPattern =
      ShortestRelationshipPattern(Some("r"), A_to_B, single = true)(null)

    val graph = QueryGraph(patternNodes = Set(A, B), shortestRelationshipPatterns = Set(shortestPath))

    graph.connectedComponents should equal(Seq(
      QueryGraph(patternNodes = Set(A)),
      QueryGraph(patternNodes = Set(B))
    ))
  }

  test("a connected pattern that has a shortest path in it") {
    val B_to_A = PatternRelationship("r3", (B, A), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val shortestPath: ShortestRelationshipPattern =
      ShortestRelationshipPattern(Some("r"), A_to_B, single = true)(null)

    val graph = QueryGraph(
      patternNodes = Set(A, B),
      patternRelationships = Set(B_to_A),
      shortestRelationshipPatterns = Set(shortestPath)
    )

    graph.connectedComponents should equal(Seq(graph))
  }

  test("two disconnected pattern relationships with arguments") {
    val graph = QueryGraph(patternNodes = Set(A, B), argumentIds = Set(C))

    graph.connectedComponents should equal(Seq(
      QueryGraph(patternNodes = Set(A), argumentIds = Set(C)),
      QueryGraph(patternNodes = Set(B), argumentIds = Set(C))
    ))
  }

  test("nodes solved by argument should be in the same component") {
    val graph = QueryGraph.empty.addPatternNodes(A, B).addArgumentIds(Seq(A, B))

    graph.connectedComponents should equal(Seq(graph))
  }

  test("one node and a relationship connected through an optional QG") {
    // MATCH (x)
    // WITH x MATCH (a), (b)-->(x)
    // OPTIONAL MATCH (a)-->(b)
    val graph = QueryGraph(
      argumentIds = Set(X),
      patternNodes = Set(A, B, X),
      patternRelationships = Set(B_to_X),
      optionalMatches = Vector(
        QueryGraph(patternNodes = Set(A, B), argumentIds = Set(A, B), patternRelationships = Set(A_to_B))
      )
    )

    val components = graph.connectedComponents
    components.toSet should equal(Set(
      QueryGraph(patternNodes = Set(A), argumentIds = Set(X)),
      QueryGraph(patternNodes = Set(B, X), patternRelationships = Set(B_to_X), argumentIds = Set(X))
    ))
  }

  test("should pick the predicates correctly on relationships when they depend on arguments") {
    //  UNWIND [0] as x match (a)-[r]->(b) where id(r) = x
    val graph = QueryGraph(
      argumentIds = Set(X),
      patternNodes = Set(A, B),
      patternRelationships = Set(A_to_B),
      selections = Selections.from(equals(varFor(A_to_B.name), varFor(X)))
    )

    val components = graph.connectedComponents
    components should equal(Seq(graph))
  }

  test("two pattern with same rel name should be in the same connected component") {
    val D = "d"
    val Y = "y"
    val D_to_Y = PatternRelationship("r12", (D, Y), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

    // MATCH (d)-[r]->(y), (b)-[r]->(x)
    val graph = QueryGraph(
      patternNodes = Set(B, X, D, Y),
      patternRelationships = Set(B_to_X, D_to_Y)
    )

    graph.connectedComponents should equal(Seq(graph))
  }

  test("query parts not connected to the arguments should not be pulled in by mistake") {
    val graph = QueryGraph(
      patternNodes = Set(A, B, C),
      argumentIds = Set(A),
      patternRelationships = Set(A_to_B),
      selections = Selections.from(equals(prop(A, "foo"), prop(C, "bar")))
    )

    graph.connectedComponents.size should equal(2)
  }

  test("single quantified path pattern") {
    // (a) ((a_inner)-[r1]->(b_inner))* (b)
    val singleQuantifiedPathPatternQG = QueryGraph(patternNodes = Set(A, B), quantifiedPathPatterns = Set(qpp(A, B)))

    singleQuantifiedPathPatternQG.connectedComponents shouldBe Seq(singleQuantifiedPathPatternQG)
  }

  test("quantified path pattern should pull in predicates") {
    // (a) ((a_inner)-[r1]->(b_inner))* (b) WHERE r1 IS NOT NULL
    val qppUnderTest = qpp(A, B)
    val predicate = isNotNull(varFor(qppUnderTest.relationshipVariableGroupings.head.groupName))
    val singleQuantifiedPathPatternQG = QueryGraph.empty
      .addQuantifiedPathPattern(qppUnderTest)
      .addPredicates(predicate)

    singleQuantifiedPathPatternQG.connectedComponents shouldBe Seq(singleQuantifiedPathPatternQG)
  }

  test("single quantified path pattern and path pattern") {
    // (a)-[r]->(b) ((b_inner)-[r1]->(c_inner))* (c)
    val singleQuantifiedPathPatternQG =
      QueryGraph(
        patternNodes = Set(A, B, C),
        patternRelationships = Set(A_to_B),
        quantifiedPathPatterns = Set(qpp(B, C))
      )

    singleQuantifiedPathPatternQG.connectedComponents shouldBe Seq(singleQuantifiedPathPatternQG)
  }

  test("single quantified path pattern and path pattern in separate components") {
    // (a)-[r1]->(b), (c) ((c_inner)-[]->(d_inner))+ (d)
    val singleQuantifiedPathPatternQG =
      QueryGraph(
        patternNodes = Set(A, B, C, D),
        patternRelationships = Set(A_to_B),
        quantifiedPathPatterns = Set(qpp(C, D))
      )

    val qq1 = QueryGraph(patternNodes = Set(A, B), patternRelationships = Set(A_to_B))
    val qq2 = QueryGraph(patternNodes = Set(C, D), quantifiedPathPatterns = Set(qpp(C, D)))

    singleQuantifiedPathPatternQG.connectedComponents.toSet shouldBe Set(qq1, qq2)
  }

  test("two consecutive quantified path patterns connecting three nodes") {
    val quantifiedPathPatternQG =
      QueryGraph(
        patternNodes = Set(A, B, C, D),
        patternRelationships = Set.empty,
        quantifiedPathPatterns = Set(qpp(A, B), qpp(C, B))
      )

    val connectedPart =
      QueryGraph(
        patternNodes = Set(A, B, C),
        patternRelationships = Set.empty,
        quantifiedPathPatterns = Set(qpp(A, B), qpp(C, B))
      )
    val otherNode = QueryGraph(patternNodes = Set(D))
    quantifiedPathPatternQG.connectedComponents.toSet shouldBe Set(connectedPart, otherNode)
  }

  test("should pick the predicates correctly on quantified path patterns when they depend on arguments") {
    //  UNWIND [[0]] AS x MATCH (a) ((a_inner)-[]->(b_inner))* (b) WHERE a_inner = x
    val graph = QueryGraph(
      argumentIds = Set(X),
      patternNodes = Set(A, B),
      quantifiedPathPatterns = Set(qpp(A, B)),
      selections = Selections.from(equals(varFor("a_inner_group"), varFor(X)))
    )

    val components = graph.connectedComponents
    components should equal(Seq(graph))
  }

  test("nodes connected via shortest path") {
    val spps = Set(spp(A, B))

    val sppQG =
      QueryGraph(
        patternNodes = Set(A, B, C),
        patternRelationships = Set.empty,
        selectivePathPatterns = spps
      )

    val connectedPart =
      QueryGraph(
        patternNodes = Set(A, B),
        patternRelationships = Set.empty,
        selectivePathPatterns = spps
      )
    val otherNode = QueryGraph(patternNodes = Set(C))
    sppQG.connectedComponents.toSet shouldBe Set(connectedPart, otherNode)
  }

  test("nodes connected via shortest path (path pattern of length 2)") {
    val spps = Set(SelectivePathPattern(
      ExhaustivePathPattern.NodeConnections(NonEmptyList(qpp(A, B), qpp(B, C))),
      Selections.empty,
      SelectivePathPattern.Selector.Any(1)
    ))

    val sppQG =
      QueryGraph(
        patternNodes = Set(A, C, D),
        patternRelationships = Set.empty,
        selectivePathPatterns = spps
      )

    val connectedPart =
      QueryGraph(
        patternNodes = Set(A, C),
        patternRelationships = Set.empty,
        selectivePathPatterns = spps
      )
    val otherNode = QueryGraph(patternNodes = Set(D))
    sppQG.connectedComponents.toSet shouldBe Set(connectedPart, otherNode)
  }

  test("2 sets of nodes connected via 2 shortest paths") {
    val sppAB = spp(A, B)
    val sppCD = spp(C, D)
    val spps = Set(sppAB, sppCD)

    val sppQG =
      QueryGraph(
        patternNodes = Set(A, B, C, D),
        patternRelationships = Set.empty,
        selectivePathPatterns = spps
      )

    val connectedParts = Set(
      QueryGraph(
        patternNodes = Set(A, B),
        patternRelationships = Set.empty,
        selectivePathPatterns = Set(sppAB)
      ),
      QueryGraph(
        patternNodes = Set(C, D),
        patternRelationships = Set.empty,
        selectivePathPatterns = Set(sppCD)
      )
    )
    sppQG.connectedComponents.toSet shouldBe connectedParts
  }

  test("1 set of nodes connected via 2 shortest paths and a relationship") {
    val sppAB = spp(A, B)
    val sppCD = spp(C, D)
    val spps = Set(sppAB, sppCD)

    val sppQG =
      QueryGraph(
        patternNodes = Set(A, B, C, D),
        patternRelationships = Set(rel(B, C)),
        selectivePathPatterns = spps
      )

    sppQG.connectedComponents shouldBe Seq(sppQG)
  }

  test("should pick the predicates correctly on selective path patterns when they depend on arguments") {
    val graph = QueryGraph(
      argumentIds = Set(X),
      patternNodes = Set(A, C),
      selectivePathPatterns = Set(SelectivePathPattern(
        ExhaustivePathPattern.NodeConnections(NonEmptyList(qpp(A, B), qpp(B, C))),
        Selections.empty,
        SelectivePathPattern.Selector.Any(1)
      )),
      selections = Selections.from(equals(varFor(B), varFor(X)))
    )

    val components = graph.connectedComponents
    components should equal(Seq(graph))
  }

  test("selective path pattern should pull in predicates without dependency on arguments") {
    val graph = QueryGraph(
      patternNodes = Set(A, C),
      selectivePathPatterns = Set(SelectivePathPattern(
        ExhaustivePathPattern.NodeConnections(NonEmptyList(qpp(A, B), qpp(B, C))),
        Selections.empty,
        SelectivePathPattern.Selector.Any(1)
      )),
      selections = Selections.from(isNotNull(varFor(B)))
    )

    val components = graph.connectedComponents
    components should equal(Seq(graph))
  }
}
