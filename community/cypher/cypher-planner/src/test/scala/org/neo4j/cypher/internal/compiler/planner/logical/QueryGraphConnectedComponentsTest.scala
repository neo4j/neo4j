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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.UsingScanHint
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.expressions.LogicalVariable
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

  private val A = v"a"
  private val B = v"b"
  private val C = v"c"
  private val D = v"d"
  private val X = v"x"
  private val A_to_B = PatternRelationship(v"r1", (A, B), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
  private val C_to_X = PatternRelationship(v"r7", (C, X), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
  private val B_to_X = PatternRelationship(v"r12", (B, X), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

  private def rel(from: LogicalVariable, to: LogicalVariable, rel: LogicalVariable): PatternRelationship =
    PatternRelationship(
      rel,
      (from, to),
      SemanticDirection.OUTGOING,
      Seq.empty,
      SimplePatternLength
    )

  private def rel(from: LogicalVariable, to: LogicalVariable): PatternRelationship = {
    rel(from, to, v"rel$from$to")
  }

  private def qpp(from: LogicalVariable, to: LogicalVariable): QuantifiedPathPattern =
    QuantifiedPathPattern(
      leftBinding = NodeBinding(v"${from}_inner_singleton", from),
      rightBinding = NodeBinding(v"${to}_inner_singleton", to),
      patternRelationships = NonEmptyList(rel(v"${from}_inner_singleton", v"${to}_inner_singleton", v"r")),
      repetition = Repetition(0, Unlimited),
      nodeVariableGroupings = Set(
        VariableGrouping(v"${from}_inner_singleton", v"${from}_inner_group"),
        VariableGrouping(v"${to}_inner_singleton", v"${to}_inner_group")
      ),
      relationshipVariableGroupings = Set(VariableGrouping(v"r", v"r_group"))
    )

  private def spp(from: LogicalVariable, to: LogicalVariable) =
    SelectivePathPattern(
      ExhaustivePathPattern.NodeConnections(NonEmptyList(qpp(from, to))),
      Selections.empty,
      SelectivePathPattern.Selector.Any(1)
    )

  test("empty query graph returns no connected querygraphs") {
    QueryGraph().connectedComponents shouldBe empty
  }

  test("single node") {
    val singleNodeQG = QueryGraph(patternNodes = Set(A.name))

    singleNodeQG.connectedComponents shouldBe Seq(singleNodeQG)
  }

  test("two disconnected nodes") {
    QueryGraph(patternNodes = Set(A.name, B.name)).connectedComponents should equal(Seq(
      QueryGraph(patternNodes = Set(A.name)),
      QueryGraph(patternNodes = Set(B.name))
    ))
  }

  test("two nodes connected through one rel") {
    val graph = QueryGraph(
      patternNodes = Set(A.name, B.name),
      patternRelationships = Set(A_to_B)
    )

    graph.connectedComponents should equal(Seq(graph))
  }

  test("two disconnected relationships") {
    val graph = QueryGraph(
      patternNodes = Set(A.name, B.name, C.name, X.name),
      patternRelationships = Set(A_to_B, C_to_X)
    )

    graph.connectedComponents should equal(Seq(
      QueryGraph(patternNodes = Set(A.name, B.name), patternRelationships = Set(A_to_B)),
      QueryGraph(patternNodes = Set(C.name, X.name), patternRelationships = Set(C_to_X))
    ))
  }

  test("two disconnected relationships with one argument") {
    val graph = QueryGraph(
      patternNodes = Set(A.name, B.name, C.name, X.name),
      patternRelationships = Set(A_to_B, C_to_X),
      argumentIds = Set(A.name)
    )

    graph.connectedComponents should equal(Seq(
      QueryGraph(patternNodes = Set(A.name, B.name), patternRelationships = Set(A_to_B), argumentIds = Set(A.name)),
      QueryGraph(patternNodes = Set(C.name, X.name), patternRelationships = Set(C_to_X), argumentIds = Set(A.name))
    ))
  }

  test("two disconnected relationships with each one argument") {
    val graph = QueryGraph(
      patternNodes = Set(A.name, B.name, C.name, X.name),
      patternRelationships = Set(A_to_B, C_to_X),
      argumentIds = Set(A.name, C.name)
    )

    graph.connectedComponents should equal(Seq(graph))
  }

  test("two nodes connected through an optional QG") {
    val graph = QueryGraph(
      patternNodes = Set(A.name, B.name),
      optionalMatches = Vector(
        QueryGraph(
          patternNodes = Set(A.name, B.name),
          argumentIds = Set(A.name, B.name),
          patternRelationships = Set(A_to_B)
        )
      )
    )

    graph.connectedComponents should equal(Seq(
      QueryGraph(patternNodes = Set(A.name)),
      QueryGraph(patternNodes = Set(B.name))
    ))
  }

  test("two disconnected pattern nodes with each one predicate") {
    val graph = QueryGraph(
      patternNodes = Set(A.name, B.name),
      selections = Selections.from(Seq(
        hasLabels(A, "Label"),
        hasLabels(B, "Label")
      ))
    )

    graph.connectedComponents should equal(Seq(
      QueryGraph(patternNodes = Set(A.name), selections = Selections.from(hasLabels(A, "Label"))),
      QueryGraph(patternNodes = Set(B.name), selections = Selections.from(hasLabels(B, "Label")))
    ))
  }

  test("two disconnected pattern nodes with one predicate connecting them") {
    val predicate = equals(prop(A, "prop"), prop(B, "prop"))

    val graph = QueryGraph(patternNodes = Set(A.name, B.name), selections = Selections.from(predicate))

    graph.connectedComponents should equal(Seq(
      QueryGraph(patternNodes = Set(A.name)),
      QueryGraph(patternNodes = Set(B.name))
    ))
  }

  test("two disconnected relationships with each predicate on one of the relationships") {
    val predicate = equals(prop(A_to_B.variable, "prop"), literalString("something"))

    val graph = QueryGraph(
      patternNodes = Set(A.name, B.name, C.name, X.name),
      patternRelationships = Set(A_to_B, C_to_X),
      selections = Selections.from(predicate)
    )

    graph.connectedComponents should equal(Seq(
      QueryGraph(
        patternNodes = Set(A.name, B.name),
        patternRelationships = Set(A_to_B),
        selections = Selections.from(predicate)
      ),
      QueryGraph(patternNodes = Set(C.name, X.name), patternRelationships = Set(C_to_X))
    ))
  }

  test("two disconnected pattern relationships with hints on one side") {
    val usingScanHint = UsingScanHint(A, labelOrRelTypeName("A"))(pos)
    val graph = QueryGraph(patternNodes = Set(A.name, B.name), hints = Set(usingScanHint))

    graph.connectedComponents should equal(Seq(
      QueryGraph(patternNodes = Set(A.name), hints = Set(usingScanHint)),
      QueryGraph(patternNodes = Set(B.name))
    ))
  }

  test("two disconnected pattern nodes with one shortest path between them") {
    val shortestPath: ShortestRelationshipPattern =
      ShortestRelationshipPattern(Some(v"r"), A_to_B, single = true)(null)

    val graph = QueryGraph(patternNodes = Set(A.name, B.name), shortestRelationshipPatterns = Set(shortestPath))

    graph.connectedComponents should equal(Seq(
      QueryGraph(patternNodes = Set(A.name)),
      QueryGraph(patternNodes = Set(B.name))
    ))
  }

  test("a connected pattern that has a shortest path in it") {
    val B_to_A = PatternRelationship(v"r3", (B, A), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val shortestPath: ShortestRelationshipPattern =
      ShortestRelationshipPattern(Some(v"r"), A_to_B, single = true)(null)

    val graph = QueryGraph(
      patternNodes = Set(A.name, B.name),
      patternRelationships = Set(B_to_A),
      shortestRelationshipPatterns = Set(shortestPath)
    )

    graph.connectedComponents should equal(Seq(graph))
  }

  test("two disconnected pattern relationships with arguments") {
    val graph = QueryGraph(patternNodes = Set(A.name, B.name), argumentIds = Set(C.name))

    graph.connectedComponents should equal(Seq(
      QueryGraph(patternNodes = Set(A.name), argumentIds = Set(C.name)),
      QueryGraph(patternNodes = Set(B.name), argumentIds = Set(C.name))
    ))
  }

  test("nodes solved by argument should be in the same component") {
    val graph = QueryGraph.empty.addPatternNodes(A.name, B.name).addArgumentIds(Seq(A.name, B.name))

    graph.connectedComponents should equal(Seq(graph))
  }

  test("one node and a relationship connected through an optional QG") {
    // MATCH (x)
    // WITH x MATCH (a), (b)-->(x)
    // OPTIONAL MATCH (a)-->(b)
    val graph = QueryGraph(
      argumentIds = Set(X.name),
      patternNodes = Set(A, B, X).map(_.name),
      patternRelationships = Set(B_to_X),
      optionalMatches = Vector(
        QueryGraph(
          patternNodes = Set(A.name, B.name),
          argumentIds = Set(A.name, B.name),
          patternRelationships = Set(A_to_B)
        )
      )
    )

    val components = graph.connectedComponents
    components.toSet should equal(Set(
      QueryGraph(patternNodes = Set(A.name), argumentIds = Set(X.name)),
      QueryGraph(patternNodes = Set(B, X).map(_.name), patternRelationships = Set(B_to_X), argumentIds = Set(X.name))
    ))
  }

  test("should pick the predicates correctly on relationships when they depend on arguments") {
    //  UNWIND [0] as x match (a)-[r]->(b) where id(r) = x
    val graph = QueryGraph(
      argumentIds = Set(X.name),
      patternNodes = Set(A.name, B.name),
      patternRelationships = Set(A_to_B),
      selections = Selections.from(equals(A_to_B.variable, X))
    )

    val components = graph.connectedComponents
    components should equal(Seq(graph))
  }

  test("two pattern with same rel name should be in the same connected component") {
    val D = v"d"
    val Y = v"y"
    val D_to_Y = PatternRelationship(v"r12", (D, Y), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

    // MATCH (d)-[r]->(y), (b)-[r]->(x)
    val graph = QueryGraph(
      patternNodes = Set(B, X, D, Y).map(_.name),
      patternRelationships = Set(B_to_X, D_to_Y)
    )

    graph.connectedComponents should equal(Seq(graph))
  }

  test("query parts not connected to the arguments should not be pulled in by mistake") {
    val graph = QueryGraph(
      patternNodes = Set(A, B, C).map(_.name),
      argumentIds = Set(A.name),
      patternRelationships = Set(A_to_B),
      selections = Selections.from(equals(prop(A, "foo"), prop(C, "bar")))
    )

    graph.connectedComponents.size should equal(2)
  }

  test("single quantified path pattern") {
    // (a) ((a_inner)-[r1]->(b_inner))* (b)
    val singleQuantifiedPathPatternQG =
      QueryGraph(patternNodes = Set(A.name, B.name), quantifiedPathPatterns = Set(qpp(A, B)))

    singleQuantifiedPathPatternQG.connectedComponents shouldBe Seq(singleQuantifiedPathPatternQG)
  }

  test("quantified path pattern should pull in predicates") {
    // (a) ((a_inner)-[r1]->(b_inner))* (b) WHERE r1 IS NOT NULL
    val qppUnderTest = qpp(A, B)
    val predicate = isNotNull(qppUnderTest.relationshipVariableGroupings.head.groupName)
    val singleQuantifiedPathPatternQG = QueryGraph.empty
      .addQuantifiedPathPattern(qppUnderTest)
      .addPredicates(predicate)

    singleQuantifiedPathPatternQG.connectedComponents shouldBe Seq(singleQuantifiedPathPatternQG)
  }

  test("single quantified path pattern and path pattern") {
    // (a)-[r]->(b) ((b_inner)-[r1]->(c_inner))* (c)
    val singleQuantifiedPathPatternQG =
      QueryGraph(
        patternNodes = Set(A, B, C).map(_.name),
        patternRelationships = Set(A_to_B),
        quantifiedPathPatterns = Set(qpp(B, C))
      )

    singleQuantifiedPathPatternQG.connectedComponents shouldBe Seq(singleQuantifiedPathPatternQG)
  }

  test("single quantified path pattern and path pattern in separate components") {
    // (a)-[r1]->(b), (c) ((c_inner)-[]->(d_inner))+ (d)
    val singleQuantifiedPathPatternQG =
      QueryGraph(
        patternNodes = Set(A, B, C, D).map(_.name),
        patternRelationships = Set(A_to_B),
        quantifiedPathPatterns = Set(qpp(C, D))
      )

    val qq1 = QueryGraph(patternNodes = Set(A, B).map(_.name), patternRelationships = Set(A_to_B))
    val qq2 = QueryGraph(patternNodes = Set(C, D).map(_.name), quantifiedPathPatterns = Set(qpp(C, D)))

    singleQuantifiedPathPatternQG.connectedComponents.toSet shouldBe Set(qq1, qq2)
  }

  test("two consecutive quantified path patterns connecting three nodes") {
    val quantifiedPathPatternQG =
      QueryGraph(
        patternNodes = Set(A, B, C, D).map(_.name),
        patternRelationships = Set.empty,
        quantifiedPathPatterns = Set(qpp(A, B), qpp(C, B))
      )

    val connectedPart =
      QueryGraph(
        patternNodes = Set(A, B, C).map(_.name),
        patternRelationships = Set.empty,
        quantifiedPathPatterns = Set(qpp(A, B), qpp(C, B))
      )
    val otherNode = QueryGraph(patternNodes = Set(D.name))
    quantifiedPathPatternQG.connectedComponents.toSet shouldBe Set(connectedPart, otherNode)
  }

  test("should pick the predicates correctly on quantified path patterns when they depend on arguments") {
    //  UNWIND [[0]] AS x MATCH (a) ((a_inner)-[]->(b_inner))* (b) WHERE a_inner = x
    val graph = QueryGraph(
      argumentIds = Set(X.name),
      patternNodes = Set(A, B).map(_.name),
      quantifiedPathPatterns = Set(qpp(A, B)),
      selections = Selections.from(equals(varFor("a_inner_group"), X))
    )

    val components = graph.connectedComponents
    components should equal(Seq(graph))
  }

  test("nodes connected via shortest path") {
    val spps = Set(spp(A, B))

    val sppQG =
      QueryGraph(
        patternNodes = Set(A, B, C).map(_.name),
        patternRelationships = Set.empty,
        selectivePathPatterns = spps
      )

    val connectedPart =
      QueryGraph(
        patternNodes = Set(A, B).map(_.name),
        patternRelationships = Set.empty,
        selectivePathPatterns = spps
      )
    val otherNode = QueryGraph(patternNodes = Set(C.name))
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
        patternNodes = Set(A, C, D).map(_.name),
        patternRelationships = Set.empty,
        selectivePathPatterns = spps
      )

    val connectedPart =
      QueryGraph(
        patternNodes = Set(A, C).map(_.name),
        patternRelationships = Set.empty,
        selectivePathPatterns = spps
      )
    val otherNode = QueryGraph(patternNodes = Set(D.name))
    sppQG.connectedComponents.toSet shouldBe Set(connectedPart, otherNode)
  }

  test("2 sets of nodes connected via 2 shortest paths") {
    val sppAB = spp(A, B)
    val sppCD = spp(C, D)
    val spps = Set(sppAB, sppCD)

    val sppQG =
      QueryGraph(
        patternNodes = Set(A, B, C, D).map(_.name),
        patternRelationships = Set.empty,
        selectivePathPatterns = spps
      )

    val connectedParts = Set(
      QueryGraph(
        patternNodes = Set(A, B).map(_.name),
        patternRelationships = Set.empty,
        selectivePathPatterns = Set(sppAB)
      ),
      QueryGraph(
        patternNodes = Set(C, D).map(_.name),
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
        patternNodes = Set(A, B, C, D).map(_.name),
        patternRelationships = Set(rel(B, C)),
        selectivePathPatterns = spps
      )

    sppQG.connectedComponents shouldBe Seq(sppQG)
  }

  test("should pick the predicates correctly on selective path patterns when they depend on arguments") {
    val graph = QueryGraph(
      argumentIds = Set(X.name),
      patternNodes = Set(A, C).map(_.name),
      selectivePathPatterns = Set(SelectivePathPattern(
        ExhaustivePathPattern.NodeConnections(NonEmptyList(qpp(A, B), qpp(B, C))),
        Selections.empty,
        SelectivePathPattern.Selector.Any(1)
      )),
      selections = Selections.from(equals(B, X))
    )

    val components = graph.connectedComponents
    components should equal(Seq(graph))
  }

  test("selective path pattern should pull in predicates without dependency on arguments") {
    val graph = QueryGraph(
      patternNodes = Set(A, C).map(_.name),
      selectivePathPatterns = Set(SelectivePathPattern(
        ExhaustivePathPattern.NodeConnections(NonEmptyList(qpp(A, B), qpp(B, C))),
        Selections.empty,
        SelectivePathPattern.Selector.Any(1)
      )),
      selections = Selections.from(isNotNull(B))
    )

    val components = graph.connectedComponents
    components should equal(Seq(graph))
  }
}
