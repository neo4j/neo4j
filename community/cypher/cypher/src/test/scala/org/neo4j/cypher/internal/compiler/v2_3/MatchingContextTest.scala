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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.GraphDatabaseFunSuite
import org.neo4j.cypher.internal.compiler.v2_3.commands._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Identifier, Literal, Property}
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.{Equals, Predicate}
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders.PatternGraphBuilder
import org.neo4j.cypher.internal.compiler.v2_3.pipes.matching.MatchingContext
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

import scala.collection.Map

class MatchingContextTest extends GraphDatabaseFunSuite with PatternGraphBuilder with QueryStateTestSupport {
  test("singleHopSingleMatch") {
    val a = createNode("a")
    val b = createNode("b")
    val r = relate(a, b, "rel")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", "rel", SemanticDirection.OUTGOING))
    createMatchingContextWithNodes(patterns, Seq("a"))

    assertMatches(getMatches("a" -> a), 1, Map("a" -> a, "b" -> b, "r" -> r))
  }

  test("singleDirectedRel") {
    val a = createNode("a")
    val b = createNode("b")
    val r = relate(a, b, "rel", "r")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", "rel", SemanticDirection.OUTGOING))
    createMatchingContextWithRels(patterns, Seq("r"))

    assertMatches(getMatches("r" -> r), 1, Map("a" -> a, "b" -> b, "r" -> r))
  }

  test("singleDirectedRelTurnedTheWrongWay") {
    val a = createNode("a")
    val b = createNode("b")
    val r = relate(a, b, "rel", "r")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", "rel", SemanticDirection.INCOMING))
    createMatchingContextWithRels(patterns, Seq("r"))

    assertMatches(getMatches("r" -> r), 1, Map("a" -> b, "b" -> a, "r" -> r))
  }

  test("singleUndirectedRel") {
    val a = createNode("a")
    val b = createNode("b")
    val r = relate(a, b, "rel")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", "rel", SemanticDirection.BOTH))
    createMatchingContextWithRels(patterns, Seq("r"))

    assertMatches(getMatches("r" -> r), 2,
      Map("a" -> a, "b" -> b, "r" -> r),
      Map("a" -> b, "b" -> a, "r" -> r))
  }

  test("twoUndirectedRel") {
    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    val r1 = relate(a, b, "rel", "r1")
    val r2 = relate(b, c, "rel", "r2")

    val patterns: Seq[Pattern] = Seq(
      RelatedTo("a", "b", "r1", "rel", SemanticDirection.BOTH),
      RelatedTo("b", "c", "r2", "rel", SemanticDirection.BOTH)
    )

    createMatchingContextWithRels(patterns, Seq("r1", "r2"))

    assertMatches(getMatches("r1" -> r1, "r2" -> r2), 1)
  }

  test("singleHopDoubleMatch") {
    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    val r1 = relate(a, b, "rel", "r1")
    val r2 = relate(a, c, "rel", "r2")

    val patterns: Seq[Pattern] = Seq(RelatedTo("pA", "pB", "pR", "rel", SemanticDirection.OUTGOING))
    createMatchingContextWithNodes(patterns, Seq("pA"))

    assertMatches(getMatches("pA" -> a), 2,
      Map("pA" -> a, "pB" -> b, "pR" -> r1),
      Map("pA" -> a, "pB" -> c, "pR" -> r2))
  }

  test("twoBoundNodesShouldWork") {
    val a = createNode("a")
    val b = createNode("b")
    val r1 = relate(a, b, "rel", "r1")

    val patterns: Seq[Pattern] = Seq(RelatedTo("pA", "pB", "pR", "rel", SemanticDirection.OUTGOING))
    createMatchingContextWithNodes(patterns, Seq("pA", "pB"))

    assertMatches(getMatches("pA" -> a, "pB" -> b), 1,
      Map("pA" -> a, "pB" -> b, "pR" -> r1))
  }

  test("boundNodeAndRel") {
    val a = createNode("a")
    val b = createNode("b")
    val r1 = relate(a, b, "rel", "r1")
    relate(a, b, "rel", "r2")

    val patterns: Seq[Pattern] = Seq(RelatedTo("pA", "pB", "pR", "rel", SemanticDirection.OUTGOING))

    createMatchingContextWith(patterns, Seq("pA"), Seq("pR"))

    assertMatches(getMatches("pA" -> a, "pR" -> r1), 1,
      Map("pA" -> a, "pB" -> b, "pR" -> r1))
  }

  test("doubleHopDoubleMatch") {
    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    val r1 = relate(a, b, "rel")
    val r2 = relate(a, c, "rel")

    val patterns: Seq[Pattern] = Seq(
      RelatedTo(SingleNode("a"), SingleNode("b"), "r1", Seq(), SemanticDirection.OUTGOING, Map.empty),
      RelatedTo(SingleNode("a"), SingleNode("c"), "r2", Seq(), SemanticDirection.OUTGOING, Map.empty)
    )
    createMatchingContextWithNodes(patterns, Seq("a"))

    assertMatches(getMatches("a" -> a), 2,
      Map("a" -> a, "b" -> c, "c" -> b, "r1" -> r2, "r2" -> r1),
      Map("a" -> a, "b" -> b, "c" -> c, "r1" -> r1, "r2" -> r2))
  }

  test("theDreadedDiamondTest") {
    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    val d = createNode("d")
    val r1 = relate(a, b, "x", "r1")
    val r2 = relate(a, c, "x", "r2")
    val r3 = relate(b, d, "x", "r3")
    val r4 = relate(c, d, "x", "r4")

    val patterns: Seq[Pattern] = Seq(
      RelatedTo(SingleNode("A"), SingleNode("B"), "pr1", Seq(), SemanticDirection.OUTGOING, Map.empty),
      RelatedTo(SingleNode("A"), SingleNode("C"), "pr2", Seq(), SemanticDirection.OUTGOING, Map.empty),
      RelatedTo(SingleNode("B"), SingleNode("D"), "pr3", Seq(), SemanticDirection.OUTGOING, Map.empty),
      RelatedTo(SingleNode("C"), SingleNode("D"), "pr4", Seq(), SemanticDirection.OUTGOING, Map.empty)
    )

    createMatchingContextWithNodes(patterns, Seq("A"))

    assertMatches(getMatches("A" -> a), 2,
      Map("A" -> a, "B" -> b, "C" -> c, "D" -> d, "pr1" -> r1, "pr2" -> r2, "pr3" -> r3, "pr4" -> r4),
      Map("A" -> a, "B" -> c, "C" -> b, "D" -> d, "pr1" -> r2, "pr2" -> r1, "pr3" -> r4, "pr4" -> r3))
  }


  test("should_be_able_to_handle_double_loops") {
    val a = createNode("a")
    val b = createNode()
    val c = createNode()
    val d = createNode()
    val e = createNode()

    relate(a, b, "x", "r1")
    relate(a, c, "x", "r2")
    relate(b, d, "x", "r3")
    relate(c, d, "x", "r4")
    relate(b, e, "IN", "r4")
    relate(c, e, "IN", "r4")

    val patterns: Seq[Pattern] = Seq(
      RelatedTo(SingleNode("A"), SingleNode("B"), "pr1", Seq(), SemanticDirection.OUTGOING, Map.empty),
      RelatedTo(SingleNode("A"), SingleNode("C"), "pr2", Seq(), SemanticDirection.OUTGOING, Map.empty),
      RelatedTo(SingleNode("B"), SingleNode("D"), "pr3", Seq(), SemanticDirection.OUTGOING, Map.empty),
      RelatedTo(SingleNode("C"), SingleNode("D"), "pr4", Seq(), SemanticDirection.OUTGOING, Map.empty),
      RelatedTo(SingleNode("B"), SingleNode("E"), "pr5", Seq("IN"), SemanticDirection.OUTGOING, Map.empty),
      RelatedTo(SingleNode("C"), SingleNode("E"), "pr6", Seq("IN"), SemanticDirection.OUTGOING, Map.empty)
    )

    createMatchingContextWithNodes(patterns, Seq("A", "E"))

    assertMatches(getMatches("A" -> a, "E" -> e), 2)
  }

  test("pinnedNodeMakesNoMatchesInDisjunctGraph") {
    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    relate(a, b, "rel")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "c", "r", "rel", SemanticDirection.OUTGOING))
    createMatchingContextWithNodes(patterns, Seq("a", "c"))

    assertMatches(getMatches("a" -> a, "c" -> c), 0)
  }

  test("pinnedNodeMakesNoMatches") {
    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    val d = createNode("d")
    val r1 = relate(a, b, "x")
    val r2 = relate(a, c, "x")
    val r3 = relate(b, d, "x")
    val r4 = relate(c, d, "x")

    val patterns: Seq[Pattern] = Seq(
      RelatedTo(SingleNode("a"), SingleNode("b"), "r1", Seq(), SemanticDirection.OUTGOING, Map.empty),
      RelatedTo(SingleNode("a"), SingleNode("c"), "r2", Seq(), SemanticDirection.OUTGOING, Map.empty),
      RelatedTo(SingleNode("b"), SingleNode("d"), "r3", Seq(), SemanticDirection.OUTGOING, Map.empty),
      RelatedTo(SingleNode("c"), SingleNode("d"), "r4", Seq(), SemanticDirection.OUTGOING, Map.empty)
    )
    createMatchingContextWithNodes(patterns, Seq("a", "b"))

    assertMatches(getMatches("a" -> a, "b" -> b), 1,
      Map("a" -> a, "b" -> b, "c" -> c, "d" -> d, "r1" -> r1, "r2" -> r2, "r3" -> r3, "r4" -> r4))
  }

  test("directionConstraintFiltersMatches") {
    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    val r1 = relate(a, b, "rel")
    val r2 = relate(c, a, "rel")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", "rel", SemanticDirection.OUTGOING))

    createMatchingContextWithNodes(patterns, Seq("a"))
    assertMatches(getMatches("a" -> a), 1, Map("a" -> a, "b" -> b, "r" -> r1))

    createMatchingContextWithNodes(patterns, Seq("b"))
    assertMatches(getMatches("b" -> a), 1, Map("b" -> a, "a" -> c, "r" -> r2))
  }

  test("typeConstraintFiltersMatches") {
    val a = createNode("a")
    val b = createNode("b")
    val r1 = relate(a, b, "t1")
    relate(a, b, "t2")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", "t1", SemanticDirection.OUTGOING))
    createMatchingContextWithNodes(patterns, Seq("a"))

    assertMatches(getMatches("a" -> a), 1, Map("a" -> a, "b" -> b, "r" -> r1))
  }

  test("variableLengthPath") {
    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    relate(a, b, "rel")
    relate(b, c, "rel")

    val patterns: Seq[Pattern] = Seq(VarLengthRelatedTo("p", SingleNode("a"), SingleNode("c"), Some(1), Some(2), Seq("rel"), SemanticDirection.OUTGOING, None, Map.empty))
    createMatchingContextWithNodes(patterns, Seq("a"))

    assertMatches(getMatches("a" -> a), 2, Map("a" -> a, "c" -> b), Map("a" -> a, "c" -> c))
  }

  test("variableLengthPathWithOneHopBefore") {
    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    val d = createNode("d")
    val r1 = relate(a, b, "rel")
    relate(b, c, "rel")
    relate(c, d, "rel")

    val patterns: Seq[Pattern] = Seq(
      RelatedTo("a", "b", "r1", "rel", SemanticDirection.OUTGOING),
      VarLengthRelatedTo("p", "b", "c", Some(1), Some(2), "rel", SemanticDirection.OUTGOING))
    createMatchingContextWithNodes(patterns, Seq("a"))

    assertMatches(getMatches("a" -> a), 2, Map("a" -> a, "r1" -> r1, "b" -> b, "c" -> c), Map("a" -> a, "r1" -> r1, "b" -> b, "c" -> d))
  }

  test("variableLengthPathWithOneHopBeforeWithDifferentType") {
    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    val d = createNode("d")
    val r1 = relate(a, b, "t1")
    relate(b, c, "t1")
    relate(c, d, "t2")

    val patterns = Seq(
      RelatedTo("a", "b", "r1", "t1", SemanticDirection.OUTGOING),
      VarLengthRelatedTo("p", "b", "c", Some(1), Some(2), "t1", SemanticDirection.OUTGOING))
    createMatchingContextWithNodes(patterns, Seq("a"))

    assertMatches(getMatches("a" -> a), 1, Map("a" -> a, "r1" -> r1, "b" -> b, "c" -> c))
  }

  test("variableLengthPathWithBranch") {
    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    val d = createNode("d")
    relate(a, b, "t1")
    relate(b, c, "t1")
    relate(b, d, "t1")

    val patterns: Seq[Pattern] = Seq(
      VarLengthRelatedTo("p", "a", "x", Some(1), Some(2), "t1", SemanticDirection.OUTGOING))
    createMatchingContextWithNodes(patterns, Seq("a"))

    assertMatches(getMatches("a" -> a), 3,
      Map("a" -> a, "x" -> b),
      Map("a" -> a, "x" -> c),
      Map("a" -> a, "x" -> d))
  }

  test("variableLengthPathWithPinnedEndNode") {
    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    val d = createNode("d")
    relate(a, b, "t1")
    relate(b, c, "t1")
    relate(b, d, "t1")

    val patterns: Seq[Pattern] = Seq(
      VarLengthRelatedTo("p", "a", "x", Some(1), Some(2), "t1", SemanticDirection.OUTGOING))
    createMatchingContextWithNodes(patterns, Seq("a", "x"))

    assertMatches(getMatches("a" -> a, "x" -> d), 1, Map("a" -> a, "x" -> d))
  }

  test("varLengthPathWithTwoPaths") {
    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    val d = createNode("d")
    relate(a, b, "t1")
    relate(a, c, "t1")
    relate(b, c, "t1")
    relate(c, d, "t1")

    val patterns: Seq[Pattern] = Seq(
      VarLengthRelatedTo("P", "A", "X", None, None, "t1", SemanticDirection.OUTGOING))
    createMatchingContextWithNodes(patterns, Seq("A", "X"))

    assertMatches(getMatches("A" -> a, "X" -> d), 2)
  }

  test("variableLengthPathInDiamond") {
    /*
    Graph:
              a
             / \
            v   v
           b--->c
           \   ^
            v /
             d


    Pattern:
              pA
             / \
            v   v
         p=pB~~>pC

    Should match two subgraphs, one where p is b-c, and one where it is b-d-c
     */

    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    val d = createNode("d")
    val r1 = relate(a, b, "rel", "r1")
    val r2 = relate(a, c, "rel", "r2")
    relate(b, d, "rel", "r3")
    relate(d, c, "rel", "r4")
    relate(b, c, "rel", "r5")

    val patterns: Seq[Pattern] = Seq(
      RelatedTo("pA", "pB", "pR1", "rel", SemanticDirection.OUTGOING),
      RelatedTo("pA", "pC", "pR2", "rel", SemanticDirection.OUTGOING),
      VarLengthRelatedTo("p", "pB", "pC", Some(1), Some(3), "rel", SemanticDirection.OUTGOING))
    createMatchingContextWithNodes(patterns, Seq("pA"))

    val traversable = getMatches("pA" -> a).toList

    assertMatches(traversable, 2,
      Map("pA" -> a, "pR1" -> r1, "pB" -> b, "pC" -> c, "pR2" -> r2))
  }

  test("predicateConcerningRelationship") {
    val a = createNode("a")
    val b = createNode("b")
    val r = relate(a, b, "rel", Map("age" -> 5))
    relate(a, b, "rel", Map("age" -> 15))

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", "rel", SemanticDirection.OUTGOING))

    createMatchingContextWithNodes(patterns, Seq("a"), Seq(Equals(Property(Identifier("r"), PropertyKey("age")), Literal(5))))

    assertMatches(getMatches("a" -> a), 1, Map("a" -> a, "b" -> b, "r" -> r))
  }

  test("predicateConcerningNode") {
    val a = createNode(Map("prop" -> "value"))
    val b = createNode("b")
    relate(a, b, "rel")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", "rel", SemanticDirection.OUTGOING))
    createMatchingContextWithNodes(patterns, Seq("a"), Seq(Equals(Property(Identifier("a"), PropertyKey("prop")), Literal("not value"))))

    getMatches("a" -> a) shouldBe empty
  }

  private var matchingContext: MatchingContext = null

  private def getMatches(params: (String, Any)*) = withQueryState { queryState =>
    val ctx = ExecutionContext().newWith(params.toMap)
    matchingContext.getMatches(ctx, queryState).toList
  }

  private def assertMatches(matches: List[Map[String, Any]], expectedSize: Int, expected: Map[String, Any]*) {
    matches should have size expectedSize
    expected.foreach(expectation => {
      withClue("Didn't find the expected row: ") {
        matches.exists(compare(_, expectation)) should be(true)
      }
    })
  }

  private def createMatchingContextWith(patterns: Seq[Pattern], nodes: Seq[String], rels: Seq[String], predicates: Seq[Predicate] = Seq[Predicate]()) {
    val nodeIdentifiers2 = nodes.map(_ -> CTNode)
    val relIdentifiers2 = rels.map(_ -> CTRelationship)

    val identifiers2 = (nodeIdentifiers2 ++ relIdentifiers2).toMap
    val symbols2 = SymbolTable(identifiers2)
    val identifiers = Pattern.identifiers(patterns)
    matchingContext = new MatchingContext(symbols2, predicates, buildPatternGraph(symbols2, patterns), identifiers)
  }

  private def createMatchingContextWithRels(patterns: Seq[Pattern], rels: Seq[String], predicates: Seq[Predicate] = Seq[Predicate]()) {
    createMatchingContextWith(patterns, Seq(), rels, predicates)
  }

  private def createMatchingContextWithNodes(patterns: Seq[Pattern], nodes: Seq[String], predicates: Seq[Predicate] = Seq[Predicate]()) {
    createMatchingContextWith(patterns, nodes, Seq(), predicates)
  }

  private def compare(matches: Map[String, Any], expectation: Map[String, Any]): Boolean = {
    expectation.foreach(kv =>
      matches.get(kv._1) match {
        case None    => return false
        case Some(x) => if (x != kv._2) return false
      })

    true
  }
}
