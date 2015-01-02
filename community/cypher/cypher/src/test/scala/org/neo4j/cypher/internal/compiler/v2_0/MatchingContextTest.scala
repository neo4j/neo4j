/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0

import org.neo4j.graphdb.Transaction
import commands._
import commands.expressions.{Identifier, Literal, Property}
import commands.values.TokenType.PropertyKey
import executionplan.builders.PatternGraphBuilder
import pipes.QueryState
import symbols._
import org.neo4j.cypher.GraphDatabaseJUnitSuite
import org.neo4j.graphdb.{Node, Direction}
import org.junit.{After, Before, Test}
import collection.Map
import org.neo4j.cypher.internal.compiler.v2_0.pipes.matching.MatchingContext

class MatchingContextTest extends GraphDatabaseJUnitSuite with PatternGraphBuilder {
  var a: Node = null
  var b: Node = null
  var c: Node = null
  var d: Node = null

  var state: QueryState = null
  var matchingContext: MatchingContext = null
  var tx: Transaction = null

  private def ctx(x: (String, Any)*) = {
    if(tx == null) tx = graph.beginTx()
    state = QueryStateHelper.queryStateFrom(graph, tx)
    ExecutionContext().newWith(x.toMap)
  }

  @Before
  def init() {
    a = createNode("a")
    b = createNode("b")
    c = createNode("c")
    d = createNode("d")
  }

  @After
  def cleanup()
  {
    if(tx != null) tx.close()
  }

  @Test def singleHopSingleMatch() {
    val r = relate(a, b, "rel")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", "rel", Direction.OUTGOING))
    createMatchingContextWithNodes(patterns, Seq("a"))

    assertMatches(getMatches("a" -> a), 1, Map("a" -> a, "b" -> b, "r" -> r))
  }

  private def getMatches(params: (String, Any)*) = matchingContext.getMatches(ctx(params:_*), state)

  @Test def singleDirectedRel() {
    val r = relate(a, b, "rel", "r")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", "rel", Direction.OUTGOING))
    createMatchingContextWithRels(patterns, Seq("r"))

    assertMatches(getMatches("r" -> r), 1, Map("a" -> a, "b" -> b, "r" -> r))
  }

  @Test def singleDirectedRelTurnedTheWrongWay() {
    val r = relate(a, b, "rel", "r")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", "rel", Direction.INCOMING))
    createMatchingContextWithRels(patterns, Seq("r"))

    assertMatches(getMatches("r" -> r), 1, Map("a" -> b, "b" -> a, "r" -> r))
  }

  @Test def singleUndirectedRel() {
    val r = relate(a, b, "rel")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", "rel", Direction.BOTH))
    createMatchingContextWithRels(patterns, Seq("r"))

    assertMatches(getMatches("r" -> r), 2,
      Map("a" -> a, "b" -> b, "r" -> r),
      Map("a" -> b, "b" -> a, "r" -> r))
  }

  @Test def twoUndirectedRel() {
    val r1 = relate(a, b, "rel", "r1")
    val r2 = relate(b, c, "rel", "r2")

    val patterns: Seq[Pattern] = Seq(
      RelatedTo("a", "b", "r1", "rel", Direction.BOTH),
      RelatedTo("b", "c", "r2", "rel", Direction.BOTH)
    )

    createMatchingContextWithRels(patterns, Seq("r1", "r2"))

    assertMatches(getMatches("r1" -> r1, "r2" -> r2), 1)
  }

  @Test def singleHopDoubleMatch() {
    val r1 = relate(a, b, "rel", "r1")
    val r2 = relate(a, c, "rel", "r2")

    val patterns: Seq[Pattern] = Seq(RelatedTo("pA", "pB", "pR", "rel", Direction.OUTGOING))
    createMatchingContextWithNodes(patterns, Seq("pA"))


    assertMatches(getMatches("pA" -> a), 2,
      Map("pA" -> a, "pB" -> b, "pR" -> r1),
      Map("pA" -> a, "pB" -> c, "pR" -> r2))
  }

  @Test def twoBoundNodesShouldWork() {
    val r1 = relate(a, b, "rel", "r1")

    val patterns: Seq[Pattern] = Seq(RelatedTo("pA", "pB", "pR", "rel", Direction.OUTGOING))
    createMatchingContextWithNodes(patterns, Seq("pA", "pB"))


    assertMatches(getMatches("pA" -> a, "pB" -> b), 1,
      Map("pA" -> a, "pB" -> b, "pR" -> r1))
  }

  @Test def boundNodeAndRel() {
    val r1 = relate(a, b, "rel", "r1")
    relate(a, b, "rel", "r2")

    val patterns: Seq[Pattern] = Seq(RelatedTo("pA", "pB", "pR", "rel", Direction.OUTGOING))

    createMatchingContextWith(patterns, Seq("pA"), Seq("pR"))

    assertMatches(getMatches("pA" -> a, "pR" -> r1), 1,
      Map("pA" -> a, "pB" -> b, "pR" -> r1))
  }

  @Test def doubleHopDoubleMatch() {
    val r1 = relate(a, b, "rel")
    val r2 = relate(a, c, "rel")

    val patterns: Seq[Pattern] = Seq(
      RelatedTo(SingleNode("a"), SingleNode("b"), "r1", Seq(), Direction.OUTGOING, Map.empty),
      RelatedTo(SingleNode("a"), SingleNode("c"), "r2", Seq(), Direction.OUTGOING, Map.empty)
    )
    createMatchingContextWithNodes(patterns, Seq("a"))

    val traversable = getMatches("a" -> a)
    assertMatches(traversable, 2,
      Map("a" -> a, "b" -> c, "c" -> b, "r1" -> r2, "r2" -> r1),
      Map("a" -> a, "b" -> b, "c" -> c, "r1" -> r1, "r2" -> r2))
  }

  @Test def theDreadedDiamondTest() {
    val r1 = relate(a, b, "x", "r1")
    val r2 = relate(a, c, "x", "r2")
    val r3 = relate(b, d, "x", "r3")
    val r4 = relate(c, d, "x", "r4")

    val patterns: Seq[Pattern] = Seq(
      RelatedTo(SingleNode("A"), SingleNode("B"), "pr1", Seq(), Direction.OUTGOING, Map.empty),
      RelatedTo(SingleNode("A"), SingleNode("C"), "pr2", Seq(), Direction.OUTGOING, Map.empty),
      RelatedTo(SingleNode("B"), SingleNode("D"), "pr3", Seq(), Direction.OUTGOING, Map.empty),
      RelatedTo(SingleNode("C"), SingleNode("D"), "pr4", Seq(), Direction.OUTGOING, Map.empty)
    )

    createMatchingContextWithNodes(patterns, Seq("A"))

    assertMatches(getMatches("A" -> a), 2,
      Map("A" -> a, "B" -> b, "C" -> c, "D" -> d, "pr1" -> r1, "pr2" -> r2, "pr3" -> r3, "pr4" -> r4),
      Map("A" -> a, "B" -> c, "C" -> b, "D" -> d, "pr1" -> r2, "pr2" -> r1, "pr3" -> r4, "pr4" -> r3))
  }

  private def createDiamondWithExtraLoop(start: Node): Node = {
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

    e
  }

  @Test def should_be_able_to_handle_double_loops() {
    val e = createDiamondWithExtraLoop(a)

    val patterns: Seq[Pattern] = Seq(
      RelatedTo(SingleNode("A"), SingleNode("B"), "pr1", Seq(), Direction.OUTGOING, Map.empty),
      RelatedTo(SingleNode("A"), SingleNode("C"), "pr2", Seq(), Direction.OUTGOING, Map.empty),
      RelatedTo(SingleNode("B"), SingleNode("D"), "pr3", Seq(), Direction.OUTGOING, Map.empty),
      RelatedTo(SingleNode("C"), SingleNode("D"), "pr4", Seq(), Direction.OUTGOING, Map.empty),
      RelatedTo(SingleNode("B"), SingleNode("E"), "pr5", Seq("IN"), Direction.OUTGOING, Map.empty),
      RelatedTo(SingleNode("C"), SingleNode("E"), "pr6", Seq("IN"), Direction.OUTGOING, Map.empty)
    )

    createMatchingContextWithNodes(patterns, Seq("A", "E"))

    val matches = getMatches("A" -> a, "E" -> e)

    assertMatches(matches, 2)
  }

  @Test def pinnedNodeMakesNoMatchesInDisjunctGraph() {
    relate(a, b, "rel")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "c", "r", "rel", Direction.OUTGOING))
    createMatchingContextWithNodes(patterns, Seq("a", "c"))

    assertMatches(getMatches("a" -> a, "c" -> c), 0)
  }

  @Test def pinnedNodeMakesNoMatches() {
    val r1 = relate(a, b, "x")
    val r2 = relate(a, c, "x")
    val r3 = relate(b, d, "x")
    val r4 = relate(c, d, "x")

    val patterns: Seq[Pattern] = Seq(
      RelatedTo(SingleNode("a"), SingleNode("b"), "r1", Seq(), Direction.OUTGOING, Map.empty),
      RelatedTo(SingleNode("a"), SingleNode("c"), "r2", Seq(), Direction.OUTGOING, Map.empty),
      RelatedTo(SingleNode("b"), SingleNode("d"), "r3", Seq(), Direction.OUTGOING, Map.empty),
      RelatedTo(SingleNode("c"), SingleNode("d"), "r4", Seq(), Direction.OUTGOING, Map.empty)
    )
    createMatchingContextWithNodes(patterns, Seq("a", "b"))

    assertMatches(getMatches("a" -> a, "b" -> b), 1,
      Map("a" -> a, "b" -> b, "c" -> c, "d" -> d, "r1" -> r1, "r2" -> r2, "r3" -> r3, "r4" -> r4))
  }

  @Test def directionConstraintFiltersMatches() {
    val r1 = relate(a, b, "rel")
    val r2 = relate(c, a, "rel")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", "rel", Direction.OUTGOING))

    createMatchingContextWithNodes(patterns, Seq("a"))
    assertMatches(getMatches("a" -> a), 1, Map("a" -> a, "b" -> b, "r" -> r1))

    createMatchingContextWithNodes(patterns, Seq("b"))
    assertMatches(getMatches("b" -> a), 1, Map("b" -> a, "a" -> c, "r" -> r2))
  }

  @Test def typeConstraintFiltersMatches() {
    val r1 = relate(a, b, "t1")
    relate(a, b, "t2")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", "t1", Direction.OUTGOING))
    createMatchingContextWithNodes(patterns, Seq("a"))

    assertMatches(getMatches("a" -> a), 1, Map("a" -> a, "b" -> b, "r" -> r1))
  }


  @Test def variableLengthPath() {
    relate(a, b, "rel")
    relate(b, c, "rel")

    val patterns: Seq[Pattern] = Seq(VarLengthRelatedTo("p", SingleNode("a"), SingleNode("c"), Some(1), Some(2), Seq("rel"), Direction.OUTGOING, None, Map.empty))
    createMatchingContextWithNodes(patterns, Seq("a"))

    assertMatches(getMatches("a" -> a), 2, Map("a" -> a, "c" -> b), Map("a" -> a, "c" -> c))
  }

  @Test def variableLengthPathWithOneHopBefore() {
    val r1 = relate(a, b, "rel")
    relate(b, c, "rel")
    relate(c, d, "rel")

    val patterns: Seq[Pattern] = Seq(
      RelatedTo("a", "b", "r1", "rel", Direction.OUTGOING),
      VarLengthRelatedTo("p", "b", "c", Some(1), Some(2), "rel", Direction.OUTGOING))
    createMatchingContextWithNodes(patterns, Seq("a"))

    assertMatches(getMatches("a" -> a), 2, Map("a" -> a, "r1" -> r1, "b" -> b, "c" -> c), Map("a" -> a, "r1" -> r1, "b" -> b, "c" -> d))
  }

  @Test def variableLengthPathWithOneHopBeforeWithDifferentType() {
    val r1 = relate(a, b, "t1")
    relate(b, c, "t1")
    relate(c, d, "t2")

    val patterns = Seq(
      RelatedTo("a", "b", "r1", "t1", Direction.OUTGOING),
      VarLengthRelatedTo("p", "b", "c", Some(1), Some(2), "t1", Direction.OUTGOING))
    createMatchingContextWithNodes(patterns, Seq("a"))

    assertMatches(getMatches("a" -> a), 1, Map("a" -> a, "r1" -> r1, "b" -> b, "c" -> c))
  }

  @Test def variableLengthPathWithBranch() {
    relate(a, b, "t1")
    relate(b, c, "t1")
    relate(b, d, "t1")

    val patterns: Seq[Pattern] = Seq(
      VarLengthRelatedTo("p", "a", "x", Some(1), Some(2), "t1", Direction.OUTGOING))
    createMatchingContextWithNodes(patterns, Seq("a"))

    assertMatches(getMatches("a" -> a), 3,
      Map("a" -> a, "x" -> b),
      Map("a" -> a, "x" -> c),
      Map("a" -> a, "x" -> d))
  }

  @Test def variableLengthPathWithPinnedEndNode() {
    relate(a, b, "t1")
    relate(b, c, "t1")
    relate(b, d, "t1")

    val patterns: Seq[Pattern] = Seq(
      VarLengthRelatedTo("p", "a", "x", Some(1), Some(2), "t1", Direction.OUTGOING))
    createMatchingContextWithNodes(patterns, Seq("a", "x"))

    assertMatches(getMatches("a" -> a, "x" -> d), 1, Map("a" -> a, "x" -> d))
  }

  @Test def varLengthPathWithTwoPaths() {
    relate(a, b, "t1")
    relate(a, c, "t1")
    relate(b, c, "t1")
    relate(c, d, "t1")

    val patterns: Seq[Pattern] = Seq(
      VarLengthRelatedTo("P", "A", "X", None, None, "t1", Direction.OUTGOING))
    createMatchingContextWithNodes(patterns, Seq("A", "X"))

    assertMatches(getMatches("A" -> a, "X" -> d), 2)
  }

  @Test def variableLengthPathInDiamond() {
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


    val r1 = relate(a, b, "rel", "r1")
    val r2 = relate(a, c, "rel", "r2")
    relate(b, d, "rel", "r3")
    relate(d, c, "rel", "r4")
    relate(b, c, "rel", "r5")

    val patterns: Seq[Pattern] = Seq(
      RelatedTo("pA", "pB", "pR1", "rel", Direction.OUTGOING),
      RelatedTo("pA", "pC", "pR2", "rel", Direction.OUTGOING),
      VarLengthRelatedTo("p", "pB", "pC", Some(1), Some(3), "rel", Direction.OUTGOING))
    createMatchingContextWithNodes(patterns, Seq("pA"))

    val traversable = getMatches("pA" -> a).toList

    assertMatches(traversable, 2,
      Map("pA" -> a, "pR1" -> r1, "pB" -> b, "pC" -> c, "pR2" -> r2))
  }

  @Test def predicateConcerningRelationship() {
    val r = relate(a, b, "rel", Map("age" -> 15))
    val r2 = relate(a, b, "rel", Map("age" -> 5))

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", "rel", Direction.OUTGOING))

    createMatchingContextWithNodes(patterns, Seq("a"), Seq(Equals(Property(Identifier("r"), PropertyKey("age")), Literal(5))))

    assertMatches(getMatches("a" -> a), 1, Map("a" -> a, "b" -> b, "r" -> r2))
  }

  @Test def predicateConcerningNode() {
    val a = createNode(Map("prop" -> "value"))
    relate(a, b, "rel")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", "rel", Direction.OUTGOING))
    createMatchingContextWithNodes(patterns, Seq("a"), Seq(Equals(Property(Identifier("a"), PropertyKey("prop")), Literal("not value"))))

    assert(getMatches("a" -> a).size === 0)
  }


  private def assertMatches(matches: Traversable[Map[String, Any]], expectedSize: Int, expected: Map[String, Any]*) {
    val matchesList = matches.toList
    assert(matchesList.size === expectedSize)

    expected.foreach(expectation => {
      if (!matches.exists(compare(_, expectation))) {

        fail("Didn't find the expected row: " + expectation + "\r\nActual: " + matches.toList)
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
