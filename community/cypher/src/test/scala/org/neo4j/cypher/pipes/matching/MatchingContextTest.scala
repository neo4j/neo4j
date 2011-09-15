/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

package org.neo4j.cypher.pipes.matching

import org.scalatest.Assertions
import org.neo4j.graphdb.Direction
import org.junit.{Ignore, Test}
import org.neo4j.cypher.{SymbolTable, GraphDatabaseTestBase}
import org.neo4j.cypher.commands.{NodeIdentifier, VarLengthRelatedTo, RelatedTo, Pattern}

class MatchingContextTest extends GraphDatabaseTestBase with Assertions {

  @Test def singleHopSingleMatch() {
    val a = createNode()
    val b = createNode()
    val r = relate(a, b, "rel")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", "rel", Direction.OUTGOING, false))
    val matchingContext = new MatchingContext(patterns, bind("a"))

    assertMatches(matchingContext.getMatches(Map("a" -> a)), 1, Map("a" -> a, "b" -> b, "r" -> r))
  }

  @Test def singleHopDoubleMatch() {
    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    val r1 = relate(a, b, "rel", "r1")
    val r2 = relate(a, c, "rel", "r2")

    val patterns: Seq[Pattern] = Seq(RelatedTo("pA", "pB", "pR", "rel", Direction.OUTGOING, false))
    val matchingContext = new MatchingContext(patterns, bind("pA"))


    assertMatches(matchingContext.getMatches(Map("pA" -> a)), 2,
      Map("pA" -> a, "pB" -> b, "pR" -> r1),
      Map("pA" -> a, "pB" -> c, "pR" -> r2))
  }

  @Test def doubleHopDoubleMatch() {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val r1 = relate(a, b, "rel")
    val r2 = relate(a, c, "rel")

    val patterns: Seq[Pattern] = Seq(
      RelatedTo("a", "b", "r1", None, Direction.OUTGOING, false),
      RelatedTo("a", "c", "r2", None, Direction.OUTGOING, false)
    )
    val matchingContext = new MatchingContext(patterns, bind("a"))

    assertMatches(matchingContext.getMatches(Map("a" -> a)), 2,
      Map("a" -> a, "b" -> c, "c" -> b, "r1" -> r2, "r2" -> r1),
      Map("a" -> a, "b" -> b, "c" -> c, "r1" -> r1, "r2" -> r2))
  }

  @Test def theDreadedDiamondTest() {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val d = createNode()

    val r1 = relate(a, b, "x")
    val r2 = relate(a, c, "x")
    val r3 = relate(b, d, "x")
    val r4 = relate(c, d, "x")

    val patterns: Seq[Pattern] = Seq(
      RelatedTo("a", "b", "r1", None, Direction.OUTGOING, false),
      RelatedTo("a", "c", "r2", None, Direction.OUTGOING, false),
      RelatedTo("b", "d", "r3", None, Direction.OUTGOING, false),
      RelatedTo("c", "d", "r4", None, Direction.OUTGOING, false)
    )

    val matchingContext = new MatchingContext(patterns, bind("a"))

    assertMatches(matchingContext.getMatches(Map("a" -> a)), 2,
      Map("a" -> a, "b" -> b, "c" -> c, "d" -> d, "r1" -> r1, "r2" -> r2, "r3" -> r3, "r4" -> r4),
      Map("a" -> a, "b" -> c, "c" -> b, "d" -> d, "r1" -> r2, "r2" -> r1, "r3" -> r4, "r4" -> r3))
  }


  @Test def pinnedNodeMakesNoMatchesInDisjunctGraph() {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    relate(a, b, "rel")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "c", "r", "rel", Direction.OUTGOING, false))
    val matchingContext = new MatchingContext(patterns, bind("a", "c"))

    assertMatches(matchingContext.getMatches(Map("a" -> a, "c" -> c)), 0)
  }

  @Test def pinnedNodeMakesNoMatches() {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val d = createNode()

    val r1 = relate(a, b, "x")
    val r2 = relate(a, c, "x")
    val r3 = relate(b, d, "x")
    val r4 = relate(c, d, "x")

    val patterns: Seq[Pattern] = Seq(
      RelatedTo("a", "b", "r1", None, Direction.OUTGOING, false),
      RelatedTo("a", "c", "r2", None, Direction.OUTGOING, false),
      RelatedTo("b", "d", "r3", None, Direction.OUTGOING, false),
      RelatedTo("c", "d", "r4", None, Direction.OUTGOING, false)
    )
    val matchingContext = new MatchingContext(patterns, bind("a", "b"))

    assertMatches(matchingContext.getMatches(Map("a" -> a, "b" -> b)), 1,
      Map("a" -> a, "b" -> b, "c" -> c, "d" -> d, "r1" -> r1, "r2" -> r2, "r3" -> r3, "r4" -> r4))
  }

  @Test def directionConstraintFiltersMatches() {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val r1 = relate(a, b, "rel")
    val r2 = relate(c, a, "rel")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", "rel", Direction.OUTGOING))
    val matchingContext = new MatchingContext(patterns, bind("a"))

    assertMatches(matchingContext.getMatches(Map("a" -> a)), 1, Map("a" -> a, "b" -> b, "r" -> r1))
    assertMatches(matchingContext.getMatches(Map("b" -> a)), 1, Map("b" -> a, "a" -> c, "r" -> r2))
  }

  @Test def typeConstraintFiltersMatches() {
    val a = createNode()
    val b = createNode()
    val r1 = relate(a, b, "t1")
    relate(a, b, "t2")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", "t1", Direction.OUTGOING))
    val matchingContext = new MatchingContext(patterns, bind("a"))

    assertMatches(matchingContext.getMatches(Map("a" -> a)), 1, Map("a" -> a, "b" -> b, "r" -> r1))
  }

  @Test def optionalRelationship() {
    val a = createNode()

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", Some("t1"), Direction.OUTGOING, optional = true))
    val matchingContext = new MatchingContext(patterns, bind("a"))

    assertMatches(matchingContext.getMatches(Map("a" -> a)), 1, Map("a" -> a, "b" -> null, "r" -> null))
  }

  @Test def optionalRelatedWithMatch() {
    val a = createNode()
    val b = createNode()
    val r1 = relate(a, b, "t1")
    relate(a, b, "t2")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", Some("t1"), Direction.OUTGOING, optional = true))
    val matchingContext = new MatchingContext(patterns, bind("a"))

    assertMatches(matchingContext.getMatches(Map("a" -> a)), 1, Map("a" -> a, "b" -> b, "r" -> r1))
  }

  @Test def optionalRelatedWithTwoBoundNodes() {
    val a = createNode()
    val b = createNode()

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", Some("t1"), Direction.OUTGOING, optional = true))
    val matchingContext = new MatchingContext(patterns, bind("a", "b"))

    assertMatches(matchingContext.getMatches(Map("a" -> a, "b" -> b)), 1, Map("a" -> a, "b" -> b, "r" -> null))
  }

  @Test def moreComplexOptionalCase() {
    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    val d = createNode("d")

    val r1 = relate(a, b, "t1", "r1")
    val r3 = relate(c, d, "t1", "r3")

    val patterns: Seq[Pattern] = Seq(
      RelatedTo("pA", "pB", "pR1", Some("t1"), Direction.OUTGOING, optional = true),
      RelatedTo("pA", "pC", "pR2", Some("t1"), Direction.OUTGOING, optional = true),
      RelatedTo("pC", "pD", "pR3", Some("t1"), Direction.OUTGOING, optional = false)
    )
    val matchingContext = new MatchingContext(patterns, bind("pA", "pD"))

    assertMatches(matchingContext.getMatches(Map("pA" -> a, "pD" -> d)), 1, Map("pA" -> a, "pB" -> b, "pR1" -> r1, "pR2" -> null, "pR3" -> r3, "pC" -> c, "pD" -> d))
  }

  @Test def optionalVariableLengthPath() {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    relate(a, b, "rel")
    relate(b, c, "rel")

    val patterns: Seq[Pattern] = Seq(VarLengthRelatedTo("p", "a", "c", 1, 2, "rel", Direction.OUTGOING, true))
    val matchingContext = new MatchingContext(patterns, bind("a"))

    assertMatches(matchingContext.getMatches(Map("a" -> a)), 2, Map("a" -> a, "c" -> b), Map("a" -> a, "c" -> c))
  }

  @Test def optionalVariableLengthPathWithPinnedEndNodes() {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val d = createNode()
    relate(a,b,"rel")
    relate(b,c,"rel")
    relate(a,c,"rel")

    val patterns: Seq[Pattern] = Seq(VarLengthRelatedTo("p", "pA", "pB", 1, 2, "rel", Direction.OUTGOING, true))
    val matchingContext = new MatchingContext(patterns, bind("pA", "pB"))

    assertMatches(matchingContext.getMatches(Map("pA" -> a, "pB"->d)), 1)
    assertMatches(matchingContext.getMatches(Map("pA" -> a, "pB"->c)), 2)
  }


  @Test def variableLengthPath() {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    relate(a, b, "rel")
    relate(b, c, "rel")

    val patterns: Seq[Pattern] = Seq(VarLengthRelatedTo("p", "a", "c", 1, 2, "rel", Direction.OUTGOING))
    val matchingContext = new MatchingContext(patterns, bind("a"))

    assertMatches(matchingContext.getMatches(Map("a" -> a)), 2, Map("a" -> a, "c" -> b), Map("a" -> a, "c" -> c))
  }

  @Test def variableLengthPathWithOneHopBefore() {

    val a = createNode()
    val b = createNode()
    val c = createNode()
    val d = createNode()
    val r1 = relate(a, b, "rel")
    relate(b, c, "rel")
    relate(c, d, "rel")

    val patterns: Seq[Pattern] = Seq(
      RelatedTo("a", "b", "r1", "rel", Direction.OUTGOING),
      VarLengthRelatedTo("p", "b", "c", 1, 2, "rel", Direction.OUTGOING))
    val matchingContext = new MatchingContext(patterns, bind("a"))

    assertMatches(matchingContext.getMatches(Map("a" -> a)), 2, Map("a" -> a, "r1" -> r1, "b" -> b, "c" -> c), Map("a" -> a, "r1" -> r1, "b" -> b, "c" -> d))
  }

  @Test def variableLengthPathWithOneHopBeforeWithDifferentType() {

    val a = createNode()
    val b = createNode()
    val c = createNode()
    val d = createNode()
    val r1 = relate(a, b, "t1")
    relate(b, c, "t1")
    relate(c, d, "t2")

    val patterns: Seq[Pattern] = Seq(
      RelatedTo("a", "b", "r1", "t1", Direction.OUTGOING),
      VarLengthRelatedTo("p", "b", "c", 1, 2, "t1", Direction.OUTGOING))
    val matchingContext = new MatchingContext(patterns, bind("a"))

    assertMatches(matchingContext.getMatches(Map("a" -> a)), 1, Map("a" -> a, "r1" -> r1, "b" -> b, "c" -> c))
  }

  @Test def variableLengthPathWithBranch() {

    val a = createNode()
    val b = createNode()
    val c = createNode()
    val d = createNode()
    relate(a, b, "t1")
    relate(b, c, "t1")
    relate(b, d, "t1")

    val patterns: Seq[Pattern] = Seq(
      VarLengthRelatedTo("p", "a", "x", 1, 2, "t1", Direction.OUTGOING))
    val matchingContext = new MatchingContext(patterns, bind("a"))

    assertMatches(matchingContext.getMatches(Map("a" -> a)), 3, Map("a" -> a, "x" -> b), Map("a" -> a, "x" -> c), Map("a" -> a, "x" -> d))
  }

  @Test def variableLengthPathWithPinnedEndNode() {

    val a = createNode()
    val b = createNode()
    val c = createNode()
    val d = createNode()
    relate(a, b, "t1")
    relate(b, c, "t1")
    relate(b, d, "t1")

    val patterns: Seq[Pattern] = Seq(
      VarLengthRelatedTo("p", "a", "x", 1, 2, "t1", Direction.OUTGOING))
    val matchingContext = new MatchingContext(patterns, bind("a", "x"))

    assertMatches(matchingContext.getMatches(Map("a" -> a, "x" -> d)), 1, Map("a" -> a, "x" -> d))
  }

  @Test def variableLengthPathInDiamond() {
    /*
    Graph:
              a
             / \
            /   \
           b----c
           \   /
            \ /
             d


    Pattern:
              pA
             / \
            /   \
         p=pB~~~pC

    Should match two subgraphs, one where p is b-c, and one where it is b-d-c
     */



    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    val d = createNode("d")
    val r1 = relate(a, b, "rel", "r1")
    val r2 = relate(a, c, "rel", "r2")
    relate(b, d, "rel", "r3")
    relate(c, d, "rel", "r4")
    relate(b, c, "rel", "r5")

    val patterns: Seq[Pattern] = Seq(
      RelatedTo("pA", "pB", "pR1", "rel", Direction.OUTGOING),
      RelatedTo("pA", "pC", "pR2", "rel", Direction.OUTGOING),
      VarLengthRelatedTo("p", "pB", "pC", 1, 3, "rel", Direction.BOTH))
    val matchingContext = new MatchingContext(patterns, bind("pA"))

    assertMatches(matchingContext.getMatches(Map("pA" -> a)), 4,
      Map("pA" -> a, "pR1" -> r1, "pB" -> b, "pC" -> c, "pR2" -> r2),
      Map("pA" -> a, "pR1" -> r2, "pB" -> c, "pC" -> b, "pR2" -> r1))
  }

  @Test
  @Ignore def zeroLengthVariableLengthPatternNotAllowed() {
    // TBD?
  }

  def bind(boundSymbols: String*): SymbolTable = {
    val toSet = boundSymbols.map(NodeIdentifier(_))
    new SymbolTable(toSet)
  }

  def assertMatches(matches: Traversable[Map[String, Any]], expectedSize: Int, expected: Map[String, Any]*) {
    val matchesList = matches.toList
    assert(matchesList.size === expectedSize)

    expected.foreach(expectation => {
      if (!matches.exists(compare(_, expectation)))
        throw new Exception("Didn't find the expected row: " + expectation)
    })

  }

  def compare(matches: Map[String, Any], expecations: Map[String, Any]): Boolean = {
    expecations.foreach(kv =>
      matches.get(kv._1) match {
        case None => return false
        case Some(x) => if (x != kv._2) return false
      })

    true
  }


}