/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.pipes.matching

import org.scalatest.Assertions
import org.neo4j.cypher.GraphDatabaseTestBase
import org.neo4j.graphdb.{Node, Direction}
import org.neo4j.cypher.internal.commands._
import expressions.Identifier
import expressions.Literal
import expressions.Property
import expressions.RelationshipFunction
import expressions.{Identifier, RelationshipFunction, Literal, Property}
import org.junit.{Before, Test}
import org.neo4j.cypher.internal.symbols._
import collection.Map
import org.neo4j.cypher.internal.executionplan.builders.PatternGraphBuilder
import org.neo4j.cypher.internal.commands.True
import org.neo4j.cypher.internal.commands.Equals
import org.neo4j.cypher.internal.commands.AllInCollection
import org.neo4j.cypher.internal.pipes.{QueryState, ExecutionContext}
import org.neo4j.cypher.internal.spi.gdsimpl.GDSBackedQueryContext

class MatchingContextTest extends GraphDatabaseTestBase with Assertions with PatternGraphBuilder {
  var a: Node = null
  var b: Node = null
  var c: Node = null
  var d: Node = null

  private def ctx(x: (String, Any)*) = ExecutionContext(state = new QueryState(graph, new GDSBackedQueryContext(graph), Map.empty)).newWith(x.toMap)

  @Before
  def init() {
    a = createNode("a")
    b = createNode("b")
    c = createNode("c")
    d = createNode("d")
  }

  @Test def singleHopSingleMatch() {
    val r = relate(a, b, "rel")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", "rel", Direction.OUTGOING, false))
    val matchingContext = createMatchingContextWithNodes(patterns, Seq("a"))

    assertMatches(matchingContext.getMatches(ctx("a" -> a)), 1, Map("a" -> a, "b" -> b, "r" -> r))
  }

  @Test def singleDirectedRel() {
    val r = relate(a, b, "rel", "r")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", "rel", Direction.OUTGOING, false))
    val matchingContext = createMatchingContextWithRels(patterns, Seq("r"))

    assertMatches(matchingContext.getMatches(ctx("r" -> r)), 1, Map("a" -> a, "b" -> b, "r" -> r))
  }

  @Test def singleDirectedRelTurnedTheWrongWay() {
    val r = relate(a, b, "rel", "r")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", "rel", Direction.INCOMING, false))
    val matchingContext = createMatchingContextWithRels(patterns, Seq("r"))

    assertMatches(matchingContext.getMatches(ctx("r" -> r)), 1, Map("a" -> b, "b" -> a, "r" -> r))
  }

  @Test def singleUndirectedRel() {
    val r = relate(a, b, "rel")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", "rel", Direction.BOTH, false))
    val matchingContext = createMatchingContextWithRels(patterns, Seq("r"))

    assertMatches(matchingContext.getMatches(ctx("r" -> r)), 2,
      Map("a" -> a, "b" -> b, "r" -> r),
      Map("a" -> b, "b" -> a, "r" -> r))
  }

  @Test def twoUndirectedRel() {
    val r1 = relate(a, b, "rel", "r1")
    val r2 = relate(b, c, "rel", "r2")

    val patterns: Seq[Pattern] = Seq(
      RelatedTo("a", "b", "r1", "rel", Direction.BOTH, false),
      RelatedTo("b", "c", "r2", "rel", Direction.BOTH, false)
    )

    val matchingContext = createMatchingContextWithRels(patterns, Seq("r1", "r2"))

    assertMatches(matchingContext.getMatches(ctx("r1" -> r1, "r2" -> r2)), 1)
  }

  @Test def singleHopDoubleMatch() {
    val r1 = relate(a, b, "rel", "r1")
    val r2 = relate(a, c, "rel", "r2")

    val patterns: Seq[Pattern] = Seq(RelatedTo("pA", "pB", "pR", "rel", Direction.OUTGOING, false))
    val matchingContext = createMatchingContextWithNodes(patterns, Seq("pA"))


    assertMatches(matchingContext.getMatches(ctx("pA" -> a)), 2,
      Map("pA" -> a, "pB" -> b, "pR" -> r1),
      Map("pA" -> a, "pB" -> c, "pR" -> r2))
  }

  @Test def twoBoundNodesShouldWork() {
    val r1 = relate(a, b, "rel", "r1")

    val patterns: Seq[Pattern] = Seq(RelatedTo("pA", "pB", "pR", "rel", Direction.OUTGOING, false))
    val matchingContext = createMatchingContextWithNodes(patterns, Seq("pA", "pB"))


    assertMatches(matchingContext.getMatches(ctx("pA" -> a, "pB" -> b)), 1,
      Map("pA" -> a, "pB" -> b, "pR" -> r1))
  }

  @Test def boundNodeAndRel() {
    val r1 = relate(a, b, "rel", "r1")
    relate(a, b, "rel", "r2")

    val patterns: Seq[Pattern] = Seq(RelatedTo("pA", "pB", "pR", "rel", Direction.OUTGOING, false))

    val matchingContext = createMatchingContextWith(patterns, Seq("pA"), Seq("pR"))

    assertMatches(matchingContext.getMatches(ctx("pA" -> a, "pR" -> r1)), 1,
      Map("pA" -> a, "pB" -> b, "pR" -> r1))
  }

  @Test def doubleHopDoubleMatch() {
    val r1 = relate(a, b, "rel")
    val r2 = relate(a, c, "rel")

    val patterns: Seq[Pattern] = Seq(
      RelatedTo("a", "b", "r1", Seq(), Direction.OUTGOING, false, True()),
      RelatedTo("a", "c", "r2", Seq(), Direction.OUTGOING, false, True())
    )
    val matchingContext = createMatchingContextWithNodes(patterns, Seq("a"))

    val traversable = matchingContext.getMatches(ctx("a" -> a))
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
      RelatedTo("A", "B", "pr1", Seq(), Direction.OUTGOING, false, True()),
      RelatedTo("A", "C", "pr2", Seq(), Direction.OUTGOING, false, True()),
      RelatedTo("B", "D", "pr3", Seq(), Direction.OUTGOING, false, True()),
      RelatedTo("C", "D", "pr4", Seq(), Direction.OUTGOING, false, True())
    )

    val matchingContext = createMatchingContextWithNodes(patterns, Seq("A"))

    assertMatches(matchingContext.getMatches(ctx("A" -> a)), 2,
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
      RelatedTo("A", "B", "pr1", Seq(), Direction.OUTGOING, false, True()),
      RelatedTo("A", "C", "pr2", Seq(), Direction.OUTGOING, false, True()),
      RelatedTo("B", "D", "pr3", Seq(), Direction.OUTGOING, false, True()),
      RelatedTo("C", "D", "pr4", Seq(), Direction.OUTGOING, false, True()),
      RelatedTo("B", "E", "pr5", Seq("IN"), Direction.OUTGOING, false, True()),
      RelatedTo("C", "E", "pr6", Seq("IN"), Direction.OUTGOING, false, True())
    )

    val matchingContext = createMatchingContextWithNodes(patterns, Seq("A", "E"))

    val matches = matchingContext.getMatches(ctx("A" -> a, "E" -> e))

    assertMatches(matches, 2)
  }

  @Test def pinnedNodeMakesNoMatchesInDisjunctGraph() {
    relate(a, b, "rel")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "c", "r", "rel", Direction.OUTGOING, false, True()))
    val matchingContext = createMatchingContextWithNodes(patterns, Seq("a", "c"))

    assertMatches(matchingContext.getMatches(ctx("a" -> a, "c" -> c)), 0)
  }

  @Test def pinnedNodeMakesNoMatches() {
    val r1 = relate(a, b, "x")
    val r2 = relate(a, c, "x")
    val r3 = relate(b, d, "x")
    val r4 = relate(c, d, "x")

    val patterns: Seq[Pattern] = Seq(
      RelatedTo("a", "b", "r1", Seq(), Direction.OUTGOING, false, True()),
      RelatedTo("a", "c", "r2", Seq(), Direction.OUTGOING, false, True()),
      RelatedTo("b", "d", "r3", Seq(), Direction.OUTGOING, false, True()),
      RelatedTo("c", "d", "r4", Seq(), Direction.OUTGOING, false, True())
    )
    val matchingContext = createMatchingContextWithNodes(patterns, Seq("a", "b"))

    assertMatches(matchingContext.getMatches(ctx("a" -> a, "b" -> b)), 1,
      Map("a" -> a, "b" -> b, "c" -> c, "d" -> d, "r1" -> r1, "r2" -> r2, "r3" -> r3, "r4" -> r4))
  }

  @Test def directionConstraintFiltersMatches() {
    val r1 = relate(a, b, "rel")
    val r2 = relate(c, a, "rel")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", "rel", Direction.OUTGOING))

    val matchingContext = createMatchingContextWithNodes(patterns, Seq("a"))
    assertMatches(matchingContext.getMatches(ctx("a" -> a)), 1, Map("a" -> a, "b" -> b, "r" -> r1))

    val matchingContext2 = createMatchingContextWithNodes(patterns, Seq("b"))
    assertMatches(matchingContext2.getMatches(ctx("b" -> a)), 1, Map("b" -> a, "a" -> c, "r" -> r2))
  }

  @Test def typeConstraintFiltersMatches() {
    val r1 = relate(a, b, "t1")
    relate(a, b, "t2")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", "t1", Direction.OUTGOING))
    val matchingContext = createMatchingContextWithNodes(patterns, Seq("a"))

    assertMatches(matchingContext.getMatches(ctx("a" -> a)), 1, Map("a" -> a, "b" -> b, "r" -> r1))
  }

  @Test def optionalRelationship() {
    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", Seq("t1"), Direction.OUTGOING, true, True()))
    val matchingContext = createMatchingContextWithNodes(patterns, Seq("a"))

    assertMatches(matchingContext.getMatches(ctx("a" -> a)), 1, Map("a" -> a, "b" -> null, "r" -> null))
  }

  @Test def doubleOptional() {
    val patterns: Seq[Pattern] = Seq(
      RelatedTo("a", "x", "r1", Seq(), Direction.OUTGOING, optional = true, predicate = True()),
      RelatedTo("b", "x", "r2", Seq(), Direction.OUTGOING, optional = true, predicate = True())
    )
    val matchingContext = createMatchingContextWithNodes(patterns, Seq("a", "b"))

    assertMatches(matchingContext.getMatches(ctx("a" -> a, "b" -> b)), 1, Map("a" -> a, "b" -> b, "r1" -> null, "r2" -> null))
  }

  @Test def optionalRelatedWithMatch() {
    val r1 = relate(a, b, "t1")
    relate(a, b, "t2")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", Seq("t1"), Direction.OUTGOING, true, True()))
    val matchingContext = createMatchingContextWithNodes(patterns, Seq("a"))

    assertMatches(matchingContext.getMatches(ctx("a" -> a)), 1, Map("a" -> a, "b" -> b, "r" -> r1))
  }

  @Test def optionalRelatedWithTwoBoundNodes() {
    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", Seq("t1"), Direction.OUTGOING, true, True()))
    val matchingContext = createMatchingContextWithNodes(patterns, Seq("a", "b"))

    assertMatches(matchingContext.getMatches(ctx("a" -> a, "b" -> b)), 1, Map("a" -> a, "b" -> b, "r" -> null))
  }

  @Test def moreComplexOptionalCase() {
    val r1 = relate(a, b, "t1", "r1")
    val r3 = relate(c, d, "t1", "r3")

    val patterns: Seq[Pattern] = Seq(
      RelatedTo("pA", "pB", "pR1", Seq("t1"), Direction.OUTGOING, true, True()),
      RelatedTo("pA", "pC", "pR2", Seq("t1"), Direction.OUTGOING, true, True()),
      RelatedTo("pC", "pD", "pR3", Seq("t1"), Direction.OUTGOING, false, True())
    )
    val matchingContext = createMatchingContextWithNodes(patterns, Seq("pA", "pD"))

    assertMatches(matchingContext.getMatches(ctx("pA" -> a, "pD" -> d)), 1, Map("pA" -> a, "pB" -> b, "pR1" -> r1, "pR2" -> null, "pR3" -> r3, "pC" -> c, "pD" -> d))
  }

  @Test def optionalVariableLengthPath() {
    relate(a, b, "rel")
    relate(b, c, "rel")

    val patterns: Seq[Pattern] = Seq(VarLengthRelatedTo("p", "a", "c", Some(1), Some(2), "rel", Direction.OUTGOING, true))
    val matchingContext = createMatchingContextWithNodes(patterns, Seq("a"))

    assertMatches(matchingContext.getMatches(ctx("a" -> a)), 2, Map("a" -> a, "c" -> b), Map("a" -> a, "c" -> c))
  }

  @Test def optionalVariableLengthPathWithPinnedEndNodes() {
    relate(a, b, "rel")
    relate(b, c, "rel")
    relate(a, c, "rel")

    val patterns: Seq[Pattern] = Seq(VarLengthRelatedTo("p", "pA", "pB", Some(1), Some(2), "rel", Direction.OUTGOING, true))
    val matchingContext = createMatchingContextWithNodes(patterns, Seq("pA", "pB"))

    assertMatches(matchingContext.getMatches(ctx("pA" -> a, "pB" -> d)), 1)
    assertMatches(matchingContext.getMatches(ctx("pA" -> a, "pB" -> c)), 2)
  }


  @Test def variableLengthPath() {
    relate(a, b, "rel")
    relate(b, c, "rel")

    val patterns: Seq[Pattern] = Seq(VarLengthRelatedTo("p", "a", "c", Some(1), Some(2), "rel", Direction.OUTGOING))
    val matchingContext = createMatchingContextWithNodes(patterns, Seq("a"))

    assertMatches(matchingContext.getMatches(ctx("a" -> a)), 2, Map("a" -> a, "c" -> b), Map("a" -> a, "c" -> c))
  }

  @Test def variableLengthPathWithOneHopBefore() {
    val r1 = relate(a, b, "rel")
    relate(b, c, "rel")
    relate(c, d, "rel")

    val patterns: Seq[Pattern] = Seq(
      RelatedTo("a", "b", "r1", "rel", Direction.OUTGOING),
      VarLengthRelatedTo("p", "b", "c", Some(1), Some(2), "rel", Direction.OUTGOING))
    val matchingContext = createMatchingContextWithNodes(patterns, Seq("a"))

    assertMatches(matchingContext.getMatches(ctx("a" -> a)), 2, Map("a" -> a, "r1" -> r1, "b" -> b, "c" -> c), Map("a" -> a, "r1" -> r1, "b" -> b, "c" -> d))
  }

  @Test def variableLengthPathWithOneHopBeforeWithDifferentType() {
    val r1 = relate(a, b, "t1")
    relate(b, c, "t1")
    relate(c, d, "t2")

    val patterns = Seq(
      RelatedTo("a", "b", "r1", "t1", Direction.OUTGOING),
      VarLengthRelatedTo("p", "b", "c", Some(1), Some(2), "t1", Direction.OUTGOING))
    val matchingContext = createMatchingContextWithNodes(patterns, Seq("a"))

    assertMatches(matchingContext.getMatches(ctx("a" -> a)), 1, Map("a" -> a, "r1" -> r1, "b" -> b, "c" -> c))
  }

  @Test def variableLengthPathWithBranch() {
    relate(a, b, "t1")
    relate(b, c, "t1")
    relate(b, d, "t1")

    val patterns: Seq[Pattern] = Seq(
      VarLengthRelatedTo("p", "a", "x", Some(1), Some(2), "t1", Direction.OUTGOING))
    val matchingContext = createMatchingContextWithNodes(patterns, Seq("a"))

    assertMatches(matchingContext.getMatches(ctx("a" -> a)), 3,
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
    val matchingContext = createMatchingContextWithNodes(patterns, Seq("a", "x"))

    assertMatches(matchingContext.getMatches(ctx("a" -> a, "x" -> d)), 1, Map("a" -> a, "x" -> d))
  }

  @Test def varLengthPathWithTwoPaths() {
    relate(a, b, "t1")
    relate(a, c, "t1")
    relate(b, c, "t1")
    relate(c, d, "t1")

    val patterns: Seq[Pattern] = Seq(
      VarLengthRelatedTo("P", "A", "X", None, None, "t1", Direction.OUTGOING))
    val matchingContext = createMatchingContextWithNodes(patterns, Seq("A", "X"))

    assertMatches(matchingContext.getMatches(ctx("A" -> a, "X" -> d)), 2)
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
    val matchingContext = createMatchingContextWithNodes(patterns, Seq("pA"))

    val traversable = matchingContext.getMatches(ctx("pA" -> a)).toList

    assertMatches(traversable, 2,
      Map("pA" -> a, "pR1" -> r1, "pB" -> b, "pC" -> c, "pR2" -> r2))
  }

  @Test def predicateConcerningRelationship() {
    val r = relate(a, b, "rel", Map("age" -> 15))
    val r2 = relate(a, b, "rel", Map("age" -> 5))

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", "rel", Direction.OUTGOING, false))

    val matchingContext = createMatchingContextWithNodes(patterns, Seq("a"), Seq(Equals(Property("r", "age"), Literal(5))))

    assertMatches(matchingContext.getMatches(ctx("a" -> a)), 1, Map("a" -> a, "b" -> b, "r" -> r2))
  }

  @Test def predicateConcerningNode() {
    val a = createNode(Map("prop" -> "value"))
    relate(a, b, "rel")

    val patterns: Seq[Pattern] = Seq(RelatedTo("a", "b", "r", "rel", Direction.OUTGOING, false))
    val matchingContext = createMatchingContextWithNodes(patterns, Seq("a"), Seq(Equals(Property("a", "prop"), Literal("not value"))))

    assert(matchingContext.getMatches(ctx("a" -> a)).size === 0)
  }

  @Test def predicateInPatternRelationship() {
    relate(a, b, "rel", Map("foo" -> "notBar"))

    val patterns = Seq(RelatedTo("a", "b", "r", Seq("rel"), Direction.OUTGOING, true, Equals(Property("r", "foo"), Literal("bar"))))
    val matchingContext = createMatchingContextWithNodes(patterns, Seq("a"))

    assertMatches(matchingContext.getMatches(ctx("a" -> a)), 1, Map("a" -> a, "b" -> null, "r" -> null))
  }

  @Test def solveDoubleOptionalProblem() {
    val e = createNode()

    relate(a, b)
    relate(a, c)
    relate(d, c)
    relate(d, e)

    val patterns = Seq(
      RelatedTo("a", "x", "r1", Seq(), Direction.OUTGOING, true, True()),
      RelatedTo("x", "b", "r2", Seq(), Direction.INCOMING, true, True())
    )

    val matchingContext = createMatchingContextWithNodes(patterns, Seq("a", "b"))

    assertMatches(matchingContext.getMatches(ctx("a" -> a, "b" -> d)), 3)
  }

  @Test def predicateInPatternRelationshipAlsoForVarLength() {
    relate(a, b, "rel", Map("foo" -> "bar"))
    relate(b, c, "rel", Map("foo" -> "notBar"))

    val pred = AllInCollection(RelationshipFunction(Identifier("p")), "r", Equals(Property("r", "foo"), Literal("bar")))
    val patterns = Seq(VarLengthRelatedTo("p", "a", "b", Some(2), Some(2), Seq(), Direction.OUTGOING, None, true, pred))
    val matchingContext = createMatchingContextWithNodes(patterns, Seq("a"))

    assertMatches(matchingContext.getMatches(ctx("a" -> a)), 1, Map("a" -> a, "p" -> null, "b" -> null))
  }

  private def assertMatches(matches: Traversable[Map[String, Any]], expectedSize: Int, expected: Map[String, Any]*) {
    val matchesList = matches.toList
    assert(matchesList.size === expectedSize)

    expected.foreach(expectation => {
      if (!matches.exists(compare(_, expectation))) {

        throw new Exception("Didn't find the expected row: " + expectation + "\r\nActual: " + matches.toList)
      }
    })
  }

  private def createMatchingContextWith(patterns: Seq[Pattern], nodes: Seq[String], rels: Seq[String], predicates: Seq[Predicate] = Seq[Predicate]()): MatchingContext = {
    val nodeIdentifiers2 = nodes.map(_ -> NodeType())
    val relIdentifiers2 = rels.map(_ -> RelationshipType())

    val identifiers2 = (nodeIdentifiers2 ++ relIdentifiers2).toMap
    val symbols2 = new SymbolTable(identifiers2)
    new MatchingContext(symbols2, predicates, buildPatternGraph(symbols2, patterns))
  }

  private def createMatchingContextWithRels(patterns: Seq[Pattern], rels: Seq[String], predicates: Seq[Predicate] = Seq[Predicate]()): MatchingContext =
    createMatchingContextWith(patterns, Seq(), rels, predicates)

  private def createMatchingContextWithNodes(patterns: Seq[Pattern], nodes: Seq[String], predicates: Seq[Predicate] = Seq[Predicate]()): MatchingContext =
    createMatchingContextWith(patterns, nodes, Seq(), predicates)

  private def compare(matches: Map[String, Any], expectation: Map[String, Any]): Boolean = {
    expectation.foreach(kv =>
      matches.get(kv._1) match {
        case None    => return false
        case Some(x) => if (x != kv._2) return false
      })

    true
  }
}