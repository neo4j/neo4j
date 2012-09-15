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
package org.neo4j.cypher.internal.executionplan.builders

import org.junit.Test
import org.neo4j.cypher.internal.commands._
import expressions.{Literal, Property}
import org.neo4j.graphdb.{RelationshipType, Direction}
import org.scalatest.Assertions
import org.neo4j.graphdb.Direction._
import org.neo4j.graphdb.DynamicRelationshipType.withName
import org.neo4j.cypher.GraphDatabaseTestBase
import org.neo4j.cypher.internal.pipes.matching.ExpanderStep
import org.neo4j.cypher.internal.commands.True

class TrailBuilderTest extends GraphDatabaseTestBase with Assertions with BuilderTest {
  val A = withName("A")
  val B = withName("B")
  val C = withName("C")
  val D = withName("D")

  /*
          (b2)
            ^
            |
          [:D]
            |
 (a)-[:A]->(b)-[:B]->(c)-[:C]->(d)
  */
  val AtoB = RelatedTo("a", "b", "pr1", Seq("A"), Direction.OUTGOING, optional = false, predicate = True())
  val BtoC = RelatedTo("b", "c", "pr2", Seq("B"), Direction.OUTGOING, optional = false, predicate = True())
  val CtoD = RelatedTo("c", "d", "pr3", Seq("C"), Direction.OUTGOING, optional = false, predicate = True())
  val BtoB2 = RelatedTo("b", "b2", "pr4", Seq("D"), Direction.OUTGOING, optional = false, predicate = True())

  @Test def find_longest_path_for_single_pattern() {
    val expected = step(0, Seq(A), Direction.INCOMING, None)


    TrailBuilder.findLongestTrail(Seq(AtoB), Seq("a", "b")) match {
      case Some(lpr@LongestTrail("a", Some("b"), lp)) => assert(lpr.step === expected.reverse())
      case Some(lpr@LongestTrail("b", Some("a"), lp)) => assert(lpr.step === expected)
      case x                                          => fail("Didn't find any paths, got: " + x)
    }
  }

  @Test def find_longest_path_between_two_points() {
    val forward2 = step(0, Seq(A), Direction.INCOMING, None)
    val forward1 = step(1, Seq(B), Direction.INCOMING, Some(forward2))

    val backward2 = step(0, Seq(B), Direction.OUTGOING, None)
    val backward1 = step(1, Seq(A), Direction.OUTGOING, Some(backward2))

    TrailBuilder.findLongestTrail(Seq(AtoB, BtoC, BtoB2), Seq("a", "c")) match {
      case Some(lpr@LongestTrail("a", Some("c"), lp)) => assert(lpr.step === backward1)
      case Some(lpr@LongestTrail("c", Some("a"), lp)) => assert(lpr.step === forward1)
      case _                                          => fail("Didn't find any paths")
    }
  }

  @Test def find_longest_path_between_two_points_with_a_predicate() {

    //()<-[r1:A]-(a)<-[r2:B]-()
    //WHERE r1.prop = 42 AND r2.prop = "FOO"

    val r1Pred = Equals(Property("pr1", "prop"), Literal(42))
    val r2Pred = Equals(Property("pr2", "prop"), Literal("FOO"))

    val forward2 = step(0, Seq(A), Direction.INCOMING, None, relPredicate = r1Pred)
    val forward1 = step(1, Seq(B), Direction.INCOMING, Some(forward2), relPredicate = r2Pred)

    val backward2 = step(0, Seq(B), Direction.OUTGOING, None, relPredicate = r2Pred)
    val backward1 = step(1, Seq(A), Direction.OUTGOING, Some(backward2), relPredicate = r1Pred)

    TrailBuilder.findLongestTrail(Seq(AtoB, BtoC, BtoB2), Seq("a", "c"), Seq(r1Pred, r2Pred)) match {
      case Some(lpr@LongestTrail("a", Some("c"), lp)) => assert(lpr.step === backward1)
      case Some(lpr@LongestTrail("c", Some("a"), lp)) => assert(lpr.step === forward1)
      case _                                          => fail("Didn't find any paths")
    }
  }

  @Test def find_longest_path_between_two_points_with_a_node_predicate() {
    //()-[pr1:A]->(a)-[pr2:B]->()
    //WHERE r1.prop = 42 AND r2.prop = "FOO"

    val nodePred = Equals(Property("b", "prop"), Literal(42))

    val forward2 = step(0, Seq(A), Direction.INCOMING, None, nodePredicate = nodePred)
    val forward1 = step(1, Seq(B), Direction.INCOMING, Some(forward2))

    val trail = TrailBuilder.findLongestTrail(Seq(AtoB, BtoC), Seq("a", "c"), Seq(nodePred))

    assert(trail.get.longestTrail.predicates.contains(nodePred))

    trail match {
      case Some(lpr@LongestTrail("c", Some("a"), lp)) => assert(lpr.step === forward1)
      case Some(lpr@LongestTrail("a", Some("c"), lp)) => assert(lpr.step === forward1.reverse())
      case _                                          => fail("Didn't find any paths")
    }
  }

  @Test def should_not_accept_trails_with_bound_points_in_the_middle() {
    //()-[pr1:A]->(a)-[pr2:B]->()
    //WHERE r1.prop = 42 AND r2.prop = "FOO"

    val LongestTrail(_,_,trail) = TrailBuilder.findLongestTrail(Seq(AtoB, BtoC), Seq("a", "b", "c"), Seq()).get

    assert(trail.size === 1)
  }

  @Test def find_longest_path_with_single_start() {
    val pr3 = step(0, Seq(C), OUTGOING, None)
    val pr2 = step(1, Seq(B), OUTGOING, Some(pr3))
    val pr1 = step(2, Seq(A), OUTGOING, Some(pr2))

    TrailBuilder.findLongestTrail(Seq(AtoB, BtoC, BtoB2, CtoD), Seq("a")) match {
      case Some(lpr@LongestTrail("a", None, lp)) => assert(lpr.step === pr1)
      case _                                     => fail("Didn't find any paths")
    }
  }

  @Test def decompose_simple_path() {
    val nodeA = createNode("A")
    val nodeB = createNode("B")
    val rel = relate(nodeA, nodeB, "LINK_T")

    val kernPath = Seq(nodeA, rel, nodeB).reverse
    val path = WrappingTrail(BoundPoint("a"), Direction.OUTGOING, "link", Seq("LINK_T"), "b", Seq.empty, null)

    val resultMap = path.decompose(kernPath)
    assert(resultMap === Map("a" -> nodeA, "b" -> nodeB, "link" -> rel))
  }

  @Test def decompose_little_longer_path() {
    val nodeA = createNode("A")
    val nodeB = createNode("B")
    val nodeC = createNode("C")
    val rel1 = relate(nodeA, nodeB, "LINK_T")
    val rel2 = relate(nodeB, nodeC, "LINK_T")

    val kernPath = Seq(nodeA, rel1, nodeB, rel2, nodeC).reverse
    val path =
      WrappingTrail(
        WrappingTrail(BoundPoint("a"), Direction.OUTGOING, "link1", Seq("LINK_T"), "b", Seq.empty, null),
        Direction.OUTGOING, "link2", Seq("LINK_T"), "c", Seq.empty, null)

    val resultMap = path.decompose(kernPath)
    assert(resultMap === Map("a" -> nodeA, "b" -> nodeB, "c" -> nodeC,
      "link1" -> rel1, "link2" -> rel2))
  }

  private def step(id: Int,
                   typ: Seq[RelationshipType],
                   direction: Direction,
                   next: Option[ExpanderStep],
                   nodePredicate: Predicate = True(),
                   relPredicate: Predicate = True()) =
    ExpanderStep(id, typ, direction, next, relPredicate = relPredicate, nodePredicate = nodePredicate)
}