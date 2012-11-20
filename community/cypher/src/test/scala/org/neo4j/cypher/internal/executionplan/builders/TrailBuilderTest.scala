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
import org.neo4j.graphdb.Direction
import org.scalatest.Assertions
import org.neo4j.graphdb.DynamicRelationshipType.withName
import org.neo4j.cypher.GraphDatabaseTestBase
import org.neo4j.cypher.internal.pipes.matching._
import org.neo4j.cypher.internal.pipes.matching.EndPoint
import org.neo4j.cypher.internal.commands.Equals
import org.neo4j.cypher.internal.pipes.matching.SingleStepTrail
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
            |
          [:A*]
            |
            v
           (e)-[:E]->(g)
            |
          [:C*]
            |
           (f)
  */
  val AtoB = RelatedTo("a", "b", "pr1", Seq("A"), Direction.OUTGOING, optional = false, predicate = True())
  val BtoC = RelatedTo("b", "c", "pr2", Seq("B"), Direction.OUTGOING, optional = false, predicate = True())
  val CtoD = RelatedTo("c", "d", "pr3", Seq("C"), Direction.OUTGOING, optional = false, predicate = True())
  val BtoB2 = RelatedTo("b", "b2", "pr4", Seq("D"), Direction.OUTGOING, optional = false, predicate = True())
  val BtoE = VarLengthRelatedTo("p", "b", "e", None, None, Seq("A"), Direction.OUTGOING, None, optional = false, predicate = True())
  val EtoF = VarLengthRelatedTo("p2", "e", "f", None, None, Seq("C"), Direction.BOTH, None, optional = false, predicate = True())
  val EtoG = RelatedTo("e", "g", "pr5", Seq("E"), Direction.OUTGOING, optional = false, predicate = True())

  @Test def find_longest_path_for_single_pattern() {
    val expectedTrail = Some(LongestTrail("a", Some("b"), SingleStepTrail(EndPoint("b"), Direction.OUTGOING, "pr1", Seq("A"), "a", None, None, AtoB)))

    assert(
      TrailBuilder.findLongestTrail(Seq(AtoB), Seq("a", "b")) ===
      expectedTrail)
  }

  @Test def single_path_is_reversed_to_be_able_to_start_from_startpoint() {
    val trail = SingleStepTrail(EndPoint("a"), Direction.INCOMING, "pr1", Seq("A"), "b", None, None, AtoB)

    val expectedTrail = Some(LongestTrail("b", None, trail))

    assert(
      TrailBuilder.findLongestTrail(Seq(AtoB), Seq("b")) === expectedTrail)
  }

  @Test def find_longest_path_between_two_points() {
    val boundPoint = EndPoint("c")
    val second = SingleStepTrail(boundPoint, Direction.OUTGOING, "pr2", Seq("B"), "b", None, None, BtoC)
    val first = SingleStepTrail(second, Direction.OUTGOING, "pr1", Seq("A"), "a", None, None, AtoB)
    val expectedTrail = Some(LongestTrail("a", Some("c"), first))

    assert(expectedTrail === TrailBuilder.findLongestTrail(Seq(AtoB, BtoC, BtoB2), Seq("a", "c")))
  }

  @Test def find_longest_path_between_two_points_with_a_predicate() {
    //()<-[r1:A]-(a)<-[r2:B]-()
    //WHERE r1.prop = 42 AND r2.prop = "FOO"
    val r1Pred = Equals(Property("pr1", "prop"), Literal(42))
    val r2Pred = Equals(Property("pr2", "prop"), Literal("FOO"))
    val predicates = Seq(r1Pred, r2Pred)

    val rewrittenR1 = Equals(MiniMapRelProperty("pr1", "prop"), Literal(42))
    val rewrittenR2 = Equals(MiniMapRelProperty("pr2", "prop"), Literal("FOO"))

    val boundPoint = EndPoint("c")
    val second = SingleStepTrail(boundPoint, Direction.OUTGOING, "pr2", Seq("B"), "b", relPred = Some(rewrittenR2), nodePred = None, pattern = BtoC)
    val first = SingleStepTrail(second, Direction.OUTGOING, "pr1", Seq("A"), "a", relPred = Some(rewrittenR1), nodePred = None, pattern = AtoB)
    val expectedTrail = Some(LongestTrail("a", Some("c"), first))

    val foundTrail = TrailBuilder.findLongestTrail(Seq(AtoB, BtoC, BtoB2), Seq("a", "c"), predicates)
    assert(expectedTrail === foundTrail)
  }

  @Test def find_longest_path_between_two_points_with_a_node_predicate() {
    //(a)-[pr1:A]->(b)-[pr2:B]->(c)
    //WHERE b.prop = 42

    val nodePred = Equals(Property("b", "prop"), Literal(42))
    val predicates = Seq(nodePred)

    val rewrittenPredicate = Equals(MiniMapNodeProperty("b", "prop"), Literal(42))

    val boundPoint = EndPoint("c")
    val second = SingleStepTrail(boundPoint, Direction.OUTGOING, "pr2", Seq("B"), "b", None, None, BtoC)
    val first = SingleStepTrail(second, Direction.OUTGOING, "pr1", Seq("A"), "a", None, Some(rewrittenPredicate), AtoB)
    val expectedTrail = Some(LongestTrail("a", Some("c"), first))

    val foundTrail = TrailBuilder.findLongestTrail(Seq(AtoB, BtoC, BtoB2), Seq("a", "c"), predicates)
    assert(expectedTrail === foundTrail)
  }

  @Test def should_not_accept_trails_with_bound_points_in_the_middle() {
    //(a)-[pr1:A]->(b)-[pr2:B]->(c)

    val LongestTrail(_, _, trail) = TrailBuilder.findLongestTrail(Seq(AtoB, BtoC), Seq("a", "b", "c"), Seq()).get

    assert(trail.size === 1)
  }

  @Test def find_longest_path_with_single_start() {
    //(a)-[pr1:A]->(b)-[pr2:B]->(c)-[pr3:B]->(d)

    val boundPoint = EndPoint("d")
    val third = SingleStepTrail(boundPoint, Direction.OUTGOING, "pr3", Seq("C"), "c", None, None, CtoD)
    val second = SingleStepTrail(third, Direction.OUTGOING, "pr2", Seq("B"), "b", None, None, BtoC)
    val first = SingleStepTrail(second, Direction.OUTGOING, "pr1", Seq("A"), "a", None, None, AtoB)
    val expectedTrail = Some(LongestTrail("a", None, first))

    val foundTrail = TrailBuilder.findLongestTrail(Seq(AtoB, BtoC, BtoB2, CtoD), Seq("a"), Seq.empty)
    assert(foundTrail === expectedTrail)
  }

  @Test def single_varlength_path() {
    //(b)-[:A*]->(e)

    val boundPoint = EndPoint("e")
    val first = VariableLengthStepTrail(boundPoint, Direction.OUTGOING, Seq("A"), 1, None, "p", None, "b", BtoE)
    val expectedTrail = Some(LongestTrail("b", None, first))

    val foundTrail = TrailBuilder.findLongestTrail(Seq(BtoE), Seq("b"), Seq.empty)
    assert(foundTrail === expectedTrail)
  }

  @Test def single_rel_followed_by_varlength_with_single_bound_point() {
    //(a)-[:A]->(b)-[:A*]->(e)

    val endPoint = EndPoint("e")
    val last = VariableLengthStepTrail(endPoint, Direction.OUTGOING, Seq("A"), 1, None, "p", None, "b", BtoE)
    val first = SingleStepTrail(last, Direction.OUTGOING, "pr1", Seq("A"), "a", None, None, AtoB)
    val expected = Some(LongestTrail("a", None, first))

    val foundTrail = TrailBuilder.findLongestTrail(Seq(AtoB, BtoE), Seq("a"), Seq.empty)
    assert(foundTrail === expected)
  }

  @Test def two_varlength_paths_with_both_ends_bound() {
    //(b)-[:A*]->(e)-[:C*]->f

    val endPoint = EndPoint("e")
    val trail = VariableLengthStepTrail(endPoint, Direction.OUTGOING, Seq("A"), 1, None, "p", None, "b", BtoE)
    val expected = Some(LongestTrail("b", None, trail))

    val foundTrail = TrailBuilder.findLongestTrail(Seq(BtoE, EtoF), Seq("b", "f"), Seq.empty)
    assert(foundTrail === expected)
  }

  @Test def mono_directional_trails_can_end_in_varlength_paths() {

    /*
    Given the pattern
    (a)-[:A]->(b)-[:B]->(c)
               |
             [:A*]
               |
               v
              (e)
    */

    val endPoint = EndPoint("e")
    val second = VariableLengthStepTrail(endPoint, Direction.OUTGOING, Seq("A"), 1, None, "p", None, "b", BtoE)
    val trail = SingleStepTrail(second, Direction.OUTGOING, "pr1", Seq("A"), "a", None, None, AtoB)

    val expected = Some(LongestTrail("a", None, trail))

    val foundTrail = TrailBuilder.findLongestTrail(Seq(AtoB, BtoE, BtoC), Seq("a"), Seq.empty)
    assert(foundTrail === expected)
  }

  @Test def mono_directional_trails_can_only_have_single_varlength_paths() {

    /*
    Given the pattern
    (a)-[:A]->(b)-[:A*]->(e)-[:C*]->(f)

    we should take as much as possible, but not the last varlength trail
    */

    val endPoint = EndPoint("e")
    val second = VariableLengthStepTrail(endPoint, Direction.OUTGOING, Seq("A"), 1, None, "p", None, "b", BtoE)
    val trail = SingleStepTrail(second, Direction.OUTGOING, "pr1", Seq("A"), "a", None, None, AtoB)

    val expected = Some(LongestTrail("a", None, trail))

    val foundTrail = TrailBuilder.findLongestTrail(Seq(AtoB, BtoE, EtoF), Seq("a"), Seq.empty)
    assert(foundTrail === expected)
  }

  @Test def mono_directional_trails_can_only_have_varlength_paths_at_the_end() {

    /*
    Given the pattern
    (a)-[:A]->(b)-[:A*]->(e)-[:E]->(g)

    we should take as much as possible, but not the last trail
    */

    val endPoint = EndPoint("e")
    val second = VariableLengthStepTrail(endPoint, Direction.OUTGOING, Seq("A"), 1, None, "p", None, "b", BtoE)
    val trail = SingleStepTrail(second, Direction.OUTGOING, "pr1", Seq("A"), "a", None, None, AtoB)

    val expected = Some(LongestTrail("a", None, trail))

    val foundTrail = TrailBuilder.findLongestTrail(Seq(AtoB, BtoE, EtoG), Seq("a"), Seq.empty)
    assert(foundTrail === expected)
  }

  @Test def two_varlength_paths_with_one_end_bound() {

    /*
    Given the pattern
    (a)-[:A]->(b)-[:B]->(c)
               |
             [:A*]
               |
               v
              (e)
    */

    val endPoint = EndPoint("e")
    val second = VariableLengthStepTrail(endPoint, Direction.OUTGOING, Seq("A"), 1, None, "p", None, "b", BtoE)
    val trail = SingleStepTrail(second, Direction.OUTGOING, "pr1", Seq("A"), "a", None, None, AtoB)

    val expected = Some(LongestTrail("a", None, trail))

    val foundTrail = TrailBuilder.findLongestTrail(Seq(AtoB, BtoE, BtoC), Seq("a"), Seq.empty)
    assert(foundTrail === expected)
  }

  @Test def should_handle_loops() {

    // (a)-[pr1:A]->b-[pr2:B]->c
    //  \                      ^
    //   --[pr5:A]->x-[pr6:B]-/

    val AtoX = RelatedTo("a", "x", "pr5", Seq("A"), Direction.OUTGOING, optional = false, predicate = True())
    val XtoC = RelatedTo("x", "c", "pr6", Seq("B"), Direction.OUTGOING, optional = false, predicate = True())

    val endPoint = EndPoint("c")
    val last = SingleStepTrail(endPoint, Direction.OUTGOING, "pr6", Seq("B"), "x", None, None, XtoC)
    val trail = SingleStepTrail(last, Direction.OUTGOING, "pr5", Seq("A"), "a", None, None, AtoX)


    val result = TrailBuilder.findLongestTrail(Seq(AtoB, BtoC, AtoX, XtoC), Seq("a", "c"), Seq.empty)
    println(result)
    val expected = Some(LongestTrail("a", Some("c"), trail))

    assert(result === expected)
  }

  @Test def should_handle_long_paths_with_unnamed_nodes() {
    // GIVEN
    // a<-[15]- (13)<-[16]- b-[17]-> (14)-[18]-> c

    val s1 = RelatedTo("  UNNAMED13", "a", "  UNNAMED15", Seq(), Direction.OUTGOING, optional = false, predicate = True())
    val s2 = RelatedTo("b", "  UNNAMED13", "  UNNAMED16", Seq(), Direction.OUTGOING, optional = false, predicate = True())
    val s3 = RelatedTo("b", "  UNNAMED14", "  UNNAMED17", Seq(), Direction.OUTGOING, optional = false, predicate = True())
    val s4 = RelatedTo("  UNNAMED14", "c", "  UNNAMED18", Seq(), Direction.OUTGOING, optional = false, predicate = True())


    val fifth = EndPoint("c")
    val fourth = SingleStepTrail(fifth , Direction.OUTGOING, "  UNNAMED18", Seq(), "  UNNAMED14", None, None, s4)
    val third  = SingleStepTrail(fourth, Direction.OUTGOING, "  UNNAMED17", Seq(), "b", None, None, s3)
    val second = SingleStepTrail(third , Direction.INCOMING, "  UNNAMED16", Seq(), "  UNNAMED13", None, None, s2)
    val first  = SingleStepTrail(second, Direction.INCOMING, "  UNNAMED15", Seq(), "a", None, None, s1)

    val result = TrailBuilder.findLongestTrail(Seq(s1, s2, s3, s4), Seq("a"), Seq.empty)
    println(result)
    val expected = Some(LongestTrail("a", None, first))

    assert(result === expected)
  }
}