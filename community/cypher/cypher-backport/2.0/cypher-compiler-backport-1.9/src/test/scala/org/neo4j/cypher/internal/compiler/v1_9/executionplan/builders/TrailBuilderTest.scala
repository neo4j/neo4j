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
package org.neo4j.cypher.internal.compiler.v1_9.executionplan.builders

import org.junit.Test
import org.neo4j.cypher.internal.compiler.v1_9.commands._
import expressions.{Identifier, Literal, Property}
import org.neo4j.graphdb.Direction
import org.scalatest.Assertions
import org.neo4j.graphdb.DynamicRelationshipType.withName
import org.neo4j.cypher.internal.compiler.v1_9.pipes.matching._
import org.neo4j.cypher.internal.compiler.v1_9.pipes.matching.EndPoint
import org.neo4j.cypher.internal.compiler.v1_9.commands.Equals
import org.neo4j.cypher.internal.compiler.v1_9.pipes.matching.SingleStepTrail
import org.neo4j.cypher.internal.compiler.v1_9.commands.True

class TrailBuilderTest extends Assertions with BuilderTest {
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
    val expectedTrail = Some(LongestTrail("a", Some("b"), SingleStepTrail(EndPoint("b"), Direction.OUTGOING, "pr1", Seq("A"), "a", True(), True(), AtoB, Seq())))

    assert(
      TrailBuilder.findLongestTrail(Seq(AtoB), Seq("a", "b")) ===
      expectedTrail)
  }

  @Test def single_path_is_reversed_to_be_able_to_start_from_startpoint() {
    val trail = SingleStepTrail(EndPoint("a"), Direction.INCOMING, "pr1", Seq("A"), "b", True(), True(), AtoB, Seq())

    val expectedTrail = Some(LongestTrail("b", None, trail))

    assert(
      TrailBuilder.findLongestTrail(Seq(AtoB), Seq("b")) === expectedTrail)
  }

  @Test def find_longest_path_between_two_points() {
    val boundPoint = EndPoint("c")
    val second = SingleStepTrail(boundPoint, Direction.OUTGOING, "pr2", Seq("B"), "b", True(), True(), BtoC, Seq())
    val first = SingleStepTrail(second, Direction.OUTGOING, "pr1", Seq("A"), "a", True(), True(), AtoB, Seq())
    val expectedTrail = Some(LongestTrail("a", Some("c"), first))

    assert(expectedTrail === TrailBuilder.findLongestTrail(Seq(AtoB, BtoC, BtoB2), Seq("a", "c")))
  }

  @Test def find_longest_path_between_two_points_with_a_predicate() {
    //()<-[r1:A]-(a)<-[r2:B]-()
    //WHERE r1.prop = 42 AND r2.prop = "FOO"
    val r1Pred = Equals(Property(Identifier("pr1"), "prop"), Literal(42))
    val r2Pred = Equals(Property(Identifier("pr2"), "prop"), Literal("FOO"))
    val predicates = Seq(r1Pred, r2Pred)

    val rewrittenR1 = Equals(Property(RelationshipIdentifier(), "prop"), Literal(42))
    val rewrittenR2 = Equals(Property(RelationshipIdentifier(), "prop"), Literal("FOO"))

    val boundPoint = EndPoint("c")
    val second = SingleStepTrail(boundPoint, Direction.OUTGOING, "pr2", Seq("B"), "b", rewrittenR2, True(), pattern = BtoC, Seq(r2Pred))
    val first = SingleStepTrail(second, Direction.OUTGOING, "pr1", Seq("A"), "a", rewrittenR1, True(), pattern = AtoB, Seq(r1Pred))
    val expectedTrail = Some(LongestTrail("a", Some("c"), first))

    val foundTrail = TrailBuilder.findLongestTrail(Seq(AtoB, BtoC, BtoB2), Seq("a", "c"), predicates)
    assert(expectedTrail === foundTrail)
  }

  @Test def find_longest_path_between_two_points_with_a_node_predicate() {
    //(a)-[pr1:A]->(b)-[pr2:B]->(c)
    //WHERE b.prop = 42

    val nodePred = Equals(Property(Identifier("b"), "prop"), Literal(42))
    val predicates = Seq(nodePred)

    val rewrittenPredicate = Equals(Property(NodeIdentifier(), "prop"), Literal(42))

    val boundPoint = EndPoint("c")
    val second = SingleStepTrail(boundPoint, Direction.OUTGOING, "pr2", Seq("B"), "b", True(), True(), BtoC, Seq())
    val first = SingleStepTrail(second, Direction.OUTGOING, "pr1", Seq("A"), "a", True(), rewrittenPredicate, AtoB, Seq(nodePred))
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
    val third = SingleStepTrail(boundPoint, Direction.OUTGOING, "pr3", Seq("C"), "c", True(), True(), CtoD, Seq())
    val second = SingleStepTrail(third, Direction.OUTGOING, "pr2", Seq("B"), "b", True(), True(), BtoC, Seq())
    val first = SingleStepTrail(second, Direction.OUTGOING, "pr1", Seq("A"), "a", True(), True(), AtoB, Seq())
    val expectedTrail = Some(LongestTrail("a", None, first))

    val foundTrail = TrailBuilder.findLongestTrail(Seq(AtoB, BtoC, BtoB2, CtoD), Seq("a"), Nil)
    assert(foundTrail === expectedTrail)
  }

  @Test def single_varlength_path() {
    //(b)-[:A*]->(e)

    val boundPoint = EndPoint("e")
    val first = VariableLengthStepTrail(boundPoint, Direction.OUTGOING, Seq("A"), 1, None, "p", None, "b", BtoE)
    val expectedTrail = Some(LongestTrail("b", None, first))

    val foundTrail = TrailBuilder.findLongestTrail(Seq(BtoE), Seq("b"), Nil)
    assert(foundTrail === expectedTrail)
  }

  @Test def single_rel_followed_by_varlength_with_single_bound_point() {
    //(a)-[:A]->(b)-[:A*]->(e)

    val endPoint = EndPoint("e")
    val last = VariableLengthStepTrail(endPoint, Direction.OUTGOING, Seq("A"), 1, None, "p", None, "b", BtoE)
    val first = SingleStepTrail(last, Direction.OUTGOING, "pr1", Seq("A"), "a", True(), True(), AtoB, Seq())
    val expected = Some(LongestTrail("a", None, first))

    val foundTrail = TrailBuilder.findLongestTrail(Seq(AtoB, BtoE), Seq("a"), Nil)
    assert(foundTrail === expected)
  }

  @Test def two_varlength_paths_with_both_ends_bound() {
    //(b)-[:A*]->(e)-[:C*]->f

    val endPoint = EndPoint("e")
    val trail = VariableLengthStepTrail(endPoint, Direction.OUTGOING, Seq("A"), 1, None, "p", None, "b", BtoE)
    val expected = Some(LongestTrail("b", None, trail))

    val foundTrail = TrailBuilder.findLongestTrail(Seq(BtoE, EtoF), Seq("b", "f"), Nil)
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
    val trail = SingleStepTrail(second, Direction.OUTGOING, "pr1", Seq("A"), "a", True(), True(), AtoB, Seq())

    val expected = Some(LongestTrail("a", None, trail))

    val foundTrail = TrailBuilder.findLongestTrail(Seq(AtoB, BtoE, BtoC), Seq("a"), Nil)
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
    val trail = SingleStepTrail(second, Direction.OUTGOING, "pr1", Seq("A"), "a", True(), True(), AtoB, Seq())

    val expected = Some(LongestTrail("a", None, trail))

    val foundTrail = TrailBuilder.findLongestTrail(Seq(AtoB, BtoE, EtoF), Seq("a"), Nil)
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
    val trail = SingleStepTrail(second, Direction.OUTGOING, "pr1", Seq("A"), "a", True(), True(), AtoB, Seq())

    val expected = Some(LongestTrail("a", None, trail))

    val foundTrail = TrailBuilder.findLongestTrail(Seq(AtoB, BtoE, EtoG), Seq("a"), Nil)
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
    val trail = SingleStepTrail(second, Direction.OUTGOING, "pr1", Seq("A"), "a", True(), True(), AtoB, Seq())

    val expected = Some(LongestTrail("a", None, trail))

    val foundTrail = TrailBuilder.findLongestTrail(Seq(AtoB, BtoE, BtoC), Seq("a"), Nil)
    assert(foundTrail === expected)
  }

  @Test def should_handle_loops() {

    // (a)-[pr1:A]->b-[pr2:B]->c
    //  \                      ^
    //   --[pr5:A]->x-[pr6:B]-/

    val AtoX = RelatedTo("a", "x", "pr5", Seq("A"), Direction.OUTGOING, optional = false, predicate = True())
    val XtoC = RelatedTo("x", "c", "pr6", Seq("B"), Direction.OUTGOING, optional = false, predicate = True())

    val endPoint = EndPoint("c")
    val last = SingleStepTrail(endPoint, Direction.OUTGOING, "pr6", Seq("B"), "x", True(), True(), XtoC, Seq())
    val trail = SingleStepTrail(last, Direction.OUTGOING, "pr5", Seq("A"), "a", True(), True(), AtoX, Seq())


    val result = TrailBuilder.findLongestTrail(Seq(AtoB, BtoC, AtoX, XtoC), Seq("a", "c"), Seq.empty)

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
    val fourth = SingleStepTrail(fifth , Direction.OUTGOING, "  UNNAMED18", Seq(), "  UNNAMED14", True(), True(), s4, Seq())
    val third  = SingleStepTrail(fourth, Direction.OUTGOING, "  UNNAMED17", Seq(), "b", True(), True(), s3, Seq())
    val second = SingleStepTrail(third , Direction.INCOMING, "  UNNAMED16", Seq(), "  UNNAMED13", True(), True(), s2, Seq())
    val first  = SingleStepTrail(second, Direction.INCOMING, "  UNNAMED15", Seq(), "a", True(), True(), s1, Seq())

    val result = TrailBuilder.findLongestTrail(Seq(s1, s2, s3, s4), Seq("a"), Seq.empty)

    val expected = Some(LongestTrail("a", None, first))

    assert(result === expected)
  }

  @Test def should_handle_predicates_in_the_middle() {
    // GIVEN
    // MATCH (a)-[r1]->(b)-[r2]->(c)<-[r3]-(d)
    // WHERE c.name = 'c ' and b.name = 'b '

    val predForB = Equals(Property(Identifier("b"), "name"), Literal("b"))
    val predForC = Equals(Property(Identifier("c"), "name"), Literal("c"))

    val expectedForB = Equals(Property(NodeIdentifier(), "name"), Literal("b"))
    val expectedForC = Equals(Property(NodeIdentifier(), "name"), Literal("c"))

    val s1 = RelatedTo("a", "b", "r1", Seq(), Direction.OUTGOING, optional = false, predicate = True())
    val s2 = RelatedTo("b", "c", "r2", Seq(), Direction.OUTGOING, optional = false, predicate = True())
    val s3 = RelatedTo("c", "d", "r3", Seq(), Direction.INCOMING, optional = false, predicate = True())

    val fourth = EndPoint("d")
    val third = SingleStepTrail(fourth, Direction.INCOMING, "r3", Seq(), "c", True(), True(), s3, Seq())
    val second = SingleStepTrail(third, Direction.OUTGOING, "r2", Seq(), "b", True(), expectedForC, s2, Seq(predForC))
    val first = SingleStepTrail(second, Direction.OUTGOING, "r1", Seq(), "a", True(), expectedForB, s1, Seq(predForB))

    val result = TrailBuilder.findLongestTrail(Seq(s1, s2, s3), Seq("a"), Seq(predForB, predForC))

    val expected = Some(LongestTrail("a", None, first))

    assert(result === expected)
  }
}
