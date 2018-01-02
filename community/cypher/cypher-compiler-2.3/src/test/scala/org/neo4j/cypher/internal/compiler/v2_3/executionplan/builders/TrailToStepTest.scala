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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_3.commands._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions._
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.{Equals, True, Predicate}
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.TokenType._
import org.neo4j.cypher.internal.compiler.v2_3.pipes.matching._
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class TrailToStepTest extends CypherFunSuite {
  val A = "A"
  val B = "B"
  val C = "C"
  val D = "D"

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
           (e)
  */
  val AtoB = RelatedTo(SingleNode("a"), SingleNode("b"), "pr1", Seq("A"), SemanticDirection.OUTGOING, Map.empty)
  val BtoC = RelatedTo(SingleNode("b"), SingleNode("c"), "pr2", Seq("B"), SemanticDirection.OUTGOING, Map.empty)
  val CtoD = RelatedTo(SingleNode("c"), SingleNode("d"), "pr3", Seq("C"), SemanticDirection.OUTGOING, Map.empty)
  val BtoB2 = RelatedTo(SingleNode("b"), SingleNode("b2"), "pr4", Seq("D"), SemanticDirection.OUTGOING, Map.empty)
  val BtoE = VarLengthRelatedTo("p", SingleNode("b"), SingleNode("e"), None, None, Seq("A"), SemanticDirection.OUTGOING, None, Map.empty)

  test("single_step") {
    val expected = step(0, Seq(A), SemanticDirection.INCOMING, None)

    val steps = SingleStepTrail(EndPoint("b"), SemanticDirection.INCOMING, "pr1", Seq("A"), "a", True(), True(), AtoB, Seq()).toSteps(0).get

    steps should equal(expected)
  }

  test("two_steps") {
    val boundPoint = EndPoint("c")
    val second = SingleStepTrail(boundPoint, SemanticDirection.INCOMING, "pr2", Seq("B"), "b", True(), True(), BtoC, Seq())
    val first = SingleStepTrail(second, SemanticDirection.INCOMING, "pr1", Seq("A"), "a", True(), True(), AtoB, Seq())

    val backward2 = step(1, Seq(B), SemanticDirection.INCOMING, None)
    val backward1 = step(0, Seq(A), SemanticDirection.INCOMING, Some(backward2))

    first.toSteps(0).get should equal(backward1)
  }

  test("two_steps_with_rel_predicates") {

    //()<-[r1:A]-(a)<-[r2:B]-()
    //WHERE r1.prop = 42 AND r2.prop = "FOO"

    val r1Pred = Equals(Property(Identifier("pr1"), PropertyKey("prop")), Literal(42))
    val r2Pred = Equals(Property(Identifier("pr2"), PropertyKey("prop")), Literal("FOO"))

    val boundPoint = EndPoint("c")
    val second = SingleStepTrail(boundPoint, SemanticDirection.INCOMING, "pr2", Seq("B"), "b", r2Pred, True(), BtoC, Seq())
    val first = SingleStepTrail(second, SemanticDirection.INCOMING, "pr1", Seq("A"), "a", r1Pred, True(), AtoB, Seq())

    val backward2 = step(1, Seq(B), SemanticDirection.INCOMING, None, relPredicate = r2Pred)
    val backward1 = step(0, Seq(A), SemanticDirection.INCOMING, Some(backward2), relPredicate = r1Pred)

    first.toSteps(0).get should equal(backward1)
  }

  test("two_steps_away_with_nodePredicate") {
    //()-[pr1:A]->(a)-[pr2:B]->()
    //WHERE r1.prop = 42 AND r2.prop = "FOO"

    val nodePred = Equals(Property(Identifier("b"), PropertyKey("prop")), Literal(42))

    val forward2 = step(1, Seq(B), SemanticDirection.INCOMING, None, nodePredicate = nodePred)
    val forward1 = step(0, Seq(A), SemanticDirection.INCOMING, Some(forward2))

    val boundPoint = EndPoint("c")
    val second = SingleStepTrail(boundPoint, SemanticDirection.INCOMING, "pr2", Seq("B"), "b", True(), nodePred, BtoC, Seq())
    val first = SingleStepTrail(second, SemanticDirection.INCOMING, "pr1", Seq("A"), "a", True(), True(), AtoB, Seq())


    first.toSteps(0).get should equal(forward1)
  }

  test("longer_pattern_with_predicates") {
    // GIVEN
    // MATCH (a)-[r1]->(b)-[r2]->(c)<-[r3]-(d)
    // WHERE c.name = 'c ' and b.name = 'b '

    val predForB = Equals(Property(NodeIdentifier(), PropertyKey("name")), Literal("b"))
    val predForC = Equals(Property(NodeIdentifier(), PropertyKey("name")), Literal("c"))

    val forward3 = step(2, Seq(), SemanticDirection.INCOMING, None)
    val forward2 = step(1, Seq(), SemanticDirection.OUTGOING, Some(forward3), nodePredicate = predForC)
    val forward1 = step(0, Seq(), SemanticDirection.OUTGOING, Some(forward2), nodePredicate = predForB)

    val fourth = EndPoint("d")
    val third = SingleStepTrail(fourth, SemanticDirection.INCOMING, "r3", Seq(), "c", True(), True(), null, Seq())
    val second = SingleStepTrail(third, SemanticDirection.OUTGOING, "r2", Seq(), "b", True(), predForC, null, Seq())
    val first = SingleStepTrail(second, SemanticDirection.OUTGOING, "r1", Seq(), "a", True(), predForB, null, Seq())

    // WHEN
    val steps = first.toSteps(0).get

    //THEN
    steps should equal(forward1)
  }

  test("three_steps") {
    val pr3 = step(2, Seq(A), SemanticDirection.OUTGOING, None)
    val pr2 = step(1, Seq(B), SemanticDirection.OUTGOING, Some(pr3))
    val pr1 = step(0, Seq(C), SemanticDirection.OUTGOING, Some(pr2))

    val boundPoint = EndPoint("a")
    val third = SingleStepTrail(boundPoint, SemanticDirection.OUTGOING, "pr1", Seq("A"), "b", True(), True(), AtoB, Seq())
    val second = SingleStepTrail(third, SemanticDirection.OUTGOING, "pr2", Seq("B"), "c", True(), True(), BtoC, Seq())
    val first = SingleStepTrail(second, SemanticDirection.OUTGOING, "pr3", Seq("C"), "d", True(), True(), CtoD, Seq())

    first.toSteps(0).get should equal(pr1)
  }

  test("single_varlength_step") {
    val expected = varlengthStep(0, Seq(A), SemanticDirection.OUTGOING, 1, None, None)

    val boundPoint = EndPoint("e")
    val trail = VariableLengthStepTrail(boundPoint, SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, Seq("A"), 1, None, "p", None, "b", BtoE)

    val result = trail.toSteps(0).get
    result should equal(expected)
  }

  private def step(id: Int,
                   typ: Seq[String],
                   direction: SemanticDirection,
                   next: Option[ExpanderStep],
                   nodePredicate: Predicate = True(),
                   relPredicate: Predicate = True()) =
    SingleStep(id, typ, direction, next, relPredicate = relPredicate, nodePredicate = nodePredicate)

  private def varlengthStep(id: Int,
                            typ: Seq[String],
                            direction: SemanticDirection,
                            min: Int,
                            max: Option[Int],
                            next: Option[ExpanderStep],
                            nodePredicate: Predicate = True(),
                            relPredicate: Predicate = True()) =
    VarLengthStep(id, typ, direction, min, max, next, relPredicate = relPredicate, nodePredicate = nodePredicate)
}
