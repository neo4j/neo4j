/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import expressions._
import expressions.Literal
import expressions.Property
import org.neo4j.graphdb.{RelationshipType, Direction}
import org.scalatest.Assertions
import org.neo4j.graphdb.Direction._
import org.neo4j.graphdb.DynamicRelationshipType.withName
import org.neo4j.cypher.GraphDatabaseTestBase
import org.neo4j.cypher.internal.pipes.matching._
import org.neo4j.cypher.internal.commands.True
import org.neo4j.cypher.internal.pipes.matching.VarLengthStep
import scala.Some
import org.neo4j.cypher.internal.pipes.matching.SingleStep
import org.neo4j.cypher.internal.commands.Equals
import org.neo4j.cypher.internal.pipes.matching.SingleStepTrail
import org.neo4j.cypher.internal.commands.True
import org.neo4j.cypher.internal.pipes.matching.VarLengthStep
import org.neo4j.cypher.internal.pipes.matching.VariableLengthStepTrail
import scala.Some
import org.neo4j.cypher.internal.pipes.matching.SingleStep
import org.neo4j.cypher.internal.pipes.matching.EndPoint
import org.neo4j.cypher.internal.commands.Equals
import org.neo4j.cypher.internal.pipes.matching.SingleStepTrail
import org.neo4j.cypher.internal.commands.True

class TrailToStepTest extends GraphDatabaseTestBase with Assertions with BuilderTest {
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
  val AtoB = RelatedTo("a", "b", "pr1", Seq("A"), Direction.OUTGOING, optional = false, predicate = True())
  val BtoC = RelatedTo("b", "c", "pr2", Seq("B"), Direction.OUTGOING, optional = false, predicate = True())
  val CtoD = RelatedTo("c", "d", "pr3", Seq("C"), Direction.OUTGOING, optional = false, predicate = True())
  val BtoB2 = RelatedTo("b", "b2", "pr4", Seq("D"), Direction.OUTGOING, optional = false, predicate = True())
  val BtoE = VarLengthRelatedTo("p", "b", "e", None, None, Seq("A"), Direction.OUTGOING, None, optional = false, predicate = True())

  @Test def single_step() {
    val expected = step(0, Seq(A), Direction.INCOMING, None)

    val steps = SingleStepTrail(EndPoint("b"), Direction.INCOMING, "pr1", Seq("A"), "a", None, None, AtoB).toSteps(0).get

    assert(steps === expected)
  }

  @Test def two_steps() {
    val boundPoint = EndPoint("c")
    val second = SingleStepTrail(boundPoint, Direction.INCOMING, "pr2", Seq("B"), "b", None, None, BtoC)
    val first = SingleStepTrail(second, Direction.INCOMING, "pr1", Seq("A"), "a", None, None, AtoB)

    val backward2 = step(1, Seq(B), Direction.INCOMING, None)
    val backward1 = step(0, Seq(A), Direction.INCOMING, Some(backward2))

    assert(first.toSteps(0).get === backward1)
  }

  @Test def two_steps_with_rel_predicates() {

    //()<-[r1:A]-(a)<-[r2:B]-()
    //WHERE r1.prop = 42 AND r2.prop = "FOO"

    val r1Pred = Equals(Property(Identifier("pr1"), "prop"), Literal(42))
    val r2Pred = Equals(Property(Identifier("pr2"), "prop"), Literal("FOO"))

    val boundPoint = EndPoint("c")
    val second = SingleStepTrail(boundPoint, Direction.INCOMING, "pr2", Seq("B"), "b", Some(r2Pred), None, BtoC)
    val first = SingleStepTrail(second, Direction.INCOMING, "pr1", Seq("A"), "a", Some(r1Pred), None, AtoB)

    val backward2 = step(1, Seq(B), Direction.INCOMING, None, relPredicate = r2Pred)
    val backward1 = step(0, Seq(A), Direction.INCOMING, Some(backward2), relPredicate = r1Pred)

    assert(first.toSteps(0).get === backward1)
  }

  @Test def two_steps_away_with_nodePredicate() {
    //()-[pr1:A]->(a)-[pr2:B]->()
    //WHERE r1.prop = 42 AND r2.prop = "FOO"

    val nodePred = Equals(Property(Identifier("b"), "prop"), Literal(42))

    val forward2 = step(1, Seq(B), Direction.INCOMING, None, nodePredicate = nodePred)
    val forward1 = step(0, Seq(A), Direction.INCOMING, Some(forward2))

    val boundPoint = EndPoint("c")
    val second = SingleStepTrail(boundPoint, Direction.INCOMING, "pr2", Seq("B"), "b", None, Some(nodePred), BtoC)
    val first = SingleStepTrail(second, Direction.INCOMING, "pr1", Seq("A"), "a", None, None, AtoB)


    assert(first.toSteps(0).get === forward1)
  }

  @Test def three_steps() {
    val pr3 = step(2, Seq(A), OUTGOING, None)
    val pr2 = step(1, Seq(B), OUTGOING, Some(pr3))
    val pr1 = step(0, Seq(C), OUTGOING, Some(pr2))

    val boundPoint = EndPoint("a")
    val third = SingleStepTrail(boundPoint, Direction.OUTGOING, "pr1", Seq("A"), "b", None, None, AtoB)
    val second = SingleStepTrail(third, Direction.OUTGOING, "pr2", Seq("B"), "c", None, None, BtoC)
    val first = SingleStepTrail(second, Direction.OUTGOING, "pr3", Seq("C"), "d", None, None, CtoD)

    assert(first.toSteps(0).get === pr1)
  }

  @Test def single_varlength_step() {
    val expected = varlengthStep(0, Seq(A), OUTGOING, 1, None, None)

    val boundPoint = EndPoint("e")
    val trail = VariableLengthStepTrail(boundPoint, Direction.OUTGOING, Seq("A"), 1, None, "p", None, "b", BtoE)

    val result = trail.toSteps(0).get
    assert(result.equals(expected))
  }

  private def step(id: Int,
                   typ: Seq[String],
                   direction: Direction,
                   next: Option[ExpanderStep],
                   nodePredicate: Predicate = True(),
                   relPredicate: Predicate = True()) =
    SingleStep(id, typ, direction, next, relPredicate = relPredicate, nodePredicate = nodePredicate)

  private def varlengthStep(id: Int,
                            typ: Seq[String],
                            direction: Direction,
                            min: Int,
                            max: Option[Int],
                            next: Option[ExpanderStep],
                            nodePredicate: Predicate = True(),
                            relPredicate: Predicate = True()) =
    VarLengthStep(id, typ, direction, min, max, next, relPredicate = relPredicate, nodePredicate = nodePredicate)
}