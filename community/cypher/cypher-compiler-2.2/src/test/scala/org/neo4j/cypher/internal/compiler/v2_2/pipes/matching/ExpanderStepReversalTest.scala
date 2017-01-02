/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.pipes.matching

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions.{Expression, Identifier, Literal, Property}
import org.neo4j.cypher.internal.compiler.v2_2.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.compiler.v2_2.commands.{Equals, Predicate, True}
import org.neo4j.cypher.internal.compiler.v2_2.pipes.QueryState
import org.neo4j.graphdb.Direction

class ExpanderStepReversalTest extends CypherFunSuite {
  val A = "A"
  val B = "B"
  val C = "C"

  val _c = step(2, Seq(C), Direction.INCOMING, None)
  val _b = step(1, Seq(B), Direction.BOTH, Some(_c))
  val _a = step(0, Seq(A), Direction.OUTGOING, Some(_b))

  val _aR = step(0, Seq(A), Direction.INCOMING, None)
  val _bR = step(1, Seq(B), Direction.BOTH, Some(_aR))
  val _cR = step(2, Seq(C), Direction.OUTGOING, Some(_bR))

  test("reverse") {
    _a.reverse() should equal(_cR)
    _cR.reverse() should equal(_a)
  }

  test("reverse_single_predicate") {
    //Given
    val step1 = step(0, A, Direction.OUTGOING, None, "pr1")
    val step1R = step(0, A, Direction.INCOMING, None, "pr1")

    //When&Then
    step1.reverse() should equal(step1R)
    step1R.reverse() should equal(step1)
  }

  test("reverse_long_trail_with_two_predicates") {

    def step(id: Int,
                     typ: Seq[String],
                     direction: Direction,
                     next: Option[ExpanderStep],
                     nodePredicate: Predicate = True(),
                     relPredicate: Predicate = True()) =
    SingleStep(id, typ, direction, next, relPredicate = relPredicate, nodePredicate = nodePredicate)


    // GIVEN
    // MATCH (a)-[r1]->(b) (b)-[r2]->(c)  (c)<-[r3]-(d)
    // WHERE c.name = 'c ' and b.name = 'b '
    val predForB = Equals(Property(Identifier("b"), PropertyKey("name")), Literal("b"))
    val predForC = Equals(Property(Identifier("c"), PropertyKey("name")), Literal("c"))

    val forward3 = step(2, Seq(), Direction.INCOMING, None)
    val forward2 = step(1, Seq(), Direction.OUTGOING, Some(forward3), nodePredicate = predForC)
    val forward1 = step(0, Seq(), Direction.OUTGOING, Some(forward2), nodePredicate = predForB)

    val reverse1 = step(0, Seq(), Direction.INCOMING, None)
    val reverse2 = step(1, Seq(), Direction.INCOMING, Some(reverse1), nodePredicate = predForB)
    val reverse3 = step(2, Seq(), Direction.OUTGOING, Some(reverse2), nodePredicate = predForC)

    //When&Then
    forward1.reverse() should equal(reverse3)
    reverse3.reverse() should equal(forward1)
  }

  test("reverse_two_steps") {
    //()-[pr1:A]->(a)-[pr2:B]->()

    val step1 = step(1, B, Direction.OUTGOING, None, "pr2")
    val step0 = step(0, A, Direction.OUTGOING, Some(step1), "pr1", "a")

    val step0R = step(0, A, Direction.INCOMING, None, "pr1")
    val step1R = step(1, B, Direction.INCOMING, Some(step0R), "pr2", "a")

    step0.reverse() should equal(step1R)
    step1R.reverse() should equal(step0)
  }

  test("reverse_with_three_steps") {
    //()-[pr0:A]->(a)-[pr1:B]->(b)-[pr2:C]->()

    val step2 = step(2, C, Direction.OUTGOING, None, "pr2")
    val step1 = step(1, B, Direction.OUTGOING, Some(step2), "pr1", "b")
    val step0 = step(0, A, Direction.OUTGOING, Some(step1), "pr0", "a")

    val step0R = step(0, A, Direction.INCOMING, None, "pr0")
    val step1R = step(1, B, Direction.INCOMING, Some(step0R), "pr1", "a")
    val step2R = step(2, C, Direction.INCOMING, Some(step1R), "pr2", "b")

    step0.reverse() should equal(step2R)
    step2R.reverse() should equal(step0)
  }

  test("reverse_predicates_with_mixed_directions") {
    //(a)-[pr0:A]->(b)-[pr1:B]-(c)<-[pr2:C]-(d)

    val step3 = step(2, C, Direction.INCOMING, None, "pr2")
    val step2 = step(1, B, Direction.BOTH, Some(step3), "pr1", "c")
    val step1 = step(0, A, Direction.OUTGOING, Some(step2), "pr0", "b")

    val step1R = step(0, A, Direction.INCOMING, None, "pr0")
    val step2R = step(1, B, Direction.BOTH, Some(step1R), "pr1", "b")
    val step3R = step(2, C, Direction.OUTGOING, Some(step2R), "pr2", "c")

    step1.reverse() should equal(step3R)
    step3R.reverse() should equal(step1)
  }

  private def step(id: Int, typ: Seq[String], direction: Direction, next: Option[ExpanderStep]) =
    SingleStep(id, typ, direction, next, True(), True())

  def step(id: Int, t: String, dir: Direction, next: Option[ExpanderStep], relName: String, nodeName: String): ExpanderStep =
    SingleStep(id, Seq(t), dir, next, relPredicate = Pred(relName), nodePredicate = Pred(nodeName))

  def step(id: Int, t: String, dir: Direction, next: Option[ExpanderStep], relName: String): ExpanderStep =
    SingleStep(id, Seq(t), dir, next, relPredicate = Pred(relName), nodePredicate = True())
}

case class Pred(identifier: String) extends Predicate {
  def isMatch(m: ExecutionContext)(implicit state: QueryState) = Some(false)

  def rewrite(f: (Expression) => Expression) = null

  def containsIsNull = false

  def arguments = Nil

  def symbolTableDependencies = Set(identifier)

  override def toString() = "Pred[%s]".format(identifier)
}
