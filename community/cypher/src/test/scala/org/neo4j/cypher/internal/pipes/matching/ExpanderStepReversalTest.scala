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
package org.neo4j.cypher.internal.pipes.matching

import org.junit.Test
import org.neo4j.graphdb.Direction
import org.scalatest.Assertions
import org.neo4j.cypher.internal.commands.{True, Predicate}
import org.neo4j.cypher.internal.commands.expressions.Expression
import org.neo4j.cypher.internal.symbols.SymbolTable
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.pipes.QueryState

class ExpanderStepReversalTest extends Assertions {
  val A = "A"
  val B = "B"
  val C = "C"

  val c = step(2, Seq(C), Direction.INCOMING, None)
  val b = step(1, Seq(B), Direction.BOTH, Some(c))
  val a = step(0, Seq(A), Direction.OUTGOING, Some(b))

  val aR = step(0, Seq(A), Direction.INCOMING, None)
  val bR = step(1, Seq(B), Direction.BOTH, Some(aR))
  val cR = step(2, Seq(C), Direction.OUTGOING, Some(bR))

  @Test def reverse() {
    assert(a.reverse() === cR)
    assert(cR.reverse() === a)
  }

  @Test def reverse_single_predicate() {
    //Given
    val step1 = step(0, A, Direction.OUTGOING, None, "pr1")
    val step1R = step(0, A, Direction.INCOMING, None, "pr1")

    //When&Then
    assert(step1.reverse() === step1R)
    assert(step1R.reverse() === step1)
  }


  @Test def reverse_two_steps() {
    //()-[pr1:A]->(a)-[pr2:B]->()
    //WHERE r1.prop = 42 AND r2.prop = "FOO"

    val step2 = step(0, A, Direction.INCOMING, None, "pr1", "b")
    val step1 = step(1, B, Direction.INCOMING, Some(step2), "pr2")

    val step1R = step(1, B, Direction.OUTGOING, None, "pr2", "b")
    val step2R = step(0, A, Direction.OUTGOING, Some(step1R), "pr1")

    assert(step1.reverse() === step2R)
    assert(step2R.reverse() === step1)
  }

  @Test def reverse_with_three_steps() {
    //()-[pr1:A]->(a)-[pr2:B]->(b)-[pr3:C]->()
    //WHERE r1.prop = 42 AND r2.prop = "FOO"

    val step3 = step(0, A, Direction.INCOMING, None, "pr1", "b")
    val step2 = step(1, B, Direction.INCOMING, Some(step3), "pr2", "c")
    val step1 = step(2, C, Direction.INCOMING, Some(step2), "pr3")

    val step1R = step(2, C, Direction.OUTGOING, None, "pr3", "c")
    val step2R = step(1, B, Direction.OUTGOING, Some(step1R), "pr2", "b")
    val step3R = step(0, A, Direction.OUTGOING, Some(step2R), "pr1")

    assert(step1.reverse() === step3R)
    assert(step3R.reverse() === step1)

  }

  @Test def reverse_predicates_with_mixed_directions() {
    //(a)-[pr1:A]->(b)-[pr2:B]->(c)-[pr3:C]->(d)
    //WHERE r1.prop = 42 AND r2.prop = "FOO"

    val step3 = step(0, A, Direction.BOTH, None, "pr1", "b")
    val step2 = step(1, B, Direction.INCOMING, Some(step3), "pr2", "c")
    val step1 = step(2, C, Direction.OUTGOING, Some(step2), "pr3")

    val step1R = step(2, C, Direction.INCOMING, None, "pr3", "c")
    val step2R = step(1, B, Direction.OUTGOING, Some(step1R), "pr2", "b")
    val step3R = step(0, A, Direction.BOTH, Some(step2R), "pr1")

    assert(step1.reverse() === step3R)
    assert(step3R.reverse() === step1)
  }

  private def step(id: Int,
                   typ: Seq[String],
                   direction: Direction,
                   next: Option[ExpanderStep]) = SingleStep(id, typ, direction, next, True(), True())

  def step(id: Int, t: String, dir: Direction, next: Option[ExpanderStep], relName: String, nodeName: String): ExpanderStep =
    SingleStep(id, Seq(t), dir, next, relPredicate = Pred(relName), nodePredicate = Pred(nodeName))

  def step(id: Int, t: String, dir: Direction, next: Option[ExpanderStep], relName: String): ExpanderStep =
    SingleStep(id, Seq(t), dir, next, relPredicate = Pred(relName), nodePredicate = True())
}

case class Pred(identifier: String) extends Predicate {
  def isMatch(m: ExecutionContext)(implicit state: QueryState) = false

  def atoms = Seq(this)

  def rewrite(f: (Expression) => Expression) = null

  def containsIsNull = false

  def children = Seq.empty

  def assertInnerTypes(symbols: SymbolTable) {}

  def symbolTableDependencies = Set(identifier)

  override def toString() = "Pred[%s]".format(identifier)
}
