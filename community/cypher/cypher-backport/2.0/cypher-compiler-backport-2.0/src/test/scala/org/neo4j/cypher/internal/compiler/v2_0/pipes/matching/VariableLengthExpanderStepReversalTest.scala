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
package org.neo4j.cypher.internal.compiler.v2_0.pipes.matching

import org.junit.Test
import org.neo4j.graphdb.{RelationshipType, DynamicRelationshipType, Direction}
import org.scalatest.Assertions
import org.neo4j.cypher.internal.compiler.v2_0.commands.True

class VariableLengthExpanderStepReversalTest extends Assertions {

  private def step(id: Int,
                   typ: Seq[String],
                   direction: Direction,
                   next: Option[ExpanderStep]) = SingleStep(id, typ, direction, next, True(), True())

  private def varStep(id: Int,
                      typ: Seq[String],
                      direction: Direction,
                      min: Int,
                      max: Option[Int],
                      next: Option[ExpanderStep]) = VarLengthStep(id, typ, direction, min, max, next, True(), True())

  val A = "A"
  val B = "B"
  val C = "C"

  @Test def reverse_single_step() {
    // ()-[:A*]->()
    val step = varStep(0, Seq(A), Direction.OUTGOING, 1, None, None)

    // ()<-[:A*]-()
    val reversed = varStep(0, Seq(A), Direction.INCOMING, 1, None, None)

    assert(step.reverse() === reversed)
    assert(reversed.reverse() === step)
  }

  @Test def reverse_two_steps() {
    // ()-[:A*]->()<-[:B*]-()
    val step2 = varStep(1, Seq(B), Direction.INCOMING, 1, None, None)
    val step1 = varStep(0, Seq(A), Direction.OUTGOING, 1, None, Some(step2))

    // ()-[:B*]->()<-[:A*]-()
    val reversed2 = varStep(0, Seq(A), Direction.INCOMING, 1, None, None)
    val reversed1 = varStep(1, Seq(B), Direction.OUTGOING, 1, None, Some(reversed2))

    assert(step1.reverse() === reversed1)
    assert(reversed1.reverse() === step1)
  }

  @Test def reverse_mixed_steps() {
    // ()-[:A*]->()-[:B]-()
    val step2 = step(1, Seq(B), Direction.INCOMING, None)
    val step1 = varStep(0, Seq(A), Direction.OUTGOING, 1, None, Some(step2))

    // ()-[:B]-()<-[:A*]-()
    val reversed2 = varStep(0, Seq(A), Direction.INCOMING, 1, None, None)
    val reversed1 = step(1, Seq(B), Direction.OUTGOING, Some(reversed2))

    assert(step1.reverse() === reversed1)
    assert(reversed1.reverse() === step1)
  }
}