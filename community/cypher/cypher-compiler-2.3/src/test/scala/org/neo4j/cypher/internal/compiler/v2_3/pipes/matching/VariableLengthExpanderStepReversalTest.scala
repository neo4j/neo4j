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
package org.neo4j.cypher.internal.compiler.v2_3.pipes.matching

import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.True
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class VariableLengthExpanderStepReversalTest extends CypherFunSuite {
  val A = "A"
  val B = "B"
  val C = "C"

  test("reverse_single_step") {
    // ()-[:A*]->()
    val step = varStep(0, Seq(A), SemanticDirection.OUTGOING, 1, None, None)

    // ()<-[:A*]-()
    val reversed = varStep(0, Seq(A), SemanticDirection.INCOMING, 1, None, None)

    step.reverse() should equal(reversed)
    reversed.reverse() should equal(step)
  }

  test("reverse_two_steps") {
    // ()-[:A*]->()<-[:B*]-()
    val step2 = varStep(1, Seq(B), SemanticDirection.INCOMING, 1, None, None)
    val step1 = varStep(0, Seq(A), SemanticDirection.OUTGOING, 1, None, Some(step2))

    // ()-[:B*]->()<-[:A*]-()
    val reversed2 = varStep(0, Seq(A), SemanticDirection.INCOMING, 1, None, None)
    val reversed1 = varStep(1, Seq(B), SemanticDirection.OUTGOING, 1, None, Some(reversed2))

    step1.reverse() should equal(reversed1)
    reversed1.reverse() should equal(step1)
  }

  test("reverse_mixed_steps") {
    // ()-[:A*]->()-[:B]-()
    val step2 = step(1, Seq(B), SemanticDirection.INCOMING, None)
    val step1 = varStep(0, Seq(A), SemanticDirection.OUTGOING, 1, None, Some(step2))

    // ()-[:B]-()<-[:A*]-()
    val reversed2 = varStep(0, Seq(A), SemanticDirection.INCOMING, 1, None, None)
    val reversed1 = step(1, Seq(B), SemanticDirection.OUTGOING, Some(reversed2))

    step1.reverse() should equal(reversed1)
    reversed1.reverse() should equal(step1)
  }

  private def step(id: Int,
                   typ: Seq[String],
                   direction: SemanticDirection,
                   next: Option[ExpanderStep]) = SingleStep(id, typ, direction, next, True(), True())

  private def varStep(id: Int,
                      typ: Seq[String],
                      direction: SemanticDirection,
                      min: Int,
                      max: Option[Int],
                      next: Option[ExpanderStep]) = VarLengthStep(id, typ, direction, min, max, next, True(), True())
}
