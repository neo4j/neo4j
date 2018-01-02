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

class StepSizeTest extends CypherFunSuite {

  test("single_step_is_1") {
    val step = SingleStep(0, Seq(), SemanticDirection.OUTGOING, None, True(), True())

    step.size should equal(Some(1))
  }

  test("two_single_step_is_2") {
    val second = SingleStep(1, Seq(), SemanticDirection.OUTGOING, None, True(), True())
    val step = SingleStep(0, Seq(), SemanticDirection.OUTGOING, Some(second), True(), True())
    step.size should equal(Some(2))
  }

  test("unlimited_varlength_is_none") {
    val step = VarLengthStep(0, Seq(), SemanticDirection.OUTGOING, 0, None, None, True(), True())
    step.size should equal(None)
  }

  test("limited_varlength_is_max") {
    val step = VarLengthStep(0, Seq(), SemanticDirection.OUTGOING, 0, Some(42), None, True(), True())
    step.size should equal(Some(42))
  }

  test("limited_varlength_plus_unlimited_is_none") {
    val second = VarLengthStep(1, Seq(), SemanticDirection.OUTGOING, 0, None, None, True(), True())
    val step = VarLengthStep(0, Seq(), SemanticDirection.OUTGOING, 0, Some(42), Some(second), True(), True())
    step.size should equal(None)
  }

  test("limited_varlength_plus_single_step_is_max_plus_1") {
    val second = SingleStep(1, Seq(), SemanticDirection.OUTGOING, None, True(), True())
    val step = VarLengthStep(0, Seq(), SemanticDirection.OUTGOING, 0, Some(42), Some(second), True(), True())
    step.size should equal(Some(43))
  }
}
