/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class RandomValuesTestSupportTest extends CypherFunSuite {

  test("reproduction clue always contains the seed repro line") {
    val clue = RandomValuesTestSupport.reproductionClue(123L, None)
    clue should include("setInitialSeed(123L)")
  }

  test("reproduction clue includes effective batch sizes when present") {
    val clue = RandomValuesTestSupport.reproductionClue(123L, Some((3, 7)))
    clue should include("effective pipelined batch size: small=3, big=7")
  }

  test("reproduction clue omits batch size line when absent") {
    val clue = RandomValuesTestSupport.reproductionClue(123L, None)
    clue should not include "pipelined batch size"
  }
}
