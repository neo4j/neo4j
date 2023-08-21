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
package org.neo4j.cypher.internal.ir

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class VarPatternLengthTest extends CypherFunSuite {

  test("intersect should give the right result") {
    val defaultLength = VarPatternLength(1, None)
    defaultLength.intersect(defaultLength) should equal(defaultLength)

    VarPatternLength(2, None) intersect VarPatternLength(4, None) should equal(VarPatternLength(4, None))
    VarPatternLength(2, Some(8)) intersect VarPatternLength(4, None) should equal(VarPatternLength(4, Some(8)))
    VarPatternLength(2, None) intersect VarPatternLength(4, Some(42)) should equal(VarPatternLength(4, Some(42)))
    VarPatternLength(0, Some(1)) intersect VarPatternLength(2, Some(13)) should equal(VarPatternLength(2, Some(1)))
  }
}
