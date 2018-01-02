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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.InputPosition

class InputPositionTest extends CypherFunSuite {

  test("should bump the input position") {
    val pos = InputPosition(2, 1, 1)

    val bumped = pos.bumped()

    bumped.offset should equal(pos.offset + 1)
    bumped.column should equal(pos.column)
    bumped.line should equal(pos.line)

    pos should not equal bumped
    pos should equal(InputPosition(2, 1, 1))
    bumped should equal(InputPosition(2, 1, 1).bumped())
  }

  test("should print offset") {
    InputPosition(2, 1, 1).toOffsetString should equal("2")
    InputPosition(2, 1, 1).bumped().toOffsetString should equal("3")
  }
}
