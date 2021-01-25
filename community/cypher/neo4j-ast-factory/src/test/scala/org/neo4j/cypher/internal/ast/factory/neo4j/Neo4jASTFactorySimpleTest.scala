/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.scalatest.FunSuiteLike

class Neo4jASTFactorySimpleTest extends ParsingTestBase with FunSuiteLike with TestName {

  test("RETURN 1 AS x") {
    assertSameAST(testName)
  }

  test("RETURN 'apoks\\tf\\np' AS x") {
    assertSameAST(testName)
  }

  test("RETURN \"apoks\\tf\\np\" AS x") {
    assertSameAST(testName)
  }

  test("RETURN 'hi' AS `call`") {
    assertSameAST(testName)
  }

  test("RETURN '\uD83D\uDCA9' AS `turd`") {
    assertSameAST(testName)
  }

  test("RETURN NOT true") {
    assertSameAST(testName)
  }

  test("RETURN filter (x IN [1,2,3] WHERE x = 2) AS k") {
    assertSameAST(testName)
  }
}
