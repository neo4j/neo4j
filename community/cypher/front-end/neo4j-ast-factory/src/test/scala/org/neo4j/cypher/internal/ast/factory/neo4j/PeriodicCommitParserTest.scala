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

import org.neo4j.cypher.internal.ast.Statement

class PeriodicCommitParserTest extends JavaccParserAstTestBase[Statement] {

  implicit private val parser: JavaccRule[Statement] = JavaccRule.Statement

  val message =
    "The PERIODIC COMMIT query hint is no longer supported. Please use CALL { ... } IN TRANSACTIONS instead. (line 1, column 7 (offset: 6))"

  test("USING PERIODIC COMMIT LOAD CSV FROM 'foo' AS l RETURN l") {
    assertFailsWithMessage(testName, message)
  }

  test("USING PERIODIC COMMIT 200 LOAD CSV FROM 'foo' AS l RETURN l") {
    assertFailsWithMessage(testName, message)
  }

  test("USING PERIODIC COMMIT RETURN 1") {
    assertFailsWithMessage(testName, message)
  }
}
