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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite

class TernaryLogicAcceptanceTest extends ExecutionEngineFunSuite {

  test("test") {
    "not null" =>> null
    "null IS NULL" =>> true

    "null = null" =>> null
    "null <> null" =>> null

    "null and null" =>> null
    "null and true" =>> null
    "true and null" =>> null
    "false and null" =>> false
    "null and false" =>> false

    "null or null" =>> null
    "null or true" =>> true
    "true or null" =>> true
    "false or null" =>> null
    "null or false" =>> null

    "null xor null" =>> null
    "null xor true" =>> null
    "true xor null" =>> null
    "false xor null" =>> null
    "null xor false" =>> null

    "null in [1,2,3]" =>> null
    "null in [1,2,3,null]" =>> null
    "null in []" =>> false
    "1 in [1,2,3, null]" =>> true
    "5 in [1,2,3, null]" =>> null
  }

  implicit class Evaluate(comparison: String) {
    def =>>(expectedResult: Any) = {
      val output = executeScalar[Any]("RETURN " + comparison)

      output should equal(expectedResult)
    }
  }
}
