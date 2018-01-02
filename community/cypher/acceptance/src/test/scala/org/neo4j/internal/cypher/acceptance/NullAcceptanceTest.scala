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

class NullAcceptanceTest extends ExecutionEngineFunSuite {

  val anyNull: AnyRef = null.asInstanceOf[AnyRef]

  test("null nodes should be silently ignored") {
    // Given empty database

    // When
    val result = execute("optional match (a:DoesNotExist) set a.prop = 42 return a")

    // Then doesn't throw
    result.toList
  }

  val expressions = Seq(
    "round(null)",
    "floor(null)",
    "ceil(null)",
    "abs(null)",
    "acos(null)",
    "asin(null)",
    "atan(null)",
    "cos(null)",
    "cot(null)",
    "exp(null)",
    "log(null)",
    "log10(null)",
    "sin(null)",
    "tan(null)",
    "haversin(null)",
    "sqrt(null)",
    "sign(null)",
    "radians(null)",
    "atan2(null, 0.3)",
    "atan2(0.3, null)",
    "null in [1,2,3]",
    "2 in null",
    "null in null",
    "ANY(x in NULL WHERE x = 42)"
  )

  expressions.foreach { expression =>
    test(expression) {
      executeScalar[Any]("RETURN " + expression) should equal(anyNull)
    }
  }
}
