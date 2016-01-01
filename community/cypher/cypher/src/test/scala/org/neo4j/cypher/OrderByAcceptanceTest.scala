/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher

import org.neo4j.cypher.internal.commons.CustomMatchers

class OrderByAcceptanceTest extends ExecutionEngineFunSuite with CustomMatchers with NewPlannerTestSupport {
  test("should support ORDER BY") {
    createNode(Map("prop" -> 1))
    createNode(Map("prop" -> 3))
    createNode(Map("prop" -> -5))
    val result = executeWithNewPlanner("match (n) return n.prop AS prop ORDER BY n.prop")
    result.toList should equal(List(
      Map("prop" -> -5),
      Map("prop" -> 1),
      Map("prop" -> 3)
    ))
  }

  test("should support ORDER BY DESC") {
    createNode(Map("prop" -> 1))
    createNode(Map("prop" -> 3))
    createNode(Map("prop" -> -5))
    val result = executeWithNewPlanner("match (n) return n.prop AS prop ORDER BY n.prop DESC")
    result.toList should equal(List(
      Map("prop" -> 3),
      Map("prop" -> 1),
      Map("prop" -> -5)
    ))
  }
}
