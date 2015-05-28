/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
    createNode("prop" -> 1)
    createNode("prop" -> 3)
    createNode("prop" -> -5)
    val result = executeWithAllPlanners("match (n) return n.prop AS prop ORDER BY n.prop")
    result.toList should equal(List(
      Map("prop" -> -5),
      Map("prop" -> 1),
      Map("prop" -> 3)
    ))
  }

  test("should support ORDER BY DESC") {
    createNode("prop" -> 1)
    createNode("prop" -> 3)
    createNode("prop" -> -5)
    val result = executeWithAllPlanners("match (n) return n.prop AS prop ORDER BY n.prop DESC")
    result.toList should equal(List(
      Map("prop" -> 3),
      Map("prop" -> 1),
      Map("prop" -> -5)
    ))
  }

  test("ORDER BY of an column introduced in RETURN should work well") {
    executeWithAllPlanners("WITH [0, 1] AS prows, [[2], [3, 4]] AS qrows UNWIND prows AS p UNWIND qrows[p] AS q WITH p, count(q) AS rng RETURN p ORDER BY rng").toList should
      equal(List(Map("p" -> 0), Map("p" -> 1)))
  }

  test("renaming columns before ORDER BY is not confusing") {
    createNode("prop" -> 1)
    createNode("prop" -> 3)
    createNode("prop" -> -5)

    executeWithAllPlanners("MATCH n RETURN n.prop AS n ORDER BY n + 2").toList should
      equal(List(
        Map("n" -> -5),
        Map("n" -> 1),
        Map("n" -> 3)
      ))
  }
}
