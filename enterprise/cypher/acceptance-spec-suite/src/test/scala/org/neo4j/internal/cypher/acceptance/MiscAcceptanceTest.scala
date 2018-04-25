/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._

class MiscAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  // This test verifies a bugfix in slotted runtime
  test("should be able to compare integers") {
    val query = """
      UNWIND range(0, 1) AS i
      UNWIND range(0, 1) AS j
      WITH i, j
      WHERE i <> j
      RETURN i, j"""

    val result = executeWith(Configs.Interpreted + Configs.Morsel, query)
    result.toList should equal(List(Map("j" -> 1, "i" -> 0), Map("j" -> 0, "i" -> 1)))
  }

  test("order by after projection") {
    val query =
      """
        |UNWIND [ 1,2 ] as x
        |UNWIND [ 3,4 ] as y
        |RETURN x AS y, y as y3
        |ORDER BY y
      """.stripMargin

    val result = executeWith(Configs.All, query, expectedDifferentResults = Configs.OldAndRule)
    result.toList should equal(List(Map("y" -> 1, "y3" -> 3), Map("y" -> 1, "y3" -> 4), Map("y" -> 2, "y3" -> 3), Map("y" -> 2, "y3" -> 4)))
  }

  test("should unwind nodes") {
    val n = createNode("prop" -> 42)

    val query = "UNWIND $nodes AS n WITH n WHERE n.prop = 42 RETURN n"
    val result = executeWith(Configs.All - Configs.Version2_3, query, params = Map("nodes" -> List(n)))

    result.toList should equal(List(Map("n" -> n)))
  }

  test("should unwind nodes from literal list") {
    val n = createNode("prop" -> 42)

    val query = "UNWIND [$node] AS n WITH n WHERE n.prop = 42 RETURN n"
    val result = executeWith(Configs.All - Configs.Version2_3, query, params = Map("node" -> n))

    result.toList should equal(List(Map("n" -> n)))
  }

  test("should unwind relationships") {
    val a = createNode()
    val b = createNode()
    val r = relate(a, b, "prop" -> 42)

    val query = "UNWIND $relationships AS r WITH r WHERE r.prop = 42 RETURN r"
    val result = executeWith(Configs.All - Configs.Version2_3, query, params = Map("relationships" -> List(r)))

    result.toList should equal(List(Map("r" -> r)))
  }

  test("should unwind relationships from literal list") {
    val a = createNode()
    val b = createNode()
    val r = relate(a, b, "prop" -> 42)

    val query = "UNWIND [$relationship] AS r WITH r WHERE r.prop = 42 RETURN r"
    val result = executeWith(Configs.All - Configs.Version2_3, query, params = Map("relationship" -> r))

    result.toList should equal(List(Map("r" -> r)))
  }

  test("should be able to use long values for LIMIT in interpreted runtime") {
    val a = createNode()
    val b = createNode()

    val limit: Long = Int.MaxValue + 1l
    // If we would use Ints for storing the limit, then we would end up with "limit 0"
    // thus, if we actually return the two nodes, then it proves that we used a long
    val query = "MATCH (n) RETURN n LIMIT " + limit
    val worksCorrectlyInConfig = Configs.Version3_4 + Configs.Version3_3 - Configs.AllRulePlanners
    // the query will work in all configs, but only have the correct result in those specified configs
    // Also: It Will work on 3.2 once 3.2.12 is out AND on 3.3 once 3.3.6 is out
    val result = executeWith(Configs.All, query, Configs.All - worksCorrectlyInConfig)
    result.toList should equal(List(Map("n" -> a), Map("n" -> b)))
  }
}
