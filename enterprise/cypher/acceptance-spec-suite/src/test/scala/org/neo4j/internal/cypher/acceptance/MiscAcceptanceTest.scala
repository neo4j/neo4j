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

    // Fixed in 3.3.3
    val result = executeWith(Configs.All, query, expectedDifferentResults = Configs.BackwardsCompatibility + Configs.AllRulePlanners)
    result.toList should equal(List(Map("y" -> 1, "y3" -> 3), Map("y" -> 1, "y3" -> 4), Map("y" -> 2, "y3" -> 3), Map("y" -> 2, "y3" -> 4)))
  }

}
