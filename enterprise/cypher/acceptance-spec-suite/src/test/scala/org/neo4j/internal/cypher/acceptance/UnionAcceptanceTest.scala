/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.cypher._
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._

class UnionAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("Should work when doing union with same return varibles") {
    createLabeledNode(Map("a" -> "a", "b" -> "b"), "A")
    createLabeledNode(Map("a" -> "a", "b" -> "b"), "B")

    val query =
      """
        |MATCH (N:A)
        |RETURN
        |N.a as A,
        |N.b as B
        |UNION
        |MATCH (M:B) RETURN
        |M.b as A,
        |M.a as B
      """.stripMargin

    val result = executeWith(Configs.CommunityInterpreted, query)
    val expected = List(Map("A" -> "a", "B" -> "b"), Map("A" -> "b", "B" -> "a"))

    result.toList should equal(expected)
  }

  test("Should work when doing union with permutated return varibles") {
    createLabeledNode(Map("a" -> "a", "b" -> "b"), "A")
    createLabeledNode(Map("a" -> "b", "b" -> "a"), "B")

    val query =
      """
        |MATCH (N:A)
        |RETURN
        |N.a as B,
        |N.b as A
        |UNION
        |MATCH (M:B) RETURN
        |M.b as A,
        |M.a as B
      """.stripMargin

    val expectedToWorkIn = Configs.CommunityInterpreted -
      TestConfiguration(Versions.V2_3 -> Versions.V3_1, Planners.Cost, Runtimes.Default)
    val result = executeWith(expectedToWorkIn, query)
    val expected = List(Map("A" -> "b", "B" -> "a"), Map("A" -> "a", "B" -> "b"))

    result.toList should equal(expected)
  }
}
