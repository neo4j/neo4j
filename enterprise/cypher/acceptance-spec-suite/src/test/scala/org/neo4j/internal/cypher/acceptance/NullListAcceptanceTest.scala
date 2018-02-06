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

import org.neo4j.cypher._
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._

class NullListAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  // Changed behaviour to comply with opencypher in 3.3
  val nullInListConfigOld = Configs.OldAndRule

  // Comparison between lists and non-lists

  test("equality between list and literal should return false") {
    val query = "WITH [1, 2] AS l1, 'foo' AS l2 RETURN l1 = l2 AS res"

    val result = executeWith(Configs.All + Configs.Morsel, query)

    result.toList should equal(List(Map("res" -> false)))
  }

  // Equality between lists with null

  test("equality of lists of different length should return false despite nulls") {
    val query = "WITH [1] AS l1, [1, null] AS l2 RETURN l1 = l2 AS res"

    val result = executeWith(Configs.All + Configs.Morsel, query)

    result.toList should equal(List(Map("res" -> false)))
  }

  test("equality between different lists with null should return false") {
    val query = "WITH [1, 2] AS l1, [null, 'foo'] AS l2 RETURN l1 = l2 AS res"

    val result = executeWith(Configs.All + Configs.Morsel, query)

    result.toList should equal(List(Map("res" -> false)))
  }

  test("equality between almost equal lists with null should return null") {
    val query = "WITH [1, 2] AS l1, [null, 2] AS l2 RETURN l1 = l2 AS res"

    val result = executeWith(Configs.All + Configs.Morsel, query, expectedDifferentResults = nullInListConfigOld)

    result.toList should equal(List(Map("res" -> null)))
  }

  // Nested Lists
  test("equality of nested lists of different length should return false despite nulls") {
    val query = "WITH [[1]] AS l1, [[1], [null]] AS l2 RETURN l1 = l2 AS res"

    val result = executeWith(Configs.All + Configs.Morsel, query)

    result.toList should equal(List(Map("res" -> false)))
  }

  test("equality between different nested lists with null should return false") {
    val query = "WITH [[1, 2], [1, 3]] AS l1, [[1, 2], [null, 'foo']] AS l2 RETURN l1 = l2 AS res"

    val result = executeWith(Configs.All + Configs.Morsel, query)

    result.toList should equal(List(Map("res" -> false)))
  }

  test("equality between almost equal nested lists with null should return null") {
    val query = "WITH [[1, 2], ['foo', 'bar']] AS l1, [[1, 2], [null, 'bar']] AS l2 RETURN l1 = l2 AS res"

    val result = executeWith(Configs.All + Configs.Morsel, query, expectedDifferentResults = nullInListConfigOld)

    result.toList should equal(List(Map("res" -> null)))
  }

  // IN with null

  test("IN with different length lists should return false despite nulls") {
    val query = "WITH [1] AS l1, [1, null] AS l2 RETURN l1 IN [l2] AS res"

    val result = executeWith(Configs.All + Configs.Morsel, query)

    result.toList should equal(List(Map("res" -> false)))
  }

  test("IN should return true if match despite nulls") {
    val query = "WITH 3 AS l1, [1, null, 3] AS l2 RETURN l1 IN l2 AS res"

    val result = executeWith(Configs.Interpreted + Configs.Morsel, query)

    result.toList should equal(List(Map("res" -> true)))
  }

  test("IN should return null if comparison with null is required") {
    val query = "WITH 4 AS l1, [1, null, 3] AS l2 RETURN l1 IN l2 AS res"

    val result = executeWith(Configs.Interpreted + Configs.Morsel, query)

    result.toList should equal(List(Map("res" -> null)))
  }

  // IN with null, list version

  test("IN should return true if correct list found despite other lists having nulls") {
    val query = "WITH [1, 2] AS l1, [[null, 'foo'], [1, 2]] AS l2 RETURN l1 IN l2 AS res"

    val result = executeWith(Configs.Interpreted + Configs.Morsel, query)

    result.toList should equal(List(Map("res" -> true)))
  }

  test("IN should return false if no match can be found, despite nulls") {
    val query = "WITH [1,2] AS l1, [[null, 'foo']] AS l2 RETURN l1 IN l2 as res"

    val result = executeWith(Configs.Interpreted + Configs.Morsel, query)

    result.toList should equal(List(Map("res" -> false)))
  }

  test("IN should return null if comparison with null is required, list version") {
    val query = "WITH [1,2] AS l1, [[null, 2]] AS l2 RETURN l1 IN l2 as res"

    val result = executeWith(Configs.Interpreted + Configs.Morsel, query, expectedDifferentResults = nullInListConfigOld)

    result.toList should equal(List(Map("res" -> null)))
  }

  test("IN should return true with previous null match, list version") {
    val query = "WITH [1,2] AS l1, [[null, 2], [1, 2]] AS l2 RETURN l1 IN l2 as res"

    val result = executeWith(Configs.Interpreted + Configs.Morsel, query)

    result.toList should equal(List(Map("res" -> true)))
  }

  test("IN should return null if comparison with null is required, list version 2") {
    val query = "WITH [1,2] AS l1, [[null, 2], [1, 3]] AS l2 RETURN l1 IN l2 as res"

    val result = executeWith(Configs.Interpreted + Configs.Morsel, query, expectedDifferentResults = nullInListConfigOld)

    result.toList should equal(List(Map("res" -> null)))
  }
}
