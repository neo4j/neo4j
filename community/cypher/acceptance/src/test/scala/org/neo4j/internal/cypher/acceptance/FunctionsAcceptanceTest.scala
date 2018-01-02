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

import org.neo4j.cypher.{NewPlannerTestSupport, ExecutionEngineFunSuite}

class FunctionsAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("split should work as expected") {
    // When
    val result = executeScalarWithAllPlanners[Long](
      "UNWIND split(\"one1two\",\"1\") AS item RETURN count(item)"
    )

    // Then
    result should equal(2)
  }

  test("toInt should work as expected") {
    // Given
    createLabeledNode(Map("age" -> "42"), "Person")

    // When
    val result = executeScalarWithAllPlanners[Long](
      "MATCH (p:Person { age: \"42\" }) WITH * MATCH (n) RETURN toInt(n.age)"
    )

    // Then
    result should equal(42)
  }

  test("toFloat should work as expected") {
    // Given
    createLabeledNode(Map("rating" -> 4), "Movie")

    // When
    val result = executeScalarWithAllPlanners[Double](
      "MATCH (m:Movie { rating: 4 }) WITH * MATCH (n) RETURN toFloat(n.rating)"
    )

    // Then
    result should equal(4.0)
  }

  test("toString should work as expected") {
    // Given
    createLabeledNode(Map("rating" -> 4), "Movie")

    // When
    val result = executeScalarWithAllPlanners[String](
      "MATCH (m:Movie { rating: 4 }) WITH * MATCH (n) RETURN toString(n.rating)"
    )

    // Then
    result should equal("4")
  }

  test("case should handle mixed number types") {
    val query =
      """WITH 0.5 AS x
        |WITH (CASE WHEN x < 1 THEN 1 ELSE 2.0 END) AS x
        |RETURN x + 1
      """.stripMargin

    val result = executeScalarWithAllPlanners[Long](query)

    result should equal(2)
  }

  test("case should handle mixed types") {
    val query =
      """WITH 0.5 AS x
        |WITH (CASE WHEN x < 1 THEN "wow" ELSE true END) AS x
        |RETURN x + "!"
      """.stripMargin

    val result = executeScalarWithAllPlanners[String](query)

    result should equal("wow!")
  }

  test("reverse function should work as expected") {
    // When
    val result = executeScalarWithAllPlanners[String]("RETURN reverse('raksO')")

    // Then
    result should equal("Oskar")
  }

  test("exists should work with dynamic property look up") {
    val node = createLabeledNode(Map("prop" -> "foo"), "Person")
    createLabeledNode("Person")

    val result = executeWithAllPlanners("MATCH (n:Person) WHERE exists(n['prop']) RETURN n")
    result.toList should equal(List(Map("n" -> node)))
  }

  test("EXISTS should work with maps") {
    // GIVEN
    val query = "WITH {name: 'Mats', name2: 'Pontus'} AS map RETURN exists(map.name)"

    // WHEN
    val result = executeScalarWithAllPlanners[Boolean](query)

    // THEN
    result shouldBe true
  }

  test("EXISTS should work with null in maps") {
    // GIVEN
    val query = "WITH {name: null} AS map RETURN exists(map.name)"

    // WHEN
    val result = executeScalarWithAllPlanners[Boolean](query)

    // THEN
    result shouldBe false
  }

  test("EXISTS should work when key is missing") {
    // GIVEN
    val query = "WITH {name: null} AS map RETURN exists(map.nonExistantKey)"

    // WHEN
    val result = executeScalarWithAllPlanners[Boolean](query)

    // THEN
    result shouldBe false
  }

  test("IS NOT NULL should work with maps") {
    // GIVEN
    val query = "WITH {name: 'Mats', name2: 'Pontus'} AS map RETURN map.name IS NOT NULL"

    // WHEN
    val result = executeScalarWithAllPlanners[Boolean](query)

    // THEN
    result shouldBe true
  }

  test("IS NOT NULL should work with null in maps") {
    // GIVEN
    val query = "WITH {name: null} AS map RETURN map.name IS NOT NULL"

    // WHEN
    val result = executeScalarWithAllPlanners[Boolean](query)

    // THEN
    result shouldBe false
  }

  test("IS NOT NULL should work when key is missing") {
    // GIVEN
    val query = "WITH {name: null} AS map RETURN map.nonExistantKey IS NOT NULL"

    // WHEN
    val result = executeScalarWithAllPlanners[Boolean](query)

    // THEN
    result shouldBe false
  }

  test("HAS should work with maps") {
    // GIVEN
    val query = "WITH {name: 'Mats', name2: 'Pontus'} AS map RETURN has(map.name2)"

    // WHEN
    val result = executeScalarWithAllPlanners[Boolean](query)

    // THEN
    result shouldBe true
  }

  test("HAS should work with null in maps") {
    // GIVEN
    val query = "WITH {name: null} AS map RETURN has(map.name)"

    // WHEN
    val result = executeScalarWithAllPlanners[Boolean](query)

    // THEN
    result shouldBe false
  }

  test("HAS should work when key is missing") {
    // GIVEN
    val query = "WITH {name: null} AS map RETURN has(map.nonExistantKey)"

    // WHEN
    val result = executeScalarWithAllPlanners[Boolean](query)

    // THEN
    result shouldBe false
  }

  test("has should work with dynamic property look up") {
    val node = createLabeledNode(Map("prop" -> "foo"), "Person")
    createLabeledNode("Person")

    val result = executeWithAllPlanners("MATCH (n:Person) WHERE has(n['prop']) RETURN n")
    result.toList should equal(List(Map("n" -> node)))
  }



}
