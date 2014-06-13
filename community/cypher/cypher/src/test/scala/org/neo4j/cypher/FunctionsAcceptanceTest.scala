/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

class FunctionsAcceptanceTest extends ExecutionEngineFunSuite {

  test("split_should_work_as_expected") {
    // When
    val result = executeScalar[Long](
      "FOREACH (y in split(\"one1two\",\"1\")| "  +
      "  CREATE (x:y)" +
      ") " +
      "WITH * " +
      "MATCH (n) " +
      "RETURN count(n)"
    )

    // Then
    result should equal(2)
  }

  test("toInt_should_work_as_expected") {
    // When
    val result = executeScalar[Long](
      "CREATE (p:Person { age: \"42\" })" +
      "WITH * " +
      "MATCH (n) " +
      "RETURN toInt(n.age)"
    )

    // Then
    result should equal(42)
  }

  test("toFloat_should_work_as_expected") {
    // When
    val result = executeScalar[Double](
      "CREATE (m:Movie { rating: 4 })" +
        "WITH * " +
        "MATCH (n) " +
        "RETURN toFloat(n.rating)"
    )

    // Then
    result should equal(4.0)
  }
}
