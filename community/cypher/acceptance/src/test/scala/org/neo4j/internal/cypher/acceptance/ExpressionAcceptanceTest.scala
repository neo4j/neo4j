/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.cypher._

import scala.collection.JavaConverters._

class ExpressionAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  test("IN should work with nested list subscripting") {
    val query = """WITH [[1, 2, 3]] AS list
                  |RETURN 3 IN list[0] AS r
                """.stripMargin

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> true)))
  }

  test("IN should work with nested literal list subscripting") {
    val query = "RETURN 3 IN [[1, 2, 3]][0] AS r"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> true)))
  }

  test("IN should work with list slices") {
    val query = """WITH [1, 2, 3] AS list
                  |RETURN 3 IN list[0..1] AS r
                """.stripMargin

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> false)))
  }

  test("IN should work with literal list slices") {
    val query = "RETURN 3 IN [1, 2, 3][0..1] AS r"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> false)))
  }

  test("accepts property access on type Any") {
    val query = "WITH [{prop: 1}, 1] AS list RETURN (list[0]).prop AS p"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("p" -> 1)))
  }

  test("fails at runtime when doing property access on non-map") {
    val query = "WITH [{prop: 1}, 1] AS list RETURN (list[1]).prop AS p"

    a [CypherTypeException] should be thrownBy {
      executeWithAllPlanners(query)
    }
  }

  test("n[0]") {
    executeScalarWithAllPlannersAndCompatibilityMode[Int]("RETURN [1, 2, 3][0]") should equal(1)
  }

  test("n['name'] in read queries") {
    createNode("Apa")
    executeScalarWithAllPlannersAndCompatibilityMode[String]("MATCH (n {name: 'Apa'}) RETURN n['nam' + 'e']") should equal("Apa")
  }

  test("n['name'] in update queries") {
    executeScalarWithAllPlannersAndCompatibilityMode[String]("CREATE (n {name: 'Apa'}) RETURN n['nam' + 'e']") should equal("Apa")
  }

  test("Uses dynamic property lookup based on parameters when there is no type information") {
    executeScalarWithAllPlannersAndCompatibilityMode[String]("WITH {expr} AS expr, {idx} AS idx RETURN expr[idx]", "expr" -> Map("name" -> "Apa").asJava, "idx" -> "name") should equal("Apa")
  }

  test("Uses dynamic property lookup based on parameters when there is lhs type information") {
    executeScalarWithAllPlannersAndCompatibilityMode[String]("CREATE (n {name: 'Apa'}) RETURN n[{idx}]", "idx" -> "name") should equal("Apa")
  }

  test("Uses dynamic property lookup based on parameters when there is rhs type information") {
    executeScalarWithAllPlannersAndCompatibilityMode[String]("WITH {expr} AS expr, {idx} AS idx RETURN expr[toString(idx)]", "expr" -> Map("name" -> "Apa").asJava, "idx" -> "name") should equal("Apa")
  }

  test("Uses collection lookup based on parameters when there is no type information") {
    executeScalarWithAllPlannersAndCompatibilityMode[String]("WITH {expr} AS expr, {idx} AS idx RETURN expr[idx]", "expr" -> List("Apa").asJava, "idx" -> 0) should equal("Apa")
  }

  test("Uses collection lookup based on parameters when there is lhs type information") {
    executeScalarWithAllPlannersAndCompatibilityMode[String]("WITH ['Apa'] AS expr RETURN expr[{idx}]", "idx" -> 0) should equal("Apa")
  }

  test("Uses collection lookup based on parameters when there is rhs type information") {
    executeScalarWithAllPlannersAndCompatibilityMode[String]("WITH {expr} AS expr, {idx} AS idx RETURN expr[toInt(idx)]", "expr" -> List("Apa").asJava, "idx" -> 0) should equal("Apa")
  }

  test("Fails at runtime when attempting to index with an Int into a Map") {
    a [CypherTypeException] should be thrownBy {
      executeScalarWithAllPlannersAndCompatibilityMode[String]("WITH {expr} AS expr, {idx} AS idx RETURN expr[idx]", "expr" -> Map("name" -> "Apa").asJava, "idx" -> 0)
    }
  }

  test("fails at runtime when trying to index into a map with a non-string") {
    a [CypherTypeException] should be thrownBy {
      executeScalarWithAllPlannersAndCompatibilityMode[Any]("RETURN {expr}[{idx}]", "expr" -> Map("name" -> "Apa").asJava, "idx" -> 12.3)
    }
  }

  test("Fails at runtime when attempting to index with a String into a Collection") {
    a [CypherTypeException] should be thrownBy {
      executeScalarWithAllPlannersAndCompatibilityMode[String]("WITH {expr} AS expr, {idx} AS idx RETURN expr[idx]", "expr" -> List("Apa").asJava, "idx" -> "name")
    }
  }

  test("fails at runtime when trying to index into a map with a non-int") {
    a [CypherTypeException] should be thrownBy {
      executeScalarWithAllPlannersAndCompatibilityMode[Any]("RETURN {expr}[{idx}]", "expr" -> List("Apa").asJava, "idx" -> List("Apa").asJava)
    }
  }

  test("fails at runtime when trying to index into something which is not a map or collection") {
    a [CypherTypeException] should be thrownBy {
      executeScalarWithAllPlannersAndCompatibilityMode[Any]("RETURN {expr}[{idx}]", "expr" -> 1, "idx" -> 12.3)
    }
  }
}
