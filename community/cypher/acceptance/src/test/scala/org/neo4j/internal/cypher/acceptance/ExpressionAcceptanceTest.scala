/*
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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher._

import scala.collection.JavaConverters._

class ExpressionAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

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

  // TCK'd
  test("should fail statically when trying to access an array with a non-integer index") {
    a [SyntaxException] should be thrownBy graph.execute("WITH [1,2,3,4,5] AS array, 3.14 AS idx RETURN array[idx]")
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

  test("should handle map projection with property selectors") {
    createNode("foo" -> 1, "bar" -> "apa")

    executeScalarWithAllPlanners[Any]("MATCH (n) RETURN n{.foo,.bar,.baz}") should equal(
      Map("foo" -> 1, "bar" -> "apa", "baz" -> null))
  }

  test("should handle map projection with property selectors and identifier selector") {
    createNode("foo" -> 1, "bar" -> "apa")

    executeScalarWithAllPlanners[Any]("WITH 42 as x MATCH (n) RETURN n{.foo,.bar,x}") should equal(
      Map("foo" -> 1, "bar" -> "apa", "x" -> 42))
  }

  test("should use the map identifier as the alias for return items") {
    createNode("foo" -> 1, "bar" -> "apa")

    executeWithAllPlanners("MATCH (n) RETURN n{.foo,.bar}").toList should equal(
      List(Map("n" -> Map("foo" -> 1, "bar" -> "apa"))))
  }

  test("map projection with all-properties selector") {
    createNode("foo" -> 1, "bar" -> "apa")

    executeWithAllPlanners("MATCH (n) RETURN n{.*}").toList should equal(
      List(Map("n" -> Map("foo" -> 1, "bar" -> "apa"))))
  }

  test("returning all properties of a node and adds other selectors") {
    createNode("foo" -> 1, "bar" -> "apa")

    executeWithAllPlanners("MATCH (n) RETURN n{.*, .baz}").toList should equal(
      List(Map("n" -> Map("foo" -> 1, "bar" -> "apa", "baz" -> null))))
  }

  test("returning all properties of a node and overwrites some with other selectors") {
    createNode("foo" -> 1, "bar" -> "apa")

    executeWithAllPlanners("MATCH (n) RETURN n{.*, bar:'apatisk'}").toList should equal(
      List(Map("n" -> Map("foo" -> 1, "bar" -> "apatisk"))))
  }

  test("projecting from a null identifier produces a null value") {
    executeWithAllPlanners("OPTIONAL MATCH (n) RETURN n{.foo, .bar}").toList should equal(
      List(Map("n" -> null)))
  }

  test("graph projections with aggregation") {

    val actor = createLabeledNode(Map("name" -> "Actor 1"), "Actor")
    relate(actor, createLabeledNode(Map("title" -> "Movie 1"), "Movie"))
    relate(actor, createLabeledNode(Map("title" -> "Movie 2"), "Movie"))

    executeWithAllPlanners(
      """MATCH (actor:Actor)-->(movie:Movie)
        |RETURN actor{ .name, movies: collect(movie{.title}) }""".stripMargin).toList should equal(
      List(Map("actor" ->
        Map("name" -> "Actor 1", "movies" -> Seq(
          Map("title" -> "Movie 2"),
          Map("title" -> "Movie 1"))))))
  }
}
