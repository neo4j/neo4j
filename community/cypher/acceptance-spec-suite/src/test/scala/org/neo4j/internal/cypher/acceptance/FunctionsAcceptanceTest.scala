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

import org.neo4j.cypher.internal.compiler.v3_1.{CRS, CartesianPoint, GeographicPoint}
import org.neo4j.cypher.{ExecutionEngineFunSuite, InvalidArgumentException, NewPlannerTestSupport, SyntaxException}

class FunctionsAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  // TCK'd
  test("split should work as expected") {
    // When
    val result = executeScalarWithAllPlannersAndCompatibilityMode[Long](
      "UNWIND split(\"one1two\",\"1\") AS item RETURN count(item)"
    )

    // Then
    result should equal(2)
  }

  // TCK'd
  test("toInt should work as expected") {
    // Given
    createLabeledNode(Map("age" -> "42"), "Person")

    // When
    val result = executeScalarWithAllPlannersAndCompatibilityMode[Long](
      "MATCH (p:Person { age: \"42\" }) WITH * MATCH (n) RETURN toInt(n.age)"
    )

    // Then
    result should equal(42)
  }

  // TCK'd
  test("toInt should work on float") {
    // When
    val result = executeScalarWithAllPlannersAndCompatibilityMode[Long](
      "WITH 82.9 as weight RETURN toInt(weight)"
    )

    // Then
    result should equal(82)
  }

  // TCK'd
  test("toInt should return null on string that is not a number") {
    // When
    val result = executeWithAllPlannersAndCompatibilityMode(
      "WITH 'foo' as foo_string, '' as empty_string RETURN toInt(foo_string) as foo, toInt(empty_string) as empty"
    )

    // Then
    result.toList should equal(List(Map("foo" -> null, "empty" -> null)))
  }

  // TCK'd
  test("toInt should handle mixed number types") {
    // When
    val result = executeWithAllPlanners("WITH [2, 2.9] AS numbers RETURN [n in numbers | toInt(n)] AS int_numbers")

    // Then
    result.toList should equal(List(Map("int_numbers" -> List(2, 2))))
  }

  // TCK'd
  test("toInt should work on string collection") {
    // When
    val result = executeWithAllPlannersAndCompatibilityMode("WITH ['2', '2.9', 'foo'] AS numbers RETURN [n in numbers | toInt(n)] AS int_numbers")

    // Then
    result.toList should equal(List(Map("int_numbers" -> List(2, 2, null))))
  }

  // TCK'd
  test("toFloat should work as expected") {
    // Given
    createLabeledNode(Map("rating" -> 4), "Movie")

    // When
    val result = executeScalarWithAllPlannersAndCompatibilityMode[Double](
      "MATCH (m:Movie { rating: 4 }) WITH * MATCH (n) RETURN toFloat(n.rating)"
    )

    // Then
    result should equal(4.0)
  }

  // TCK'd
  test("toFloat should handle mixed number types") {
    // When
    val result = executeWithAllPlanners("WITH [3.4, 3] AS numbers RETURN [n in numbers | toFloat(n)] AS float_numbers")

    // Then
    result.toList should equal(List(Map("float_numbers" -> List(3.4, 3.0))))
  }

  // TCK'd
  test("toFloat should return null on string that is not a number") {
    // When
    val result = executeWithAllPlannersAndCompatibilityMode(
      "WITH 'foo' as foo_string, '' as empty_string RETURN toFloat(foo_string) as foo, toFloat(empty_string) as empty"
    )

    // Then
    result.toList should equal(List(Map("foo" -> null, "empty" -> null)))
  }

  // TCK'd
  test("toFloat should work on string collection") {
    // When
    val result = executeWithAllPlannersAndCompatibilityMode("WITH ['1', '2', 'foo'] AS numbers RETURN [n in numbers | toFloat(n)] AS float_numbers")

    // Then
    result.toList should equal(List(Map("float_numbers" -> List(1.0, 2.0, null))))
  }

  // TCK'd
  test("toString should work as expected") {
    // Given
    createLabeledNode(Map("rating" -> 4), "Movie")

    // When
    val result = executeScalarWithAllPlannersAndCompatibilityMode[String](
      "MATCH (m:Movie { rating: 4 }) WITH * MATCH (n) RETURN toString(n.rating)"
    )

    // Then
    result should equal("4")
  }

  // TCK'd
  test("toString should handle booleans from properties") {
    // Given
    createLabeledNode(Map("watched" -> true), "Movie")

    // When
    val result = executeScalarWithAllPlanners[String](
      "MATCH (m:Movie) RETURN toString(m.watched)"
    )

    // Then
    result should equal("true")
  }

  // TCK'd
  test("toString should handle booleans as inlined input") {
    // Given
    val query = "RETURN toString(1 < 0)"

    // When
    val result = executeScalarWithAllPlanners[String](query)

    // Then
    result should equal("false")
  }

  // TCK'd
  test("toString should handle booleans directly") {
    // Given
    val query = "RETURN toString(true)"

    // When
    val result = executeScalarWithAllPlanners[String](query)

    // Then
    result should equal("true")
  }

  // TCK'd
  test("toString should work on an integer collection") {
    // When
    val result = executeWithAllPlannersAndCompatibilityMode("WITH [1, 2, 3] AS numbers RETURN [n in numbers | toString(n)] AS string_numbers")

    // Then
    result.toList should equal(List(Map("string_numbers" -> List("1", "2", "3"))))
  }

  // TCK'd
  test("properties should work on nodes") {

    // When
    val result = executeScalarWithAllPlanners[Map[String,Any]](
      "CREATE (n:Person {name: 'Popeye', level: 9001}) RETURN properties(n) AS m"
    )

    // Then
    result should equal(Map("name" -> "Popeye", "level" -> 9001))
  }

  // TCK'd
  test("properties should work on relationships") {

    // When
    val result = executeScalarWithAllPlanners[Map[String,Any]](
      "CREATE ()-[r:SWOOSHES {name: 'Popeye', level: 9001}]->() RETURN properties(r) AS m"
    )

    // Then
    result should equal(Map("name" -> "Popeye", "level" -> 9001))
  }

  // TCK'd
  test("properties should work on maps") {

    // When
    val result = executeScalarWithAllPlanners[Map[String,Any]](
      "RETURN properties({name: 'Popeye', level: 9001})"
    )

    // Then
    result should equal(Map("name" -> "Popeye", "level" -> 9001))
  }

  // TCK'd
  test("properties should fail when called with an INTEGER argument") {
    a[SyntaxException] shouldBe thrownBy {
      executeScalarWithAllPlanners[Map[String, Any]](
        "RETURN properties(1)"
      )
    }
  }

  // TCK'd
  test("properties should fail when called with a STRING argument") {
    a[SyntaxException] shouldBe thrownBy {
      executeScalarWithAllPlanners[Map[String, Any]](
        "RETURN properties('Hallo')"
      )
    }
  }

  // TCK'd
  test("properties should fail when called with a LIST OF BOOLEAN argument") {
    a[SyntaxException] shouldBe thrownBy {
      executeScalarWithAllPlanners[Map[String, Any]](
        "RETURN properties([true, false])"
      )
    }
  }

  // TCK'd
  test("properties(null) should be null") {

    // When
    val result = executeScalarWithAllPlanners[Map[String,Any]]("RETURN properties(null)")

    // Then
    result should be(null)
  }

  // TCK'd
  test("reverse function should work as expected") {
    // When
    val result = executeScalarWithAllPlannersAndCompatibilityMode[String]("RETURN reverse('raksO')")

    // Then
    result should equal("Oskar")
  }

  // TCK'd
  test("exists should work with dynamic property look up") {
    val node = createLabeledNode(Map("prop" -> "foo"), "Person")
    createLabeledNode("Person")

    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (n:Person) WHERE exists(n['prop']) RETURN n")
    result.toList should equal(List(Map("n" -> node)))
  }

  // TCK'd
  test("EXISTS should work with maps") {
    // GIVEN
    val query = "WITH {name: 'Mats', name2: 'Pontus'} AS map RETURN exists(map.name)"

    // WHEN
    val result = executeScalarWithAllPlannersAndCompatibilityMode[Boolean](query)

    // THEN
    result shouldBe true
  }

  // TCK'd
  test("EXISTS should work with null in maps") {
    // GIVEN
    val query = "WITH {name: null} AS map RETURN exists(map.name)"

    // WHEN
    val result = executeScalarWithAllPlannersAndCompatibilityMode[Boolean](query)

    // THEN
    result shouldBe false
  }

  // TCK'd
  test("EXISTS should work when key is missing") {
    // GIVEN
    val query = "WITH {name: null} AS map RETURN exists(map.nonExistentKey)"

    // WHEN
    val result = executeScalarWithAllPlannersAndCompatibilityMode[Boolean](query)

    // THEN
    result shouldBe false
  }

  // TCK'd
  test("IS NOT NULL should work with maps") {
    // GIVEN
    val query = "WITH {name: 'Mats', name2: 'Pontus'} AS map RETURN map.name IS NOT NULL"

    // WHEN
    val result = executeScalarWithAllPlannersAndCompatibilityMode[Boolean](query)

    // THEN
    result shouldBe true
  }

  // TCK'd
  test("IS NOT NULL should work with null in maps") {
    // GIVEN
    val query = "WITH {name: null} AS map RETURN map.name IS NOT NULL"

    // WHEN
    val result = executeScalarWithAllPlannersAndCompatibilityMode[Boolean](query)

    // THEN
    result shouldBe false
  }

  // TCK'd
  test("IS NOT NULL should work when key is missing") {
    // GIVEN
    val query = "WITH {name: null} AS map RETURN map.nonExistantKey IS NOT NULL"

    // WHEN
    val result = executeScalarWithAllPlannersAndCompatibilityMode[Boolean](query)

    // THEN
    result shouldBe false
  }

  // TCK'd
  test("percentileDisc should work in the valid range") {
    createNode("prop" -> 10.0)
    createNode("prop" -> 20.0)
    createNode("prop" -> 30.0)

    executeScalarWithAllPlannersAndCompatibilityMode[Double]("MATCH (n) RETURN percentileDisc(n.prop, 0.0)") should equal (10.0 +- 0.1)
    executeScalarWithAllPlannersAndCompatibilityMode[Double]("MATCH (n) RETURN percentileDisc(n.prop, 0.5)") should equal (20.0 +- 0.1)
    executeScalarWithAllPlannersAndCompatibilityMode[Double]("MATCH (n) RETURN percentileDisc(n.prop, 1.0)") should equal (30.0 +- 0.1)
  }

  // TCK'd
  test("percentileCont should work in the valid range") {
    createNode("prop" -> 10.0)
    createNode("prop" -> 20.0)
    createNode("prop" -> 30.0)

    executeScalarWithAllPlannersAndCompatibilityMode[Double]("MATCH (n) RETURN percentileCont(n.prop, 0)") should equal (10.0 +- 0.1)
    executeScalarWithAllPlannersAndCompatibilityMode[Double]("MATCH (n) RETURN percentileCont(n.prop, 0.5)") should equal (20.0 +- 0.1)
    executeScalarWithAllPlannersAndCompatibilityMode[Double]("MATCH (n) RETURN percentileCont(n.prop, 1)") should equal (30.0 +- 0.1)
  }

  // TCK'd
  test("percentileCont should fail nicely at runtime") {
    createNode("prop" -> 10.0)
    an [InvalidArgumentException] shouldBe thrownBy(
      executeWithAllPlanners("MATCH (n) RETURN percentileCont(n.prop, {param})", "param" -> 1000)
    )
  }

  // TCK'd
  test("percentileDisc should fail nicely at runtime") {
    createNode("prop" -> 10.0)
    an [InvalidArgumentException] shouldBe thrownBy(
      executeWithAllPlanners("MATCH (n) RETURN percentileCont(n.prop, {param})", "param" -> 1000)
    )
  }

  // TCK'd
  test("percentileDisc should fail nicely on more involved query") {
    for (i <- 1 to 10) {
      val node = createLabeledNode("START")
      (1 to i).foreach(_ => relate(node, createNode()))
    }

    an [InvalidArgumentException] shouldBe thrownBy {
      // mixing up the argument order
      executeWithAllPlanners("""MATCH (n:START)
                               |WITH n, size( (n)-->() ) AS deg
                               |WHERE deg > 2
                               |WITH deg
                               |LIMIT 100
                               |RETURN percentileDisc(0.90, deg), deg
                               """.stripMargin)
    }
  }

  test("id on a node should work in both runtimes")  {
    // GIVEN
    val expected = createNode().getId

    // WHEN
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n) RETURN id(n)")

    // THEN
    result.toList should equal(List(Map("id(n)" -> expected)))

  }

  test("id on a rel should work in both runtimes")  {
    // GIVEN
    val expected = relate(createNode(), createNode()).getId

    // WHEN
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH ()-[r]->() RETURN id(r)")

    // THEN
    result.toList should equal(List(Map("id(r)" -> expected)))
  }

  // TCK'd
  test("type should work in both runtimes")  {
    // GIVEN
    relate(createNode(), createNode(), "T")

    // WHEN
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH ()-[r]->() RETURN type(r)")

    // THEN
    result.toList should equal(List(Map("type(r)" -> "T")))
  }

  // TCK'd
  test("nested type should work in both runtimes")  {
    val intermediate = createNode()
    // GIVEN
    relate(createNode(), intermediate, "T1")
    relate(intermediate, createNode(), "T2")

    // WHEN
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH ()-[r1]->()-[r2]->() RETURN type(r1), type(r2)")

    // THEN
    result.toList should equal(List(Map("type(r1)" -> "T1", "type(r2)" -> "T2")))
  }

  // TCK'd
  test("type should handle optional when null")  {
    // GIVEN
    createNode()

    // WHEN
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (a) OPTIONAL MATCH (a)-[r:NOT_THERE]->() RETURN type(r)")

    // THEN
    result.toList should equal(List(Map("type(r)" -> null)))
  }

  // TCK'd
  test("type should handle optional when both null and match")  {
    // GIVEN
    relate(createNode(), createNode(), "T")

    // WHEN
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (a) OPTIONAL MATCH (a)-[r:T]->() RETURN type(r)")

    // THEN
    result.toList should equal(List(Map("type(r)" -> "T"), Map("type(r)" -> null)))
  }

  // TCK'd
  test("type should fail nicely when not given a relationship")  {
    // GIVEN
    relate(createNode(), createNode(), "T")

    val query = "MATCH (a)-[r]->() WITH [r, 1] as coll RETURN [x in coll | type(x) ]"

    //Expect
    a [SyntaxException] shouldBe thrownBy(eengine.execute(s"CYPHER runtime=compiled $query", Map.empty[String,Any], graph.session()))
    a [SyntaxException] shouldBe thrownBy(eengine.execute(s"CYPHER runtime=interpreted $query", Map.empty[String,Any], graph.session()))
    a [SyntaxException] shouldBe thrownBy(eengine.execute(s"CYPHER planner=cost $query", Map.empty[String,Any], graph.session()))
    a [SyntaxException] shouldBe thrownBy(eengine.execute(s"CYPHER planner=rule $query", Map.empty[String,Any], graph.session()))
  }

  // TCK'd
  test("should handle a list of values that individually are OK") {
    //"match (n) return toString(n) "
    val result = executeWithAllPlanners("RETURN [x in [1, 2.3, true, 'apa' ] | toString(x) ] as col")
    result.toList should equal(List(Map("col"-> Seq("1", "2.3", "true", "apa"))))
  }
}
