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

import org.neo4j.cypher.internal.compiler.v3_0.commands.expressions.{CRS, CartesianPoint, GeographicPoint}
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport, SyntaxException}

class FunctionsAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("split should work as expected") {
    // When
    val result = executeScalarWithAllPlannersAndCompatibilityMode[Long](
      "UNWIND split(\"one1two\",\"1\") AS item RETURN count(item)"
    )

    // Then
    result should equal(2)
  }

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

  test("toInt should work on float") {
    // When
    val result = executeScalarWithAllPlannersAndCompatibilityMode[Long](
      "WITH 82.9 as weight RETURN toInt(weight)"
    )

    // Then
    result should equal(82)
  }

  test("toInt should return null on string that is not a number") {
    // When
    val result = executeWithAllPlannersAndCompatibilityMode(
      "WITH 'foo' as foo_string, '' as empty_string RETURN toInt(foo_string) as foo, toInt(empty_string) as empty"
    )

    // Then
    result.toList should equal(List(Map("foo" -> null, "empty" -> null)))
  }

  test("toInt should handle mixed number types") {
    // When
    val result = executeWithAllPlanners("WITH [2, 2.9] AS numbers RETURN [n in numbers | toInt(n)] AS int_numbers")

    // Then
    result.toList should equal(List(Map("int_numbers" -> List(2, 2))))
  }

  test("toInt should fail on type Any") {
    // When
    val query = "WITH [2, 2.9, '1.7'] AS numbers RETURN [n in numbers | toInt(n)] AS int_numbers"
    val error = intercept[SyntaxException](executeWithAllPlannersAndCompatibilityMode(query))

    // Then
    error.getMessage should (include("Type mismatch: expected Float, Integer, Number or String but was Any")
      or include("Type mismatch: expected Float, Integer or String but was Any") )
  }

  test("toInt should work on string collection") {
    // When
    val result = executeWithAllPlannersAndCompatibilityMode("WITH ['2', '2.9', 'foo'] AS numbers RETURN [n in numbers | toInt(n)] AS int_numbers")

    // Then
    result.toList should equal(List(Map("int_numbers" -> List(2, 2, null))))
  }

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

  test("toFloat should handle mixed number types") {
    // When
    val result = executeWithAllPlanners("WITH [3.4, 3] AS numbers RETURN [n in numbers | toFloat(n)] AS float_numbers")

    // Then
    result.toList should equal(List(Map("float_numbers" -> List(3.4, 3.0))))
  }

  test("toFloat should return null on string that is not a number") {
    // When
    val result = executeWithAllPlannersAndCompatibilityMode(
      "WITH 'foo' as foo_string, '' as empty_string RETURN toFloat(foo_string) as foo, toFloat(empty_string) as empty"
    )

    // Then
    result.toList should equal(List(Map("foo" -> null, "empty" -> null)))
  }

  test("toFloat should fail on type Any") {
    // When
    val query = "WITH [3.4, 3, '5'] AS numbers RETURN [n in numbers | toFloat(n)] AS float_numbers"
    val error = intercept[SyntaxException](executeWithAllPlannersAndCompatibilityMode(query))

    // Then
    error.getMessage should (include("Type mismatch: expected Float, Integer, Number or String but was Any")
      or include("Type mismatch: expected Float, Integer or String but was Any") )
  }

  test("toFloat should work on string collection") {
    // When
    val result = executeWithAllPlannersAndCompatibilityMode("WITH ['1', '2', 'foo'] AS numbers RETURN [n in numbers | toFloat(n)] AS float_numbers")

    // Then
    result.toList should equal(List(Map("float_numbers" -> List(1.0, 2.0, null))))
  }

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

  test("toString should handle booleans as inlined input") {
    // Given
    val query = "RETURN toString(1 < 0)"

    // When
    val result = executeScalarWithAllPlanners[String](query)

    // Then
    result should equal("false")
  }

  test("toString should handle booleans directly") {
    // Given
    val query = "RETURN toString(true)"

    // When
    val result = executeScalarWithAllPlanners[String](query)

    // Then
    result should equal("true")
  }

  test("toString should fail on type Any") {
    // When
    val query = "WITH [2, 2.9, '1.7'] AS numbers RETURN [n in numbers | toString(n)] AS int_numbers"
    val error = intercept[SyntaxException](executeWithAllPlannersAndCompatibilityMode(query))

    // Then
    error.getMessage should (include("Type mismatch: expected Boolean, Float, Integer or String but was Any")
      or include("Type mismatch: expected Float, Integer or String but was Any") )
  }

  test("toString should work on an integer collection") {
    // When
    val result = executeWithAllPlannersAndCompatibilityMode("WITH [1, 2, 3] AS numbers RETURN [n in numbers | toString(n)] AS string_numbers")

    // Then
    result.toList should equal(List(Map("string_numbers" -> List("1", "2", "3"))))
  }

  test("properties should work on nodes") {

    // When
    val result = executeScalarWithAllPlanners[Map[String,Any]](
      "CREATE (n:Person {name: 'Popeye', level: 9001}) RETURN properties(n) AS m"
    )

    // Then
    result should equal(Map("name" -> "Popeye", "level" -> 9001))
  }

  test("properties should work on relationships") {

    // When
    val result = executeScalarWithAllPlanners[Map[String,Any]](
      "CREATE ()-[r:SWOOSHES {name: 'Popeye', level: 9001}]->() RETURN properties(r) AS m"
    )

    // Then
    result should equal(Map("name" -> "Popeye", "level" -> 9001))
  }

  test("properties should work on maps") {

    // When
    val result = executeScalarWithAllPlanners[Map[String,Any]](
      "RETURN properties({name: 'Popeye', level: 9001})"
    )

    // Then
    result should equal(Map("name" -> "Popeye", "level" -> 9001))
  }

  test("properties should fail when called with an INTEGER argument") {
    a[SyntaxException] shouldBe thrownBy {
      executeScalarWithAllPlanners[Map[String, Any]](
        "RETURN properties(1)"
      )
    }
  }

  test("properties should fail when called with a STRING argument") {
    a[SyntaxException] shouldBe thrownBy {
      executeScalarWithAllPlanners[Map[String, Any]](
        "RETURN properties('Hallo')"
      )
    }
  }

  test("properties should fail when called with a LIST OF BOOLEAN argument") {
    a[SyntaxException] shouldBe thrownBy {
      executeScalarWithAllPlanners[Map[String, Any]](
        "RETURN properties([true, false])"
      )
    }
  }

  test("properties(null) should be null") {

    // When
    val result = executeScalarWithAllPlanners[Map[String,Any]]("RETURN properties(null)")

    // Then
    result should be(null)
  }

  test("case should handle mixed number types") {
    val query =
      """WITH 0.5 AS x
        |WITH (CASE WHEN x < 1 THEN 1 ELSE 2.0 END) AS x
        |RETURN x + 1
      """.stripMargin

    val result = executeScalarWithAllPlannersAndCompatibilityMode[Long](query)

    result should equal(2)
  }

  test("case should handle mixed types") {
    val query =
      """WITH 0.5 AS x
        |WITH (CASE WHEN x < 1 THEN "wow" ELSE true END) AS x
        |RETURN x + "!"
      """.stripMargin

    val result = executeScalarWithAllPlannersAndCompatibilityMode[String](query)

    result should equal("wow!")
  }

  test("reverse function should work as expected") {
    // When
    val result = executeScalarWithAllPlannersAndCompatibilityMode[String]("RETURN reverse('raksO')")

    // Then
    result should equal("Oskar")
  }

  test("exists should work with dynamic property look up") {
    val node = createLabeledNode(Map("prop" -> "foo"), "Person")
    createLabeledNode("Person")

    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (n:Person) WHERE exists(n['prop']) RETURN n")
    result.toList should equal(List(Map("n" -> node)))
  }

  test("EXISTS should work with maps") {
    // GIVEN
    val query = "WITH {name: 'Mats', name2: 'Pontus'} AS map RETURN exists(map.name)"

    // WHEN
    val result = executeScalarWithAllPlannersAndCompatibilityMode[Boolean](query)

    // THEN
    result shouldBe true
  }

  test("EXISTS should work with null in maps") {
    // GIVEN
    val query = "WITH {name: null} AS map RETURN exists(map.name)"

    // WHEN
    val result = executeScalarWithAllPlannersAndCompatibilityMode[Boolean](query)

    // THEN
    result shouldBe false
  }

  test("EXISTS should work when key is missing") {
    // GIVEN
    val query = "WITH {name: null} AS map RETURN exists(map.nonExistantKey)"

    // WHEN
    val result = executeScalarWithAllPlannersAndCompatibilityMode[Boolean](query)

    // THEN
    result shouldBe false
  }

  test("IS NOT NULL should work with maps") {
    // GIVEN
    val query = "WITH {name: 'Mats', name2: 'Pontus'} AS map RETURN map.name IS NOT NULL"

    // WHEN
    val result = executeScalarWithAllPlannersAndCompatibilityMode[Boolean](query)

    // THEN
    result shouldBe true
  }

  test("IS NOT NULL should work with null in maps") {
    // GIVEN
    val query = "WITH {name: null} AS map RETURN map.name IS NOT NULL"

    // WHEN
    val result = executeScalarWithAllPlannersAndCompatibilityMode[Boolean](query)

    // THEN
    result shouldBe false
  }

  test("IS NOT NULL should work when key is missing") {
    // GIVEN
    val query = "WITH {name: null} AS map RETURN map.nonExistantKey IS NOT NULL"

    // WHEN
    val result = executeScalarWithAllPlannersAndCompatibilityMode[Boolean](query)

    // THEN
    result shouldBe false
  }

  test("percentileDisc should work in the valid range") {
    createNode("prop" -> 10.0)
    createNode("prop" -> 20.0)
    createNode("prop" -> 30.0)

    executeScalarWithAllPlannersAndCompatibilityMode[Double]("MATCH (n) RETURN percentileDisc(n.prop, 0.0)") should equal (10.0 +- 0.1)
    executeScalarWithAllPlannersAndCompatibilityMode[Double]("MATCH (n) RETURN percentileDisc(n.prop, 0.5)") should equal (20.0 +- 0.1)
    executeScalarWithAllPlannersAndCompatibilityMode[Double]("MATCH (n) RETURN percentileDisc(n.prop, 1.0)") should equal (30.0 +- 0.1)
  }

  test("percentileCont should work in the valid range") {
    createNode("prop" -> 10.0)
    createNode("prop" -> 20.0)
    createNode("prop" -> 30.0)

    executeScalarWithAllPlannersAndCompatibilityMode[Double]("MATCH (n) RETURN percentileCont(n.prop, 0)") should equal (10.0 +- 0.1)
    executeScalarWithAllPlannersAndCompatibilityMode[Double]("MATCH (n) RETURN percentileCont(n.prop, 0.5)") should equal (20.0 +- 0.1)
    executeScalarWithAllPlannersAndCompatibilityMode[Double]("MATCH (n) RETURN percentileCont(n.prop, 1)") should equal (30.0 +- 0.1)
  }

  test("point function should work with literal map") {
    val result = executeWithAllPlanners("RETURN point({latitude: 12.78, longitude: 56.7}) as point")
    result should useProjectionWith("Point")
    result.toList should equal(List(Map("point" -> GeographicPoint(56.7, 12.78, CRS.WGS84))))
  }

  test("point function should work with literal map and cartesian coordinates") {
    val result = executeWithAllPlanners("RETURN point({x: 2.3, y: 4.5, crs: 'cartesian'}) as point")
    result should useProjectionWith("Point")
    result.toList should equal(List(Map("point" -> CartesianPoint(2.3, 4.5))))
  }

  test("point function should work with previous map") {
    val result = executeWithAllPlanners("WITH {latitude: 12.78, longitude: 56.7} as data RETURN point(data) as point")
    result should useProjectionWith("Point")
    result.toList should equal(List(Map("point" -> GeographicPoint(56.7, 12.78, CRS.WGS84))))
  }

  test("distance function should work on co-located points") {
    val result = executeWithAllPlanners("WITH point({latitude: 12.78, longitude: 56.7}) as point RETURN distance(point,point) as dist")
    result should useProjectionWith("Point", "Distance")
    result.toList should equal(List(Map("dist" -> 0.0)))
  }

  test("distance function should work on nearby cartesian points") {
    val result = executeWithAllPlanners(
      """
        |WITH point({x: 2.3, y: 4.5, crs: 'cartesian'}) as p1, point({x: 1.1, y: 5.4, crs: 'cartesian'}) as p2
        |RETURN distance(p1,p2) as dist
      """.stripMargin)
    result should useProjectionWith("Point", "Distance")
    result.columnAs("dist").next().asInstanceOf[Double] should equal(1.5)
  }

  test("distance function should work on nearby points") {
    val result = executeWithAllPlanners(
      """
        |WITH point({longitude: 12.78, latitude: 56.7}) as p1, point({latitude: 56.71, longitude: 12.79}) as p2
        |RETURN distance(p1,p2) as dist
      """.stripMargin)
    result should useProjectionWith("Point", "Distance")
    Math.round(result.columnAs("dist").next().asInstanceOf[Double]) should equal(1270)
  }

  test("distance function should work on distant points") {
    val result = executeWithAllPlanners(
      """
        |WITH point({latitude: 56.7, longitude: 12.78}) as p1, point({longitude: -51.9, latitude: -16.7}) as p2
        |RETURN distance(p1,p2) as dist
      """.stripMargin)
    result should useProjectionWith("Point", "Distance")
    Math.round(result.columnAs("dist").next().asInstanceOf[Double]) should equal(10116214)
  }

  test("distance function should measure distance from Copenhagen train station to Neo4j in Malmö") {
    val result = executeWithAllPlanners(
      """
        |WITH point({latitude: 55.672874, longitude: 12.564590}) as p1, point({latitude: 55.611784, longitude: 12.994341}) as p2
        |RETURN distance(p1,p2) as dist
      """.stripMargin)
    result should useProjectionWith("Point", "Distance")
    Math.round(result.columnAs("dist").next().asInstanceOf[Double]) should equal(27842)
  }

  test("distance function should work with two null inputs") {
    val result = executeWithAllPlanners("RETURN distance(null, null) as dist")
    result.toList should equal(List(Map("dist" -> null)))
  }

  test("distance function should return null with lhs null input") {
    val result = executeWithAllPlanners("""WITH point({latitude: 55.672874, longitude: 12.564590}) as p1
                                          |RETURN distance(null, p1) as dist""".stripMargin)
    result.toList should equal(List(Map("dist" -> null)))
  }

  test("distance function should return null with rhs null input") {
    val result = executeWithAllPlanners("""WITH point({latitude: 55.672874, longitude: 12.564590}) as p1
                                          |RETURN distance(p1, null) as dist""".stripMargin)
    result.toList should equal(List(Map("dist" -> null)))
  }

  test("distance function should fail on wrong type") {
    val error = intercept[SyntaxException](executeWithAllPlanners("RETURN distance(1, 2) as dist"))
    assert(error.getMessage.contains("Type mismatch: expected Point but was Integer"))
  }

  test("point function should work with node properties") {
    // Given
    createLabeledNode(Map("latitude" -> 12.78, "longitude" -> 56.7), "Place")

    // When
    val result = executeWithAllPlanners("MATCH (p:Place) RETURN point({latitude: p.latitude, longitude: p.longitude}) as point")

    // Then
    result should useProjectionWith("Point")
    result.toList should equal(List(Map("point" -> GeographicPoint(56.7, 12.78, CRS.WGS84))))
  }

  test("point function should work with relationship properties") {
    // Given
    val r = relate(createNode(), createNode(), "PASS_THROUGH", Map("latitude" -> 12.78, "longitude" -> 56.7))

    // When
    val result = executeWithAllPlanners("MATCH ()-[r:PASS_THROUGH]->() RETURN point({latitude: r.latitude, longitude: r.longitude}) as point")

    // Then
    result should useProjectionWith("Point")
    result.toList should equal(List(Map("point" -> GeographicPoint(56.7, 12.78, CRS.WGS84))))
  }

  test("point function should work with node as map") {
    // Given
    createLabeledNode(Map("latitude" -> 12.78, "longitude" -> 56.7), "Place")

    // When
    val result = executeWithAllPlanners("MATCH (p:Place) RETURN point(p) as point")

    // Then
    result should useProjectionWith("Point")
    result.toList should equal(List(Map("point" -> GeographicPoint(56.7, 12.78, CRS.WGS84))))
  }

  test("point function should work with null input") {
    val result = executeWithAllPlanners("RETURN point(null) as p")
    result.toList should equal(List(Map("p" -> null)))
  }

  test("point function should fail on wrong type") {
    val error = intercept[SyntaxException](executeWithAllPlanners("RETURN point(1) as dist"))
    assert(error.getMessage.contains("Type mismatch: expected Map, Node or Relationship but was Integer"))
  }

  ignore("point function should be assignable to node property") {
    // Given
    createLabeledNode("Place")

    // When
    val result = executeWithAllPlanners("MATCH (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78}) RETURN p.location")

    // Then
    result should useProjectionWith("Point")
    result.toList should equal(List(Map("point" -> GeographicPoint(56.7, 12.78, CRS.WGS84))))
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

  test("type should work in both runtimes")  {
    // GIVEN
    relate(createNode(), createNode(), "T")

    // WHEN
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH ()-[r]->() RETURN type(r)")

    // THEN
    result.toList should equal(List(Map("type(r)" -> "T")))
  }

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

  test("type should handle optional when null")  {
    // GIVEN
    createNode()

    // WHEN
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (a) OPTIONAL MATCH (a)-[r:NOT_THERE]->() RETURN type(r)")

    // THEN
    result.toList should equal(List(Map("type(r)" -> null)))
  }

  test("type should handle optional when both null and match")  {
    // GIVEN
    relate(createNode(), createNode(), "T")

    // WHEN
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (a) OPTIONAL MATCH (a)-[r:T]->() RETURN type(r)")

    // THEN
    result.toList should equal(List(Map("type(r)" -> "T"), Map("type(r)" -> null)))
  }

  test("type should fail nicely when not given a relationship")  {
    // GIVEN
    relate(createNode(), createNode(), "T")
    val query: String = "MATCH (a)-[r]->() WITH [r, 1] as coll RETURN [x in coll | type(x) ]"

    //Expect
    a [SyntaxException] shouldBe thrownBy(eengine.execute(s"CYPHER runtime=compiled $query", Map.empty[String,Any], graph.session()))
    a [SyntaxException] shouldBe thrownBy(eengine.execute(s"CYPHER runtime=interpreted $query", Map.empty[String,Any], graph.session()))
    a [SyntaxException] shouldBe thrownBy(eengine.execute(s"CYPHER planner=cost $query", Map.empty[String,Any], graph.session()))
    a [SyntaxException] shouldBe thrownBy(eengine.execute(s"CYPHER planner=rule $query", Map.empty[String,Any], graph.session()))
  }
}
