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

import org.neo4j.cypher.internal.compiler.v3_2.{CRS, CartesianPoint, GeographicPoint}
import org.neo4j.cypher.{ExecutionEngineFunSuite, InvalidArgumentException, NewPlannerTestSupport, SyntaxException}

class SpatialFunctionsAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("point function should work with literal map") {
    val result = executeWithAllPlanners("RETURN point({latitude: 12.78, longitude: 56.7}) as point")
    result should useProjectionWith("Point")
    result.toList should equal(List(Map("point" -> GeographicPoint(56.7, 12.78, CRS.WGS84))))
  }

  test("point function should work with literal map and cartesian coordinates") {
    val result = executeWithAllPlanners("RETURN point({x: 2.3, y: 4.5, crs: 'cartesian'}) as point")
    result should useProjectionWith("Point")
    result.toList should equal(List(Map("point" -> CartesianPoint(2.3, 4.5, CRS.Cartesian))))
  }

  test("point function should work with literal map and geographic coordinates") {
    val result = executeWithAllPlanners("RETURN point({longitude: 2.3, latitude: 4.5, crs: 'WGS-84'}) as point")
    result should useProjectionWith("Point")
    result.toList should equal(List(Map("point" -> GeographicPoint(2.3, 4.5, CRS.WGS84))))
  }

  test("point function should not work with literal map and incorrect cartesian CRS") {
    an [InvalidArgumentException] shouldBe thrownBy(
      executeWithAllPlanners("RETURN point({x: 2.3, y: 4.5, crs: 'cart'}) as point")
    )
  }

  test("point function should not work with literal map and incorrect geographic CRS") {
    an [InvalidArgumentException] shouldBe thrownBy(
      executeWithAllPlanners("RETURN point({x: 2.3, y: 4.5, crs: 'WGS84'}) as point")
    )
  }

  test("point function should work with integer arguments") {
    val result = executeWithAllPlanners("RETURN point({x: 2, y: 4}) as point")
    result should useProjectionWith("Point")
    result.toList should equal(List(Map("point" -> CartesianPoint(2, 4, CRS.Cartesian))))
  }

  test("should fail properly if missing cartesian coordinates") {
    an [InvalidArgumentException] shouldBe thrownBy(
      executeWithAllPlanners("RETURN point({params}) as point", "params" -> Map("y" -> 1.0, "crs" -> "cartesian"))
    )
  }

  test("should fail properly if missing geographic longitude") {
    an [InvalidArgumentException] shouldBe thrownBy(
      executeWithAllPlanners("RETURN point({params}) as point", "params" -> Map("latitude" -> 1.0, "crs" -> "WGS-84"))
    )
  }

  test("should fail properly if missing geographic latitude") {
    an [InvalidArgumentException] shouldBe thrownBy(
      executeWithAllPlanners("RETURN point({params}) as point", "params" -> Map("longitude" -> 1.0, "crs" -> "WGS-84"))
    )
  }

  test("should fail properly if unknown coordinate system") {
    an [InvalidArgumentException] shouldBe thrownBy(
      executeWithAllPlanners("RETURN point({params}) as point", "params" -> Map("x" -> 1, "y" -> 2, "crs" -> "WGS-1337"))
    )
  }

  test("should default to Cartesian if missing cartesian CRS") {
    val result = executeWithAllPlanners("RETURN point({x: 2.3, y: 4.5}) as point")
    result should useProjectionWith("Point")
    result.toList should equal(List(Map("point" -> CartesianPoint(2.3, 4.5, CRS.Cartesian))))
  }

  test("should default to WGS84 if missing geographic CRS") {
    val result = executeWithAllPlanners("RETURN point({longitude: 2.3, latitude: 4.5}) as point")
    result should useProjectionWith("Point")
    result.toList should equal(List(Map("point" -> GeographicPoint(2.3, 4.5, CRS.WGS84))))
  }

  test("should allow Geographic CRS with x/y coordinates") {
    val result = executeWithAllPlanners("RETURN point({x: 2.3, y: 4.5, crs: 'WGS-84'}) as point")
    result should useProjectionWith("Point")
    result.toList should equal(List(Map("point" -> GeographicPoint(2.3, 4.5, CRS.WGS84))))
  }

  test("should not allow Cartesian CRS with latitude/longitude coordinates") {
    an [InvalidArgumentException] shouldBe thrownBy(
      executeWithAllPlanners("RETURN point({longitude: 2.3, latitude: 4.5, crs: 'cartesian'}) as point")
    )
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
    assert(error.getMessage.contains("Type mismatch: expected Point or Geometry but was Integer"))
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
}
