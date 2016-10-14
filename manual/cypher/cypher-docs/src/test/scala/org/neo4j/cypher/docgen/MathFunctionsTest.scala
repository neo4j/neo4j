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
package org.neo4j.cypher.docgen

import org.neo4j.cypher.docgen.tooling._

class MathFunctionsTest extends DocumentingTest {

  override def outputPath = "target/docs/dev/ql/functions"

  override def doc = new DocBuilder {
    doc("Mathematical functions", "query-functions-mathematical")
    initQueries(
      """CREATE (alice:A {name:'Alice', age: 38, eyes: 'brown'}),
        |       (bob:B {name: 'Bob', age: 25, eyes: 'blue'}),
        |       (charlie:C {name: 'Charlie', age: 53, eyes: 'green'}),
        |       (daniel:D {name: 'Daniel', age: 54, eyes: 'brown'}),
        |       (eskil:E {name: 'Eskil', age: 41, eyes: 'blue', array: ['one', 'two', 'three']}),
        |
        |       (alice)-[:KNOWS]->(bob),
        |       (alice)-[:KNOWS]->(charlie),
        |       (bob)-[:KNOWS]->(daniel),
        |       (charlie)-[:KNOWS]->(daniel),
        |       (bob)-[:MARRIED]->(eskil)"""
    )
    synopsis(
      "These functions all operate on numerical expressions only, and will return an error if used on any other values. See also <<query-operators-mathematical>>..")
    p("The following graph is used for the examples below:")
    graphViz()
    section("Number functions", "query-functions-numeric") {
      section("abs()", "functions-abs") {
        p("`abs()` returns the absolute value of a number.")
        function("`abs( expression )`", ("expression", "A numeric expression."))
        query("MATCH (a), (e) WHERE a.name = 'Alice' AND e.name = 'Eskil' RETURN a.age, e.age, abs(a.age - e.age)",
              ResultAssertions((r) => {
                r.toList should equal(List(Map("a.age" -> 38L, "e.age" -> 41L, "abs(a.age - e.age)" -> 3L)))
              })) {
          p("The absolute value of the age difference is returned.")
          resultTable()
        }
      }
      section("ceil()", "functions-ceil") {
        p("`ceil()` returns the smallest integer greater than or equal to the argument.")
        function("`ceil( expression )`", ("expression", "A numeric expression."))
        query("RETURN ceil(0.1)", ResultAssertions((r) => {
          r.toList.head("ceil(0.1)") should equal(1.0)
        })) {
          p("The ceil of `0.1`.")
          resultTable()
        }
      }
      section("floor()", "functions-floor") {
        p("`floor()` returns the greatest integer less than or equal to the expression.")
        function("`floor( expression )`", ("expression", "A numeric expression."))
        query("RETURN floor(0.9)", ResultAssertions((r) => {
          r.toList.head("floor(0.9)") should equal(0.0)
        })) {
          p("The floor of `0.9` is returned.")
          resultTable()
        }
      }
      section("round()", "functions-round") {
        p("`round()` returns the numerical expression, rounded to the nearest integer.")
        function("`round( expression )`", ("expression", "A numeric expression that represents the angle in radians."))
        query("RETURN round(3.141592)", ResultAssertions((r) => {
          r.toList.head("round(3.141592)") should equal(3.0)
        })) {
          p("`3.0` is returned.")
          resultTable()
        }
      }
      section("sign()", "functions-sign") {
        p("`sign()` returns the signum of a number -- zero if the expression is zero, `-1` for any negative number, and `1` for any positive number.")
        function("`sign( expression )`", ("expression", "A numeric expression."))
        query("RETURN sign(-17), sign(0.1)", ResultAssertions((r) => {
          r.toList.head("sign(-17)") should equal(-1L)
          r.toList.head("sign(0.1)") should equal(1L)
        })) {
          p("The signs of `-17` and `0.1` are returned.")
          resultTable()
        }
      }
      section("rand()", "functions-rand") {
        p("`rand()` returns a random number in the range from 0 (inclusive) to 1 (exclusive), [0,1). The numbers returned follow an approximate uniform distribution.")
        function("`rand()`")
        query("RETURN rand()", ResultAssertions((r) => {
          r.toList.head("rand()").asInstanceOf[Double] should be >= 0.0
          r.toList.head("rand()").asInstanceOf[Double] should be < 1.0
        })) {
          p("A random number is returned.")
          resultTable()
        }
      }
    }
    section("Logarithmic functions", "query-functions-logarithmic") {
      section("log()", "functions-log") {
        p("`log()` returns the natural logarithm of the expression.")
        function("`log( expression )`", ("expression", "A numeric expression."))
        query("RETURN log(27)", ResultAssertions((r) => {
          r.toList.head("log(27)") should equal(3.295836866004329)
        })) {
          p("The natural logarithm of `27` is returned.")
          resultTable()
        }
      }
      section("log10()", "functions-log10") {
        p("`log10()` returns the common logarithm (base 10) of the expression.")
        function("`log10( expression )`", ("expression", "A numeric expression."))
        query("RETURN log10(27)", ResultAssertions((r) => {
          r.toList.head("log10(27)") should equal(1.4313637641589874)
        })) {
          p("The common logarithm of `27` is returned.")
          resultTable()
        }
      }
      section("exp()", "functions-exp") {
        p("`exp()` returns `e^n`, where `e` is the base of the natural logarithm, and `n` is the value of the argument expression.")
        function("`e( expression )`", ("expression", "A numeric expression."))
        query("RETURN exp(2)", ResultAssertions((r) => {
          r.toList.head("exp(2)").asInstanceOf[Double] should equal(Math.E * Math.E +- 0.00000001)
        })) {
          p("`e` to the power of `2` is returned.")
          resultTable()
        }
      }
      section("e()", "functions-e") {
        p("`e()` returns the base of the natural logarithm, `e`.")
        function("`e()`")
        query("RETURN e()", ResultAssertions((r) => {
          r.toList.head("e()") should equal(Math.E)
        })) {
          p("The base of the natural logarithm, `e`, is returned.")
          resultTable()
        }
      }
      section("sqrt()", "functions-sqrt") {
        p("`sqrt()` returns the square root of a number.")
        function("`sqrt( expression )`", ("expression", "A numeric expression."))
        query("RETURN sqrt(256)", ResultAssertions((r) => {
          r.toList.head("sqrt(256)") should equal(16.0)
        })) {
          p("The square root of `256` is returned.")
          resultTable()
        }
      }
    }
    section("Trigonometric functions", "query-functions-trigonometric") {
      p("All trigonometric functions operate on radians, unless otherwise specified.")
      section("sin()", "functions-sin") {
        p("`sin()` returns the sine of the expression.")
        function("`sin( expression )`", ("expression", "A numeric expression that represents the angle in radians."))
        query("RETURN sin(0.5)", ResultAssertions((r) => {
          r.toList.head("sin(0.5)") should equal(0.479425538604203)
        })) {
          p("The sine of `0.5` is returned.")
          resultTable()
        }
      }
      section("cos()", "functions-cos") {
        p("`cos()` returns the cosine of the expression.")
        function("`cos( expression )`", ("expression", "A numeric expression that represents the angle in radians."))
        query("RETURN cos(0.5)", ResultAssertions((r) => {
          r.toList.head("cos(0.5)") should equal(0.8775825618903728)
        })) {
          p("The cosine of `0.5`.")
          resultTable()
        }
      }
      section("tan()", "functions-tan") {
        p("`tan()` returns the tangent of the expression.")
        function("`tan( expression )`", ("expression", "A numeric expression that represents the angle in radians."))
        query("RETURN tan(0.5)", ResultAssertions((r) => {
          r.toList.head("tan(0.5)") should equal(0.5463024898437905)
        })) {
          p("The tangent of `0.5` is returned.")
          resultTable()
        }
      }
      section("cot()", "functions-cot") {
        p("`cot()` returns the cotangent of the expression.")
        function("`cot( expression )`", ("expression", "A numeric expression that represents the angle in radians."))
        query("RETURN cot(0.5)", ResultAssertions((r) => {
          r.toList.head("cot(0.5)") should equal(1.830487721712452)
        })) {
          p("The cotangent of `0.5`.")
          resultTable()
        }
      }
      section("asin()", "functions-asin") {
        p("`asin()` returns the arcsine of the expression, in radians.")
        function("`asin( expression )`", ("expression", "A numeric expression that represents the angle in radians."))
        query("RETURN asin(0.5)", ResultAssertions((r) => {
          r.toList.head("asin(0.5)") should equal(0.5235987755982989)
        })) {
          p("The arcsine of `0.5`.")
          resultTable()
        }
      }
      section("acos()", "functions-acos") {
        p("`acos()` returns the arccosine of the expression, in radians.")
        function("`abs( expression )`", ("expression", "A numeric expression that represents the angle in radians."))
        query("RETURN acos(0.5)", ResultAssertions((r) => {
          r.toList.head("acos(0.5)") should equal(1.0471975511965979)
        })) {
          p("The arccosine of `0.5`.")
          resultTable()
        }
      }
      section("atan()", "functions-atan") {
        p("`atan()` returns the arctangent of the expression, in radians.")
        function("`atan( expression )`", ("expression", "A numeric expression that represents the angle in radians."))
        query("RETURN atan(0.5)", ResultAssertions((r) => {
          r.toList.head("atan(0.5)") should equal(0.4636476090008061)
        })) {
          p("The arctangent of `0.5`.")
          resultTable()
        }
      }
      section("atan2()", "functions-atan2") {
        p("`atan2()` returns the arctangent2 of a set of coordinates, in radians.")
        function("`atan2( expression1, expression2 )`",
                 ("expression1", "A numeric expression for y that represents the angle in radians."),
                 ("expression2", "A numeric expression for x that represents the angle in radians."))
        query("RETURN atan2(0.5, 0.6)", ResultAssertions((r) => {
          r.toList.head("atan2(0.5, 0.6)") should equal(0.6947382761967033)
        })) {
          p("The arctangent2 of `0.5` and `0.6`.")
          resultTable()
        }
      }
      section("pi()", "functions-pi") {
        p("`pi()` returns the mathematical constant pi.")
        function("`pi()`")
        query("RETURN pi()", ResultAssertions((r) => {
          r.toList.head("pi()") should equal(3.141592653589793)
        })) {
          p("The constant pi is returned.")
          resultTable()
        }
      }
      section("degrees()", "functions-degrees") {
        p("`degrees()` converts radians to degrees.")
        function("`degrees( expression )`",
                 ("expression", "A numeric expression that represents the angle in radians."))
        query("RETURN degrees(3.14159)", ResultAssertions((r) => {
          r.toList.head("degrees(3.14159)").asInstanceOf[Double] should equal(180.0 +- 0.001)
        })) {
          p("The number of degrees in something close to pi.")
          resultTable()
        }
      }
      section("radians()", "functions-radians") {
        p("`radians()` converts degrees to radians.")
        function("`radians( expression )`",
                 ("expression", "A numeric expression that represents the angle in degrees."))
        query("RETURN radians(180)", ResultAssertions((r) => {
          r.toList.head("radians(180)") should equal(3.141592653589793)
        })) {
          p("The number of radians in `180` degrees is returned (pi).")
          resultTable()
        }
      }
      section("haversin()", "functions-haversin") {
        p("`haversin()` returns half the versine of the expression.")
        function("`haversin( expression )`",
                 ("expression", "A numeric expression that represents the angle in radians."))
        query("RETURN haversin(0.5)", ResultAssertions((r) => {
          r.toList.head("haversin(0.5)") should equal(0.06120871905481362)
        })) {
          p("The haversine of `0.5` is returned.")
          resultTable()
        }
      }
      section("Spherical distance using the haversin function") {
        p( """The `haversin()` function may be used to compute the distance on the surface of a sphere between two
             |points (each given by their latitude and longitude). In this example the spherical distance (in km)
             |between Berlin in Germany (at lat 52.5, lon 13.4) and San Mateo in California (at lat 37.5, lon -122.3)
             |is calculated using an average earth radius of 6371 km.""")
        query( """CREATE (ber:City {lat: 52.5, lon: 13.4}), (sm:City {lat: 37.5, lon: -122.3})
                 |RETURN 2 * 6371 * asin(sqrt(haversin(radians( sm.lat - ber.lat ))
                 |       + cos(radians( sm.lat )) * cos(radians( ber.lat )) *
                 |       haversin(radians( sm.lon - ber.lon )))) AS dist""".stripMargin, ResultAssertions((r) => {
          r.toList.head("dist").asInstanceOf[Double] should equal(9129.0 +- 1)
        })) {
          p("The estimated distance between Berlin and San Mateo is returned.")
          resultTable()
        }
      }
    }
  }.build()
}
