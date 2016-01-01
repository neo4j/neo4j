/**
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
package org.neo4j.cypher.docgen.refcard
import org.neo4j.cypher.{ ExecutionResult, QueryStatisticsTestSupport }
import org.neo4j.cypher.docgen.RefcardTest

class MathematicalFunctionsTest extends RefcardTest with QueryStatisticsTestSupport {
  val graphDescription = List("ROOT KNOWS A")
  val title = "Mathematical Functions"
  val css = "general c2-1 c3-3 c4-4 c5-3 c6-5"

  override def assert(name: String, result: ExecutionResult) {
    name match {
      case "returns-one" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 1)
      case "returns-none" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 0)
    }
  }

  override def parameters(name: String): Map[String, Any] =
    name match {
      case "parameters=value" =>
        Map("value" -> "Bob")
      case "parameters=expression" =>
        Map("expr" -> .5)
      case "" =>
        Map()
    }

  def text = """
###assertion=returns-one parameters=expression
START n=node(%ROOT%)
RETURN

abs({expr})
###

The absolute value.

###assertion=returns-one parameters=expression
START n=node(%ROOT%)
RETURN

rand()
###

A random value. Returns a new value for each call. Also useful for selecting subset or random ordering.

###assertion=returns-one parameters=expression
START n=node(%ROOT%)
RETURN

round({expr})

,floor({expr}),ceil({expr})
###

Round to the nearest integer, +ceil+ and +floor+ find the next integer up or down.

###assertion=returns-one parameters=expression
START n=node(%ROOT%)
RETURN

sqrt({expr})
###

The square root.

###assertion=returns-one parameters=expression
START n=node(%ROOT%)
RETURN

sign({expr})
###

`0` if zero, `-1` if negative, `1` if positive.

###assertion=returns-one parameters=expression
START n=node(%ROOT%)
RETURN

sin({expr})

,cos({expr}),tan({expr}),cot({expr}),asin({expr}),acos({expr}),atan({expr}),atan2({expr},{expr}),haversin({expr})
###

Trigonometric functions, also `cos`, `tan`, `cot`, `asin`, `acos`, `atan`, `atan2`, `haversin`.

###assertion=returns-one parameters=expression
START n=node(%ROOT%)
RETURN

degrees({expr}), radians({expr}), pi()
###

Converts radians into degrees, use `radians` for the reverse. `pi` for π.

###assertion=returns-one parameters=expression
START n=node(%ROOT%)
RETURN

log10({expr}), log({expr}), exp({expr}), e()
###

Logarithm base 10, natural logarithm, `e` to the power of the parameter. Value of `e`.
             """
}
