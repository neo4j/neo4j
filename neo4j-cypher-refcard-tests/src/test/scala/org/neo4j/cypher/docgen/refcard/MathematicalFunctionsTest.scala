/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import org.neo4j.cypher.{ ExecutionResult, StatisticsChecker }
import org.neo4j.cypher.docgen.RefcardTest

class MathematicalFunctionsTest extends RefcardTest with StatisticsChecker {
  val graphDescription = List("ROOT KNOWS A")
  val title = "Mathematical Functions"
  val css = "general c2-2 c3-2 c4-4 c5-3 c6-5"

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
        Map("numerical_expression" -> .5)
      case "" =>
        Map()
    }

  def text = """
###assertion=returns-one parameters=expression
START n=node(%ROOT%)
RETURN

ABS({numerical_expression})
###

The absolute value.

###assertion=returns-one parameters=expression
START n=node(%ROOT%)
RETURN

RAND()
###

A random value. Returns a new value for each call. Also useful for selecting subset or random ordering.

###assertion=returns-one parameters=expression
START n=node(%ROOT%)
RETURN

ROUND({numerical_expression})
###

Round to the nearest integer.

###assertion=returns-one parameters=expression
START n=node(%ROOT%)
RETURN

FLOOR({numerical_expression})
###

The integral part of the decimal number..

###assertion=returns-one parameters=expression
START n=node(%ROOT%)
RETURN

SQRT({numerical_expression})
###

The square root.

###assertion=returns-one parameters=expression
START n=node(%ROOT%)
RETURN

SIGN({numerical_expression})
###

`0` if zero, `-1` if negative, `1` if positive.

###assertion=returns-one parameters=expression
START n=node(%ROOT%)
RETURN

SIN({numerical_expression})
###

Trigonometric functions, also `COS`, `TAN`, `COT`, `ASIN`, `ACOS`, `ATAN`.

###assertion=returns-one parameters=expression
START n=node(%ROOT%)
RETURN

DEGREES({numerical_expression}),
  RADIANS({numerical_expression}), PI()
###

Converts radians into degrees, use `RADIANS` for the reverse. `PI` for π.

###assertion=returns-one parameters=expression
START n=node(%ROOT%)
RETURN

LOG10({numerical_expression}), LOG({numerical_expression}),
  EXP({numerical_expression}), E()
###

Logarithm base 10, natural logarithm, `e` to the power of the paramameter. Value of `e`.
             """
}
