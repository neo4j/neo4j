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

class StringFunctionsTest extends RefcardTest with StatisticsChecker {
  val graphDescription = List("ROOT KNOWS A", "A KNOWS B", "B KNOWS C", "C KNOWS ROOT")
  val section = "refcard"
  val title = "String Functions"

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
        Map("expression" -> 16)
      case "parameters=replace" =>
        Map("original" -> "Hi", "search" -> "i", "replacement" -> "ello")
      case "parameters=sub" =>
        Map("original" -> "String", "begin" -> 3, "substring_length" -> 2)
      case "" =>
        Map()
    }

  override val properties: Map[String, Map[String, Any]] = Map(
    "A" -> Map("propertyName" -> "Andrés"),
    "B" -> Map("propertyName" -> "Tobias"),
    "C" -> Map("propertyName" -> "Chris"),
    "ROOT" -> Map("propertyName" -> 1))

  def text = """.String Functions
["refcard", cardcss="general c2-2 c3-2"]
----
###assertion=returns-one parameters=expression
START n=node(%ROOT%)
RETURN

STR({expression})
###

String representation of the expression.

###assertion=returns-one parameters=replace
START n=node(%ROOT%)
RETURN

REPLACE({original}, {search}, {replacement})
###

Replace all occurrences of `search` with `replacement`.
All arguments are be expressions.

###assertion=returns-one parameters=sub
START n=node(%ROOT%)
RETURN

SUBSTRING({original}, {begin}, {substring_length})
###

Get part of a string.
The `substring_length` argument is optional.

###assertion=returns-one parameters=sub
START n=node(%ROOT%)
RETURN

LEFT({original}, {substring_length})
###

The first part of a string.

###assertion=returns-one parameters=sub
START n=node(%ROOT%)
RETURN

RIGHT({original}, {substring_length})
###

The last part of a string.

###assertion=returns-one parameters=sub
START n=node(%ROOT%)
RETURN

LTRIM({original})
###

No whitespace on the left side.

###assertion=returns-one parameters=sub
START n=node(%ROOT%)
RETURN

RTRIM({original})
###

No whitespace on the right side.

###assertion=returns-one parameters=sub
START n=node(%ROOT%)
RETURN

TRIM({original})
###

No whitespace on the left or right side.

###assertion=returns-one parameters=sub
START n=node(%ROOT%)
RETURN

LOWER({original})
###

Lowercase.

###assertion=returns-one parameters=sub
START n=node(%ROOT%)
RETURN

UPPER({original})
###

Uppercase.
----
"""
}
