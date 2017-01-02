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
package org.neo4j.cypher.docgen.refcard

import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.cypher.docgen.RefcardTest
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.InternalExecutionResult

class StringFunctionsTest extends RefcardTest with QueryStatisticsTestSupport {
  val graphDescription = List("ROOT KNOWS A", "A KNOWS B", "B KNOWS C", "C KNOWS ROOT")
  val title = "String Functions"
  val css = "general c2-2 c3-2 c4-1 c5-3 c6-5"
  override val linkId = "query-functions-string"

  override def assert(name: String, result: InternalExecutionResult) {
    name match {
      case "returns-one" =>
        assertStats(result, nodesCreated = 0)
        assert(result.size === 1)
      case "returns-none" =>
        assertStats(result, nodesCreated = 0)
        assert(result.size === 0)
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
        Map("original" -> "String", "begin" -> 3, "substring_length" -> 2, "sub_length" -> 2)
      case "parameters=split" =>
        Map("original" -> "A,B,C", "delimiter" -> ",")
      case "" =>
        Map()
    }

  override val properties: Map[String, Map[String, Any]] = Map(
    "A" -> Map("property" -> "Andrés"),
    "B" -> Map("property" -> "Tobias"),
    "C" -> Map("property" -> "Chris"),
    "ROOT" -> Map("property" -> 1))

  def text = """
###assertion=returns-one parameters=expression
RETURN

toString({expression})
###

String representation of the expression.

###assertion=returns-one parameters=replace
RETURN

replace({original}, {search}, {replacement})
###

Replace all occurrences of `search` with `replacement`.
All arguments are be expressions.

###assertion=returns-one parameters=sub
RETURN

substring({original}, {begin}, {sub_length})
###

Get part of a string.
The `sub_length` argument is optional.

###assertion=returns-one parameters=sub
RETURN

left({original}, {sub_length}),
  right({original}, {sub_length})
###

The first part of a string. The last part of the string.

###assertion=returns-one parameters=sub
RETURN

trim({original}), ltrim({original}),
  rtrim({original})
###

Trim all whitespace, or on left or right side.

###assertion=returns-one parameters=sub
RETURN

upper({original}), lower({original})
###

UPPERCASE and lowercase.

###assertion=returns-one parameters=split
RETURN

split({original}, {delimiter})
###

Split a string into a collection of strings.
"""
}
