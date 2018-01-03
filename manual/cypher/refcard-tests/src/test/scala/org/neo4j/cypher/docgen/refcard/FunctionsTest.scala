/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult

class FunctionsTest extends RefcardTest with QueryStatisticsTestSupport {
  val graphDescription = List("ROOT KNOWS A", "A KNOWS B", "B KNOWS C", "C KNOWS ROOT")
  val title = "Functions"
  val css = "general c2-2 c3-2 c4-2 c5-3 c6-5"
  override val linkId = "query-function"

  override def assert(name: String, result: InternalExecutionResult) {
    name match {
      case "returns-one" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 1)
      case "returns-none" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 0)
      case "toInt" =>
        assert(result.toList === List(Map("toInt({expr})" -> 10)))
      case "toFloat" =>
        assert(result.toList === List(Map("toFloat({expr})" -> 10.1)))
    }
  }

  override def parameters(name: String): Map[String, Any] =
    name match {
      case "parameters=default" =>
        Map("defaultValue" -> "Bob")
      case "parameters=toInt" =>
        Map("expr" -> "10")
      case "parameters=toFloat" =>
        Map("expr" -> "10.1")
      case "parameters=map" =>
        Map("expr" -> Map("name" -> "Bob"))
      case "" =>
        Map()
    }

  override val properties: Map[String, Map[String, Any]] = Map(
    "A" -> Map("property" -> "Andrés"),
    "B" -> Map("property" -> "Tobias"),
    "C" -> Map("property" -> "Chris"))

  def text = """
###assertion=returns-one parameters=default
MATCH n WHERE id(n) = %A%
RETURN

coalesce(n.property, {defaultValue})###

The first non-++NULL++ expression.

###assertion=returns-one
RETURN

timestamp()###

Milliseconds since midnight, January 1, 1970 UTC.

###assertion=returns-one
MATCH (n)-[nodeOrRelationship]->(m)
WHERE id(n) = %A% AND id(m) = %B%
RETURN

id(nodeOrRelationship)###

The internal id of the relationship or node.

###assertion=toInt parameters=toInt
RETURN

toInt({expr})###

Converts the given input into an integer if possible; otherwise it returns +NULL+.

###assertion=toFloat parameters=toFloat
RETURN

toFloat({expr})###

Converts the given input into a floating point number if possible; otherwise it returns +NULL+.

###assertion=returns-one parameters=map
RETURN

keys({expr})###

Returns a collection of string representations for the property names of a node, relationship, or map."""
}
