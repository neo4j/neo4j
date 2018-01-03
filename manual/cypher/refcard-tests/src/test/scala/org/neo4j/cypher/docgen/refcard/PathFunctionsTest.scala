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

import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult
import org.neo4j.cypher.{ ExecutionResult, QueryStatisticsTestSupport }
import org.neo4j.cypher.docgen.RefcardTest

class PathFunctionsTest extends RefcardTest with QueryStatisticsTestSupport {
  val graphDescription = List("ROOT KNOWS A", "A:Person KNOWS B:Person", "B KNOWS C:Person", "C KNOWS ROOT")
  val title = "Path Functions"
  val css = "general c3-3 c4-3 c5-2 c6-5"
  override val linkId = "query-functions-collection"

  override def assert(name: String, result: InternalExecutionResult) {
    name match {
      case "returns-one" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 1)
      case "returns-three" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 3)
      case "returns-none" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 0)
      case "friends" =>
        assertStats(result, nodesCreated = 0, propertiesSet = 1)
        assert(result.toList.size === 0)
    }
  }

  override def parameters(name: String): Map[String, Any] =
    name match {
      case "parameters=value" =>
        Map("value" -> "Bob")
      case "parameters=range" =>
        Map("first_num" -> 2, "last_num" -> 18, "step" -> 3)
      case "" =>
        Map()
    }

  override val properties: Map[String, Map[String, Any]] = Map(
    "A" -> Map("prop" -> "Andrés"),
    "B" -> Map("prop" -> "Tobias"),
    "C" -> Map("prop" -> "Chris"))

  def text = """
###assertion=returns-one
MATCH path = (n)-->(m)
WHERE id(n) = %A% AND id(m) = %B%
RETURN

length(path)
###

The number of relationships in the path.

###assertion=returns-one
MATCH path = (n)-->(m)
WHERE id(n) = %A% AND id(m) = %B%
RETURN

nodes(path)
###

The nodes in the path as a collection.

###assertion=returns-one
MATCH path = (n)-->(m)
WHERE id(n) = %A% AND id(m) = %B%
RETURN

relationships(path)
###

The relationships in the path as a collection.

###assertion=returns-one
MATCH path = (n)-->(m)
WHERE id(n) = %A% AND id(m) = %B%
RETURN

extract(x IN nodes(path) | x.prop)
###

Extract properties from the nodes in a path.
"""
}
