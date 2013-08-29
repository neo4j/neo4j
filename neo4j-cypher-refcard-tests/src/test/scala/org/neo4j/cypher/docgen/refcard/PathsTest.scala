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

class PathsTest extends RefcardTest with StatisticsChecker {
  val graphDescription = List("A:Person KNOWS ROOT")
  val title = "Paths"
  val css = "general c2-2 c3-2 c5-2 c6-2"

  override def assert(name: String, result: ExecutionResult) {
    name match {
      case "one" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 1)
    }
  }

  override def parameters(name: String): Map[String, Any] =
    name match {
      case "parameters=aname" =>
        Map("value" -> "Alice")
      case "parameters=bname" =>
        Map("value" -> "Bob")
      case "" =>
        Map()
    }

  override val properties: Map[String, Map[String, Any]] = Map(
    "A" -> Map("name" -> "Alice"))

  def text = """
###assertion=one parameters=aname
MATCH p =

shortestPath((n1:Person)-[*..6]-(n2:Person))

WHERE n1.name = "Alice"
RETURN p###

Find a single shortest path.

###assertion=one parameters=aname
MATCH p =

allShortestPaths((n1:Person)-->(n2:Person))

WHERE n1.name = "Alice"
RETURN p###

Find all shortest paths.

"""
}
