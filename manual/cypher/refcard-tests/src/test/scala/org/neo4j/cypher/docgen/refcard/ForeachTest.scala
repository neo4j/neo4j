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

class ForeachTest extends RefcardTest with QueryStatisticsTestSupport {
  val graphDescription = List("ROOT KNOWS A", "A:Person KNOWS B:Person", "B KNOWS C:Person", "C KNOWS ROOT")
  val title = "FOREACH"
  val css = "write c4-3 c5-5 c6-2"
  override val linkId = "query-foreach"

  override def assert(name: String, result: InternalExecutionResult) {
    name match {
      case "foreach" =>
        assertStats(result, nodesCreated = 3, labelsAdded = 3, propertiesSet = 3)
        assert(result.toList.size === 0)
      case "friends" =>
        assertStats(result, nodesCreated = 0, propertiesSet = 1)
        assert(result.toList.size === 0)
    }
  }

  override def parameters(name: String): Map[String, Any] =
    name match {
      case "" =>
        Map()
    }

  override val properties: Map[String, Map[String, Any]] = Map(
    "A" -> Map("prop" -> "Andrés"),
    "B" -> Map("prop" -> "Tobias"),
    "C" -> Map("prop" -> "Chris")
  )

  def text = """
###assertion=friends
MATCH path = (begin)-[*]->(end)
WHERE id(begin) = %A% AND id(end) = %B%

FOREACH (r IN rels(path) |
  SET r.marked = TRUE)
###
Execute a mutating operation for each relationship of a path.

###assertion=foreach
WITH ["Alice", "Bob", "Charlie"] AS coll

FOREACH (value IN coll |
 CREATE (:Person {name:value}))
###
Execute a mutating operation for each element in a collection.
"""
}
