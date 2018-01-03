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

class MergeTest extends RefcardTest with QueryStatisticsTestSupport {
  val graphDescription = List("A:Person KNOWS B:Person")
  val title = "MERGE"
  val css = "write c4-3 c5-4 c6-1"
  override val linkId = "query-merge"

  override def assert(name: String, result: InternalExecutionResult) {
    name match {
      case "merge" =>
        assertStats(result, nodesCreated = 1, propertiesSet = 2, labelsAdded = 1)
        assert(result.toList.size === 1)
      case "merge-rel" =>
        assertStats(result, relationshipsCreated = 1)
        assert(result.toList.size === 1)
      case "merge-sub" =>
        assertStats(result, relationshipsCreated = 1, nodesCreated = 1, propertiesSet = 1, labelsAdded = 1)
        assert(result.toList.size === 1)
    }
  }

  override def parameters(name: String): Map[String, Any] =
    name match {
      case "parameters=aname" =>
        Map("value" -> "Charlie")
      case "parameters=names" =>
        Map("value1" -> "Alice", "value2" -> "Bob", "value3" -> "Charlie")
      case "" =>
        Map()
    }

  override val properties: Map[String, Map[String, Any]] = Map(
    "A" -> Map("name" -> "Alice"),
    "B" -> Map("name" -> "Bob"))

  def text = """
###assertion=merge parameters=aname
//

MERGE (n:Person {name: {value}})
ON CREATE SET n.created = timestamp()
ON MATCH SET
    n.counter = coalesce(n.counter, 0) + 1,
    n.accessTime = timestamp()

RETURN n###

Match pattern or create it if it does not exist.
Use +ON CREATE+ and +ON MATCH+ for conditional updates.

###assertion=merge-rel parameters=names
//

MATCH (a:Person {name: {value1}}),
      (b:Person {name: {value2}})
MERGE (a)-[r:LOVES]->(b)

RETURN r###

+MERGE+ finds or creates a relationship between the nodes.

###assertion=merge-sub parameters=names
//

MATCH (a:Person {name: {value1}})
MERGE
  (a)-[r:KNOWS]->(b:Person {name: {value3}})

RETURN r, b###

+MERGE+ finds or creates subgraphs attached to the node.
"""
}
