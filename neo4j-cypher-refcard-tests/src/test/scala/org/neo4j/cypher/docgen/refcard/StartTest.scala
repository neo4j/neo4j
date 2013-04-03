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

class StartTest extends RefcardTest with StatisticsChecker {
  val graphDescription = List("ROOT LINK A", "A LINK B", "B LINK C", "C LINK ROOT")
  val section = "refcard"
  val title = "Start"

  override def assert(name: String, result: ExecutionResult) {
    name match {
      case "create" =>
        assertStats(result, nodesCreated = 1, relationshipsCreated = 1, propertiesSet = 0)
        assert(result.toList.size === 1)
      case "all-nodes" =>
        assertStats(result, deletedNodes = 0, relationshipsCreated = 0, propertiesSet = 0, deletedRelationships = 0)
        assert(result.toList.size === 4)
      case "single-node-by-id" =>
        assertStats(result, deletedNodes = 0, relationshipsCreated = 0, propertiesSet = 0, deletedRelationships = 0)
        assert(result.toList.size === 1)
      case "multiple-nodes-by-id" =>
        assertStats(result, deletedNodes = 0, relationshipsCreated = 0, propertiesSet = 0, deletedRelationships = 0)
        assert(result.toList.size === 2)
      case "multiple-start-nodes-by-id" =>
        assertStats(result, deletedNodes = 0, relationshipsCreated = 0, propertiesSet = 0, deletedRelationships = 0)
        assert(result.toList.size === 1)
    }
  }

  override val properties: Map[String, Map[String, Any]] = Map(
    "A" -> Map("value" -> 10),
    "B" -> Map("value" -> 20),
    "C" -> Map("value" -> 30))

  def text = """.START
[refcard]
----
###assertion=all-nodes
//

START n=node(*)

RETURN n###

Start from all nodes.

### assertion=single-node-by-id
//

START n=node(1)

RETURN n###

Start from the node with id 1.

### assertion=multiple-nodes-by-id
//

START n=node(1, 2)

RETURN n###

Multiple nodes.

### assertion=multiple-start-nodes-by-id
//

START n=node(1), m=node(2)

RETURN n,m###

Multiple starting points.
----
"""
}
/*
START n=node:indexName(key="value")

Query the index for an exact value and put the result into n. Use node_auto_index for the auto-index.
START n=node:indexName("lucene query")

Query the index using a full Lucene query and put the result in n. 
*/
