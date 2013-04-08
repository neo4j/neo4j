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
  override def indexProps: List[String] = List("value", "name")

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
      case "index-match" =>
        assertStats(result)
        assert(result.toList.size === 1)
      case "index-query" =>
        assertStats(result)
        assert(result.dumpToString.contains("Bob"))
    }
  }

  override def parameters(name: String): Map[String, Any] =
    name match {
      case "parameters=ids" =>
        Map("id1" -> 1, "id2" -> 2)
      case "parameters=multiple" =>
        Map("ids" -> List(1, 2))
      case "parameters=index-match" =>
        Map("key" -> "value", "value" -> 20)
      case "parameters=index-query" =>
        Map("query" -> "name:Bob")
      case "" =>
        Map()
    }

  override val properties: Map[String, Map[String, Any]] = Map(
    "A" -> Map("value" -> 10),
    "B" -> Map("value" -> 20, "name" -> "Bob"),
    "C" -> Map("value" -> 30))

  def text = """.START
[refcard]
----
###assertion=all-nodes
//

START n=node(*)

RETURN n###

Start from all nodes.

### assertion=multiple-nodes-by-id parameters=multiple
//

START n=node({ids})

RETURN n###

Start from one or more nodes specified by id.

### assertion=multiple-start-nodes-by-id parameters=ids
//

START n=node({id1}), m=node({id2})

RETURN n,m###

Multiple starting points.

### assertion=index-match parameters=index-match
//

START n=node:nodeIndexName({key}={value})

RETURN n###

Query the index with an exact query.
Use `node_auto_index` for the automatic index.
----
"""
}

/*

### assertion=index-match parameters=index-query
//

START n=node:nodeIndexName({query})

RETURN n###

Query the index using a full Lucene query. 
A query can look like this: "name:Bob"

*/
