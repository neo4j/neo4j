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

class CreateTest extends RefcardTest with QueryStatisticsTestSupport {
  val graphDescription = List("ROOT LINK A", "A LINK B", "B LINK C", "C LINK ROOT")
  val title = "CREATE"
  val css = "write c4-3 c5-4 c6-1"
  override val linkId = "query-create"

  override def assert(name: String, result: InternalExecutionResult) {
    name match {
      case "create-node" =>
        assertStats(result, nodesCreated = 1, propertiesSet = 1)
        assert(result.toList.size === 1)
      case "create-node-from-map" =>
        assertStats(result, nodesCreated = 1, propertiesSet = 1)
        assert(result.toList.size === 1)
      case "create-nodes-from-maps" =>
        assertStats(result, nodesCreated = 2, propertiesSet = 2)
        assert(result.toList.size === 2)
      case "create-rel" =>
        assertStats(result, relationshipsCreated = 1)
        assert(result.dumpToString.contains("KNOWS"))
      case "create-rel-prop" =>
        assertStats(result, relationshipsCreated = 1, propertiesSet = 1)
        assert(result.toList.size === 1)
    }
  }

  override def parameters(name: String): Map[String, Any] =
    name match {
      case "parameters=aname" =>
        Map("value" -> "Bob")
      case "parameters=map" =>
        Map("map" -> Map("name" -> "Bob"))
      case "parameters=maps" =>
        Map("collectionOfMaps" -> List(Map("name" -> "Bob"), Map("name" -> "Carl")))
      case "parameters=ayear" =>
        Map("value" -> 2007)
      case "" =>
        Map()
    }

  override val properties: Map[String, Map[String, Any]] = Map(
    "A" -> Map("value" -> 10),
    "B" -> Map("value" -> 20),
    "C" -> Map("value" -> 30))

  def text = """
###assertion=create-node parameters=aname
//

CREATE (n {name: {value}})

RETURN n###

Create a node with the given properties.

###assertion=create-node-from-map parameters=map
//

CREATE (n {map})

RETURN n###

Create a node with the given properties.

###assertion=create-nodes-from-maps parameters=maps
//

CREATE (n {collectionOfMaps})

RETURN n###

Create nodes with the given properties.

###assertion=create-rel
MATCH n, m
WHERE id(n) = %A% AND id(m) = %B%

CREATE (n)-[r:KNOWS]->(m)

RETURN r###

Create a relationship with the given type and direction; bind an identifier to it.

###assertion=create-rel-prop parameters=ayear
MATCH n, m
WHERE id(n) = %A% AND id(m) = %B%

CREATE (n)-[:LOVES {since: {value}}]->(m)

RETURN n###

Create a relationship with the given type, direction, and properties.
"""
}
