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

class CreateTest extends RefcardTest with StatisticsChecker {
  val graphDescription = List("ROOT LINK A", "A LINK B", "B LINK C", "C LINK ROOT")
  val section = "refcard"
  val title = "Create"

  override def assert(name: String, result: ExecutionResult) {
    name match {
      case "create-node" =>
        assertStats(result, nodesCreated = 1, propertiesSet = 1)
        assert(result.toList.size === 1)
      case "create-rel" =>
        assertStats(result, relationshipsCreated = 1)
        assert(result.dumpToString.contains("KNOWS"))
      case "create-rel-prop" =>
        assertStats(result, relationshipsCreated = 1, propertiesSet = 1)
        assert(result.toList.size === 1)
    }
  }

  override val properties: Map[String, Map[String, Any]] = Map(
    "A" -> Map("value" -> 10),
    "B" -> Map("value" -> 20),
    "C" -> Map("value" -> 30))

  def text = """.CREATE
[refcard]
----
###assertion=create-node
//

CREATE (n { name :"Name" }) 

RETURN n###

Create a node with the given properties.

###assertion=create-rel
START n=node(%A%), m=node(%B%)

CREATE n-[r:KNOWS]->m

RETURN r###

Create a relationship with the given type and direction; bind an identifier to it.

###assertion=create-rel-prop
START n=node(%A%), m=node(%B%)

CREATE n-[:LOVES {since: 2007}]->m

RETURN n###

Create a relationship with the given type, direction, and properties.
----
"""
}
