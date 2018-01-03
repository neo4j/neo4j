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

class SetTest extends RefcardTest with QueryStatisticsTestSupport {
  val graphDescription = List("ROOT LINK A")
  val title = "SET"
  val css = "write c2-2 c4-3 c5-4 c6-2"
  override val linkId = "query-set"

  override def assert(name: String, result: InternalExecutionResult) {
    name match {
      case "set" =>
        assertStats(result, propertiesSet = 2)
        assert(result.dumpToString().contains("a value"))
      case "set-label" =>
        assertStats(result, nodesCreated = 1, labelsAdded = 1)
        assert(result.dumpToString().contains("Person"))
      case "map" =>
        assertStats(result, nodesCreated = 1, propertiesSet = 1)
        assert(result.dumpToString().contains("a value"))
    }
  }

  override def parameters(name: String): Map[String, Any] =
    name match {
      case "parameters=set" => Map("value1" -> "a value", "value2" -> "another value")
      case "parameters=map" => Map("map" -> Map("property" -> "a value"))
      case "" => Map()
    }

  override val properties: Map[String, Map[String, Any]] = Map(
    "A" -> Map("value" -> 10))

  def text = """
###assertion=set parameters=set
MATCH (n) WHERE id(n) = %A%

SET n.property1 = {value1},
    n.property2 = {value2}

RETURN n.property1###

Update or create a property.

###assertion=map parameters=map
CREATE (n)

SET n = {map}

RETURN n.property###

Set all properties.
This will remove any existing properties.

###assertion=map parameters=map
CREATE (n)

SET n += {map}

RETURN n.property###

Add and update properties, while keeping existing ones.

###assertion=set-label
CREATE (n)

SET n:Person

RETURN labels(n)###

Adds a label `Person` to a node.
"""
}
