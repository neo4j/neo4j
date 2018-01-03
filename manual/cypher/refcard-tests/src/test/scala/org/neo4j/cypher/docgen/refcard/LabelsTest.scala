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

class LabelsTest extends RefcardTest with QueryStatisticsTestSupport {
  val graphDescription = List("A:Person KNOWS ROOT")
  val title = "Labels"
  val css = "general c2-1 c3-2 c4-1 c5-3 c6-3"
  override val linkId = "cypherdoc-labels-constraints-and-indexes"

  override def assert(name: String, result: InternalExecutionResult) {
    name match {
      case "related" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 1)
      case "add-labels" =>
        assertStats(result, labelsAdded = 3)
        assert(result.toList.size === 1)
      case "create-rel" =>
        assertStats(result, nodesCreated = 1, relationshipsCreated = 1, propertiesSet = 1, labelsAdded = 1)
        assert(result.toList.size === 1)
      case "create" =>
        assertStats(result, nodesCreated = 1, propertiesSet = 1, labelsAdded = 1, nodesDeleted = 1)
        assert(result.toList.size === 1)
      case "remove-label" =>
        assertStats(result, labelsRemoved = 1)
        assert(result.toList.size === 0)
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
###assertion=create parameters=bname
//

CREATE (n:Person {name: {value}})

DELETE n
RETURN n###

Create a node with label and property.

###assertion=create parameters=bname
//

MERGE (n:Person {name: {value}})

DELETE n
RETURN n###

Matches or creates unique node(s) with label and property.

###assertion=add-labels
MATCH (n:Person)

SET n:Spouse:Parent:Employee

RETURN n###

Add label(s) to a node.

###assertion=related
//

MATCH (n:Person)

RETURN n###

Matches nodes labeled `Person`.

###assertion=related parameters=aname
//

MATCH (n:Person)
WHERE n.name = {value}

RETURN n###

Matches nodes labeled `Person` with the given `name`.

###assertion=related
MATCH (n:Person)

WHERE (n:Person)

RETURN n###

Checks existence of label on node.

###assertion=related
MATCH (n:Person)
RETURN

labels(n)

###

Labels of the node.

###assertion=remove-label
MATCH (n:Person)

REMOVE n:Person

###

Remove label from node.
             """
}
