/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.docgen.cookbook

import org.neo4j.cypher.docgen.ArticleTest
import org.neo4j.cypher.{ExecutionResult, StatisticsChecker}

class LinkedListTest extends ArticleTest with StatisticsChecker {
  val graphDescription = List("ROOT LINK A", "A LINK B", "B LINK C", "C LINK ROOT")
  val section = "cookbook"
  val title = "Linked List"


  override def assert(name: String, result: ExecutionResult) {
    name match {
      case "create" =>
        assertStats(result, nodesCreated = 1, relationshipsCreated = 1, propertiesSet = 0)
        assert(result.toList.size === 0)
      case "add"    =>
        assertStats(result, nodesCreated = 1, relationshipsCreated = 2, propertiesSet = 1, deletedRelationships = 1)
        assert(result.toList.size === 0)
      case "delete" =>
        assertStats(result, deletedNodes = 1, relationshipsCreated = 1, deletedRelationships = 2)
        assert(result.toList.size === 0)
    }
  }

  override val properties: Map[String, Map[String, Any]] = Map(
    "A" -> Map("value" -> 10),
    "B" -> Map("value" -> 20),
    "C" -> Map("value" -> 30)
  )


  def text = """
Linked Lists
============

A powerful feature of using a graph database, is that you can create your own in-graph data structures - like a linked
list.

This datastructure uses a single node as the list reference. The reference has an outgoing relationship to the head of
the list, and an incoming relationship from the last element of the list. If the list is empty, the reference will point
to it self.

Something like this:

###graph-image###

To initialize an empty linked list, we simply create an empty node, and make it link to itself.

###no-results empty-graph assertion=create
CREATE root-[:LINK]->root // no ‘value’ property assigned to root
RETURN root###


Adding values is done by finding the relationship where the new value should be placed in, and replacing it with
a new node, and two relationships to it.

###no-results assertion=add
START root=node(%ROOT%)
MATCH root-[:LINK*0..]->before,// before could be same as root
      after-[:LINK*0..]->root, // after could be same as root
      before-[old:LINK]->after
WHERE before.value? < 25  // This is the value. It would normally be supplied through a parameter
  AND 25 < after.value?
CREATE before-[:LINK]->({value:25})-[:LINK]->after
DELETE old###

Deleting a value, conversely, is done by finding the node with the value, and the two relationships going in and out
from it, and replacing with a new value.

###no-results assertion=delete
START root=node(%ROOT%)
MATCH root-[:LINK*0..]->before,
      before-[delBefore:LINK]->del-[delAfter:LINK]->after,
      after-[:LINK*0..]->root
WHERE del.value = 10
CREATE before-[:LINK]->after
DELETE del, delBefore, delAfter###
"""

}
