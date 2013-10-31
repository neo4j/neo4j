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
package org.neo4j.cypher.docgen.cookbook

import org.neo4j.cypher.docgen.ArticleTest
import org.neo4j.cypher.{ExecutionResult, StatisticsChecker}
import org.neo4j.visualization.graphviz.GraphStyle
import org.neo4j.visualization.graphviz.AsciiDocSimpleStyle

class LinkedListTest extends ArticleTest with StatisticsChecker {
  val graphDescription = List("ROOT LINK A", "A LINK B", "B LINK C", "C LINK ROOT")
  val section = "cookbook"
  val title = "Linked List"

  override protected def getGraphvizStyle: GraphStyle = 
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()

  override def assert(name: String, result: ExecutionResult) {
    val list: List[Map[String, Any]] = result.toList
    name match {
      case "create" =>
        assert(list.size === 1)
        assertStats(result, nodesCreated = 1, relationshipsCreated = 1, propertiesSet = 0)
      case "add"    =>
        assert(list.size === 0)
        assertStats(result, nodesCreated = 1, relationshipsCreated = 2, propertiesSet = 1, relationshipsDeleted = 1)
      case "delete" =>
        assert(list.size === 0)
        assertStats(result, nodesDeleted = 1, relationshipsCreated = 1, relationshipsDeleted = 2)
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

A powerful feature of using a graph database, is that you can create your own in-graph data structures -- like a linked
list.

This datastructure uses a single node as the list reference. The reference has an outgoing relationship to the head of
the list, and an incoming relationship from the last element of the list. If the list is empty, the reference will point
to it self.

Something like this:

###graph-image graph [rankdir=RL]###

To initialize an empty linked list, we simply create an empty node, and make it link to itself.

###no-results empty-graph assertion=create
CREATE root-[:LINK]->root // no ‘value’ property assigned to root
RETURN root###


Adding values is done by finding the relationship where the new value should be placed in, and replacing it with
a new node, and two relationships to it.

###no-results assertion=add
MATCH root-[:LINK*0..]->before,// before could be same as root
      after-[:LINK*0..]->root, // after could be same as root
      before-[old:LINK]->after
WHERE root.name = 'ROOT'
  AND before.value < 25  // This is the value, which would normally
  AND 25 < after.value   // be supplied through a parameter.
CREATE before-[:LINK]->({value:25})-[:LINK]->after
DELETE old###

Deleting a value, conversely, is done by finding the node with the value, and the two relationships going in and out
from it, and replacing with a new value.

###no-results assertion=delete
MATCH root-[:LINK*0..]->before,
      before-[delBefore:LINK]->del-[delAfter:LINK]->after,
      after-[:LINK*0..]->root
WHERE root.name = 'ROOT'
  AND del.value = 10
CREATE before-[:LINK]->after
DELETE del, delBefore, delAfter###
             """
}
