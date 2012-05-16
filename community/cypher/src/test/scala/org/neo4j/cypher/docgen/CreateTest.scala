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
package org.neo4j.cypher.docgen

import org.junit.Test
import org.neo4j.cypher.CuteGraphDatabaseService.gds2cuteGds
import org.neo4j.graphdb.{Node, Relationship}

class CreateTest extends DocumentingTestBase {
  def graphDescription = List()

  def section = "Create"

  @Test def create_single_node() {
    testQuery(
      title = "Create single node",
      text = "Creating a single node is done by issuing the following query.",
      queryText = "create n",
      returns = "Nothing is returned from this query, except the count of affected nodes.",
      assertions = (p) => {})
  }

  @Test def create_single_node_with_properties() {
    testQuery(
      title = "Create single node and set properties",
      text = "The values for the properties can be any scalar expressions.",
      queryText = "create (n {name : 'Andres', title : 'Developer'})",
      returns = "Nothing is returned from this query.",
      assertions = (p) => {})
  }

  @Test def create_single_node_and_return_it() {
    testQuery(
      title = "Return created node",
      text = "Creating a single node is done by issuing the following query.",
      queryText = "create (a {name : 'Andres'}) return a",
      returns = "The newly created node is returned.",
      assertions = (p) => assert(p.size === 1)
    )
  }

  def createTwoNodes: (Long, Long) = {
    db.inTx(() => {
      val a = db.createNode()
      val b = db.createNode()

      (a.getId, b.getId)
    })
  }

  @Test def connect_two_nodes_with_a_relationship() {
    val (aId, bId) = createTwoNodes

    testQuery(
      title = "Create a relationship between two nodes",
      text = "To create a relationship between two nodes, we first get the two nodes. " +
        "Once the nodes are loaded, we simply create a relationship between them.",
      queryText = "start a=node(" + aId + "), b=node(" + bId + ") create a-[r:REL]->b return r",
      returns = "The created relationship is returned.",
      assertions = (p) => assert(p.size === 1)
    )
  }

  @Test def set_property_to_an_iterable() {
    val (aId, bId) = db.inTx(() => {
      val a = db.createNode()
      val b = db.createNode()

      a.setProperty("name", "Andres")
      b.setProperty("name", "Michael")

      (a.getId, b.getId)
    })

    testQuery(
      title = "Set a property to an array",
      text = """When you set a property to an expression that returns a collection of values,
Cypher will turn that into an array. All the elements in the collection must be of the same type
for this to work.""",
      queryText = "start n = node(" + aId + "," + bId + ") with collect(n.name) as names create (new { name : names }) return new",
      returns = "A node with an array property named name is returned.",
      assertions = (p) => {
        val createdNode = p.toList.head("new").asInstanceOf[Node]
        assert(createdNode.getProperty("name") === Array("Andres", "Michael"))
      }
    )
  }


  @Test def create_relationship_with_properties() {
    val (aId, bId) = db.inTx(() => {
      val a = db.createNode()
      val b = db.createNode()

      a.setProperty("name", "Andres")
      b.setProperty("name", "Michael")

      (a.getId, b.getId)
    })

    testQuery(
      title = "Create a relationship and set properties",
      text = "Setting properties on relationships is done in a similar manner to how it's done when creating nodes." +
        "Note that the values can be any expression.",
      queryText = "start a=node(" + aId + "), b=node(" + bId + ") create a-[r:REL {name : a.name + '<->' + b.name }]->b return r",
      returns = "The newly created relationship is returned.",
      assertions = (p) => {
        val result = p.toList
        assert(result.size === 1)
        val r = result.head("r").asInstanceOf[Relationship]
        assert(r.getProperty("name") === "Andres<->Michael")
      }
    )
  }
}