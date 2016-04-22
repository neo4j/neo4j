/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.graphdb.{Label, Node, Relationship}
import org.neo4j.kernel.GraphDatabaseQueryService

class CreateTest extends DocumentingTestBase with QueryStatisticsTestSupport with SoftReset {

  def section = "Create"

  @Test def create_single_node() {
    testQuery(
      title = "Create single node",
      text = "Creating a single node is done by issuing the following query.",
      queryText = "CREATE (n)",
      optionalResultExplanation = "Nothing is returned from this query, except the count of affected nodes.",
      assertions = (p) => assertStats(p, nodesCreated = 1))
  }

  @Test def create_multiple_nodes() {
    testQuery(
      title = "Create multiple nodes",
      text = "Creating multiple nodes is done by separating them with a comma.",
      queryText = "CREATE (n), (m)",
      optionalResultExplanation = "",
      assertions = (p) => assertStats(p, nodesCreated = 2))
  }

  @Test def create_single_node_with_label() {
    testQuery(
      title = "Create a node with a label",
      text = "To add a label when creating a node, use the syntax below.",
      queryText = "CREATE (n:Person)",
      optionalResultExplanation = "Nothing is returned from this query.",
      assertions = (p) => assertStats(p, nodesCreated = 1, labelsAdded = 1))
  }

  @Test def create_single_node_with_labels() {
    testQuery(
      title = "Create a node with multiple labels",
      text = "To add labels when creating a node, use the syntax below. In this case, we add two labels.",
      queryText = "CREATE (n:Person:Swedish)",
      optionalResultExplanation = "Nothing is returned from this query.",
      assertions = (p) => assertStats(p, nodesCreated = 1, labelsAdded = 2))
  }

  @Test def create_single_node_with_labels_and_properties() {
    testQuery(
      title = "Create node and add labels and properties",
      text = "When creating a new node with labels, you can add properties at the same time.",
      queryText = "CREATE (n:Person {name: 'Andres', title: 'Developer'})",
      optionalResultExplanation = "Nothing is returned from this query.",
      assertions = (p) => assertStats(p, nodesCreated = 1, propertiesWritten = 2, labelsAdded = 1))
  }

  @Test def create_single_node_and_return_it() {
    testQuery(
      title = "Return created node",
      text = "Creating a single node is done by issuing the following query.",
      queryText = "CREATE (a {name: 'Andres'}) RETURN a",
      optionalResultExplanation = "The newly created node is returned.",
      assertions = (p) => assert(p.size === 1))
  }

  def createTwoPersonNodesWithNames(db: GraphDatabaseQueryService) = {
    db.inTx {
      db.createNode(Label.label("Person")).setProperty("name", "Node A")
      db.createNode(Label.label("Person")).setProperty("name", "Node B")
    }
  }

  @Test def connect_two_nodes_with_a_relationship() {
    prepareAndTestQuery(
      title = "Create a relationship between two nodes",
      text = "To create a relationship between two nodes, we first get the two nodes. " +
        "Once the nodes are loaded, we simply create a relationship between them.",
      queryText = "MATCH (a:Person), (b:Person) WHERE a.name = 'Node A' AND b.name = 'Node B' CREATE (a)-[r:RELTYPE]->(b) RETURN r",
      optionalResultExplanation = "The created relationship is returned by the query.",
      prepare = createTwoPersonNodesWithNames,
      assertions = (p) => assert(p.size === 1))
  }

  @Test def set_property_to_a_collection() {
    val createTwoNodesWithProperty = (db: GraphDatabaseQueryService) => db.inTx {
      db.createNode().setProperty("name", "Andres")
      db.createNode().setProperty("name", "Michael")
    }

    prepareAndTestQuery(
      title = "Set a property to an array",
      text = """When you set a property to an expression that returns a list of values,
Cypher will turn that into an array. All the elements in the list must be of the same type
for this to work.""",
      queryText = "MATCH (n) WHERE exists(n.name) WITH collect(n.name) AS names CREATE (new {name: names}) RETURN new",
      optionalResultExplanation = "A node with an array property named name is returned.",
      prepare = createTwoNodesWithProperty,
      assertions = (p) => {
        val createdNode = p.toList.head("new").asInstanceOf[Node]
        assert(createdNode.getProperty("name") === Array("Andres", "Michael"))
      })
  }

  @Test def create_full_path_in_one_go() {
    testQuery(
      title = "Create a full path",
      text =
        """When you use `CREATE` and a pattern, all parts of the pattern that are not already in scope at this time
will be created. """,
      queryText = "CREATE p = (andres {name: 'Andres'})-[:WORKS_AT]->(neo)<-[:WORKS_AT]-(michael {name: 'Michael'}) return p",
      optionalResultExplanation = "This query creates three nodes and two relationships in one go, assigns it to a path variable, " +
        "and returns it.",
      assertions = (p) => assertStats(p, nodesCreated = 3, relationshipsCreated = 2, propertiesWritten = 2))
  }

  @Test def create_relationship_with_properties() {
    prepareAndTestQuery(
      title = "Create a relationship and set properties",
      text = "Setting properties on relationships is done in a similar manner to how it's done when creating nodes. " +
        "Note that the values can be any expression.",
      queryText = "MATCH (a:Person), (b:Person) WHERE a.name = 'Node A' AND b.name = 'Node B' CREATE (a)-[r:RELTYPE {name: a.name + '<->' + b.name}]->(b) RETURN r",
      optionalResultExplanation = "The newly created relationship is returned by the example query.",
      prepare = createTwoPersonNodesWithNames,
      assertions = (p) => {
        val result = p.toList
        assert(result.size === 1)
        val r = result.head("r").asInstanceOf[Relationship]
        assert(r.getProperty("name") === "Node A<->Node B")
      })
  }

  @Test def create_single_node_from_map() {
    testQuery(
      title = "Create node with a parameter for the properties",
      text = """
You can also create a graph entity from a map.
All the key/value pairs in the map will be set as properties on the created relationship or node.
In this case we add a `Person` label to the node as well.
""",
      parameters = Map("props" -> Map("name" -> "Andres", "position" -> "Developer")),
      queryText = "CREATE (n:Person {props}) RETURN n",
      optionalResultExplanation = "",
      assertions = (p) => assertStats(p, nodesCreated = 1, propertiesWritten = 2, labelsAdded = 1))
  }

  @Test def create_multiple_nodes_from_maps() {
    testQuery(
      title = "Create multiple nodes with a parameter for their properties",
      text = "By providing Cypher an array of maps, it will create a node for each map.",
      parameters = Map("props" -> List(
        Map("name" -> "Andres", "position" -> "Developer"),
        Map("name" -> "Michael", "position" -> "Developer"))),
      queryText = "UNWIND {props} AS map CREATE (n) SET n = map",
      optionalResultExplanation = "",
      assertions = (p) => assertStats(p, nodesCreated = 2, propertiesWritten = 4))
  }
}
