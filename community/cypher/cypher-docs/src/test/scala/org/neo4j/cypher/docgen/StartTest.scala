/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import scala.collection.JavaConverters._
import org.junit.Assert.assertThat
import org.junit.Assert.assertEquals
import org.junit.matchers.JUnitMatchers.hasItem
import org.junit.Test
import org.neo4j.graphdb.{ Relationship, Node }
import org.neo4j.graphdb.index.Index

class StartTest extends DocumentingTestBase {
  override def graphDescription = List("A KNOWS B", "A KNOWS C")
  override def indexProps = List("name")

  def section: String = "Start"

  @Test def nodes_by_id() {
    testQuery(
      title = "Node by id",
      text = "Binding a node as a starting point is done with the `node(*)` function. \n" +
        "[NOTE]\n" +
        "Neo4j reuses its internal ids when nodes and relationships are deleted, " +
        "which means it's bad practice to refer to them this way. " +
        "Instead, use application generated ids.",
      queryText = "start n=node(%A%) return n",
      optionalResultExplanation = "The corresponding node is returned.",
      (p) => assertThat(p.columnAs[Node]("n").toList.asJava, hasItem(node("A"))))
  }

  @Test def relationships_by_id() {
    testQuery(
      title = "Relationship by id",
      text = "Binding a relationship as a starting point is done with the `relationship(*)` function, which can also be abbreviated `rel(*)`." +
        " See <<start-node-by-id>> for more information on Neo4j ids.",
      queryText = "start r=relationship(0) return r",
      optionalResultExplanation = "The relationship with id +0+ is returned.",
      (p) => assertThat(p.columnAs[Relationship]("r").toList.asJava, hasItem(rel(0))))
  }

  @Test def multiple_nodes_by_id() {
    testQuery(
      title = "Multiple nodes by id",
      text = "Multiple nodes are selected by listing them separated by commas.",
      queryText = "start n=node(%A%, %B%, %C%) return n",
      optionalResultExplanation = "This returns the nodes listed in the `START` statement.",
      (p) => assertEquals(List(node("A"), node("B"), node("C")), p.columnAs[Node]("n").toList))
  }

  @Test def all_the_nodes() {
    testQuery(
      title = "All nodes",
      text = """
To get all the nodes, use an asterisk.
This can be done with relationships as well.

TIP: The preferred way to do this is to use a `MATCH` clause, see <<match-get-all-nodes>> in <<query-match>> for how to do that.
""",
      queryText = "start n=node(*) return n",
      optionalResultExplanation = "This query returns all the nodes in the graph.",
      (p) => assertEquals(List(node("A"), node("B"), node("C")), p.columnAs[Node]("n").toList))
  }

  @Test def nodes_by_index() {
    generateConsole = false
    testQuery(
      title = "Node by index lookup",
      text = "When the starting point can be found by using index lookups, it can be done like this: `node:index-name(key = \"value\")`. In this example, there exists a node index named `nodes`.",
      queryText = """start n=node:nodes(name = "A") return n""",
      optionalResultExplanation = """The query returns the node indexed with the name "+A+".""",
      (p) => assertEquals(List(Map("n" -> node("A"))), p.toList))
  }

  @Test def relationships_by_index() {
    generateConsole = false
    db.inTx {
      val r = db.getRelationshipById(0)
      val property = "name"
      val value = "Andrés"
      r.setProperty(property, value)
      val relIndex: Index[Relationship] = db.index().forRelationships("rels")
      relIndex.add(r, property, value)
    }

    // TODO this should be changed to use the standard graph for this section somehow.
    testQuery(
      title = "Relationship by index lookup",
      text = "When the starting point can be found by using index lookups, it can be done like this: `relationship:index-name(key = \"value\")`.",
      queryText = """start r=relationship:rels(name = "Andrés") return r""",
      optionalResultExplanation = """The relationship indexed with the +name+ property set to "+Andrés+" is returned by the query.""",
      (p) => assertEquals(List(Map("r" -> rel(0))), p.toList))
  }

  @Test def nodes_by_index_query() {
    generateConsole = false
    testQuery(
      title = "Node by index query",
      text = "When the starting point can be found by more complex Lucene queries, this is the syntax to use: `node:index-name(\"query\")`." +
        "This allows you to write more advanced index queries.",
      queryText = """start n=node:nodes("name:A") return n""",
      optionalResultExplanation = """The node indexed with name "A" is returned by the query.""",
      (p) => assertEquals(List(Map("n" -> node("A"))), p.toList))
  }

  @Test def start_with_multiple_nodes() {
    testQuery(
      title = "Multiple starting points",
      text = "Sometimes you want to bind multiple starting points. Just list them separated by commas.",
      queryText = """start a=node(%A%), b=node(%B%) return a,b""",
      optionalResultExplanation = """Both the nodes +A+ and the +B+  are returned.""",
      p => assertEquals(List(Map("a" -> node("A"), "b" -> node("B"))), p.toList))
  }
}
