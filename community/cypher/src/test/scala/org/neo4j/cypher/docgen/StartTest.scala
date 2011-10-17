/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import org.neo4j.graphdb.{Relationship, Node}
import org.neo4j.graphdb.index.Index
import org.neo4j.cypher.CuteGraphDatabaseService.gds2cuteGds

class StartTest extends DocumentingTestBase {
  def graphDescription = List("A KNOWS B", "A KNOWS C")

  override def indexProps = List("name")

  def section: String = "Start"

  @Test def nodes_by_id() {
    testQuery(
      title = "Node by id",
      text = "Binding a node as a start point is done with the node(*) function .",
      queryText = "start n=node(%A%) return n",
      returns = "The reference node is returned",
      (p) => assertThat(p.columnAs[Node]("n").toList.asJava, hasItem(node("A"))))
  }

  @Test def relationships_by_id() {
    testQuery(
      title = "Relationship by id",
      text = "Binding a relationship as a start point is done with the relationship(*) function, which can also be abbreviated rel(*).",
      queryText = "start r=relationship(0) return r",
      returns = "The relationshop with id 0 is returned",
      (p) => assertThat(p.columnAs[Relationship]("r").toList.asJava, hasItem(rel(0))))
  }

  @Test def multiple_nodes_by_id() {
    testQuery(
      title = "Multiple nodes by id",
      text = "Multiple nodes are selected by listing them separated by commas.",
      queryText = "start n=node(%A%, %B%, %C%) return n",
      returns = "The nodes listed in the START statement.",
      (p) => assertEquals(List(node("A"), node("B"), node("C")), p.columnAs[Node]("n").toList))
  }

  @Test def nodes_by_index() {
    testQuery(
      title = "Node by index lookup",
      text = "If the start point can be found by index lookups, it can be done like this: node:index-name(key = \"value\"). In this example, there exists a node index named 'nodes'.",
      queryText = """start n=node:nodes(name = "A") return n""",
      returns = """The node indexed with name "A" is returned""",
      (p) => assertEquals(List(Map("n" -> node("A"))), p.toList))
  }

  @Test def relationships_by_index() {
    db.inTx(()=>{
      val r = db.getRelationshipById(0)
      val property = "property"
      val value = "some_value"
      r.setProperty(property, value)
      val relIndex: Index[Relationship] = db.index().forRelationships("rels")
      relIndex.add(r, property, value)
    })

    testQuery(
      title = "Relationship by index lookup",
      text = "If the start point can be found by index lookups, it can be done like this: relationship:index-name(key = \"value\"].",
      queryText = """start r=relationship:rels(property = "some_value") return r""",
      returns = """The relationship indexed with property "some_value" is returned""",
      (p) => assertEquals(List(Map("r" -> rel(0))), p.toList))
  }

  @Test def nodes_by_index_query() {
    testQuery(
      title = "Node by index query",
      text = "If the start point can be found by index more complex lucene queries: node:index-name(\"query\")." +
        "This allows you to write more advanced index queries",
      queryText = """start n=node:nodes("name:A") return n""",
      returns = """The node indexed with name "A" is returned""",
      (p) => assertEquals(List(Map("n" -> node("A"))), p.toList))
  }

  @Test def start_with_multiple_nodes() {
    testQuery(
      title = "Multiple start points",
      text = "Sometimes you want to bind multiple start points. Just list them separated by commas.",
      queryText = """start a=node(%A%), b=node(%B%) return a,b""",
      returns = """Both the A and the B node are returned""",
      p => assertEquals(List(Map("a"->node("A"), "b"->node("B"))), p.toList))
  }
}

