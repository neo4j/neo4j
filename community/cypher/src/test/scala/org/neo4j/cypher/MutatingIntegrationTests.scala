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
package org.neo4j.cypher

import org.junit.Test
import org.junit.Assert._
import collection.JavaConverters._
import org.scalatest.Assertions
import org.neo4j.graphdb.{NotFoundException, Relationship, Node}

class MutatingIntegrationTests extends ExecutionEngineHelper with Assertions {

  val stats = QueryStatistics.empty

  @Test
  def create_a_single_node() {
    val before = graph.getAllNodes.asScala.size

    val result = parseAndExecute("create node a = {}")

    assert(result.queryStatistics() === stats.copy(
      nodesCreated = 1
    ))
    assert(graph.getAllNodes.asScala.size === before + 1)
  }

  @Test
  def create_a_single_node_with_props_and_return_it() {
    val before = graph.getAllNodes.asScala.size

    val result = parseAndExecute("create node a = {name : 'Andres'} return a")

    assert(result.queryStatistics() === stats.copy(
      nodesCreated = 1,
      propertiesSet = 1
    ))
    assert(graph.getAllNodes.asScala.size === before + 1)

    assert(graph.getAllNodes.asScala.size === before + 1)

    val list = result.toList
    assert(list.size === 1)
    val createdNode = list.head("a").asInstanceOf[Node]
    assert(createdNode.getProperty("name") === "Andres")
  }

  @Test
  def start_with_a_node_and_create_a_new_node_with_the_same_properties() {
    createNode("age" -> 15)

    val result = parseAndExecute("start a = node(1) with a create node b = {age : a.age * 2} return b")

    assert(result.queryStatistics() === stats.copy(
      nodesCreated = 1,
      propertiesSet = 1
    ))

    val list = result.toList
    assert(list.size === 1)
    val createdNode = list.head("b").asInstanceOf[Node]
    assert(createdNode.getProperty("age") === 30)
  }

  @Test
  def create_two_nodes_and_a_relationship_between_them() {
    val result = parseAndExecute("create node a = {}, node b = {}, rel a-[r:REL]->b return r")

    assert(result.queryStatistics() === stats.copy(
      nodesCreated = 2,
      relationshipsCreated = 1
    ))

    val list = result.toList
    assert(list.size === 1)

    val createdNode = list.head("r").asInstanceOf[Relationship]
  }

  @Test
  def create_one_node_and_dumpToString() {
    createNode("age" -> 15)

    val result = parseAndExecute("create node a = {name:'Cypher'}")

    assert(result.queryStatistics() === stats.copy(
      nodesCreated = 1,
      propertiesSet = 1
    ))

    val txt = result.dumpToString()
    println(txt)
  }

  @Test
  def deletes_single_node() {
    val a = createNode().getId

    val result = parseAndExecute("start a = node(1) with a delete a")
    assert(result.queryStatistics() === stats.copy(
      deletedNodes = 1
    ))

    assert(result.toList === List())
    intercept[NotFoundException](graph.getNodeById(a))
  }

  @Test
  def multiple_deletes_should_not_break_anything() {
    (1 to 4).foreach( i => createNode() )

    val result = parseAndExecute("start a = node(1),b=node(2,3,4) with a delete a")
    assert(result.queryStatistics() === stats.copy(
      deletedNodes = 1
    ))

    assert(result.toList === List())
  }

  @Test
  def deletes_all_relationships() {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val d = createNode()

    relate(a, b)
    relate(a, c)
    relate(a, d)

    val result = parseAndExecute("start a = node(1) match a-[r]->() with r delete r")
    assert(result.queryStatistics() === stats.copy(
      deletedRelationships = 3
    ))

    assert(a.getRelationships.asScala.size === 0)
  }

  @Test
  def create_multiple_relationships_in_one_query() {
    val a = createNode()
    val b = createNode()
    val c = createNode()

    val result = parseAndExecute("create node n = {} with n start x = node(1,2,3) with n,x create rel n-[:REL]->x")
    val statistics = result.queryStatistics()
    assert(statistics === stats.copy(
      nodesCreated = 1,
      relationshipsCreated = 3
    ))

    assert(a.getRelationships.asScala.size === 1)
    assert(b.getRelationships.asScala.size === 1)
    assert(c.getRelationships.asScala.size === 1)
  }

  @Test
  def set_a_property() {
    val a = createNode("name" -> "Andres")

    val result = parseAndExecute("start n=node(1) with n set n.name = 'Michael'")
    val statistics = result.queryStatistics()
    assert(statistics === stats.copy(
      propertiesSet = 1
    ))

    assert(a.getProperty("name") === "Michael")
  }

  @Test
  def set_a_property_to_an_expression() {
    val a = createNode("name" -> "Andres")

    val result = parseAndExecute("start n=node(1) with n set n.name = n.name + ' was here'")
    val statistics = result.queryStatistics()
    assert(statistics === stats.copy(
      propertiesSet = 1
    ))

    assert(a.getProperty("name") === "Andres was here")
  }

  @Test
  def set_a_property_to_a_collection() {
    createNode("Andres")
    createNode("Michael")
    createNode("Peter")

    val result = parseAndExecute("start n=node(1,2,3) with collect(n.name) as names create node {name : names}")
    val statistics = result.queryStatistics()
    assert(statistics === stats.copy(
      propertiesSet = 1,
      nodesCreated = 1
    ))

    assert(graph.getNodeById(4).getProperty("name") === Array("Andres", "Michael", "Peter"))
  }

  @Test
  def set_a_property_to_an_empty_collection() {
    createNode("Andres")

    val result = parseAndExecute("start n=node(1) with filter(x in collect(n.name) : x = 12) as names create node {x : names}")
    val statistics = result.queryStatistics()
    assert(statistics === stats.copy(
      propertiesSet = 1,
      nodesCreated = 1
    ))

    assert(graph.getNodeById(2).getProperty("x") === Array())
  }

  @Test
  def create_node_from_map_values() {
    parseAndExecute("create node n = {a} return n", "a" -> Map("name" -> "Andres", "age" -> 66))
    val n = graph.createdNodes.dequeue()
    assert(n.getProperty("name") === "Andres")
    assert(n.getProperty("age") === 66)
  }

  @Test
  def set_property_for_null_removes_the_property() {
    val n = createNode("name"->"Michael")
    parseAndExecute("start n = node(1) with n set n.name = null return n")

    assertFalse("Property should have been removed", n.hasProperty("name"))
  }

  @Test
  def create_rel_from_map_values() {
    createNode()
    createNode()

    val r = parseAndExecute("start a = node(1), b = node(2) with a,b create rel a-[r:REL {param}]->b return r", "param" -> Map("name" -> "Andres", "age" -> 66)).
      toList.head("r").asInstanceOf[Relationship]

    assert(r.getProperty("name") === "Andres")
    assert(r.getProperty("age") === 66)
  }

  @Test
  def mark_nodes_in_path() {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    relate(a, b)
    relate(b, c)

    val q = """
start a = node(1), c = node(3)
match p=a-->b-->c
=== p ===
foreach(n in nodes(p) :
  set n.marked = true
)
    """

    parseAndExecute(q)

    assertTrue(a.getProperty("marked").asInstanceOf[Boolean])
    assertTrue(b.getProperty("marked").asInstanceOf[Boolean])
    assertTrue(c.getProperty("marked").asInstanceOf[Boolean])
  }
}