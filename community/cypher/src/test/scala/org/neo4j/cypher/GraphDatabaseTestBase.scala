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

import org.neo4j.kernel.AbstractGraphDatabase
import org.neo4j.test.ImpermanentGraphDatabase
import org.junit.{After, Before}
import org.neo4j.graphdb.{RelationshipType, DynamicRelationshipType, Relationship, Node}
import scala.collection.JavaConverters._
import org.scalatest.junit.JUnitSuite
import collection.Map

class GraphDatabaseTestBase extends JUnitSuite {
  var graph: AbstractGraphDatabase = null
  var refNode: Node = null
  var nodes: List[Node] = null

  @Before
  def baseInit() {
    graph = new ImpermanentGraphDatabase()
    refNode = graph.getReferenceNode
  }

  @After
  def cleanUp() {
    if (graph != null) graph.shutdown()
  }

  def indexNode(n: Node, idxName: String, key: String, value: String) {
    inTx(() => n.getGraphDatabase.index.forNodes(idxName).add(n, key, value))
  }
  def indexRel(r: Relationship, idxName: String, key: String, value: String) {
    inTx(() => r.getGraphDatabase.index.forRelationships(idxName).add(r, key, value))
  }

  def createNode(): Node = createNode(Map[String, Any]())

  def createNode(name: String): Node = createNode(Map[String, Any]("name" -> name))

  def createNode(props: Map[String, Any]): Node = {
    inTx(() => {
      val node = graph.createNode()

      props.foreach((kv) => node.setProperty(kv._1, kv._2))
      node
    }).asInstanceOf[Node]
  }

  def createNode(values: (String, Any)*): Node = createNode(values.toMap)

  def inTx[T](f: () => T): T = {
    val tx = graph.beginTx

    val result = f.apply()

    tx.success()
    tx.finish()

    result
  }

  def nodeIds = nodes.map(_.getId).toArray

  def relate(a: Node, b: Node): Relationship = relate(a, b, "REL")

  def relate(n1: Node, n2: Node, relType: String, name: String): Relationship = relate(n1, n2, relType, Map("name" -> name))

  def relate(n1: Node, n2: Node, relType: String, props: Map[String, Any] = Map()): Relationship = {
    inTx(() => {
      val r = n1.createRelationshipTo(n2, DynamicRelationshipType.withName(relType))

      props.foreach((kv) => r.setProperty(kv._1, kv._2))
      r
    })
  }

  def relate(x: ((String, String), String)): Relationship = inTx(() => {
    x match {
      case ((from, relType), to) => {
        val f = node(from)
        val t = node(to)
        f.createRelationshipTo(t, DynamicRelationshipType.withName(relType))
      }
    }
  })

  def node(name: String): Node = nodes.find(_.getProperty("name") == name).get

  def relType(name: String): RelationshipType = graph.getRelationshipTypes.asScala.find(_.name() == name).get

  def createNodes(names: String*): List[Node] = {
    nodes = names.map(x => createNode(Map("name" -> x))).toList
    nodes
  }

  def createDiamond(): (Node, Node, Node, Node) = {
    //    Graph:
    //             (a)
    //             / \
    //            v   v
    //          (b)  (c)
    //           \   /
    //            v v
    //            (d)

    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    val d = createNode("d")

    relate(a, b)
    relate(b, d)
    relate(a, c)
    relate(c, d)
    (a, b, c, d)
  }
}