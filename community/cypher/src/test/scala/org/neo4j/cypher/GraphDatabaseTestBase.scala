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
package org.neo4j.cypher

import org.junit.{After, Before}
import scala.collection.JavaConverters._
import collection.Map
import org.neo4j.graphdb._
import org.neo4j.test.ImpermanentGraphDatabase
import org.neo4j.kernel.ThreadToStatementContextBridge
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.cypher.internal.helpers.GraphIcing
import org.neo4j.cypher.internal.spi.PlanContext
import org.scalatest.Assertions
import org.neo4j.cypher.internal.spi.gdsimpl.TransactionBoundPlanContext
import org.neo4j.tooling.GlobalGraphOperations
import org.neo4j.kernel.api.DataWriteOperations

class GraphDatabaseTestBase extends GraphIcing with Assertions {

  var graph: GraphDatabaseAPI with Snitch = null
  var refNode: Node = null
  var nodes: List[Node] = null

  @Before
  def baseInit() {
    graph = new ImpermanentGraphDatabase() with Snitch
    refNode = getReferenceNode
  }


  def getReferenceNode = graph.inTx(graph.getReferenceNode)

  def assertInTx(f: => Option[String]) {
    graph.inTx {
      assert(f)
    }
  }

  @After
  def cleanUp() {
    if (graph != null) graph.shutdown()
  }

  def indexNode(n: Node, idxName: String, key: String, value: String) {
    graph.inTx(n.getGraphDatabase.index.forNodes(idxName).add(n, key, value))
  }

  def indexRel(r: Relationship, idxName: String, key: String, value: String) {
    graph.inTx(r.getGraphDatabase.index.forRelationships(idxName).add(r, key, value))
  }

  def nodeId(n: Node) = graph.inTx {
    n.getId
  }

  def relationshipId(r: Relationship) = graph.inTx {
    r.getId
  }

  def labels(n: Node) = graph.inTx {
    n.getLabels.iterator().asScala.map(_.toString).toSet
  }

  def countNodes() = graph.inTx {
    GlobalGraphOperations.at(graph).getAllNodes.asScala.size
  }

  def countRelationships() = graph.inTx {
    GlobalGraphOperations.at(graph).getAllRelationships.asScala.size
  }

  def createNode(): Node = createNode(Map[String, Any]())

  def createNode(name: String): Node = createNode(Map[String, Any]("name" -> name))

  def createNode(props: Map[String, Any]): Node = {
    graph.inTx {
      val node = graph.createNode()

      props.foreach((kv) => node.setProperty(kv._1, kv._2))
      node
    }
  }

  def createLabeledNode(props: Map[String, Any], labels: String*): Node = {
    val n = createNode()

    graph.inTx {
      labels.foreach {
        name => n.addLabel(DynamicLabel.label(name))
      }

      props.foreach {
        case (k, v) => n.setProperty(k, v)
      }
    }

    n
  }

  def createLabeledNode(labels: String*): Node = createLabeledNode(Map[String, Any](), labels: _*)

  def createNode(values: (String, Any)*): Node = createNode(values.toMap)

  def deleteAllEntities() = graph.inTx {
    val relIterator = GlobalGraphOperations.at(graph).getAllRelationships.iterator()

    while (relIterator.hasNext) {
      relIterator.next().delete()
    }

    val nodeIterator = GlobalGraphOperations.at(graph).getAllNodes.iterator()
    while (nodeIterator.hasNext) {
      nodeIterator.next().delete()
    }
  }


  def execStatement[T](f: (DataWriteOperations => T)): T = {
    val tx = graph.beginTx
    val result = f(statement.dataWriteOperations())
    tx.success()
    tx.finish()
    result
  }

  def nodeIds = nodes.map(_.getId).toArray

  val REL = DynamicRelationshipType.withName("REL")

  def relate(a: Node, b: Node): Relationship = relate(a, b, "REL")

  def relate(a: Node, b: Node, pk: (String, Any)*): Relationship = relate(a, b, "REL", pk.toMap)

  def relate(n1: Node, n2: Node, relType: String, name: String): Relationship = relate(n1, n2, relType, Map("name" -> name))

  def relate(a: Node, b: Node, c: Node*) {
    (Seq(a, b) ++ c).reduce((n1, n2) => {
      relate(n1, n2)
      n2
    })
  }

  def relate(n1: Node, n2: Node, relType: String, props: Map[String, Any] = Map()): Relationship = graph.inTx {
    val r = n1.createRelationshipTo(n2, DynamicRelationshipType.withName(relType))

    props.foreach((kv) => r.setProperty(kv._1, kv._2))
    r
  }

  def relate(x: ((String, String), String)): Relationship = graph.inTx {
    x match {
      case ((from, relType), to) => {
        val f = node(from)
        val t = node(to)
        f.createRelationshipTo(t, DynamicRelationshipType.withName(relType))
      }
    }
  }

  def node(name: String): Node = graph.inTx {
    nodes.find(_.getProperty("name") == name).get
  }

  def relType(name: String): RelationshipType = GlobalGraphOperations.at(graph).getAllRelationshipTypes.asScala.find(_.name() == name).get

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

  def statement = graph.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge]).statement()

  def planContext: PlanContext = new TransactionBoundPlanContext(statement, graph)
}

trait DeletedReferenceNode {

  self: GraphDatabaseTestBase =>

  override def getReferenceNode: Node = {
    RichGraph(graph).inTx(graph.getReferenceNode.delete())
    null
  }
}

trait Snitch extends GraphDatabaseAPI {
  val createdNodes = collection.mutable.Queue[Node]()

  abstract override def createNode(): Node = {
    val n = super.createNode()
    createdNodes.enqueue(n)
    n
  }
}
