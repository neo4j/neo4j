/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.spec.graphtemplate

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.mutable.ArrayBuffer

case class TemplateId(id: Int)

object TemplateId {

  trait Generator {
    def apply(): TemplateId
  }

  object Generator {

    def sequential: Generator = {
      val iter = Iterator.iterate(1)(_ + 1).map(TemplateId(_))
      () => iter.next()
    }
  }
}

case class NodeTemplate(
  id: TemplateId,
  name: Option[String],
  labels: Seq[String]
)

case class RelationshipTemplate(
  id: TemplateId,
  name: Option[String],
  relType: Option[String],
  from: TemplateId,
  to: TemplateId,
  directedness: Directedness
)

sealed trait Directedness

object Directedness {
  case object Directed extends Directedness
  case object Undirected extends Directedness
}

class GraphTemplate() {
  private val idGen = TemplateId.Generator.sequential
  private val nodes = ArrayBuffer.empty[NodeTemplate]
  private val rels = ArrayBuffer.empty[RelationshipTemplate]

  private var lastId: TemplateId = _

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: GraphTemplate => this.nodes.equals(other.nodes) && this.rels.equals(other.rels)
      case _                    => false
    }
  }

  def getId = lastId

  def addNode(name: Option[String], labels: Seq[String]): GraphTemplate = {
    name.foreach { name =>
      if (nodes.exists(_.name.contains(name))) {
        throw new IllegalArgumentException(s"Node with name $name already exists in template")
      }
    }
    lastId = idGen()
    nodes += NodeTemplate(lastId, name, labels)
    this
  }

  def addNode(name: String, labels: String*): GraphTemplate = addNode(Some(name), labels)

  def addNode(): GraphTemplate = addNode(None, Seq.empty)

  def addNode(name: String): GraphTemplate = addNode(Some(name), Seq.empty)

  def addNode(labels: Seq[String]): GraphTemplate = addNode(None, labels)

  def addRel(name: String, pair: (String, String)): GraphTemplate = {
    val (from, to) = pair
    addRel(
      nodes.find(_.name.contains(from)).get.id,
      nodes.find(_.name.contains(to)).get.id,
      Some(name),
      None,
      Directedness.Directed
    )
  }

  def addRel(name: String, pair: (String, String), relType: String): GraphTemplate = {
    val (from, to) = pair
    addRel(
      nodes.find(_.name.contains(from)).get.id,
      nodes.find(_.name.contains(to)).get.id,
      Some(name),
      Some(relType),
      Directedness.Directed
    )
  }

  def addRel(pair: (String, String)): GraphTemplate = {
    val (from, to) = pair
    addRel(
      nodes.find(_.name.contains(from)).get.id,
      nodes.find(_.name.contains(to)).get.id,
      None,
      None,
      Directedness.Directed
    )
  }

  def addRel(from: TemplateId, to: TemplateId): GraphTemplate =
    addRel(from, to, Directedness.Directed)

  def addRel(from: TemplateId, to: TemplateId, dir: Directedness): GraphTemplate =
    addRel(from, to, None, None, dir)

  def addRel(from: TemplateId, to: TemplateId, name: String, relType: String): GraphTemplate =
    addRel(from, to, name, relType, Directedness.Directed)

  def addRel(from: TemplateId, to: TemplateId, name: String, relType: String, dir: Directedness): GraphTemplate =
    addRel(from, to, Some(name), Some(relType), dir)

  def addRel(
    from: TemplateId,
    to: TemplateId,
    name: Option[String],
    relType: Option[String],
    dir: Directedness
  ): GraphTemplate = {
    name.foreach { name =>
      if (rels.exists(_.name.contains(name))) {
        throw new IllegalArgumentException(s"Relationship with name $name already exists in template")
      }
    }
    lastId = idGen()
    rels += RelationshipTemplate(lastId, name, relType, from, to, dir)
    this
  }

  def instantiate[Node, Rel](instantiator: TemplateInstantiator[Node, Rel]): InstantiatedGraph[Node, Rel] = {
    val nodesByNameBuilder = Map.newBuilder[String, Node]
    val nodesById = {
      val builder = Map.newBuilder[TemplateId, Node]
      for (node <- nodes) {
        val n = instantiator.createNode(node.labels)
        builder += (node.id -> n)
        node.name.foreach(name => nodesByNameBuilder += (name -> n))
      }
      builder.result()
    }

    val relsByNameBuilder = Map.newBuilder[String, Rel]
    val allRels = {
      val builder = Seq.newBuilder[Rel]
      for (rel <- rels) {
        val r = instantiator.createRel(nodesById(rel.from), nodesById(rel.to), rel.relType)
        builder += r
        rel.name.foreach(name => relsByNameBuilder += (name -> r))
      }
      builder.result()
    }

    InstantiatedGraph(
      nodesByNameBuilder.result(),
      nodesById.values.toSeq,
      relsByNameBuilder.result(),
      allRels
    )
  }
}

class GraphTemplateTest extends CypherFunSuite {

  test("node label propagated") {
    val template = new GraphTemplate()
      .addNode("a", "L1", "L2")

    val labels = template.instantiate(new Graph())
      .node("a")
      .labels

    labels should contain only ("L1", "L2")
  }

  test("rel type propagated") {
    val template = new GraphTemplate()
      .addNode("a")
      .addNode("b")
      .addRel("r", "a" -> "b", "R1")

    val relType = template.instantiate(new Graph())
      .rel("r")
      .relType

    relType shouldBe Some("R1")
  }

  test("rel from propagated") {
    val template = new GraphTemplate()
      .addNode("a")
      .addNode("b")
      .addRel("r", "a" -> "b")

    val res = template.instantiate(new Graph())

    res.rel("r").from shouldBe res.node("a")
  }

  test("rel to propagated") {
    val template = new GraphTemplate()
      .addNode("a")
      .addNode("b")
      .addRel("r", "a" -> "b")

    val res = template.instantiate(new Graph())

    res.rel("r").to shouldBe res.node("b")
  }

  private class Node(val labels: Seq[String])

  private class Rel(val from: Node, val to: Node, val relType: Option[String])

  private class Graph extends TemplateInstantiator[Node, Rel] {
    val nodes = ArrayBuffer.empty[Node]
    val rels = ArrayBuffer.empty[Rel]

    def createNode(labels: Seq[String]): Node = {
      val n = new Node(labels)
      nodes += n
      n
    }

    def createRel(from: Node, to: Node, relType: Option[String]): Rel = {
      val r = new Rel(from, to, relType)
      rels += r
      r
    }
  }
}
