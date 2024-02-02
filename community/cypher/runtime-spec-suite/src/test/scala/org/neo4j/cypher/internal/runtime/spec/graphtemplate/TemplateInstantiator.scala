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

import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.Transaction

trait TemplateInstantiator[Node, Rel] {
  def createNode(labels: Seq[String]): Node
  def createRel(from: Node, to: Node, relType: Option[String]): Rel
}

case class InstantiatedGraph[Node, Rel](
  namedNodes: Map[String, Node],
  nodes: Seq[Node],
  namedRels: Map[String, Rel],
  rels: Seq[Rel]
) {
  def node(name: String): Node = namedNodes(name)
  def rel(name: String): Rel = namedRels(name)
}

class TransactionTemplateInstantiator(tx: Transaction, defaultRelType: String = "R")
    extends TemplateInstantiator[Node, Relationship] {

  def createNode(labels: Seq[String]): Node =
    tx.createNode(labels.map(Label.label): _*)

  def createRel(from: Node, to: Node, relType: Option[String]): Relationship =
    from.createRelationshipTo(to, RelationshipType.withName(relType.getOrElse(defaultRelType)))
}
