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
package org.neo4j.cypher.testing.impl.embedded

import org.neo4j.cypher.testing.api
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Path
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.spatial.Point

import java.util

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala

object EmbeddedRecordConverter {

  def convertMap(embeddedValue: util.Map[String, AnyRef]): Map[String, AnyRef] =
    embeddedValue.asScala.map { case (key, value) => (key, convertValue(value)) }.toMap

  private def convertValue(embeddedValue: AnyRef): AnyRef = embeddedValue match {
    case null                  => null
    case value: Node           => convertNode(value)
    case value: Relationship   => convertRelationship(value)
    case value: Path           => convertPath(value)
    case _: Point              => throw new IllegalStateException("Point type is not supported yet")
    case value: util.Map[_, _] => convertMap(value.asInstanceOf[util.Map[String, AnyRef]])
    case value: util.List[_]   => convertList(value.asInstanceOf[util.List[AnyRef]])
    case value                 => value.asInstanceOf[AnyRef]
  }

  private def convertList(embeddedValue: util.List[AnyRef]): Seq[AnyRef] =
    embeddedValue.asScala.toList.map(convertValue)

  private def convertNode(embeddedValue: Node): api.Node = {
    val properties = convertMap(embeddedValue.getAllProperties)
    val labels = embeddedValue.getLabels.asScala.toList.map(_.name)
    api.Node(embeddedValue.getId, labels, properties)
  }

  private def convertRelationship(embeddedValue: Relationship): api.Relationship = {
    val properties = convertMap(embeddedValue.getAllProperties)
    val relType = embeddedValue.getType.name
    api.Relationship(embeddedValue.getId, relType, properties, embeddedValue.getStartNodeId, embeddedValue.getEndNodeId)
  }

  private def convertPath(embeddedValue: Path): api.Path = {
    val startNode = convertNode(embeddedValue.startNode)
    var latestNode = startNode

    val connections = embeddedValue.relationships.asScala.toList.map(embeddedRelationship => {
      val relationship = convertRelationship(embeddedRelationship)
      if (embeddedRelationship.getStartNodeId == latestNode.id) {
        latestNode = convertNode(embeddedRelationship.getEndNode)
        api.Outgoing(relationship, latestNode)
      } else {
        latestNode = convertNode(embeddedRelationship.getStartNode)
        api.Incoming(relationship, latestNode)
      }
    })

    api.Path(startNode, connections)
  }
}
