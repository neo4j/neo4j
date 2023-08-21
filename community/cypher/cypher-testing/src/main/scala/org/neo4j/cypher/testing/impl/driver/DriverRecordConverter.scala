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
package org.neo4j.cypher.testing.impl.driver

import org.neo4j.cypher.testing.api
import org.neo4j.driver.Value
import org.neo4j.driver.types.MapAccessor
import org.neo4j.driver.types.Node
import org.neo4j.driver.types.Path
import org.neo4j.driver.types.Point
import org.neo4j.driver.types.Relationship

import java.util

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala

object DriverRecordConverter {

  def convertValue(driverValue: Value): AnyRef = {
    val value = driverValue.asObject
    convertValue(value)
  }

  private def convertValue(driverValue: Any): AnyRef = driverValue match {
    case null                  => null
    case value: Node           => convertNode(value)
    case value: Relationship   => convertRelationship(value)
    case value: Path           => convertPath(value)
    case _: Point              => throw new IllegalStateException("Point type is not supported yet")
    case value: util.Map[_, _] => convertMap(value.asInstanceOf[util.Map[String, AnyRef]])
    case value: util.List[_]   => convertList(value.asInstanceOf[util.List[AnyRef]])
    case value                 => value.asInstanceOf[AnyRef]
  }

  private def convertNode(driverValue: Node): api.Node = {
    val labels = driverValue.labels.asScala.toList
    val properties = convertMap(driverValue)
    api.Node(driverValue.id, labels, properties)
  }

  private def convertRelationship(driverValue: Relationship): api.Relationship = {
    val properties = convertMap(driverValue)
    api.Relationship(driverValue.id, driverValue.`type`, properties, driverValue.startNodeId, driverValue.endNodeId)
  }

  private def convertMap(driverValue: MapAccessor): Map[String, AnyRef] =
    driverValue.keys.asScala.map(key => (key, convertValue(driverValue.get(key)))).toMap

  private def convertPath(driverValue: Path): api.Path = {
    val startNode = convertNode(driverValue.start())

    val connections = driverValue.asScala.toList.map(segment => {
      // Segment always starts at start node and ends at end node, no matter direction of relationship
      // Direction of relationship is determined as the relationship and segment sharing start node
      val relationship = segment.relationship
      val outgoing = relationship.startNodeId == segment.start.id
      val convertedRelationship = convertRelationship(relationship)
      val endNode = convertNode(segment.end)
      if (outgoing) {
        api.Outgoing(convertedRelationship, endNode)
      } else {
        api.Incoming(convertedRelationship, endNode)
      }
    })

    api.Path(startNode, connections)
  }

  private def convertMap(driverValue: util.Map[String, AnyRef]): Map[String, AnyRef] =
    driverValue.asScala.map { case (key, value) => (key, convertValue(value)) }.toMap

  private def convertList(driverValue: util.List[AnyRef]): Seq[AnyRef] =
    driverValue.asScala.map(convertValue).toList

}
