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
package org.neo4j.cypher.internal.spi.gdsimpl

import org.neo4j.cypher.internal.spi.{Operations, QueryContext}
import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.DynamicRelationshipType.withName
import collection.JavaConverters._

class GDSBackedQueryContext(graph: GraphDatabaseService) extends QueryContext {

  def close {}

  def createNode: Node = graph.createNode

  def createRelationship(start: Node, end: Node, relType: String): Relationship =
    start.createRelationshipTo(end, withName(relType))

  def getRelationshipsFor(node: Node, dir: Direction, types: Seq[String]): Iterable[Relationship] =
    if (types.isEmpty) {
      node.getRelationships(dir).asScala
    } else {
      node.getRelationships(dir, types.map(withName): _*).asScala
    }


  def nodeOps: Operations[Node] = {
    new Operations[Node] {
      def delete(obj: Node) {
        obj.delete()
      }

      def setProperty(obj: Node, propertyKey: String, value: Any) {
        obj.setProperty(propertyKey, value)
      }

      def removeProperty(obj: Node, propertyKey: String) {
        obj.removeProperty(propertyKey)
      }

      def getProperty(obj: Node, propertyKey: String): Any = {
        obj.getProperty(propertyKey)
      }

      def hasProperty(obj: Node, propertyKey: String): Boolean = {
        obj.hasProperty(propertyKey)
      }

      def propertyKeys(obj: Node): Iterable[String] = {
        obj.getPropertyKeys.asScala
      }

      def getById(id: Long): Node = {
        graph.getNodeById(id)
      }
    }
  }

  def relationshipOps: Operations[Relationship] = {
    new Operations[Relationship] {
      def delete(obj: Relationship) {
        obj.delete()
      }

      def setProperty(obj: Relationship, propertyKey: String, value: Any) {
        obj.setProperty(propertyKey, value)
      }

      def removeProperty(obj: Relationship, propertyKey: String) {
        obj.removeProperty(propertyKey)
      }

      def getProperty(obj: Relationship, propertyKey: String): Any =
        obj.getProperty(propertyKey)

      def hasProperty(obj: Relationship, propertyKey: String): Boolean =
        obj.hasProperty(propertyKey)

      def propertyKeys(obj: Relationship): Iterable[String] =
        obj.getPropertyKeys.asScala

      def getById(id: Long): Relationship =
        graph.getRelationshipById(id)
    }
  }
}