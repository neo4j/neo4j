/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.spi.v1_9

import org.neo4j.cypher.internal.compiler.v1_9.spi.{Operations, QueryContext}
import org.neo4j.graphdb._
import org.neo4j.graphdb.DynamicRelationshipType.withName
import collection.JavaConverters._
import java.lang.{Iterable=>JIterable}
import org.neo4j.tooling.GlobalGraphOperations
import org.neo4j.cypher.EntityNotFoundException
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.impl.core.{ThreadToStatementContextBridge, NodeManager}
import org.neo4j.kernel.impl.api.KernelStatement

class GDSBackedQueryContext(graph: GraphDatabaseService) extends QueryContext {

  def close() {}

  def createNode(): Node = graph.createNode

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
        obj.getProperty(propertyKey, null)
      }

      def hasProperty(obj: Node, propertyKey: String): Boolean = {
        obj.hasProperty(propertyKey)
      }

      def propertyKeys(obj: Node): Iterable[String] = {
        obj.getPropertyKeys.asScala
      }

      def getById(id: Long): Node =
        try {
          graph.getNodeById(id)
        } catch {
          case e: NotFoundException => throw new EntityNotFoundException(e.getMessage)
        }

      def indexGet(name: String, key: String, value: Any): Iterator[Node] =
        graph.index.forNodes(name).get(key, value).iterator().asScala

      def indexQuery(name: String, query: Any): Iterator[Node] =
        graph.index.forNodes(name).query(query).iterator().asScala

      def all: Iterator[Node] =
        GlobalGraphOperations.at(graph).getAllNodes.iterator().asScala

      def isDeleted(node: Node): Boolean = {
        val nodeManager: ThreadToStatementContextBridge = graph.asInstanceOf[GraphDatabaseAPI].getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge])

        val statement : KernelStatement = nodeManager.getKernelTransactionBoundToThisThread( true ).acquireStatement().asInstanceOf[KernelStatement]
        statement.hasTxStateWithChanges && statement.txState().nodeIsDeletedInThisTx(node.getId)
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
        obj.getProperty(propertyKey, null)

      def hasProperty(obj: Relationship, propertyKey: String): Boolean =
        obj.hasProperty(propertyKey)

      def propertyKeys(obj: Relationship): Iterable[String] =
        obj.getPropertyKeys.asScala

      def getById(id: Long): Relationship = try {
        graph.getRelationshipById(id)
      } catch {
        case e: NotFoundException => throw new EntityNotFoundException(e.getMessage)
      }

      def indexGet(name: String, key: String, value: Any): Iterator[Relationship] =
        graph.index.forRelationships(name).get(key, value).iterator().asScala

      def indexQuery(name: String, query: Any): Iterator[Relationship] =
        graph.index.forRelationships(name).query(query).iterator().asScala

      def all: Iterator[Relationship] =
        GlobalGraphOperations.at(graph).getAllRelationships.iterator().asScala

      def isDeleted(rel: Relationship): Boolean = {
        val nodeManager: ThreadToStatementContextBridge = graph.asInstanceOf[GraphDatabaseAPI].getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge])

        val statement : KernelStatement = nodeManager.getKernelTransactionBoundToThisThread( true ).acquireStatement().asInstanceOf[KernelStatement]
        statement.hasTxStateWithChanges && statement.txState().relationshipIsDeletedInThisTx(rel.getId)
      }
    }
  }
}
