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
import org.neo4j.graphdb._
import org.neo4j.kernel.{ThreadToStatementContextBridge, GraphDatabaseAPI}
import org.neo4j.kernel.api.StatementContext
import collection.JavaConverters._
import org.neo4j.graphdb.DynamicRelationshipType.withName

class TransactionBoundQueryContext(graph: GraphDatabaseAPI) extends QueryContext {
  val tx: Transaction = graph.beginTx()
  private val ctx: StatementContext = graph
    .getDependencyResolver
    .resolveDependency(classOf[ThreadToStatementContextBridge])
    .getCtxForWriting

  def addLabelsToNode(node: Long, labelIds: Iterable[Long]): Int = labelIds.foldLeft(0) {
    case (count, labelId) => if ( ctx.addLabelToNode(labelId, node) ) count+1 else count 
  }

  def close(success: Boolean) {
    if ( success )
      tx.success()
    else
      tx.failure()
    tx.finish()
  }

  def createNode(): Node =
    graph.createNode()

  def createRelationship(start: Node, end: Node, relType: String) =
    start.createRelationshipTo(end, withName(relType))

  def getLabelName(id: Long) =
    ctx.getLabelName(id)

  def getLabelsForNode(node: Long) =
    ctx.getLabelsForNode(node).asScala.map(_.asInstanceOf[Long])


  override def isLabelSetOnNode(label: Long, node: Long) =
    ctx.isLabelSetOnNode(label, node)

  def getOrCreateLabelId(labelName: String) =
    ctx.getOrCreateLabelId(labelName)

  def getRelationshipsFor(node: Node, dir: Direction, types: Seq[String]) = types match {
    case Seq() => node.getRelationships(dir).asScala
    case _     => node.getRelationships(dir, types.map(withName): _*).asScala
  }


  val nodeOps = new NodeOperations

  val relationshipOps = new RelationshipOperations

  def removeLabelsFromNode(node: Long, labelIds: Iterable[Long]): Int = labelIds.foldLeft(0) {
    case (count, labelId) => if ( ctx.removeLabelFromNode(labelId, node) ) count+1 else count
  }

  def replaceLabelsOfNode(node: Long, labelIds: Iterable[Long]) {
    ???
  }

  class NodeOperations extends BaseOperations[Node] {
    def delete(obj: Node) {
      obj.delete()
    }

    def getById(id: Long) = graph.getNodeById(id)
  }

  class RelationshipOperations extends BaseOperations[Relationship] {
    def delete(obj: Relationship) {
      obj.delete()
    }

    def getById(id: Long) = graph.getRelationshipById(id)
  }

  abstract class BaseOperations[T <: PropertyContainer] extends Operations[T] {
    def getProperty(obj: T, propertyKey: String) = obj.getProperty(propertyKey)

    def hasProperty(obj: T, propertyKey: String) = obj.hasProperty(propertyKey)

    def propertyKeys(obj: T) = obj.getPropertyKeys.asScala

    def removeProperty(obj: T, propertyKey: String) {
      obj.removeProperty(propertyKey)
    }

    def setProperty(obj: T, propertyKey: String, value: Any) {
      obj.setProperty(propertyKey, value)
    }
  }

  def getTransaction = tx
}