/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.spi

import java.net.URL

import org.neo4j.cypher.internal.compiler.v3_2.commands.expressions.{Expander, KernelPredicate, UserDefinedAggregator}
import org.neo4j.cypher.internal.compiler.v3_2.pipes.matching.PatternNode
import org.neo4j.cypher.internal.compiler.v3_2.{IndexDescriptor, InternalQueryStatistics}
import org.neo4j.cypher.internal.frontend.v3_2.SemanticDirection
import org.neo4j.graphdb.{Node, Path, PropertyContainer, Relationship}

import scala.collection.Iterator

/*
 * Developer note: This is an attempt at an internal graph database API, which defines a clean cut between
 * two layers, the query engine layer and, for lack of a better name, the core database layer.
 *
 * Building the query engine layer on top of an internal layer means we can move much faster, not
 * having to worry about deprecations and so on. It is also acceptable if this layer is a bit clunkier, in this
 * case we are, for instance, not exposing any node or relationship objects, but provide direct methods for manipulating
 * them by ids instead.
 *
 * The driver for this was clarifying who is responsible for ensuring query isolation. By exposing a query concept in
 * the core layer, we can move that responsibility outside of the scope of cypher.
 */
trait QueryContext extends TokenContext {

  // See QueryContextAdaptation if you need a dummy that overrides all methods as ??? for writing a test

  type EntityAccessor

  def entityAccessor: EntityAccessor

  def transactionalContext: QueryTransactionalContext

  def nodeOps: Operations[Node]

  def relationshipOps: Operations[Relationship]

  def createNode(): Node

  def createRelationship(start: Node, end: Node, relType: String): Relationship

  def createRelationship(start: Long, end: Long, relType: Int): Relationship

  def getOrCreateRelTypeId(relTypeName: String): Int

  def getRelationshipsForIds(node: Node, dir: SemanticDirection, types: Option[Seq[Int]]): Iterator[Relationship]

  def getOrCreateLabelId(labelName: String): Int

  def getLabelsForNode(node: Long): Iterator[Int]

  def isLabelSetOnNode(label: Int, node: Long): Boolean

  def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int

  def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int

  def getPropertiesForNode(node: Long): Iterator[Int]

  def getPropertiesForRelationship(relId: Long): Iterator[Int]

  def getOrCreatePropertyKeyId(propertyKey: String): Int

  def addIndexRule(descriptor: IndexDescriptor): IdempotentResult[IndexDescriptor]

  def dropIndexRule(descriptor: IndexDescriptor)

  def indexSeek(index: IndexDescriptor, values: Seq[Any]): Iterator[Node]

  def indexSeekByRange(index: IndexDescriptor, value: Any): Iterator[Node]

  def indexScanByContains(index: IndexDescriptor, value: String): Iterator[Node]

  def indexScanByEndsWith(index: IndexDescriptor, value: String): Iterator[Node]

  def indexScan(index: IndexDescriptor): Iterator[Node]

  def lockingUniqueIndexSeek(index: IndexDescriptor, value: Any): Option[Node]

  def getNodesByLabel(id: Int): Iterator[Node]

  def getOrCreateFromSchemaState[K, V](key: K, creator: => V): V

  /* return true if the constraint was created, false if preexisting, throws if failed */
  def createNodeKeyConstraint(descriptor: IndexDescriptor): Boolean

  def dropNodeKeyConstraint(descriptor: IndexDescriptor)

  /* return true if the constraint was created, false if preexisting, throws if failed */
  def createUniqueConstraint(descriptor: IndexDescriptor): Boolean

  def dropUniqueConstraint(descriptor: IndexDescriptor)

  /* return true if the constraint was created, false if preexisting, throws if failed */
  def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int): Boolean

  def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int)

  /* return true if the constraint was created, false if preexisting, throws if failed */
  def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int): Boolean

  def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int)

  def getOptStatistics: Option[InternalQueryStatistics] = None

  def getImportURL(url: URL): Either[String,URL]

  /**
   * This should not be used. We'll remove sooner (or later). Don't do it.
   */
  def withAnyOpenQueryContext[T](work: (QueryContext) => T): T

  def relationshipStartNode(rel: Relationship): Node

  def relationshipEndNode(rel: Relationship): Node

  def nodeGetDegree(node: Long, dir: SemanticDirection): Int

  def nodeGetDegree(node: Long, dir: SemanticDirection, relTypeId: Int): Int

  def nodeIsDense(node: Long): Boolean

  // Legacy dependency between kernel and compiler
  def variableLengthPathExpand(node: PatternNode,
                               realNode: Node,
                               minHops: Option[Int],
                               maxHops: Option[Int],
                               direction: SemanticDirection,
                               relTypes: Seq[String]): Iterator[Path]

  def singleShortestPath(left: Node, right: Node, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path],
                         filters: Seq[KernelPredicate[PropertyContainer]]): Option[Path]

  def allShortestPath(left: Node, right: Node, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path],
                      filters: Seq[KernelPredicate[PropertyContainer]]): Iterator[Path]

  def nodeCountByCountStore(labelId: Int): Long

  def relationshipCountByCountStore(startLabelId: Int, typeId: Int, endLabelId: Int): Long

  def lockNodes(nodeIds: Long*)

  def lockRelationships(relIds: Long*)

  def callReadOnlyProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]]

  def callReadWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]]

  def callSchemaWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]]

  def callDbmsProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]]

  def callFunction(name: QualifiedName, args: Seq[Any], allowed: Array[String]): AnyRef

  def aggregateFunction(name: QualifiedName, allowed: Array[String]): UserDefinedAggregator

    // Check if a runtime value is a node, relationship, path or some such value returned from
  // other query context values by calling down to the underlying database
  def isGraphKernelResultValue(v: Any): Boolean

  def detachDeleteNode(node: Node): Int

  def assertSchemaWritesAllowed(): Unit

}

trait Operations[T <: PropertyContainer] {
  def delete(obj: T)

  def setProperty(obj: Long, propertyKeyId: Int, value: Any)

  def removeProperty(obj: Long, propertyKeyId: Int)

  def getProperty(obj: Long, propertyKeyId: Int): Any

  def hasProperty(obj: Long, propertyKeyId: Int): Boolean

  def propertyKeyIds(obj: Long): Iterator[Int]

  def getById(id: Long): T

  def indexGet(name: String, key: String, value: Any): Iterator[T]

  def indexQuery(name: String, query: Any): Iterator[T]

  def isDeletedInThisTx(obj: T): Boolean

  def all: Iterator[T]

  def acquireExclusiveLock(obj: Long): Unit

  def releaseExclusiveLock(obj: Long): Unit
}

trait QueryTransactionalContext {

  type ReadOps
  type DbmsOps

  def readOperations: ReadOps

  def dbmsOperations: DbmsOps

  def isTopLevelTx: Boolean

  def close(success: Boolean)

  def commitAndRestartTx()

  def kernelStatisticProvider: KernelStatisticProvider
}

