/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime

import java.net.URL

import org.neo4j.collection.primitive.PrimitiveLongIterator
import org.neo4j.cypher.internal.planner.v3_4.spi.{IdempotentResult, IndexDescriptor, KernelStatisticProvider, TokenContext}
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection
import org.neo4j.cypher.internal.v3_4.logical.plans.QualifiedName
import org.neo4j.graphdb.{Node, Path, PropertyContainer}
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor
import org.neo4j.internal.kernel.api.{CursorFactory, IndexReference, Read, Write, _}
import org.neo4j.kernel.api.dbms.DbmsOperations
import org.neo4j.kernel.impl.api.store.RelationshipIterator
import org.neo4j.kernel.impl.core.EmbeddedProxySPI
import org.neo4j.kernel.impl.factory.DatabaseInfo
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value
import org.neo4j.values.virtual.{ListValue, NodeValue, RelationshipValue}

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

  def entityAccessor: EmbeddedProxySPI

  def transactionalContext: QueryTransactionalContext

  def withActiveRead: QueryContext

  def resources: CloseableResource

  def nodeOps: Operations[NodeValue]

  def relationshipOps: Operations[RelationshipValue]

  def createNode(): Node

  def createNodeId(): Long

  def createRelationship(start: Long, end: Long, relType: Int): RelationshipValue

  def getOrCreateRelTypeId(relTypeName: String): Int

  def getRelationshipsForIds(node: Long, dir: SemanticDirection, types: Option[Array[Int]]): Iterator[RelationshipValue]

  def getRelationshipsForIdsPrimitive(node: Long, dir: SemanticDirection, types: Option[Array[Int]]): RelationshipIterator

  def getRelationshipsCursor(node: Long, dir: SemanticDirection, types: Option[Array[Int]]): RelationshipSelectionCursor

  def getRelationshipFor(relationshipId: Long, typeId: Int, startNodeId: Long, endNodeId: Long): RelationshipValue

  def getOrCreateLabelId(labelName: String): Int

  def getLabelsForNode(node: Long): ListValue

  def isLabelSetOnNode(label: Int, node: Long): Boolean

  def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int

  def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int

  def getOrCreatePropertyKeyId(propertyKey: String): Int

  def addIndexRule(descriptor: IndexDescriptor): IdempotentResult[IndexReference]

  def dropIndexRule(descriptor: IndexDescriptor)

  def indexReference(label: Int, properties: Int*): IndexReference

  def indexSeek(index: IndexReference, queries: Seq[IndexQuery]): Iterator[NodeValue]

  def indexScanByContains(index: IndexReference, value: String): Iterator[NodeValue]

  def indexScanByEndsWith(index: IndexReference, value: String): Iterator[NodeValue]

  def indexScan(index: IndexReference): Iterator[NodeValue]

  def indexScanPrimitive(index: IndexReference): PrimitiveLongIterator

  def lockingUniqueIndexSeek(index: IndexReference, queries: Seq[IndexQuery.ExactPredicate]): Option[NodeValue]

  def getNodesByLabel(id: Int): Iterator[NodeValue]

  def getNodesByLabelPrimitive(id: Int): PrimitiveLongIterator

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

  def getOptStatistics: Option[QueryStatistics] = None

  def getImportURL(url: URL): Either[String,URL]

  /**
   * This should not be used. We'll remove sooner (or later). Don't do it.
   */
  def withAnyOpenQueryContext[T](work: (QueryContext) => T): T

  /*
  This is an ugly hack to get multi threading to work
   */
  def createNewQueryContext(): QueryContext

  def edgeGetStartNode(edge: RelationshipValue): NodeValue

  def edgeGetEndNode(edge: RelationshipValue): NodeValue

  def nodeGetDegree(node: Long, dir: SemanticDirection): Int

  def nodeGetDegree(node: Long, dir: SemanticDirection, relTypeId: Int): Int

  def nodeIsDense(node: Long): Boolean

  def asObject(value: AnyValue): Any

  // Legacy dependency between kernel and compiler
  def variableLengthPathExpand(realNode: Long, minHops: Option[Int], maxHops: Option[Int], direction: SemanticDirection, relTypes: Seq[String]): Iterator[Path]

  def singleShortestPath(left: Long, right: Long, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path],
                         filters: Seq[KernelPredicate[PropertyContainer]]): Option[Path]

  def allShortestPath(left: Long, right: Long, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path],
                      filters: Seq[KernelPredicate[PropertyContainer]]): Iterator[Path]

  def nodeCountByCountStore(labelId: Int): Long

  def relationshipCountByCountStore(startLabelId: Int, typeId: Int, endLabelId: Int): Long

  def lockNodes(nodeIds: Long*)

  def lockRelationships(relIds: Long*)

  def callReadOnlyProcedure(id: Int, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]]
  def callReadOnlyProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]]

  def callReadWriteProcedure(id: Int, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]]
  def callReadWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]]

  def callSchemaWriteProcedure(id: Int, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]]
  def callSchemaWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]]

  def callDbmsProcedure(id: Int, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]]
  def callDbmsProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]]

  def callFunction(id: Int, args: Seq[AnyValue], allowed: Array[String]): AnyValue
  def callFunction(name: QualifiedName, args: Seq[AnyValue], allowed: Array[String]): AnyValue

  def aggregateFunction(id: Int, allowed: Array[String]): UserDefinedAggregator
  def aggregateFunction(name: QualifiedName, allowed: Array[String]): UserDefinedAggregator

    // Check if a runtime value is a node, relationship, path or some such value returned from
  // other query context values by calling down to the underlying database
  def isGraphKernelResultValue(v: Any): Boolean

  def detachDeleteNode(id: Long): Int

  def assertSchemaWritesAllowed(): Unit

}

trait Operations[T] {
  def delete(id: Long)

  def setProperty(obj: Long, propertyKeyId: Int, value: Value)

  def removeProperty(obj: Long, propertyKeyId: Int)

  def getProperty(obj: Long, propertyKeyId: Int): Value

  def hasProperty(obj: Long, propertyKeyId: Int): Boolean

  def propertyKeyIds(obj: Long): Iterator[Int]

  def getById(id: Long): T

  def isDeletedInThisTx(id: Long): Boolean

  def all: Iterator[T]

  def allPrimitive: PrimitiveLongIterator

  def acquireExclusiveLock(obj: Long): Unit

  def releaseExclusiveLock(obj: Long): Unit

  def getByIdIfExists(id: Long): Option[T]
}

trait QueryTransactionalContext extends CloseableResource {

  def cursors : CursorFactory

  def dataRead: Read

  def stableDataRead: Read

  def markAsStable(): Unit

  def tokenRead: TokenRead

  def schemaRead: SchemaRead

  def dataWrite: Write

  def dbmsOperations: DbmsOperations

  def isTopLevelTx: Boolean

  def close(success: Boolean)

  def commitAndRestartTx()

  def kernelStatisticProvider: KernelStatisticProvider

  def databaseInfo: DatabaseInfo
}

trait KernelPredicate[T] {
  def test(obj: T): Boolean
}

trait Expander {
  def addRelationshipFilter(newFilter: KernelPredicate[PropertyContainer]): Expander
  def addNodeFilter(newFilter: KernelPredicate[PropertyContainer]): Expander
  def nodeFilters: Seq[KernelPredicate[PropertyContainer]]
  def relFilters: Seq[KernelPredicate[PropertyContainer]]
}

trait UserDefinedAggregator {
  def update(args: IndexedSeq[Any]): Unit
  def result: Any
}

trait CloseableResource {
  def close(success: Boolean)
}

