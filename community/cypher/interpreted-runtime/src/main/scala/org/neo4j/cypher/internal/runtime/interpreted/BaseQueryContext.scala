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
package org.neo4j.cypher.internal.runtime.interpreted

import java.net.URL

import org.eclipse.collections.api.iterator.LongIterator
import org.neo4j.cypher.internal.planner.v3_5.spi.{IdempotentResult, IndexDescriptor}
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.v3_5.expressions.SemanticDirection
import org.neo4j.cypher.internal.v3_5.logical.plans.{IndexOrder, QualifiedName}
import org.neo4j.graphdb.{Path, PropertyContainer}
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext
import org.neo4j.internal.kernel.api.{IndexQuery, IndexReference, NodeValueIndexCursor}
import org.neo4j.kernel.impl.api.store.RelationshipIterator
import org.neo4j.kernel.impl.core.EmbeddedProxySPI
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.virtual.{ListValue, MapValue, NodeValue, RelationshipValue}

import scala.collection.Iterator

abstract class BaseQueryContext extends QueryContext {

  def notSupported(): Nothing

  override def entityAccessor: EmbeddedProxySPI = notSupported()

  override def transactionalContext: QueryTransactionalContext = notSupported()

  override def withActiveRead: QueryContext = notSupported()

  override def resources: ResourceManager = notSupported()

  override def nodeOps: Operations[NodeValue] = notSupported()

  override def relationshipOps: Operations[RelationshipValue] = notSupported()

  override def createNode(labels: Array[Int]): NodeValue = notSupported()

  override def createNodeId(labels: Array[Int]): Long = notSupported()

  override def createRelationship(start: Long, end: Long,
                                  relType: Int): RelationshipValue = notSupported()

  override def getOrCreateRelTypeId(relTypeName: String): Int = notSupported()

  override def getRelationshipsForIds(node: Long, dir: SemanticDirection,
                                      types: Option[Array[Int]]): Iterator[RelationshipValue] = notSupported()

  override def getRelationshipsForIdsPrimitive(node: Long,
                                               dir: SemanticDirection,
                                               types: Option[Array[Int]]): RelationshipIterator = notSupported()

  override def getRelationshipsCursor(node: Long, dir: SemanticDirection,
                                      types: Option[Array[Int]]): RelationshipSelectionCursor = notSupported()

  override def getRelationshipFor(relationshipId: Long, typeId: Int, startNodeId: Long,
                                  endNodeId: Long): RelationshipValue = notSupported()

  override def getOrCreateLabelId(labelName: String): Int = notSupported()

  override def isLabelSetOnNode(label: Int, node: Long): Boolean = notSupported()

  override def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int = notSupported()

  override def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int = notSupported()

  override def getOrCreatePropertyKeyId(propertyKey: String): Int = notSupported()

  override def getOrCreatePropertyKeyIds(propertyKeys: Array[String]): Array[Int] = notSupported()

  override def addIndexRule(descriptor: IndexDescriptor): IdempotentResult[IndexReference] = notSupported()

  override def dropIndexRule(descriptor: IndexDescriptor): Unit = notSupported()

  override def indexReference(label: Int, properties: Int*): IndexReference = notSupported()


  override def getNodesByLabel(id: Int): Iterator[NodeValue] = notSupported()

  override def getNodesByLabelPrimitive(id: Int): LongIterator = notSupported()

  override def createNodeKeyConstraint(descriptor: IndexDescriptor): Boolean = notSupported()

  override def dropNodeKeyConstraint(descriptor: IndexDescriptor): Unit = notSupported()

  override def createUniqueConstraint(descriptor: IndexDescriptor): Boolean = notSupported()

  override def dropUniqueConstraint(descriptor: IndexDescriptor): Unit = notSupported()

  override def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int): Boolean = notSupported()

  override def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int): Unit = notSupported()

  override def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int): Boolean = notSupported()

  override def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int): Unit = notSupported()

  override def getImportURL(url: URL): Either[String, URL] = notSupported()

  /**
    * This should not be used. We'll remove sooner (or later). Don't do it.
    */
  override def withAnyOpenQueryContext[T](work: QueryContext => T): T = notSupported()

  override def createNewQueryContext(): QueryContext = notSupported()

  override def nodeIsDense(node: Long): Boolean = notSupported()

  override def asObject(value: AnyValue): AnyRef = notSupported()

  override def variableLengthPathExpand(realNode: Long, minHops: Option[Int],
                                        maxHops: Option[Int],
                                        direction: SemanticDirection,
                                        relTypes: Seq[String]): Iterator[Path] = notSupported()

  override def singleShortestPath(left: Long, right: Long, depth: Int,
                                  expander: Expander,
                                  pathPredicate: KernelPredicate[Path],
                                  filters: Seq[KernelPredicate[PropertyContainer]]): Option[Path] = notSupported()

  override def allShortestPath(left: Long, right: Long, depth: Int,
                               expander: Expander,
                               pathPredicate: KernelPredicate[Path],
                               filters: Seq[KernelPredicate[PropertyContainer]]): Iterator[Path] = notSupported()

  override def nodeCountByCountStore(labelId: Int): Long = notSupported()

  override def relationshipCountByCountStore(startLabelId: Int, typeId: Int, endLabelId: Int): Long = notSupported()

  override def lockNodes(nodeIds: Long*): Unit = notSupported()

  override def lockRelationships(relIds: Long*): Unit = notSupported()

  override def callReadOnlyProcedure(id: Int, args: Seq[Any],
                                     allowed: Array[String],
                                     context: ProcedureCallContext): Iterator[Array[AnyRef]] = notSupported()

  override def callReadOnlyProcedure(name: QualifiedName,
                                     args: Seq[Any],
                                     allowed: Array[String],
                                     context: ProcedureCallContext): Iterator[Array[AnyRef]] = notSupported()

  override def callReadWriteProcedure(id: Int, args: Seq[Any],
                                      allowed: Array[String],
                                      context: ProcedureCallContext): Iterator[Array[AnyRef]] = notSupported()

  override def callReadWriteProcedure(name: QualifiedName,
                                      args: Seq[Any],
                                      allowed: Array[String],
                                      context: ProcedureCallContext): Iterator[Array[AnyRef]] = notSupported()

  override def callSchemaWriteProcedure(id: Int, args: Seq[Any],
                                        allowed: Array[String],
                                        context: ProcedureCallContext): Iterator[Array[AnyRef]] = notSupported()

  override def callSchemaWriteProcedure(name: QualifiedName,
                                        args: Seq[Any],
                                        allowed: Array[String],
                                        context: ProcedureCallContext): Iterator[Array[AnyRef]] = notSupported()

  override def callDbmsProcedure(id: Int, args: Seq[Any],
                                 allowed: Array[String],
                                 context: ProcedureCallContext): Iterator[Array[AnyRef]] = notSupported()

  override def callDbmsProcedure(name: QualifiedName,
                                 args: Seq[Any],
                                 allowed: Array[String],
                                 context: ProcedureCallContext): Iterator[Array[AnyRef]] = notSupported()

  override def callFunction(id: Int, args: Seq[AnyValue],
                            allowed: Array[String]): AnyValue = notSupported()

  override def callFunction(name: QualifiedName,
                            args: Seq[AnyValue],
                            allowed: Array[String]): AnyValue = notSupported()

  override def aggregateFunction(id: Int,
                                 allowed: Array[String]): UserDefinedAggregator = notSupported()

  override def aggregateFunction(name: QualifiedName,
                                 allowed: Array[String]): UserDefinedAggregator = notSupported()

  override def detachDeleteNode(id: Long): Int = notSupported()

  override def assertSchemaWritesAllowed(): Unit = notSupported()

  override def getLabelName(id: Int): String = notSupported()

  override def getOptLabelId(labelName: String): Option[Int] = notSupported()

  override def getLabelId(labelName: String): Int = notSupported()

  override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] = notSupported()

  override def getPropertyKeyId(propertyKeyName: String): Int = notSupported()

  override def getRelTypeName(id: Int): String = notSupported()

  override def getOptRelTypeId(relType: String): Option[Int] = notSupported()

  override def getRelTypeId(relType: String): Int = notSupported()

  override def nodeGetOutgoingDegree(node: Long): Int = notSupported()

  override def nodeGetOutgoingDegree(node: Long, relationship: Int): Int = notSupported()

  override def nodeGetIncomingDegree(node: Long): Int = notSupported()

  override def nodeGetIncomingDegree(node: Long, relationship: Int): Int = notSupported()

  override def nodeGetTotalDegree(node: Long): Int = notSupported()

  override def nodeGetTotalDegree(node: Long, relationship: Int): Int = notSupported()

  override def relationshipGetStartNode(relationship: RelationshipValue): NodeValue = notSupported()

  override def relationshipGetEndNode(relationship: RelationshipValue): NodeValue = notSupported()

  override def getLabelsForNode(id: Long): ListValue = notSupported()

  override def getPropertyKeyName(token: Int): String = notSupported()

  override def nodeAsMap(id: Long): MapValue = notSupported()

  override def relationshipAsMap(id: Long): MapValue = notSupported()

  override def indexSeek[RESULT <: AnyRef](index: IndexReference,
                                           needsValues: Boolean,
                                           indexOrder: IndexOrder,
                                           queries: Seq[IndexQuery]): NodeValueIndexCursor = notSupported()

  override def indexSeekByContains[RESULT <: AnyRef](index: IndexReference,
                                                     needsValues: Boolean,
                                                     indexOrder: IndexOrder,
                                                     value: TextValue): NodeValueIndexCursor = notSupported()

  override def indexSeekByEndsWith[RESULT <: AnyRef](index: IndexReference,
                                                     needsValues: Boolean,
                                                     indexOrder: IndexOrder,
                                                     value: TextValue): NodeValueIndexCursor = notSupported()

  override def indexScan[RESULT <: AnyRef](index: IndexReference,
                                           needsValues: Boolean,
                                           indexOrder: IndexOrder): NodeValueIndexCursor = notSupported()

  override def lockingUniqueIndexSeek[RESULT](index: IndexReference,
                                              queries: Seq[IndexQuery.ExactPredicate]): NodeValueIndexCursor = notSupported()
}
