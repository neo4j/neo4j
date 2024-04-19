/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.planning

import org.eclipse.collections.api.map.primitive.IntObjectMap
import org.eclipse.collections.api.set.primitive.IntSet
import org.neo4j.common.EntityType
import org.neo4j.configuration.Config
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.macros.TranslateExceptionMacros.translateException
import org.neo4j.cypher.internal.macros.TranslateExceptionMacros.translateIterator
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingLongIterator
import org.neo4j.cypher.internal.runtime.ConstraintInfo
import org.neo4j.cypher.internal.runtime.EntityTransformer
import org.neo4j.cypher.internal.runtime.Expander
import org.neo4j.cypher.internal.runtime.IndexInfo
import org.neo4j.cypher.internal.runtime.KernelPredicate
import org.neo4j.cypher.internal.runtime.NodeOperations
import org.neo4j.cypher.internal.runtime.NodeReadOperations
import org.neo4j.cypher.internal.runtime.Operations
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryTransactionalContext
import org.neo4j.cypher.internal.runtime.ReadOperations
import org.neo4j.cypher.internal.runtime.ReadQueryContext
import org.neo4j.cypher.internal.runtime.RelationshipIterator
import org.neo4j.cypher.internal.runtime.RelationshipOperations
import org.neo4j.cypher.internal.runtime.RelationshipReadOperations
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.UserDefinedAggregator
import org.neo4j.cypher.internal.runtime.interpreted.DelegatingQueryTransactionalContext
import org.neo4j.dbms.database.DatabaseContext
import org.neo4j.dbms.database.DatabaseManager
import org.neo4j.graphdb.Entity
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Path
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.PropertyIndexQuery
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor
import org.neo4j.internal.kernel.api.TokenReadSession
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.internal.schema.IndexDescriptor
import org.neo4j.internal.schema.IndexProviderDescriptor
import org.neo4j.internal.schema.IndexType
import org.neo4j.kernel.impl.query.FunctionInformation
import org.neo4j.logging.LogProvider
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Value
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue

import java.net.URL
import scala.collection.Iterator

class ExceptionTranslatingReadQueryContext(val inner: ReadQueryContext) extends ReadQueryContext with ExceptionTranslationSupport {

  override def resources: ResourceManager = inner.resources

  override def transactionalContext =
    new ExceptionTranslatingTransactionalContext(inner.transactionalContext)

  override def getLabelsForNode(node: Long, nodeCursor: NodeCursor): ListValue =
    translateException(tokenNameLookup, inner.getLabelsForNode(node, nodeCursor))

  override def getTypeForRelationship(id: Long, cursor: RelationshipScanCursor): AnyValue =
    translateException(tokenNameLookup, inner.getTypeForRelationship(id, cursor))

  override def getLabelName(id: Int): String =
    translateException(tokenNameLookup, inner.getLabelName(id))

  override def getOptLabelId(labelName: String): Option[Int] =
    translateException(tokenNameLookup, inner.getOptLabelId(labelName))

  override def getLabelId(labelName: String): Int =
    translateException(tokenNameLookup, inner.getLabelId(labelName))

  override val nodeReadOps: NodeReadOperations =
    new ExceptionTranslatingReadOperations[VirtualNodeValue, NodeCursor](inner.nodeReadOps) with NodeReadOperations

  override val relationshipReadOps: RelationshipReadOperations =
    new ExceptionTranslatingReadOperations[VirtualRelationshipValue, RelationshipScanCursor](inner.relationshipReadOps) with RelationshipReadOperations

  override def getPropertyKeyName(propertyKeyId: Int): String =
    translateException(tokenNameLookup, inner.getPropertyKeyName(propertyKeyId))

  override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] =
    translateException(tokenNameLookup, inner.getOptPropertyKeyId(propertyKeyName))

  override def getPropertyKeyId(propertyKey: String): Int =
    translateException(tokenNameLookup, inner.getPropertyKeyId(propertyKey))

  override def getAllIndexes(): Map[IndexDescriptor, IndexInfo] =
    translateException(tokenNameLookup, inner.getAllIndexes())

  override def indexExists(name: String): Boolean =
    translateException(tokenNameLookup, inner.indexExists(name))

  override def constraintExists(name: String): Boolean =
    translateException(tokenNameLookup, inner.constraintExists(name))

  override def constraintExists(matchFn: ConstraintDescriptor => Boolean, entityId: Int, properties: Int*): Boolean =
    translateException(tokenNameLookup, inner.constraintExists(matchFn, entityId, properties: _*))

  override def indexReference(indexType: IndexType, entityId: Int, entityType: EntityType, properties: Int*): IndexDescriptor =
    translateException(tokenNameLookup, inner.indexReference(indexType, entityId, entityType, properties:_*))

  override def lookupIndexReference(entityType: EntityType): IndexDescriptor =
    translateException(tokenNameLookup, inner.lookupIndexReference(entityType))

  override def fulltextIndexReference(entityIds: List[Int], entityType: EntityType, properties: Int*): IndexDescriptor =
    translateException(tokenNameLookup, inner.fulltextIndexReference(entityIds, entityType, properties:_*))

  override def nodeIndexSeek(index: IndexReadSession,
                             needsValues: Boolean,
                             indexOrder: IndexOrder,
                             values: Seq[PropertyIndexQuery]): NodeValueIndexCursor =
    translateException(tokenNameLookup, inner.nodeIndexSeek(index, needsValues, indexOrder, values))

  override def relationshipIndexSeek(index: IndexReadSession,
                                     needsValues: Boolean,
                                     indexOrder: IndexOrder,
                                     values: Seq[PropertyIndexQuery]): RelationshipValueIndexCursor =
    translateException(tokenNameLookup, inner.relationshipIndexSeek(index, needsValues, indexOrder, values))

  override def relationshipIndexSeekByContains(index: IndexReadSession,
                                               needsValues: Boolean,
                                               indexOrder: IndexOrder,
                                               value: TextValue): RelationshipValueIndexCursor =
    translateException(tokenNameLookup, inner.relationshipIndexSeekByContains(index, needsValues, indexOrder, value))

  override def relationshipIndexSeekByEndsWith(index: IndexReadSession,
                                               needsValues: Boolean,
                                               indexOrder: IndexOrder,
                                               value: TextValue): RelationshipValueIndexCursor =
    translateException(tokenNameLookup, inner.relationshipIndexSeekByEndsWith(index, needsValues, indexOrder, value))

  override def relationshipIndexScan(index: IndexReadSession,
                                     needsValues: Boolean,
                                     indexOrder: IndexOrder): RelationshipValueIndexCursor =
    translateException(tokenNameLookup, inner.relationshipIndexScan(index, needsValues, indexOrder))

  override def getNodesByLabel(tokenReadSession: TokenReadSession, id: Int, indexOrder: IndexOrder): ClosingLongIterator =
    translateException(tokenNameLookup, inner.getNodesByLabel(tokenReadSession, id, indexOrder))

  override def nodeAsMap(id: Long, nodeCursor: NodeCursor, propertyCursor: PropertyCursor): MapValue =
    translateException(tokenNameLookup, inner.nodeAsMap(id, nodeCursor, propertyCursor))

  override def relationshipAsMap(id: Long, relationshipCursor: RelationshipScanCursor, propertyCursor: PropertyCursor): MapValue =
    translateException(tokenNameLookup, inner.relationshipAsMap(id, relationshipCursor, propertyCursor))

  override def nodeGetOutgoingDegreeWithMax(maxDegree: Int, node: Long, nodeCursor: NodeCursor): Int = translateException(tokenNameLookup, inner.nodeGetOutgoingDegreeWithMax(maxDegree, node, nodeCursor))

  override def nodeGetOutgoingDegreeWithMax(maxDegree: Int, node: Long, relationship: Int, nodeCursor: NodeCursor): Int = translateException(tokenNameLookup, inner.nodeGetOutgoingDegreeWithMax(maxDegree, node, relationship, nodeCursor))


  override def nodeGetIncomingDegreeWithMax(maxDegree: Int, node: Long, nodeCursor: NodeCursor): Int = translateException(tokenNameLookup, inner.nodeGetIncomingDegreeWithMax(maxDegree, node, nodeCursor))


  override def nodeGetIncomingDegreeWithMax(maxDegree: Int, node: Long, relationship: Int, nodeCursor: NodeCursor): Int = translateException(tokenNameLookup, inner.nodeGetIncomingDegreeWithMax(maxDegree, node, relationship, nodeCursor))


  override def nodeGetTotalDegreeWithMax(maxDegree: Int, node: Long, nodeCursor: NodeCursor): Int = translateException(tokenNameLookup, inner.nodeGetTotalDegreeWithMax(maxDegree, node, nodeCursor))


  override def nodeGetTotalDegreeWithMax(maxDegree: Int, node: Long, relationship: Int, nodeCursor: NodeCursor): Int = translateException(tokenNameLookup, inner.nodeGetTotalDegreeWithMax(maxDegree, node, relationship, nodeCursor))


  override def nodeGetOutgoingDegree(node: Long, nodeCursor: NodeCursor): Int =
    translateException(tokenNameLookup, inner.nodeGetOutgoingDegree(node, nodeCursor))

  override def nodeGetOutgoingDegree(node: Long, relationship: Int, nodeCursor: NodeCursor): Int =
    translateException(tokenNameLookup, inner.nodeGetOutgoingDegree(node, relationship, nodeCursor))

  override def nodeGetIncomingDegree(node: Long, nodeCursor: NodeCursor): Int =
    translateException(tokenNameLookup, inner.nodeGetIncomingDegree(node, nodeCursor))

  override def nodeGetIncomingDegree(node: Long, relationship: Int, nodeCursor: NodeCursor): Int =
    translateException(tokenNameLookup, inner.nodeGetIncomingDegree(node, relationship, nodeCursor))

  override def nodeGetTotalDegree(node: Long, nodeCursor: NodeCursor): Int =
    translateException(tokenNameLookup, inner.nodeGetTotalDegree(node, nodeCursor))

  override def singleNode(id: Long, cursor: NodeCursor): Unit =
    translateException(tokenNameLookup, inner.singleNode(id,cursor))

  override def singleRelationship(id: Long, cursor: RelationshipScanCursor): Unit =
    translateException(tokenNameLookup, inner.singleRelationship(id,cursor))

  override def nodeGetTotalDegree(node: Long, relationship: Int, nodeCursor: NodeCursor): Int =
    translateException(tokenNameLookup, inner.nodeGetTotalDegree(node, relationship, nodeCursor))

  override def getAllConstraints(): Map[ConstraintDescriptor, ConstraintInfo] =
    translateException(tokenNameLookup, inner.getAllConstraints())

  override def callReadOnlyProcedure(id: Int, args: Array[AnyValue], context: ProcedureCallContext): Iterator[Array[AnyValue]] =
    translateIterator(tokenNameLookup, inner.callReadOnlyProcedure(id, args, context))

  override def callReadWriteProcedure(id: Int, args: Array[AnyValue], context: ProcedureCallContext): Iterator[Array[AnyValue]] =
    translateIterator(tokenNameLookup, inner.callReadWriteProcedure(id, args, context))

  override def callSchemaWriteProcedure(id: Int, args: Array[AnyValue], context: ProcedureCallContext): Iterator[Array[AnyValue]] =
    translateIterator(tokenNameLookup, inner.callSchemaWriteProcedure(id, args, context))

  override def callDbmsProcedure(id: Int, args: Array[AnyValue], context: ProcedureCallContext): Iterator[Array[AnyValue]] =
    translateIterator(tokenNameLookup, inner.callDbmsProcedure(id, args, context))

  override def callFunction(id: Int, args: Array[AnyValue]): AnyValue =
    translateException(tokenNameLookup, inner.callFunction(id, args))

  override def callBuiltInFunction(id: Int, args: Array[AnyValue]): AnyValue =
    translateException(tokenNameLookup, inner.callBuiltInFunction(id, args))

  override def aggregateFunction(id: Int): UserDefinedAggregator =
    translateException(tokenNameLookup, inner.aggregateFunction(id))

  override def builtInAggregateFunction(id: Int): UserDefinedAggregator =
    translateException(tokenNameLookup, inner.builtInAggregateFunction(id))

  override def isLabelSetOnNode(label: Int, node: Long, nodeCursor: NodeCursor): Boolean =
    translateException(tokenNameLookup, inner.isLabelSetOnNode(label, node, nodeCursor))

  override def isAnyLabelSetOnNode(labels: Array[Int], node: Long, nodeCursor: NodeCursor): Boolean =
    translateException(tokenNameLookup, inner.isAnyLabelSetOnNode(labels, node, nodeCursor))

  override def isTypeSetOnRelationship(typ: Int, relationship: Long, relationshipCursor: RelationshipScanCursor): Boolean =
    translateException(tokenNameLookup, inner.isTypeSetOnRelationship(typ, relationship, relationshipCursor))

  override def getRelTypeId(relType: String): Int =
    translateException(tokenNameLookup, inner.getRelTypeId(relType))

  override def getRelTypeName(id: Int): String =
    translateException(tokenNameLookup, inner.getRelTypeName(id))

  override def nodeLockingUniqueIndexSeek(index: IndexDescriptor,
                                          values: Seq[PropertyIndexQuery.ExactPredicate]): NodeValueIndexCursor =
    translateException(tokenNameLookup, inner.nodeLockingUniqueIndexSeek(index, values))

  override def getImportURL(url: URL): Either[String, URL] =
    translateException(tokenNameLookup, inner.getImportURL(url))

  override def getRelationshipsForIds(node: Long, dir: SemanticDirection, types: Array[Int]): ClosingLongIterator with RelationshipIterator =
    translateException(tokenNameLookup, inner.getRelationshipsForIds(node, dir, types))

  override def getRelationshipsByType(tokenReadSession: TokenReadSession,  relType: Int, indexOrder: IndexOrder): ClosingLongIterator with RelationshipIterator =
    translateException(tokenNameLookup, inner.getRelationshipsByType(tokenReadSession, relType, indexOrder))

  override def nodeCursor(): NodeCursor = translateException(tokenNameLookup, inner.nodeCursor())

  override def traversalCursor(): RelationshipTraversalCursor = translateException(tokenNameLookup, inner.traversalCursor())

  override def scanCursor(): RelationshipScanCursor = translateException(tokenNameLookup, inner.scanCursor())

  override def relationshipById(relationshipId: Long, startNodeId: Long, endNodeId: Long, typeId: Int): VirtualRelationshipValue =
    translateException(tokenNameLookup, inner.relationshipById(relationshipId, startNodeId, endNodeId, typeId))

  override def nodeIndexSeekByContains(index: IndexReadSession,
                                       needsValues: Boolean,
                                       indexOrder: IndexOrder,
                                       value: TextValue): NodeValueIndexCursor =
    translateException(tokenNameLookup, inner.nodeIndexSeekByContains(index, needsValues, indexOrder, value))

  override def nodeIndexSeekByEndsWith(index: IndexReadSession,
                                       needsValues: Boolean,
                                       indexOrder: IndexOrder,
                                       value: TextValue): NodeValueIndexCursor =
    translateException(tokenNameLookup, inner.nodeIndexSeekByEndsWith(index, needsValues, indexOrder, value))

  override def nodeIndexScan(index: IndexReadSession,
                             needsValues: Boolean,
                             indexOrder: IndexOrder): NodeValueIndexCursor =
    translateException(tokenNameLookup, inner.nodeIndexScan(index, needsValues, indexOrder))

  override def nodeHasCheapDegrees(node: Long, nodeCursor: NodeCursor): Boolean =
    translateException(tokenNameLookup, inner.nodeHasCheapDegrees(node, nodeCursor))

  override def asObject(value: AnyValue): AnyRef =
    translateException(tokenNameLookup, inner.asObject(value))

  override def getTxStateNodePropertyOrNull(nodeId: Long,
                                            propertyKey: Int): Value =
    translateException(tokenNameLookup, inner.getTxStateNodePropertyOrNull(nodeId, propertyKey))

  override def getTxStateRelationshipPropertyOrNull(relId: Long, propertyKey: Int): Value =
    translateException(tokenNameLookup, inner.getTxStateRelationshipPropertyOrNull(relId, propertyKey))

  override def singleShortestPath(left: Long, right: Long, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path], filters: Seq[KernelPredicate[Entity]], memoryTracker: MemoryTracker): Option[Path] =
    translateException(tokenNameLookup, inner.singleShortestPath(left, right, depth, expander, pathPredicate, filters, memoryTracker))

  override def allShortestPath(left: Long, right: Long, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path], filters: Seq[KernelPredicate[Entity]], memoryTracker: MemoryTracker): ClosingIterator[Path] =
    translateException(tokenNameLookup, inner.allShortestPath(left, right, depth, expander, pathPredicate, filters, memoryTracker))

  override def nodeCountByCountStore(labelId: Int): Long =
    translateException(tokenNameLookup, inner.nodeCountByCountStore(labelId))

  override def relationshipCountByCountStore(startLabelId: Int, typeId: Int, endLabelId: Int): Long =
    translateException(tokenNameLookup, inner.relationshipCountByCountStore(startLabelId, typeId, endLabelId))

  override def lockNodes(nodeIds: Long*): Unit =
    translateException(tokenNameLookup, inner.lockNodes(nodeIds:_*))

  override def lockRelationships(relIds: Long*): Unit =
    translateException(tokenNameLookup, inner.lockRelationships(relIds:_*))

  override def getOptRelTypeId(relType: String): Option[Int] =
    translateException(tokenNameLookup, inner.getOptRelTypeId(relType))

  override def assertShowIndexAllowed(): Unit = translateException(tokenNameLookup, inner.assertShowIndexAllowed())

  override def assertShowConstraintAllowed(): Unit = translateException(tokenNameLookup, inner.assertShowConstraintAllowed())

  override def contextWithNewTransaction(): QueryContext = new ExceptionTranslatingQueryContext(inner.contextWithNewTransaction())

  override def systemGraph: GraphDatabaseService = translateException(tokenNameLookup, inner.systemGraph)

  override def logProvider: LogProvider = translateException(tokenNameLookup, inner.logProvider)

  override def providedLanguageFunctions(): Seq[FunctionInformation] = translateException(tokenNameLookup, inner.providedLanguageFunctions)

  override def entityTransformer: EntityTransformer = translateException(tokenNameLookup, inner.entityTransformer)

  override def close(): Unit = inner.close()

  class ExceptionTranslatingReadOperations[T, CURSOR](inner: ReadOperations[T, CURSOR])
    extends ReadOperations[T, CURSOR] {

    override def getById(id: Long): T =
      translateException(tokenNameLookup, inner.getById(id))

    override def getProperty(id: Long, propertyKeyId: Int, cursor: CURSOR, propertyCursor: PropertyCursor, throwOnDeleted: Boolean): Value =
      translateException(tokenNameLookup, inner.getProperty(id, propertyKeyId, cursor, propertyCursor, throwOnDeleted))

    override def hasProperty(id: Long, propertyKeyId: Int, cursor: CURSOR, propertyCursor: PropertyCursor): Boolean =
      translateException(tokenNameLookup, inner.hasProperty(id, propertyKeyId, cursor, propertyCursor))

    override def propertyKeyIds(id: Long, cursor: CURSOR, propertyCursor: PropertyCursor): Array[Int] =
      translateException(tokenNameLookup, inner.propertyKeyIds(id, cursor, propertyCursor))

    override def all: ClosingLongIterator =
      translateException(tokenNameLookup, inner.all)

    override def isDeletedInThisTx(id: Long): Boolean =
      translateException(tokenNameLookup, inner.isDeletedInThisTx(id))

    override def entityExists(id: Long): Boolean =
      translateException(tokenNameLookup, inner.entityExists(id))

    override def getTxStateProperty(obj: Long, propertyKeyId: Int): Value =
      translateException(tokenNameLookup, inner.getTxStateProperty(obj, propertyKeyId))

    override def hasTxStatePropertyForCachedProperty(entityId: Long, propertyKeyId: Int): Option[Boolean] =
      translateException(tokenNameLookup, inner.hasTxStatePropertyForCachedProperty(entityId, propertyKeyId))

    override def acquireExclusiveLock(obj: Long): Unit =
      translateException(tokenNameLookup, inner.acquireExclusiveLock(obj))

    override def releaseExclusiveLock(obj: Long): Unit =
      translateException(tokenNameLookup, inner.releaseExclusiveLock(obj))
  }

  class ExceptionTranslatingTransactionalContext(inner: QueryTransactionalContext) extends DelegatingQueryTransactionalContext(inner) {
    override def close(): Unit = translateException(tokenNameLookup, super.close())

    override def rollback(): Unit = translateException(tokenNameLookup, super.rollback())
  }
}

class ExceptionTranslatingQueryContext(override val inner: QueryContext) extends ExceptionTranslatingReadQueryContext(inner)
  with QueryContext with ExceptionTranslationSupport {

  override def createParallelQueryContext(): QueryContext = {
    new ExceptionTranslatingQueryContext(inner.createParallelQueryContext())
  }

  override def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int =
    translateException(tokenNameLookup, inner.setLabelsOnNode(node, labelIds))

  override def createNodeId(labels: Array[Int]): Long =
    translateException(tokenNameLookup, inner.createNodeId(labels))

  override val nodeWriteOps: NodeOperations =
    new ExceptionTranslatingOperations[VirtualNodeValue, NodeCursor](inner.nodeWriteOps) with NodeOperations

  override val relationshipWriteOps: RelationshipOperations =
    new ExceptionTranslatingOperations[VirtualRelationshipValue, RelationshipScanCursor](inner.relationshipWriteOps) with RelationshipOperations

  override def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int =
    translateException(tokenNameLookup, inner.removeLabelsFromNode(node, labelIds))

  override def getOrCreateLabelId(labelName: String): Int =
    translateException(tokenNameLookup, inner.getOrCreateLabelId(labelName))

  override def getOrCreateTypeId(relTypeName: String): Int =
    translateException(tokenNameLookup, inner.getOrCreateTypeId(relTypeName))

  override def getOrCreatePropertyKeyId(propertyKey: String): Int =
    translateException(tokenNameLookup, inner.getOrCreatePropertyKeyId(propertyKey))

  override def getOrCreatePropertyKeyIds(propertyKeys: Array[String]): Array[Int] =
    translateException(tokenNameLookup, inner.getOrCreatePropertyKeyIds(propertyKeys))

  override def addBtreeIndexRule(entityId: Int, entityType: EntityType, propertyKeyIds: Seq[Int], name: Option[String], provider: Option[String], indexConfig: IndexConfig): IndexDescriptor =
    translateException(tokenNameLookup, inner.addBtreeIndexRule(entityId, entityType, propertyKeyIds, name, provider, indexConfig))

  override def addRangeIndexRule(entityId: Int, entityType: EntityType, propertyKeyIds: Seq[Int], name: Option[String], provider: Option[IndexProviderDescriptor]): IndexDescriptor =
    translateException(tokenNameLookup, inner.addRangeIndexRule(entityId, entityType, propertyKeyIds, name, provider))

  override def addLookupIndexRule(entityType: EntityType, name: Option[String], provider: Option[IndexProviderDescriptor]): IndexDescriptor =
    translateException(tokenNameLookup, inner.addLookupIndexRule(entityType, name, provider))

  override def addFulltextIndexRule(entityIds: List[Int], entityType: EntityType, propertyKeyIds: Seq[Int], name: Option[String], provider: Option[IndexProviderDescriptor], indexConfig: IndexConfig): IndexDescriptor =
    translateException(tokenNameLookup, inner.addFulltextIndexRule(entityIds, entityType, propertyKeyIds, name, provider, indexConfig))

  override def addTextIndexRule(entityId: Int, entityType: EntityType, propertyKeyIds: Seq[Int], name: Option[String], provider: Option[IndexProviderDescriptor]): IndexDescriptor =
    translateException(tokenNameLookup, inner.addTextIndexRule(entityId, entityType, propertyKeyIds, name, provider))

  override def addPointIndexRule(entityId: Int, entityType: EntityType, propertyKeyIds: Seq[Int], name: Option[String], provider: Option[IndexProviderDescriptor], indexConfig: IndexConfig): IndexDescriptor =
    translateException(tokenNameLookup, inner.addPointIndexRule(entityId, entityType, propertyKeyIds, name, provider, indexConfig))

  override def dropIndexRule(labelId: Int, propertyKeyIds: Seq[Int]): Unit =
    translateException(tokenNameLookup, inner.dropIndexRule(labelId, propertyKeyIds))

  override def dropIndexRule(name: String): Unit =
    translateException(tokenNameLookup, inner.dropIndexRule(name))

  override def createNodeKeyConstraint(labelId: Int, propertyKeyIds: Seq[Int], name: Option[String], provider: Option[String], indexConfig: IndexConfig): Unit =
    translateException(tokenNameLookup, inner.createNodeKeyConstraint(labelId, propertyKeyIds, name, provider, indexConfig))

  override def dropNodeKeyConstraint(labelId: Int, propertyKeyIds: Seq[Int]): Unit =
    translateException(tokenNameLookup, inner.dropNodeKeyConstraint(labelId, propertyKeyIds))

  override def createUniqueConstraint(labelId: Int, propertyKeyIds: Seq[Int], name: Option[String], provider: Option[String], indexConfig: IndexConfig): Unit =
    translateException(tokenNameLookup, inner.createUniqueConstraint(labelId, propertyKeyIds, name, provider, indexConfig))

  override def dropUniqueConstraint(labelId: Int, propertyKeyIds: Seq[Int]): Unit =
    translateException(tokenNameLookup, inner.dropUniqueConstraint(labelId, propertyKeyIds))

  override def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int, name: Option[String]): Unit =
    translateException(tokenNameLookup, inner.createNodePropertyExistenceConstraint(labelId, propertyKeyId, name))

  override def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int): Unit =
    translateException(tokenNameLookup, inner.dropNodePropertyExistenceConstraint(labelId, propertyKeyId))

  override def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int, name: Option[String]): Unit =
    translateException(tokenNameLookup, inner.createRelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId, name))

  override def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int): Unit =
    translateException(tokenNameLookup, inner.dropRelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId))

  override def dropNamedConstraint(name: String): Unit =
    translateException(tokenNameLookup, inner.dropNamedConstraint(name))

  override def createRelationshipId(start: Long, end: Long, relType: Int): Long =
    translateException(tokenNameLookup, inner.createRelationshipId(start, end, relType))

  override def getOrCreateRelTypeId(relTypeName: String): Int =
    translateException(tokenNameLookup, inner.getOrCreateRelTypeId(relTypeName))

  override def detachDeleteNode(node: Long): Int =
    translateException(tokenNameLookup, inner.detachDeleteNode(node))

  override def assertSchemaWritesAllowed(): Unit = translateException(tokenNameLookup, inner.assertSchemaWritesAllowed())

  override def getDatabaseManager: DatabaseManager[DatabaseContext] = translateException(tokenNameLookup, inner.getDatabaseManager)

  override def getConfig: Config = translateException(tokenNameLookup, inner.getConfig)

  override def nodeApplyChanges(id: Long,
                                addedLabels: IntSet,
                                removedLabels: IntSet,
                                properties: IntObjectMap[Value]): Unit =
    translateException(tokenNameLookup, inner.nodeApplyChanges(id, addedLabels, removedLabels, properties))

  override def relationshipApplyChanges(relationship: Long,
                                        properties: IntObjectMap[Value]): Unit =
    translateException(tokenNameLookup, inner.relationshipApplyChanges(relationship, properties))

  class ExceptionTranslatingOperations[T, CURSOR](inner: Operations[T, CURSOR])
    extends ExceptionTranslatingReadOperations[T, CURSOR](inner) with Operations[T, CURSOR] {

    override def delete(id: Long): Boolean =
      translateException(tokenNameLookup, inner.delete(id))

    override def setProperty(id: Long, propertyKey: Int, value: Value): Unit =
      translateException(tokenNameLookup, inner.setProperty(id, propertyKey, value))

    override def setProperties(obj: Long,
                               properties: IntObjectMap[Value]): Unit = translateException(tokenNameLookup, inner.setProperties(obj, properties))

    override def removeProperty(id: Long, propertyKeyId: Int): Boolean =
      translateException(tokenNameLookup, inner.removeProperty(id, propertyKeyId))
  }
}
