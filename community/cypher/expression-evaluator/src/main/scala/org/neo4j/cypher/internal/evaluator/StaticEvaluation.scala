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
package org.neo4j.cypher.internal.evaluator

import org.eclipse.collections.api.map.primitive.IntObjectMap
import org.eclipse.collections.api.set.primitive.IntSet
import org.neo4j.common.EntityType
import org.neo4j.configuration.Config
import org.neo4j.csv.reader.CharReadable
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.runtime.ClosingLongIterator
import org.neo4j.cypher.internal.runtime.ConstraintInfo
import org.neo4j.cypher.internal.runtime.ConstraintInformation
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.EntityTransformer
import org.neo4j.cypher.internal.runtime.IndexInfo
import org.neo4j.cypher.internal.runtime.IndexInformation
import org.neo4j.cypher.internal.runtime.NodeOperations
import org.neo4j.cypher.internal.runtime.NodeReadOperations
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryTransactionalContext
import org.neo4j.cypher.internal.runtime.RelationshipIterator
import org.neo4j.cypher.internal.runtime.RelationshipOperations
import org.neo4j.cypher.internal.runtime.RelationshipReadOperations
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.dbms.database.DatabaseContext
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.internal.kernel.api.Procedures
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.PropertyIndexQuery
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor
import org.neo4j.internal.kernel.api.TokenReadSession
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext
import org.neo4j.internal.kernel.api.procs.UserAggregationReducer
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.internal.schema.IndexDescriptor
import org.neo4j.internal.schema.IndexProviderDescriptor
import org.neo4j.internal.schema.IndexType
import org.neo4j.internal.schema.constraints.PropertyTypeSet
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.exceptions.Status.HasStatus
import org.neo4j.kernel.api.index.IndexUsageStats
import org.neo4j.kernel.impl.query.FunctionInformation
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.logging.InternalLogProvider
import org.neo4j.scheduler.JobScheduler
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Value
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.MapValueBuilder
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue

import java.net.URI

object StaticEvaluation {

  def from(procedures: Procedures) = new StaticEvaluator(() => new SimplifiedStaticQueryContext(procedures))

  class StaticEvaluator(makeQueryContext: () => QueryContext) extends SimpleInternalExpressionEvaluator {

    override def queryState(nExpressionSlots: Int, slottedParams: Array[AnyValue]) = new QueryState(
      query = makeQueryContext(),
      resources = null,
      params = slottedParams,
      cursors = null,
      queryIndexes = Array.empty,
      selectivityTrackerStorage = SimpleInternalExpressionEvaluator.alwaysNewSelectivityTrackerStorage,
      nodeLabelTokenReadSession = None,
      relTypeTokenReadSession = None,
      expressionVariables = new Array(nExpressionSlots),
      subscriber = QuerySubscriber.DO_NOTHING_SUBSCRIBER,
      queryMemoryTracker = null,
      memoryTrackerForOperatorProvider = null
    )

    override def evaluate(expression: Expression, params: MapValue, context: CypherRow): AnyValue = {
      try {
        super.evaluate(expression, params, context)
      } catch {
        case e: EvaluationException =>
          // all errors in expression evaluation are wrapped in generic EvaluationException,
          // let's see if there is a more interesting error wrapped in it (interesting means an error with a status code in this context) .
          var unwrapped: Throwable = e
          while (unwrapped.isInstanceOf[EvaluationException])
            unwrapped = unwrapped.getCause
          if (unwrapped != null && unwrapped.isInstanceOf[HasStatus]) {
            throw unwrapped
          } else if (
            unwrapped != null && unwrapped.isInstanceOf[IllegalStateException] && unwrapped.getMessage.startsWith(
              "Unknown field"
            )
          ) {
            throw unwrapped
          } else {
            // there isn't an exception with a status wrapped in the EvaluationException,
            // so let's throw the origin exception
            throw e
          }
      }
    }
  }

  private class SimplifiedStaticQueryContext(procedures: Procedures) extends EmptyQueryContext {

    override def callFunction(id: Int, args: Array[AnyValue], ctx: ProcedureCallContext): AnyValue =
      procedures.functionCall(id, args, ctx)

    override def callBuiltInFunction(id: Int, args: Array[AnyValue], ctx: ProcedureCallContext): AnyValue = {
      procedures.builtInFunctionCall(id, args, ctx)
    }
  }

  private def notAvailable(): Nothing =
    throw new RuntimeException("Operation not available in static context.")

  private trait EmptyQueryContext extends QueryContext {

    override def transactionalContext: QueryTransactionalContext = notAvailable()

    override def resources: ResourceManager = notAvailable()

    override def nodeReadOps: NodeReadOperations = notAvailable()

    override def relationshipReadOps: RelationshipReadOperations = notAvailable()

    override def nodeWriteOps: NodeOperations = notAvailable()

    override def relationshipWriteOps: RelationshipOperations = notAvailable()

    override def createNodeId(labels: Array[Int]): Long = notAvailable()

    override def createRelationshipId(start: Long, end: Long, relType: Int): Long = notAvailable()

    override def getOrCreateRelTypeId(relTypeName: String): Int = notAvailable()

    override def nodeCursor(): NodeCursor = notAvailable()

    override def nodeLabelIndexCursor(): NodeLabelIndexCursor = notAvailable()

    override def relationshipTypeIndexCursor(): RelationshipTypeIndexCursor = notAvailable()

    override def traversalCursor(): RelationshipTraversalCursor = notAvailable()

    override def scanCursor(): RelationshipScanCursor = notAvailable()

    override def getRelationshipsForIds(
      node: Long,
      dir: SemanticDirection,
      types: Array[Int]
    ): ClosingLongIterator with RelationshipIterator = notAvailable()

    override def getRelationshipsByType(
      tokenReadSession: TokenReadSession,
      relType: Int,
      indexOrder: IndexOrder
    ): ClosingLongIterator with RelationshipIterator = notAvailable()

    override def relationshipById(id: Long, startNode: Long, endNode: Long, `type`: Int): VirtualRelationshipValue =
      notAvailable()

    override def relationshipAsMap(
      relationship: VirtualRelationshipValue,
      relationshipCursor: RelationshipScanCursor,
      propertyCursor: PropertyCursor,
      builder: MapValueBuilder,
      seenTokens: IntSet
    ): MapValue = notAvailable()

    override def getOrCreateLabelId(labelName: String): Int = notAvailable()

    override def getOrCreateTypeId(relTypeName: String): Int = notAvailable()

    override def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int = notAvailable()

    override def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int = notAvailable()

    override def getOrCreatePropertyKeyId(propertyKey: String): Int = notAvailable()

    override def getOrCreatePropertyKeyIds(propertyKeys: Array[String]): Array[Int] = notAvailable()

    override def validateIndexProvider(
      schemaDescription: String,
      providerString: String,
      indexType: IndexType
    ): IndexProviderDescriptor = notAvailable()

    override def addRangeIndexRule(
      entityId: Int,
      entityType: EntityType,
      propertyKeyIds: Seq[Int],
      name: Option[String],
      provider: Option[IndexProviderDescriptor]
    ): IndexDescriptor = notAvailable()

    override def addLookupIndexRule(
      entityType: EntityType,
      name: Option[String],
      provider: Option[IndexProviderDescriptor]
    ): IndexDescriptor = notAvailable()

    override def addFulltextIndexRule(
      entityIds: List[Int],
      entityType: EntityType,
      propertyKeyIds: Seq[Int],
      name: Option[String],
      provider: Option[IndexProviderDescriptor],
      indexConfig: IndexConfig
    ): IndexDescriptor = notAvailable()

    override def addTextIndexRule(
      entityId: Int,
      entityType: EntityType,
      propertyKeyIds: Seq[Int],
      name: Option[String],
      provider: Option[IndexProviderDescriptor]
    ): IndexDescriptor = notAvailable()

    override def addPointIndexRule(
      entityId: Int,
      entityType: EntityType,
      propertyKeyIds: Seq[Int],
      name: Option[String],
      provider: Option[IndexProviderDescriptor],
      indexConfig: IndexConfig
    ): IndexDescriptor = notAvailable()

    override def addVectorIndexRule(
      entityId: Int,
      entityType: EntityType,
      propertyKeyIds: Seq[Int],
      name: Option[String],
      provider: Option[IndexProviderDescriptor],
      indexConfig: IndexConfig
    ): IndexDescriptor = notAvailable()

    override def dropIndexRule(name: String): Unit = notAvailable()

    override def getAllIndexes(): Map[IndexDescriptor, IndexInfo] = notAvailable()

    override def getIndexUsageStatistics(index: IndexDescriptor): IndexUsageStats = notAvailable()

    override def getIndexInformation(name: String): IndexInformation = notAvailable()

    override def getIndexInformation(index: IndexDescriptor): IndexInformation = notAvailable()

    override def indexExists(name: String): Boolean = notAvailable()

    override def constraintExists(name: String): Boolean = notAvailable()

    override def constraintExists(matchFn: ConstraintDescriptor => Boolean, entityId: Int, properties: Int*): Boolean =
      notAvailable()

    override def indexReference(
      indexType: IndexType,
      entityId: Int,
      entityType: EntityType,
      properties: Int*
    ): IndexDescriptor = notAvailable()

    override def lookupIndexReference(entityType: EntityType): IndexDescriptor = notAvailable()

    override def fulltextIndexReference(
      entityIds: List[Int],
      entityType: EntityType,
      properties: Int*
    ): IndexDescriptor = notAvailable()

    override def nodeIndexSeek(
      index: IndexReadSession,
      needsValues: Boolean,
      indexOrder: IndexOrder,
      queries: Seq[PropertyIndexQuery]
    ): NodeValueIndexCursor = notAvailable()

    override def nodeIndexSeekByContains(
      index: IndexReadSession,
      needsValues: Boolean,
      indexOrder: IndexOrder,
      value: TextValue
    ): NodeValueIndexCursor = notAvailable()

    override def nodeIndexSeekByEndsWith(
      index: IndexReadSession,
      needsValues: Boolean,
      indexOrder: IndexOrder,
      value: TextValue
    ): NodeValueIndexCursor = notAvailable()

    override def nodeIndexScan(
      index: IndexReadSession,
      needsValues: Boolean,
      indexOrder: IndexOrder
    ): NodeValueIndexCursor = notAvailable()

    override def nodeLockingUniqueIndexSeek(
      index: IndexDescriptor,
      queries: Seq[PropertyIndexQuery.ExactPredicate]
    ): NodeValueIndexCursor = notAvailable()

    override def relationshipIndexSeek(
      index: IndexReadSession,
      needsValues: Boolean,
      indexOrder: IndexOrder,
      queries: Seq[PropertyIndexQuery]
    ): RelationshipValueIndexCursor = notAvailable()

    override def relationshipLockingUniqueIndexSeek(
      index: IndexDescriptor,
      queries: Seq[PropertyIndexQuery.ExactPredicate]
    ): RelationshipValueIndexCursor = notAvailable()

    override def relationshipIndexSeekByContains(
      index: IndexReadSession,
      needsValues: Boolean,
      indexOrder: IndexOrder,
      value: TextValue
    ): RelationshipValueIndexCursor = notAvailable()

    override def relationshipIndexSeekByEndsWith(
      index: IndexReadSession,
      needsValues: Boolean,
      indexOrder: IndexOrder,
      value: TextValue
    ): RelationshipValueIndexCursor = notAvailable()

    override def relationshipIndexScan(
      index: IndexReadSession,
      needsValues: Boolean,
      indexOrder: IndexOrder
    ): RelationshipValueIndexCursor = notAvailable()

    override def getNodesByLabel(
      tokenReadSession: TokenReadSession,
      id: Int,
      indexOrder: IndexOrder
    ): ClosingLongIterator = notAvailable()

    override def createNodeKeyConstraint(
      labelId: Int,
      propertyKeyIds: Seq[Int],
      name: Option[String],
      provider: Option[IndexProviderDescriptor]
    ): Unit = notAvailable()

    override def createRelationshipKeyConstraint(
      relTypeId: Int,
      propertyKeyIds: Seq[Int],
      name: Option[String],
      provider: Option[IndexProviderDescriptor]
    ): Unit = notAvailable()

    override def createNodeUniqueConstraint(
      labelId: Int,
      propertyKeyIds: Seq[Int],
      name: Option[String],
      provider: Option[IndexProviderDescriptor]
    ): Unit = notAvailable()

    override def createRelationshipUniqueConstraint(
      relTypeId: Int,
      propertyKeyIds: Seq[Int],
      name: Option[String],
      provider: Option[IndexProviderDescriptor]
    ): Unit = notAvailable()

    override def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int, name: Option[String]): Unit =
      notAvailable()

    override def createRelationshipPropertyExistenceConstraint(
      relTypeId: Int,
      propertyKeyId: Int,
      name: Option[String]
    ): Unit = notAvailable()

    override def createNodePropertyTypeConstraint(
      labelId: Int,
      propertyKeyId: Int,
      propertyTypes: PropertyTypeSet,
      name: Option[String]
    ): Unit = notAvailable()

    override def createRelationshipPropertyTypeConstraint(
      relTypeId: Int,
      propertyKeyId: Int,
      propertyTypes: PropertyTypeSet,
      name: Option[String]
    ): Unit = notAvailable()

    override def dropNamedConstraint(name: String): Unit = notAvailable()

    override def getConstraintInformation(name: String): ConstraintInformation = notAvailable()

    override def getConstraintInformation(
      matchFn: ConstraintDescriptor => Boolean,
      entityId: Int,
      properties: Int*
    ): ConstraintInformation = notAvailable()

    override def getAllConstraints(): Map[ConstraintDescriptor, ConstraintInfo] = notAvailable()

    override def getImportDataConnection(uri: URI): CharReadable = notAvailable()

    override def nodeHasCheapDegrees(node: Long, nodeCursor: NodeCursor): Boolean = notAvailable()

    override def asObject(value: AnyValue): AnyRef = notAvailable()

    override def nodeCountByCountStore(labelId: Int): Long = notAvailable()

    override def relationshipCountByCountStore(startLabelId: Int, typeId: Int, endLabelId: Int): Long = notAvailable()

    override def lockNodes(nodeIds: Long*): Unit = notAvailable()

    override def lockRelationships(relIds: Long*): Unit = notAvailable()

    override def callReadOnlyProcedure(
      id: Int,
      args: Array[AnyValue],
      context: ProcedureCallContext
    ): Iterator[Array[AnyValue]] = notAvailable()

    override def callReadWriteProcedure(
      id: Int,
      args: Array[AnyValue],
      context: ProcedureCallContext
    ): Iterator[Array[AnyValue]] = notAvailable()

    override def callSchemaWriteProcedure(
      id: Int,
      args: Array[AnyValue],
      context: ProcedureCallContext
    ): Iterator[Array[AnyValue]] = notAvailable()

    override def callDbmsProcedure(
      id: Int,
      args: Array[AnyValue],
      context: ProcedureCallContext
    ): Iterator[Array[AnyValue]] = notAvailable()

    override def aggregateFunction(id: Int, context: ProcedureCallContext): UserAggregationReducer = notAvailable()

    override def builtInAggregateFunction(id: Int, context: ProcedureCallContext): UserAggregationReducer =
      notAvailable()

    override def detachDeleteNode(id: Long): Int = notAvailable()

    override def assertSchemaWritesAllowed(): Unit = notAvailable()

    override def nodeApplyChanges(
      node: Long,
      addedLabels: IntSet,
      removedLabels: IntSet,
      properties: IntObjectMap[Value]
    ): Unit = notAvailable()

    override def relationshipApplyChanges(relationship: Long, properties: IntObjectMap[Value]): Unit = notAvailable()

    override def assertShowIndexAllowed(): Unit = notAvailable()

    override def assertShowConstraintAllowed(): Unit = notAvailable()

    override def getLabelName(id: Int): String = notAvailable()

    override def getOptLabelId(labelName: String): Option[Int] = None

    override def getLabelId(labelName: String): Int = notAvailable()

    override def getPropertyKeyName(id: Int): String = notAvailable()

    override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] = None

    override def getPropertyKeyId(propertyKeyName: String): Int = notAvailable()

    override def getRelTypeName(id: Int): String = notAvailable()

    override def getOptRelTypeId(relType: String): Option[Int] = None

    override def getRelTypeId(relType: String): Int = notAvailable()

    override def nodeById(id: Long): VirtualNodeValue = notAvailable()

    override def relationshipById(id: Long): VirtualRelationshipValue = notAvailable()

    override def nodePropertyIds(node: Long, nodeCursor: NodeCursor, propertyCursor: PropertyCursor): Array[Int] =
      notAvailable()

    override def propertyKey(name: String): Int = notAvailable()

    override def nodeLabel(name: String): Int = notAvailable()
    override def nodeDeletedInThisTransaction(id: Long): Boolean = notAvailable()

    override def relationshipType(name: String): Int = notAvailable()

    override def relationshipTypeName(typ: Int): String = notAvailable()

    override def getTypeForRelationship(id: Long, relationshipCursor: RelationshipScanCursor): AnyValue =
      notAvailable()

    override def nodeHasProperty(
      node: Long,
      property: Int,
      nodeCursor: NodeCursor,
      propertyCursor: PropertyCursor
    ): Boolean = notAvailable()

    override def relationshipPropertyIds(
      node: Long,
      relationshipScanCursor: RelationshipScanCursor,
      propertyCursor: PropertyCursor
    ): Array[Int] = notAvailable()

    override def relationshipHasProperty(
      node: Long,
      property: Int,
      relationshipScanCursor: RelationshipScanCursor,
      propertyCursor: PropertyCursor
    ): Boolean = notAvailable()

    override def relationshipDeletedInThisTransaction(id: Long): Boolean = notAvailable()

    override def nodeGetOutgoingDegreeWithMax(maxDegree: Int, node: Long, nodeCursor: NodeCursor): Int = notAvailable()

    override def nodeGetOutgoingDegreeWithMax(
      maxDegree: Int,
      node: Long,
      relationship: Int,
      nodeCursor: NodeCursor
    ): Int = notAvailable()

    override def nodeGetIncomingDegreeWithMax(maxDegree: Int, node: Long, nodeCursor: NodeCursor): Int = notAvailable()

    override def nodeGetIncomingDegreeWithMax(
      maxDegree: Int,
      node: Long,
      relationship: Int,
      nodeCursor: NodeCursor
    ): Int = notAvailable()

    override def nodeGetTotalDegreeWithMax(maxDegree: Int, node: Long, nodeCursor: NodeCursor): Int = notAvailable()

    override def nodeGetTotalDegreeWithMax(maxDegree: Int, node: Long, relationship: Int, nodeCursor: NodeCursor): Int =
      notAvailable()

    override def nodeGetOutgoingDegree(node: Long, nodeCursor: NodeCursor): Int = notAvailable()

    override def nodeGetOutgoingDegree(node: Long, relationship: Int, nodeCursor: NodeCursor): Int = notAvailable()

    override def nodeGetIncomingDegree(node: Long, nodeCursor: NodeCursor): Int = notAvailable()

    override def nodeGetIncomingDegree(node: Long, relationship: Int, nodeCursor: NodeCursor): Int = notAvailable()

    override def nodeGetTotalDegree(node: Long, nodeCursor: NodeCursor): Int = notAvailable()

    override def nodeGetTotalDegree(node: Long, relationship: Int, nodeCursor: NodeCursor): Int = notAvailable()

    override def singleNode(id: Long, cursor: NodeCursor): Unit = notAvailable()

    override def singleRelationship(id: Long, cursor: RelationshipScanCursor): Unit = notAvailable()

    override def getLabelsForNode(id: Long, nodeCursor: NodeCursor): ListValue = notAvailable()

    override def isLabelSetOnNode(label: Int, id: Long, nodeCursor: NodeCursor): Boolean = notAvailable()

    override def areLabelsSetOnNode(labels: Array[Int], id: Long, nodeCursor: NodeCursor): Boolean = notAvailable()

    override def isAnyLabelSetOnNode(labels: Array[Int], id: Long, nodeCursor: NodeCursor): Boolean = notAvailable()

    override def isALabelSetOnNode(node: Long, nodeCursor: NodeCursor): Boolean = notAvailable()

    override def isTypeSetOnRelationship(typ: Int, id: Long, relationshipCursor: RelationshipScanCursor): Boolean =
      notAvailable()

    override def areTypesSetOnRelationship(
      types: Array[Int],
      id: Long,
      relationshipCursor: RelationshipScanCursor
    ): Boolean = notAvailable()

    override def areTypesSetOnRelationship(
      types: Array[Int],
      obj: VirtualRelationshipValue,
      relationshipCursor: RelationshipScanCursor
    ): Boolean = notAvailable()

    override def nodeAsMap(
      id: Long,
      nodeCursor: NodeCursor,
      propertyCursor: PropertyCursor,
      builder: MapValueBuilder,
      seenTokens: IntSet
    ): MapValue = notAvailable()

    override def relationshipAsMap(
      id: Long,
      relationshipCursor: RelationshipScanCursor,
      propertyCursor: PropertyCursor,
      builder: MapValueBuilder,
      seenTokens: IntSet
    ): MapValue = notAvailable()

    override def callFunction(id: Int, args: Array[AnyValue], ctx: ProcedureCallContext): AnyValue = notAvailable()

    override def getTxStateNodePropertyOrNull(nodeId: Long, propertyKey: Int): Value = notAvailable()

    override def getTxStateRelationshipPropertyOrNull(relId: Long, propertyKey: Int): Value = notAvailable()

    override def getTransactionType: KernelTransaction.Type = notAvailable()

    override def contextWithNewTransaction(): QueryContext = notAvailable()

    override def close(): Unit = notAvailable()

    override def systemGraph: GraphDatabaseService = notAvailable()

    override def jobScheduler: JobScheduler = notAvailable()

    override def logProvider: InternalLogProvider = notAvailable()

    override def providedLanguageFunctions: Seq[FunctionInformation] = notAvailable()

    override def getDatabaseContextProvider: DatabaseContextProvider[DatabaseContext] = notAvailable()

    override def getConfig: Config = notAvailable()

    override def entityTransformer: EntityTransformer = notAvailable()

    override def procedureCallContext(fcnId: Int): ProcedureCallContext = {
      new ProcedureCallContext(fcnId, true, "", false, "")
    }

    override def procedureCallContext(procId: Int, outputFields: Array[String]): ProcedureCallContext = {
      new ProcedureCallContext(procId, outputFields, true, "", false, "")
    }
  }

}
