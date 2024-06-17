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
package org.neo4j.cypher.internal.runtime.interpreted

import org.eclipse.collections.api.map.primitive.IntObjectMap
import org.eclipse.collections.api.set.primitive.IntSet
import org.eclipse.collections.impl.factory.primitive.IntSets
import org.neo4j.common.EntityType
import org.neo4j.common.TokenNameLookup
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.csv.reader.CharReadable
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.runtime
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingLongIterator
import org.neo4j.cypher.internal.runtime.ConstraintInfo
import org.neo4j.cypher.internal.runtime.ConstraintInformation
import org.neo4j.cypher.internal.runtime.EntityTransformer
import org.neo4j.cypher.internal.runtime.IndexInfo
import org.neo4j.cypher.internal.runtime.IndexInformation
import org.neo4j.cypher.internal.runtime.IndexStatus
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.KernelAPISupport.asKernelIndexOrder
import org.neo4j.cypher.internal.runtime.KernelAPISupport.isImpossibleIndexQuery
import org.neo4j.cypher.internal.runtime.NodeValueHit
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.ReadQueryContext
import org.neo4j.cypher.internal.runtime.RelationshipIterator
import org.neo4j.cypher.internal.runtime.RelationshipValueHit
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.ThreadSafeResourceManager
import org.neo4j.cypher.internal.runtime.ValuedNodeIndexCursor
import org.neo4j.cypher.internal.runtime.ValuedRelationshipIndexCursor
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.PrimitiveCursorIterator
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.RelationshipCursorIterator
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.RelationshipTypeCursorIterator
import org.neo4j.cypher.operations.CursorUtils
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.dbms.database.DatabaseContext
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.exceptions.EntityNotFoundException
import org.neo4j.exceptions.FailedIndexException
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.NotFoundException
import org.neo4j.graphdb.Relationship
import org.neo4j.internal.helpers.collection.Iterators
import org.neo4j.internal.kernel.api
import org.neo4j.internal.kernel.api.IndexQueryConstraints
import org.neo4j.internal.kernel.api.IndexQueryConstraints.ordered
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.InternalIndexState
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.PropertyIndexQuery
import org.neo4j.internal.kernel.api.PropertyIndexQuery.ExactPredicate
import org.neo4j.internal.kernel.api.Read
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor
import org.neo4j.internal.kernel.api.SchemaReadCore
import org.neo4j.internal.kernel.api.TokenPredicate
import org.neo4j.internal.kernel.api.TokenRead
import org.neo4j.internal.kernel.api.TokenReadSession
import org.neo4j.internal.kernel.api.TokenSet
import org.neo4j.internal.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException
import org.neo4j.internal.kernel.api.helpers.Nodes
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections.allCursor
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections.incomingCursor
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections.outgoingCursor
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext
import org.neo4j.internal.kernel.api.procs.UserAggregationReducer
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.internal.schema.IndexDescriptor
import org.neo4j.internal.schema.IndexPrototype
import org.neo4j.internal.schema.IndexProviderDescriptor
import org.neo4j.internal.schema.IndexType
import org.neo4j.internal.schema.SchemaDescriptor
import org.neo4j.internal.schema.SchemaDescriptors
import org.neo4j.internal.schema.constraints.PropertyTypeSet
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.StatementConstants
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.kernel.api.exceptions.schema.EquivalentSchemaRuleAlreadyExistsException
import org.neo4j.kernel.api.index.IndexUsageStats
import org.neo4j.kernel.impl.api.index.IndexProviderNotFoundException
import org.neo4j.kernel.impl.core.TransactionalEntityFactory
import org.neo4j.kernel.impl.query.FunctionInformation
import org.neo4j.kernel.impl.query.QueryExecutionEngine
import org.neo4j.kernel.impl.util.DefaultValueMapper
import org.neo4j.kernel.impl.util.NodeEntityWrappingNodeValue
import org.neo4j.kernel.impl.util.PathWrappingPathValue
import org.neo4j.kernel.impl.util.RelationshipEntityWrappingValue
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.logging.InternalLogProvider
import org.neo4j.logging.internal.LogService
import org.neo4j.scheduler.JobScheduler
import org.neo4j.storageengine.api.PropertySelection
import org.neo4j.storageengine.api.RelationshipVisitor
import org.neo4j.values.AnyValue
import org.neo4j.values.ValueMapper
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.ListValue.IntegralRangeListValue
import org.neo4j.values.virtual.ListValueBuilder
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.MapValueBuilder
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.values.virtual.VirtualValues

import java.net.URI
import java.util
import java.util.Locale

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.control.NonFatal

sealed class TransactionBoundQueryContext(
  transactionalContext: TransactionalContextWrapper,
  resources: ResourceManager,
  closeable: Option[AutoCloseable] = None
)(implicit indexSearchMonitor: IndexSearchMonitor)
    extends TransactionBoundReadQueryContext(transactionalContext, resources, closeable) with QueryContext {

  override val nodeWriteOps: NodeWriteOperations = new NodeWriteOperations
  override val relationshipWriteOps: RelationshipWriteOperations = new RelationshipWriteOperations

  private def writes() = transactionalContext.dataWrite

  private def tokenWrite = transactionalContext.tokenWrite

  override def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int = labelIds.foldLeft(0) {
    case (count, labelId) => if (writes().nodeAddLabel(node, labelId)) count + 1 else count
  }

  override def createNodeId(labels: Array[Int]): Long = writes().nodeCreateWithLabels(labels)

  override def createRelationshipId(start: Long, end: Long, relType: Int): Long =
    writes().relationshipCreate(start, relType, end)

  override def getOrCreateRelTypeId(relTypeName: String): Int =
    transactionalContext.tokenWrite.relationshipTypeGetOrCreateForName(relTypeName)

  override def getOrCreateLabelId(labelName: String): Int = {
    val id = tokenRead.nodeLabel(labelName)
    if (id != TokenRead.NO_TOKEN) id
    else tokenWrite.labelGetOrCreateForName(labelName)
  }

  override def getOrCreateTypeId(relTypeName: String): Int = {
    val id = tokenRead.relationshipType(relTypeName)
    if (id != TokenRead.NO_TOKEN) id
    else tokenWrite.relationshipTypeGetOrCreateForName(relTypeName)
  }

  override def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int = labelIds.foldLeft(0) {
    case (count, labelId) =>
      if (transactionalContext.dataWrite.nodeRemoveLabel(node, labelId)) count + 1 else count
  }

  override def getOrCreatePropertyKeyId(propertyKey: String): Int =
    tokenWrite.propertyKeyGetOrCreateForName(propertyKey)

  override def getOrCreatePropertyKeyIds(propertyKeys: Array[String]): Array[Int] = {
    val ids = new Array[Int](propertyKeys.length)
    tokenWrite.propertyKeyGetOrCreateForNames(propertyKeys, ids)
    ids
  }

  override def validateIndexProvider(
    schemaDescription: String,
    providerString: String,
    indexType: IndexType
  ): IndexProviderDescriptor = {
    val schemaWrite = transactionalContext.schemaWrite
    try {
      val providerDescriptor = schemaWrite.indexProviderByName(providerString)
      val providerIndexType = schemaWrite.indexTypeByProviderName(providerString)

      if (!providerIndexType.equals(indexType)) {
        val indexProviders =
          schemaWrite.indexProvidersByType(indexType).asScala.toList.map(_.name()).sorted.mkString("['", "', '", "']")
        val indexDescription =
          if (providerIndexType.isLookup) "token lookup" else providerIndexType.name().toLowerCase(Locale.ROOT)
        throw new InvalidArgumentsException(
          s"""Could not create $schemaDescription with specified index provider '$providerString'.
             |To create $indexDescription index, please use 'CREATE $providerIndexType INDEX ...'.
             |The available index providers for the given type: $indexProviders.""".stripMargin
        )
      }

      providerDescriptor
    } catch {
      case e: IndexProviderNotFoundException =>
        val indexProviders =
          schemaWrite.indexProvidersByType(indexType).asScala.toList.map(_.name()).mkString("['", "', '", "']")
        // Throw nicer error on old providers
        val message =
          if (
            providerString.equalsIgnoreCase("native-btree-1.0") ||
            providerString.equalsIgnoreCase("lucene+native-3.0")
          )
            s"""Could not create $schemaDescription with specified index provider '$providerString'.
               |Invalid index type b-tree, use range, point or text index instead.
               |The available index providers for the given type: $indexProviders.""".stripMargin
          else
            s"""Could not create $schemaDescription with specified index provider '$providerString'.
               |The available index providers for the given type: $indexProviders.""".stripMargin

        throw new InvalidArgumentsException(message, e)
    }
  }

  override def addRangeIndexRule(
    entityId: Int,
    entityType: EntityType,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ): IndexDescriptor = {
    val (descriptor, prototype) =
      getIndexDescriptorAndPrototype(IndexType.RANGE, entityId, entityType, propertyKeyIds, name, provider)
    addIndexRule(descriptor, prototype)
  }

  override def addLookupIndexRule(
    entityType: EntityType,
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ): IndexDescriptor = {
    val descriptor = SchemaDescriptors.forAnyEntityTokens(entityType)
    val prototype = provider.map(IndexPrototype.forSchema(descriptor, _)).getOrElse(
      IndexPrototype.forSchema(descriptor)
    ).withIndexType(IndexType.LOOKUP)
    val namedPrototype = name.map(n => prototype.withName(n)).getOrElse(prototype)
    addIndexRule(descriptor, namedPrototype)
  }

  override def addFulltextIndexRule(
    entityIds: List[Int],
    entityType: EntityType,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor],
    indexConfig: IndexConfig
  ): IndexDescriptor = {
    val descriptor = SchemaDescriptors.fulltext(entityType, entityIds.toArray, propertyKeyIds.toArray)
    val prototype =
      provider.map(p => IndexPrototype.forSchema(descriptor, p)).getOrElse(IndexPrototype.forSchema(descriptor))
        .withIndexType(IndexType.FULLTEXT)
        .withIndexConfig(indexConfig)
    val namedPrototype = name.map(n => prototype.withName(n)).getOrElse(prototype)
    addIndexRule(descriptor, namedPrototype)
  }

  override def addTextIndexRule(
    entityId: Int,
    entityType: EntityType,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ): IndexDescriptor = {
    val (descriptor, prototype) =
      getIndexDescriptorAndPrototype(IndexType.TEXT, entityId, entityType, propertyKeyIds, name, provider)
    addIndexRule(descriptor, prototype)
  }

  override def addPointIndexRule(
    entityId: Int,
    entityType: EntityType,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor],
    indexConfig: IndexConfig
  ): IndexDescriptor = {
    val (descriptor, prototype) =
      getIndexDescriptorAndPrototype(IndexType.POINT, entityId, entityType, propertyKeyIds, name, provider)
    val prototypeWithConfig = prototype.withIndexConfig(indexConfig)
    addIndexRule(descriptor, prototypeWithConfig)
  }

  override def addVectorIndexRule(
    entityId: Int,
    entityType: EntityType,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor],
    indexConfig: IndexConfig
  ): IndexDescriptor = {
    val (descriptor, prototype) =
      getIndexDescriptorAndPrototype(IndexType.VECTOR, entityId, entityType, propertyKeyIds, name, provider)
    val prototypeWithConfig = prototype.withIndexConfig(indexConfig)
    addIndexRule(descriptor, prototypeWithConfig)
  }

  private def getIndexDescriptorAndPrototype(
    indexType: IndexType,
    entityId: Int,
    entityType: EntityType,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ): (SchemaDescriptor, IndexPrototype) = {
    val descriptor = entityType match {
      case EntityType.NODE         => SchemaDescriptors.forLabel(entityId, propertyKeyIds: _*)
      case EntityType.RELATIONSHIP => SchemaDescriptors.forRelType(entityId, propertyKeyIds: _*)
    }
    val prototype = provider.map(p => IndexPrototype.forSchema(descriptor, p)).getOrElse(
      IndexPrototype.forSchema(descriptor)
    ).withIndexType(indexType)
    val namedPrototype = name.map(n => prototype.withName(n)).getOrElse(prototype)
    (descriptor, namedPrototype)
  }

  private def addIndexRule(descriptor: SchemaDescriptor, prototype: IndexPrototype): IndexDescriptor = {
    try {
      transactionalContext.schemaWrite.indexCreate(prototype)
    } catch {
      case e: EquivalentSchemaRuleAlreadyExistsException =>
        val schemaRead = transactionalContext.schemaRead
        val indexReference = schemaRead.index(descriptor).next()
        if (schemaRead.indexGetState(indexReference) == InternalIndexState.FAILED) {
          val message = schemaRead.indexGetFailure(indexReference)
          throw new FailedIndexException(indexReference.userDescription(transactionalContext.tokenRead), message)
        }
        throw e
    }
  }

  override def dropIndexRule(name: String): Unit =
    transactionalContext.schemaWrite.indexDrop(name)

  override def createNodeKeyConstraint(
    labelId: Int,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ): Unit = {
    val indexPrototype = getNodeUniqueIndexPrototype(labelId, propertyKeyIds, name, provider)
    transactionalContext.schemaWrite.keyConstraintCreate(indexPrototype)
  }

  override def createRelationshipKeyConstraint(
    relTypeId: Int,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ): Unit = {
    val indexPrototype = getRelationshipUniqueIndexPrototype(relTypeId, propertyKeyIds, name, provider)
    transactionalContext.schemaWrite.keyConstraintCreate(indexPrototype)
  }

  override def createNodeUniqueConstraint(
    labelId: Int,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ): Unit = {
    val indexPrototype = getNodeUniqueIndexPrototype(labelId, propertyKeyIds, name, provider)
    transactionalContext.schemaWrite.uniquePropertyConstraintCreate(indexPrototype)
  }

  override def createRelationshipUniqueConstraint(
    relTypeId: Int,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ): Unit = {
    val indexPrototype = getRelationshipUniqueIndexPrototype(relTypeId, propertyKeyIds, name, provider)
    transactionalContext.schemaWrite.uniquePropertyConstraintCreate(indexPrototype)
  }

  private def getNodeUniqueIndexPrototype(
    labelId: Int,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ) =
    getUniqueIndexPrototype(SchemaDescriptors.forLabel(labelId, propertyKeyIds: _*), name, provider)

  private def getRelationshipUniqueIndexPrototype(
    relTypeId: Int,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ) =
    getUniqueIndexPrototype(SchemaDescriptors.forRelType(relTypeId, propertyKeyIds: _*), name, provider)

  private def getUniqueIndexPrototype(
    descriptor: SchemaDescriptor,
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ) =
    provider.map(provider => IndexPrototype.uniqueForSchema(descriptor, provider))
      .getOrElse(IndexPrototype.uniqueForSchema(descriptor)).withName(name.orNull)

  override def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int, name: Option[String]): Unit =
    transactionalContext.schemaWrite.nodePropertyExistenceConstraintCreate(
      SchemaDescriptors.forLabel(labelId, propertyKeyId),
      name.orNull,
      false
    )

  override def createRelationshipPropertyExistenceConstraint(
    relTypeId: Int,
    propertyKeyId: Int,
    name: Option[String]
  ): Unit =
    transactionalContext.schemaWrite.relationshipPropertyExistenceConstraintCreate(
      SchemaDescriptors.forRelType(relTypeId, propertyKeyId),
      name.orNull,
      false
    )

  override def createNodePropertyTypeConstraint(
    labelId: Int,
    propertyKeyId: Int,
    propertyTypes: PropertyTypeSet,
    name: Option[String]
  ): Unit =
    transactionalContext.schemaWrite.propertyTypeConstraintCreate(
      SchemaDescriptors.forLabel(labelId, propertyKeyId),
      name.orNull,
      propertyTypes,
      false
    )

  override def createRelationshipPropertyTypeConstraint(
    relTypeId: Int,
    propertyKeyId: Int,
    propertyTypes: PropertyTypeSet,
    name: Option[String]
  ): Unit =
    transactionalContext.schemaWrite.propertyTypeConstraintCreate(
      SchemaDescriptors.forRelType(relTypeId, propertyKeyId),
      name.orNull,
      propertyTypes,
      false
    )

  override def dropNamedConstraint(name: String): Unit =
    transactionalContext.schemaWrite.constraintDrop(name, false)

  override def detachDeleteNode(node: Long): Int = transactionalContext.dataWrite.nodeDetachDelete(node)

  override def assertSchemaWritesAllowed(): Unit =
    transactionalContext.schemaWrite

  override def getDatabaseContextProvider: DatabaseContextProvider[DatabaseContext] = {
    val dependencyResolver = transactionalContext.graph.getDependencyResolver
    dependencyResolver.resolveDependency(classOf[DatabaseContextProvider[_ <: DatabaseContext]]).asInstanceOf[
      DatabaseContextProvider[DatabaseContext]
    ]
  }

  override def nodeApplyChanges(
    node: Long,
    addedLabels: IntSet,
    removedLabels: IntSet,
    properties: IntObjectMap[Value]
  ): Unit = {
    writes().nodeApplyChanges(node, addedLabels, removedLabels, properties)
  }

  override def relationshipApplyChanges(relationship: Long, properties: IntObjectMap[Value]): Unit = {
    writes().relationshipApplyChanges(relationship, properties)
  }

  class NodeWriteOperations extends NodeReadOperations with org.neo4j.cypher.internal.runtime.NodeOperations {

    override def delete(id: Long): Boolean = {
      writes().nodeDelete(id)
    }

    override def removeProperty(id: Long, propertyKeyId: Int): Boolean = {
      try {
        !(writes().nodeRemoveProperty(id, propertyKeyId) eq Values.NO_VALUE)
      } catch {
        case _: api.exceptions.EntityNotFoundException => false
      }
    }

    override def setProperty(id: Long, propertyKeyId: Int, value: Value): Unit = {
      try {
        writes().nodeSetProperty(id, propertyKeyId, value)
      } catch {
        case _: api.exceptions.EntityNotFoundException => // ignore
      }
    }

    override def setProperties(obj: Long, properties: IntObjectMap[Value]): Unit = {
      try {
        writes().nodeApplyChanges(obj, IntSets.immutable.empty(), IntSets.immutable.empty(), properties)
      } catch {
        case _: api.exceptions.EntityNotFoundException => // ignore
      }
    }
  }

  class RelationshipWriteOperations extends RelationshipReadOperations
      with org.neo4j.cypher.internal.runtime.RelationshipOperations {

    override def delete(id: Long): Boolean = {
      writes().relationshipDelete(id)
    }

    override def removeProperty(id: Long, propertyKeyId: Int): Boolean = {
      try {
        !(writes().relationshipRemoveProperty(id, propertyKeyId) eq Values.NO_VALUE)
      } catch {
        case _: api.exceptions.EntityNotFoundException => false
      }
    }

    override def setProperty(id: Long, propertyKeyId: Int, value: Value): Unit = {
      try {
        writes().relationshipSetProperty(id, propertyKeyId, value)
      } catch {
        case _: api.exceptions.EntityNotFoundException => // ignore
      }
    }

    override def setProperties(obj: Long, properties: IntObjectMap[Value]): Unit = {
      try {
        writes().relationshipApplyChanges(obj, properties)
      } catch {
        case _: api.exceptions.EntityNotFoundException => // ignore
      }
    }
  }
}

private[internal] class TransactionBoundReadQueryContext(
  val transactionalContext: TransactionalContextWrapper,
  val resources: ResourceManager,
  private val closeable: Option[AutoCloseable] = None
)(implicit indexSearchMonitor: IndexSearchMonitor)
    extends TransactionBoundReadTokenContext(transactionalContext) with ReadQueryContext {

  override val nodeReadOps: NodeReadOperations = new NodeReadOperations
  override val relationshipReadOps: RelationshipReadOperations = new RelationshipReadOperations

  // NOTE: Not supported in parallel runtime. Entity values hold a reference to InternalTransaction, which is not thread-safe.
  private[internal] lazy val entityAccessor: TransactionalEntityFactory =
    transactionalContext.kernelTransactionalContext.transaction()

  private[internal] lazy val valueMapper: ValueMapper[java.lang.Object] =
    new DefaultValueMapper(transactionalContext.kernelTransactionalContext.transaction())

  override def createParallelQueryContext(initialHeapMemory: Long): QueryContext = {
    val newTransactionalContext = transactionalContext.createParallelTransactionalContext()

    // Transfer some initial heap memory to the newly created execution context memory tracker,
    // to prevent it from immediately grabbing memory from the transaction pool in case it turns out to be short-lived
    newTransactionalContext.memoryTracker.releaseHeap(initialHeapMemory)

    // Create a single-threaded copy of ResourceManager and attach it to the thread-safe resource manager
    val newResourceManager = new ResourceManager(resources.monitor, newTransactionalContext.memoryTracker)
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(resources.isInstanceOf[ThreadSafeResourceManager])
    resources.trace(newResourceManager)

    new ParallelTransactionBoundQueryContext(newTransactionalContext, newResourceManager)(indexSearchMonitor)
  }

  // We cannot assign to value because of periodic commit
  protected def reads(): Read = transactionalContext.dataRead

  private def allocateNodeCursor() = transactionalContext.cursors.allocateNodeCursor(
    transactionalContext.cursorContext,
    transactionalContext.memoryTracker
  )

  protected def tokenRead: TokenRead = transactionalContext.tokenRead

  override def singleNode(id: Long, cursor: NodeCursor): Unit = {
    reads().singleNode(id, cursor)
  }

  override def singleRelationship(id: Long, cursor: RelationshipScanCursor): Unit = {
    reads().singleRelationship(id, cursor)
  }

  override def getLabelsForNode(node: Long, nodeCursor: NodeCursor): ListValue = {
    val labelSet = getLabelTokenSetForNode(node, nodeCursor)
    val labelArray = new Array[TextValue](labelSet.numberOfTokens())
    var i = 0
    while (i < labelSet.numberOfTokens()) {
      labelArray(i) = Values.stringValue(tokenRead.nodeLabelName(labelSet.token(i)))
      i += 1
    }
    VirtualValues.list(labelArray: _*)
  }

  override def isALabelSetOnNode(node: Long, nodeCursor: NodeCursor): Boolean = {
    singleNode(node, nodeCursor)
    if (nodeCursor.next()) {
      nodeCursor.hasLabel()
    } else {
      // NOTE: always returning false would be nicer but is not correct according to TCK
      if (reads().nodeDeletedInTransaction(node)) {
        throw new EntityNotFoundException(s"Node with id $node has been deleted in this transaction")
      } else {
        false
      }
    }
  }

  private def getLabelTokenSetForNode(node: Long, nodeCursor: NodeCursor) = {
    singleNode(node, nodeCursor)
    if (nodeCursor.next()) {
      nodeCursor.labels()
    } else {
      // NOTE: always returning TokenSet.NONE would be nicer here but is not correct according to TCK
      if (reads().nodeDeletedInTransaction(node)) {
        throw new EntityNotFoundException(s"Node with id $node has been deleted in this transaction")
      } else {
        TokenSet.NONE
      }
    }
  }

  override def getTypeForRelationship(id: Long, cursor: RelationshipScanCursor): AnyValue = {
    reads().singleRelationship(id, cursor)
    if (cursor.next()) {
      Values.stringValue(tokenRead.relationshipTypeName(cursor.`type`()))
    } else if (reads().relationshipDeletedInTransaction(id)) {

      // We are slightly inconsistent here in that we allow the user to read the types of relationships which have
      // been deleted in this transaction if they can be read (which is heavily dependent on underlying
      // implementations), but not otherwise. This is mainly to be backwards compatible. Any proper solution would
      // require that expected behaviour be well defined, like for example if
      // https://github.com/opencypher/openCypher/pull/533 was accepted.

      try {
        Values.stringValue(tokenRead.relationshipTypeName(cursor.`type`()))
      } catch {
        case _: RelationshipTypeIdNotFoundKernelException =>
          throw new EntityNotFoundException(s"Relationship with id $id has been deleted in this transaction")
        case e: Throwable => throw e
      }

    } else {
      // If the relationship was deleted by another transaction we return null
      Values.NO_VALUE
    }
  }

  override def isLabelSetOnNode(label: Int, node: Long, nodeCursor: NodeCursor): Boolean = {
    CursorUtils.nodeHasLabel(reads(), nodeCursor, node, label)
  }

  override def areLabelsSetOnNode(labels: Array[Int], id: Long, nodeCursor: NodeCursor): Boolean =
    CursorUtils.nodeHasLabels(reads(), nodeCursor, id, labels)

  override def isAnyLabelSetOnNode(labels: Array[Int], id: Long, nodeCursor: NodeCursor): Boolean = {
    CursorUtils.nodeHasAnyLabel(reads(), nodeCursor, id, labels)
  }

  override def isTypeSetOnRelationship(typ: Int, id: Long, relationshipCursor: RelationshipScanCursor): Boolean = {
    CursorUtils.relationshipHasType(reads(), relationshipCursor, id, typ)
  }

  override def areTypesSetOnRelationship(
    types: Array[Int],
    id: Long,
    relationshipCursor: RelationshipScanCursor
  ): Boolean = {
    CursorUtils.relationshipHasTypes(reads(), relationshipCursor, id, types)
  }

  override def areTypesSetOnRelationship(
    types: Array[Int],
    obj: VirtualRelationshipValue,
    relationshipCursor: RelationshipScanCursor
  ): Boolean = {
    CursorUtils.relationshipHasTypes(reads(), relationshipCursor, obj, types)
  }

  override def getRelationshipsForIds(
    node: Long,
    dir: SemanticDirection,
    types: Array[Int]
  ): ClosingLongIterator with RelationshipIterator = {
    val cursor = allocateNodeCursor()
    try {
      val read = reads()
      val cursors = transactionalContext.cursors
      val cursorContext = transactionalContext.cursorContext
      read.singleNode(node, cursor)
      if (!cursor.next()) ClosingLongIterator.emptyClosingRelationshipIterator
      else {
        val selectionCursor = dir match {
          case OUTGOING => outgoingCursor(cursors, cursor, types, cursorContext)
          case INCOMING => incomingCursor(cursors, cursor, types, cursorContext)
          case BOTH     => allCursor(cursors, cursor, types, cursorContext)
        }
        resources.trace(selectionCursor)
        new RelationshipCursorIterator(selectionCursor)
      }
    } finally {
      cursor.close()
    }
  }

  override def getRelationshipsByType(
    session: TokenReadSession,
    relType: Int,
    indexOrder: IndexOrder
  ): ClosingLongIterator with RelationshipIterator = {
    val read = reads()
    val typeCursor =
      transactionalContext.cursors.allocateRelationshipTypeIndexCursor(
        transactionalContext.cursorContext,
        transactionalContext.memoryTracker
      )
    read.relationshipTypeScan(
      session,
      typeCursor,
      ordered(asKernelIndexOrder(indexOrder)),
      new TokenPredicate(relType),
      transactionalContext.cursorContext
    )
    resources.trace(typeCursor)
    new RelationshipTypeCursorIterator(read, typeCursor)
  }

  override def nodeCursor(): NodeCursor =
    transactionalContext.cursors.allocateNodeCursor(
      transactionalContext.cursorContext,
      transactionalContext.memoryTracker
    )

  override def nodeLabelIndexCursor(): NodeLabelIndexCursor =
    transactionalContext.cursors.allocateNodeLabelIndexCursor(
      transactionalContext.cursorContext,
      transactionalContext.memoryTracker
    )

  override def relationshipTypeIndexCursor(): RelationshipTypeIndexCursor =
    transactionalContext.cursors.allocateRelationshipTypeIndexCursor(
      transactionalContext.cursorContext,
      transactionalContext.memoryTracker
    )

  override def traversalCursor(): RelationshipTraversalCursor =
    transactionalContext.cursors.allocateRelationshipTraversalCursor(
      transactionalContext.cursorContext,
      transactionalContext.memoryTracker
    )

  override def scanCursor(): RelationshipScanCursor =
    transactionalContext.cursors.allocateRelationshipScanCursor(
      transactionalContext.cursorContext,
      transactionalContext.memoryTracker
    )

  override def relationshipById(
    relationshipId: Long,
    startNodeId: Long,
    endNodeId: Long,
    typeId: Int
  ): VirtualRelationshipValue =
    VirtualValues.relationship(relationshipId, startNodeId, endNodeId, typeId)

  override def nodeIndexSeek(
    index: IndexReadSession,
    needsValues: Boolean,
    indexOrder: IndexOrder,
    predicates: Seq[PropertyIndexQuery]
  ): NodeValueIndexCursor = {
    if (predicates.exists(isImpossibleIndexQuery)) {
      NodeValueIndexCursor.EMPTY
    } else {
      innerNodeIndexSeek(index, needsValues, indexOrder, predicates: _*)
    }
  }

  override def relationshipIndexSeek(
    index: IndexReadSession,
    needsValues: Boolean,
    indexOrder: IndexOrder,
    predicates: Seq[PropertyIndexQuery]
  ): RelationshipValueIndexCursor = {
    if (predicates.exists(isImpossibleIndexQuery)) {
      RelationshipValueIndexCursor.EMPTY
    } else {
      innerRelationshipIndexSeek(index, needsValues, indexOrder, predicates: _*)
    }
  }

  override def relationshipLockingUniqueIndexSeek(
    index: IndexDescriptor,
    queries: Seq[PropertyIndexQuery.ExactPredicate]
  ): RelationshipValueIndexCursor = {

    val cursor = transactionalContext.cursors.allocateRelationshipValueIndexCursor(
      transactionalContext.cursorContext,
      transactionalContext.memoryTracker
    )
    indexSearchMonitor.lockingUniqueIndexSeek(index, queries)
    if (queries.exists(q => q.value() eq Values.NO_VALUE)) {
      cursor.close()
      RelationshipValueHit.EMPTY
    } else {
      val resultRelId = reads().lockingRelationshipUniqueIndexSeek(index, cursor, queries: _*)
      if (StatementConstants.NO_SUCH_RELATIONSHIP == resultRelId) {
        cursor.close()
        RelationshipValueHit.EMPTY
      } else {
        resources.trace(cursor)
        val values = queries.map(_.value()).toArray
        new RelationshipValueHit(cursor, values)
      }
    }
  }

  override def relationshipIndexSeekByContains(
    index: IndexReadSession,
    needsValues: Boolean,
    indexOrder: IndexOrder,
    value: TextValue
  ): RelationshipValueIndexCursor =
    innerRelationshipIndexSeek(
      index,
      needsValues,
      indexOrder,
      PropertyIndexQuery.stringContains(index.reference().schema().getPropertyIds()(0), value)
    )

  override def relationshipIndexSeekByEndsWith(
    index: IndexReadSession,
    needsValues: Boolean,
    indexOrder: IndexOrder,
    value: TextValue
  ): RelationshipValueIndexCursor =
    innerRelationshipIndexSeek(
      index,
      needsValues,
      indexOrder,
      PropertyIndexQuery.stringSuffix(index.reference().schema().getPropertyIds()(0), value)
    )

  override def relationshipIndexScan(
    index: IndexReadSession,
    needsValues: Boolean,
    indexOrder: IndexOrder
  ): RelationshipValueIndexCursor = {
    val relCursor = allocateAndTraceRelationshipValueIndexCursor()
    reads().relationshipIndexScan(
      index,
      relCursor,
      IndexQueryConstraints.constrained(asKernelIndexOrder(indexOrder), needsValues)
    )
    relCursor
  }

  override def lookupIndexReference(entityType: EntityType): IndexDescriptor = {
    val descriptor = SchemaDescriptors.forAnyEntityTokens(entityType)
    Iterators.single(transactionalContext.schemaRead.index(descriptor))
  }

  override def fulltextIndexReference(
    entityIds: List[Int],
    entityType: EntityType,
    properties: Int*
  ): IndexDescriptor = {
    val descriptor = SchemaDescriptors.fulltext(entityType, entityIds.toArray, properties.toArray)
    Iterators.single(transactionalContext.schemaRead.index(descriptor))
  }

  override def indexReference(
    indexType: IndexType,
    entityId: Int,
    entityType: EntityType,
    properties: Int*
  ): IndexDescriptor = {
    val descriptor = entityType match {
      case EntityType.NODE         => SchemaDescriptors.forLabel(entityId, properties: _*)
      case EntityType.RELATIONSHIP => SchemaDescriptors.forRelType(entityId, properties: _*)
    }
    // Get all indexes matching the schema
    val indexes = transactionalContext.schemaRead.index(descriptor)

    // Return the wanted index type if it exists
    while (indexes.hasNext) {
      val i = indexes.next()
      if (i.getIndexType.equals(indexType)) return i
    }

    // No such index existed, throw same exception type that Iterators.single gives if no index exists
    throw new NoSuchElementException(s"No such ${indexType.toString.toLowerCase(Locale.ROOT)} index exists.")
  }

  private def innerNodeIndexSeek(
    index: IndexReadSession,
    needsValues: Boolean,
    indexOrder: IndexOrder,
    queries: PropertyIndexQuery*
  ): NodeValueIndexCursor = {

    val nodeCursor: NodeValueIndexCursor = allocateAndTraceNodeValueIndexCursor()
    val actualValues =
      if (needsValues && queries.forall(_.isInstanceOf[ExactPredicate]))
        // We don't need property values from the index for an exact seek
        {
          queries.map(_.asInstanceOf[ExactPredicate].value()).toArray
        } else {
        null
      }
    val needsValuesFromIndexSeek = actualValues == null && needsValues
    reads().nodeIndexSeek(
      transactionalContext.kernelQueryContext,
      index,
      nodeCursor,
      IndexQueryConstraints.constrained(asKernelIndexOrder(indexOrder), needsValuesFromIndexSeek),
      queries: _*
    )
    if (needsValues && actualValues != null) {
      new ValuedNodeIndexCursor(nodeCursor, actualValues)
    } else {
      nodeCursor
    }
  }

  private def innerRelationshipIndexSeek(
    index: IndexReadSession,
    needsValues: Boolean,
    indexOrder: IndexOrder,
    queries: PropertyIndexQuery*
  ): RelationshipValueIndexCursor = {

    val relCursor = allocateAndTraceRelationshipValueIndexCursor()
    val actualValues =
      if (needsValues && queries.forall(_.isInstanceOf[ExactPredicate]))
        // We don't need property values from the index for an exact seek
        {
          queries.map(_.asInstanceOf[ExactPredicate].value()).toArray
        } else {
        null
      }
    val needsValuesFromIndexSeek = actualValues == null && needsValues
    reads().relationshipIndexSeek(
      transactionalContext.kernelQueryContext,
      index,
      relCursor,
      IndexQueryConstraints.constrained(asKernelIndexOrder(indexOrder), needsValuesFromIndexSeek),
      queries: _*
    )
    if (needsValues && actualValues != null) {
      new ValuedRelationshipIndexCursor(relCursor, actualValues)
    } else {
      relCursor
    }
  }

  override def nodeIndexScan(
    index: IndexReadSession,
    needsValues: Boolean,
    indexOrder: IndexOrder
  ): NodeValueIndexCursor = {
    val nodeCursor = allocateAndTraceNodeValueIndexCursor()
    reads().nodeIndexScan(
      index,
      nodeCursor,
      IndexQueryConstraints.constrained(asKernelIndexOrder(indexOrder), needsValues)
    )
    nodeCursor
  }

  override def nodeIndexSeekByContains(
    index: IndexReadSession,
    needsValues: Boolean,
    indexOrder: IndexOrder,
    value: TextValue
  ): NodeValueIndexCursor =
    innerNodeIndexSeek(
      index,
      needsValues,
      indexOrder,
      PropertyIndexQuery.stringContains(index.reference().schema().getPropertyIds()(0), value)
    )

  override def nodeIndexSeekByEndsWith(
    index: IndexReadSession,
    needsValues: Boolean,
    indexOrder: IndexOrder,
    value: TextValue
  ): NodeValueIndexCursor =
    innerNodeIndexSeek(
      index,
      needsValues,
      indexOrder,
      PropertyIndexQuery.stringSuffix(index.reference().schema().getPropertyIds()(0), value)
    )

  override def nodeLockingUniqueIndexSeek(
    index: IndexDescriptor,
    queries: Seq[PropertyIndexQuery.ExactPredicate]
  ): NodeValueIndexCursor = {

    val cursor = transactionalContext.cursors.allocateNodeValueIndexCursor(
      transactionalContext.cursorContext,
      transactionalContext.memoryTracker
    )
    try {
      indexSearchMonitor.lockingUniqueIndexSeek(index, queries)
      if (queries.exists(q => q.value() eq Values.NO_VALUE)) {
        NodeValueHit.EMPTY
      } else {
        val resultNodeId = reads().lockingNodeUniqueIndexSeek(index, cursor, queries: _*)
        if (StatementConstants.NO_SUCH_NODE == resultNodeId) {
          NodeValueHit.EMPTY
        } else {
          val values = queries.map(_.value()).toArray
          new NodeValueHit(resultNodeId, values, reads())
        }
      }
    } finally {
      cursor.close()
    }
  }

  override def nodeAsMap(
    id: Long,
    nodeCursor: NodeCursor,
    propertyCursor: PropertyCursor,
    builder: MapValueBuilder,
    seenTokens: IntSet
  ): MapValue = {
    reads().singleNode(id, nodeCursor)
    if (!nodeCursor.next()) VirtualValues.EMPTY_MAP
    else {
      val tokens = tokenRead
      nodeCursor.properties(propertyCursor, PropertySelection.ALL_PROPERTIES.excluding(p => seenTokens.contains(p)))
      while (propertyCursor.next()) {
        builder.add(tokens.propertyKeyName(propertyCursor.propertyKey()), propertyCursor.propertyValue())
      }
      builder.build()
    }
  }

  override def relationshipAsMap(
    id: Long,
    relationshipCursor: RelationshipScanCursor,
    propertyCursor: PropertyCursor,
    builder: MapValueBuilder,
    seenTokens: IntSet
  ): MapValue = {
    reads().singleRelationship(id, relationshipCursor)
    if (!relationshipCursor.next()) VirtualValues.EMPTY_MAP
    else {
      val tokens = tokenRead
      relationshipCursor.properties(propertyCursor)
      val builder = new MapValueBuilder()
      while (propertyCursor.next()) {
        builder.add(tokens.propertyKeyName(propertyCursor.propertyKey()), propertyCursor.propertyValue())
      }
      builder.build()
    }
  }

  override def relationshipAsMap(
    relationship: VirtualRelationshipValue,
    relationshipCursor: RelationshipScanCursor,
    propertyCursor: PropertyCursor,
    builder: MapValueBuilder,
    seenTokens: IntSet
  ): MapValue = {
    CursorUtils.relationshipAsMap(
      reads(),
      tokenRead,
      relationship,
      relationshipCursor,
      propertyCursor,
      builder: MapValueBuilder,
      seenTokens: IntSet
    )
  }

  override def getNodesByLabel(
    tokenReadSession: TokenReadSession,
    id: Int,
    indexOrder: IndexOrder
  ): ClosingLongIterator = {
    val cursor = allocateAndTraceNodeLabelIndexCursor()
    reads().nodeLabelScan(
      tokenReadSession,
      cursor,
      ordered(asKernelIndexOrder(indexOrder)),
      new TokenPredicate(id),
      transactionalContext.cursorContext
    )
    new PrimitiveCursorIterator {
      override protected def fetchNext(): Long = if (cursor.next()) cursor.nodeReference() else -1L

      override def close(): Unit = {
        cursor.close()
      }
    }
  }

  override def nodeGetOutgoingDegreeWithMax(maxDegree: Int, node: Long, nodeCursor: NodeCursor): Int = {
    reads().singleNode(node, nodeCursor)
    if (!nodeCursor.next()) 0
    else {
      Nodes.countWithMax(maxDegree, nodeCursor, org.neo4j.graphdb.Direction.OUTGOING)
    }
  }

  override def nodeGetIncomingDegreeWithMax(maxDegree: Int, node: Long, nodeCursor: NodeCursor): Int = {
    reads().singleNode(node, nodeCursor)
    if (!nodeCursor.next()) 0
    else {
      Nodes.countWithMax(maxDegree, nodeCursor, org.neo4j.graphdb.Direction.INCOMING)
    }
  }

  override def nodeGetTotalDegreeWithMax(maxDegree: Int, node: Long, nodeCursor: NodeCursor): Int = {
    reads().singleNode(node, nodeCursor)
    if (!nodeCursor.next()) 0
    else {
      Nodes.countWithMax(maxDegree, nodeCursor, org.neo4j.graphdb.Direction.BOTH)
    }
  }

  override def nodeGetOutgoingDegree(node: Long, nodeCursor: NodeCursor): Int = {
    reads().singleNode(node, nodeCursor)
    if (!nodeCursor.next()) 0
    else Nodes.countOutgoing(nodeCursor)
  }

  override def nodeGetIncomingDegree(node: Long, nodeCursor: NodeCursor): Int = {
    reads().singleNode(node, nodeCursor)
    if (!nodeCursor.next()) 0
    else Nodes.countIncoming(nodeCursor)
  }

  override def nodeGetTotalDegree(node: Long, nodeCursor: NodeCursor): Int = {
    reads().singleNode(node, nodeCursor)
    if (!nodeCursor.next()) 0
    else Nodes.countAll(nodeCursor)
  }

  override def nodeGetOutgoingDegreeWithMax(
    maxDegree: Int,
    node: Long,
    relationship: Int,
    nodeCursor: NodeCursor
  ): Int = {
    reads().singleNode(node, nodeCursor)
    if (!nodeCursor.next()) 0
    else {
      Nodes.countWithMax(maxDegree, nodeCursor, relationship, org.neo4j.graphdb.Direction.OUTGOING)
    }
  }

  override def nodeGetIncomingDegreeWithMax(
    maxDegree: Int,
    node: Long,
    relationship: Int,
    nodeCursor: NodeCursor
  ): Int = {
    reads().singleNode(node, nodeCursor)
    if (!nodeCursor.next()) 0
    else {
      Nodes.countWithMax(maxDegree, nodeCursor, relationship, org.neo4j.graphdb.Direction.INCOMING)
    }
  }

  override def nodeGetTotalDegreeWithMax(maxDegree: Int, node: Long, relationship: Int, nodeCursor: NodeCursor): Int = {
    reads().singleNode(node, nodeCursor)
    if (!nodeCursor.next()) 0
    else {
      Nodes.countWithMax(maxDegree, nodeCursor, relationship, org.neo4j.graphdb.Direction.BOTH)
    }
  }

  override def nodeGetOutgoingDegree(node: Long, relationship: Int, nodeCursor: NodeCursor): Int = {
    reads().singleNode(node, nodeCursor)
    if (!nodeCursor.next()) 0
    else Nodes.countOutgoing(nodeCursor, relationship)
  }

  override def nodeGetIncomingDegree(node: Long, relationship: Int, nodeCursor: NodeCursor): Int = {
    reads().singleNode(node, nodeCursor)
    if (!nodeCursor.next()) 0
    else Nodes.countIncoming(nodeCursor, relationship)
  }

  override def nodeGetTotalDegree(node: Long, relationship: Int, nodeCursor: NodeCursor): Int = {
    reads().singleNode(node, nodeCursor)
    if (!nodeCursor.next()) 0
    else Nodes.countAll(nodeCursor, relationship)
  }

  override def nodeHasCheapDegrees(node: Long, nodeCursor: NodeCursor): Boolean = {
    reads().singleNode(node, nodeCursor)
    if (!nodeCursor.next()) false
    else nodeCursor.supportsFastDegreeLookup
  }

  override def asObject(value: AnyValue): AnyRef = value.map(valueMapper)

  override def getTxStateNodePropertyOrNull(nodeId: Long, propertyKey: Int): Value = {
    if (nodeId != -1L) {
      val ops = reads()
      if (ops.nodeDeletedInTransaction(nodeId)) {
        throw new EntityNotFoundException(
          s"Node with id $nodeId has been deleted in this transaction"
        )
      }

      ops.nodePropertyChangeInBatchOrNull(nodeId, propertyKey)
    } else {
      null
    }
  }

  override def getTxStateRelationshipPropertyOrNull(relId: Long, propertyKey: Int): Value = {
    if (relId != -1L) {
      val ops = reads()
      if (ops.relationshipDeletedInTransaction(relId)) {
        throw new EntityNotFoundException(
          s"Relationship with id $relId has been deleted in this transaction"
        )
      }

      ops.relationshipPropertyChangeInBatchOrNull(relId, propertyKey)
    } else {
      null
    }
  }

  class NodeReadOperations
      extends org.neo4j.cypher.internal.runtime.NodeReadOperations {

    override def propertyKeyIds(id: Long, nodeCursor: NodeCursor, propertyCursor: PropertyCursor): Array[Int] = {
      reads().singleNode(id, nodeCursor)
      if (!nodeCursor.next()) Array.empty
      else {
        val buffer = ArrayBuffer[Int]()
        nodeCursor.properties(propertyCursor)
        while (propertyCursor.next()) {
          buffer += propertyCursor.propertyKey()
        }
        buffer.toArray
      }
    }

    override def propertyKeyIds(obj: VirtualNodeValue, cursor: NodeCursor, propertyCursor: PropertyCursor): Array[Int] =
      propertyKeyIds(obj.id(), cursor, propertyCursor)

    override def getProperty(
      id: Long,
      propertyKeyId: Int,
      nodeCursor: NodeCursor,
      propertyCursor: PropertyCursor,
      throwOnDeleted: Boolean
    ): Value = {
      CursorUtils.nodeGetProperty(reads(), nodeCursor, id, propertyCursor, propertyKeyId, throwOnDeleted)
    }

    override def getProperty(
      obj: VirtualNodeValue,
      propertyKeyId: Int,
      cursor: NodeCursor,
      propertyCursor: PropertyCursor,
      throwOnDeleted: Boolean
    ): Value =
      CursorUtils.nodeGetProperty(reads(), cursor, obj.id(), propertyCursor, propertyKeyId, throwOnDeleted)

    override def getProperties(
      node: Long,
      properties: Array[Int],
      cursor: NodeCursor,
      propertyCursor: PropertyCursor
    ): Array[Value] = {
      CursorUtils.propertiesGet(properties, node, reads(), cursor, propertyCursor)
    }

    override def getProperties(
      node: VirtualNodeValue,
      properties: Array[Int],
      cursor: NodeCursor,
      propertyCursor: PropertyCursor
    ): Array[Value] = {
      CursorUtils.propertiesGet(properties, node.id(), reads(), cursor, propertyCursor)
    }

    override def getTxStateProperty(nodeId: Long, propertyKeyId: Int): Value =
      getTxStateNodePropertyOrNull(nodeId, propertyKeyId)

    override def hasProperty(
      id: Long,
      propertyKey: Int,
      nodeCursor: NodeCursor,
      propertyCursor: PropertyCursor
    ): Boolean = {
      CursorUtils.nodeHasProperty(reads(), nodeCursor, id, propertyCursor, propertyKey)
    }

    override def hasProperty(
      node: VirtualNodeValue,
      propertyKey: Int,
      nodeCursor: NodeCursor,
      propertyCursor: PropertyCursor
    ): Boolean = {
      CursorUtils.nodeHasProperty(reads(), nodeCursor, node.id(), propertyCursor, propertyKey)
    }

    override def hasTxStatePropertyForCachedProperty(nodeId: Long, propertyKeyId: Int): Option[Boolean] = {
      if (isDeletedInThisTx(nodeId)) {
        // Node deleted in TxState
        Some(false)
      } else {
        val nodePropertyInTx = reads().nodePropertyChangeInBatchOrNull(nodeId, propertyKeyId)
        nodePropertyInTx match {
          case null        => None // no changes in TxState.
          case IsNoValue() => Some(false) // property removed in TxState
          case _           => Some(true) // property changed in TxState
        }
      }
    }

    override def getById(id: Long): VirtualNodeValue = VirtualValues.node(id)

    override def all: ClosingLongIterator = {
      val nodeCursor = allocateAndTraceNodeCursor()
      reads().allNodesScan(nodeCursor)
      new PrimitiveCursorIterator {
        override protected def fetchNext(): Long = if (nodeCursor.next()) nodeCursor.nodeReference() else -1L

        override def close(): Unit = nodeCursor.close()
      }
    }

    override def isDeletedInThisTx(id: Long): Boolean = reads().nodeDeletedInTransaction(id)

    override def acquireExclusiveLock(obj: Long): Unit =
      transactionalContext.locks.acquireExclusiveNodeLock(obj)

    override def releaseExclusiveLock(obj: Long): Unit =
      transactionalContext.locks.releaseExclusiveNodeLock(obj)

    override def entityExists(id: Long): Boolean = id >= 0 && reads().nodeExists(id)
  }

  class RelationshipReadOperations extends org.neo4j.cypher.internal.runtime.RelationshipReadOperations {

    override def propertyKeyIds(
      id: Long,
      relationshipScanCursor: RelationshipScanCursor,
      propertyCursor: PropertyCursor
    ): Array[Int] = {
      reads().singleRelationship(id, relationshipScanCursor)
      if (!relationshipScanCursor.next()) Array.empty
      else {
        val buffer = ArrayBuffer[Int]()
        relationshipScanCursor.properties(propertyCursor)
        while (propertyCursor.next()) {
          buffer += propertyCursor.propertyKey()
        }
        buffer.toArray
      }
    }

    override def propertyKeyIds(
      rel: VirtualRelationshipValue,
      relationshipScanCursor: RelationshipScanCursor,
      propertyCursor: PropertyCursor
    ): Array[Int] = {
      CursorUtils.relationshipPropertyIds(reads(), rel, relationshipScanCursor, propertyCursor)
    }

    override def getProperty(
      id: Long,
      propertyKeyId: Int,
      relationshipCursor: RelationshipScanCursor,
      propertyCursor: PropertyCursor,
      throwOnDeleted: Boolean
    ): Value = {
      CursorUtils
        .relationshipGetProperty(reads(), relationshipCursor, id, propertyCursor, propertyKeyId, throwOnDeleted)
    }

    override def getProperty(
      obj: VirtualRelationshipValue,
      propertyKeyId: Int,
      cursor: RelationshipScanCursor,
      propertyCursor: PropertyCursor,
      throwOnDeleted: Boolean
    ): Value = {
      CursorUtils
        .relationshipGetProperty(reads(), cursor, obj, propertyCursor, propertyKeyId, throwOnDeleted)
    }

    override def getProperties(
      id: Long,
      properties: Array[Int],
      cursor: RelationshipScanCursor,
      propertyCursor: PropertyCursor
    ): Array[Value] = {
      CursorUtils.propertiesGet(properties, id, reads(), cursor, propertyCursor)
    }

    override def getProperties(
      id: VirtualRelationshipValue,
      properties: Array[Int],
      cursor: RelationshipScanCursor,
      propertyCursor: PropertyCursor
    ): Array[Value] = {
      CursorUtils.propertiesGet(properties, id, reads(), cursor, propertyCursor)
    }

    override def hasProperty(
      id: Long,
      propertyKey: Int,
      relationshipCursor: RelationshipScanCursor,
      propertyCursor: PropertyCursor
    ): Boolean = {
      CursorUtils.relationshipHasProperty(reads(), relationshipCursor, id, propertyCursor, propertyKey)
    }

    override def hasProperty(
      obj: VirtualRelationshipValue,
      propertyKey: Int,
      relationshipCursor: RelationshipScanCursor,
      propertyCursor: PropertyCursor
    ): Boolean = {
      CursorUtils.relationshipHasProperty(reads(), relationshipCursor, obj, propertyCursor, propertyKey)
    }

    override def getById(id: Long): VirtualRelationshipValue =
      try {
        VirtualValues.relationship(id)
      } catch {
        case e: NotFoundException => throw new EntityNotFoundException(s"Relationship with id $id", e)
      }

    override def entityExists(id: Long): Boolean = id >= 0 && reads().relationshipExists(id)

    override def all: ClosingLongIterator = {
      val relCursor = allocateAndTraceRelationshipScanCursor()
      reads().allRelationshipsScan(relCursor)
      new PrimitiveCursorIterator {
        override protected def fetchNext(): Long = if (relCursor.next()) relCursor.relationshipReference() else -1L

        override def close(): Unit = relCursor.close()
      }
    }

    override def isDeletedInThisTx(id: Long): Boolean =
      reads().relationshipDeletedInTransaction(id)

    override def acquireExclusiveLock(obj: Long): Unit =
      transactionalContext.locks.acquireExclusiveRelationshipLock(obj)

    override def releaseExclusiveLock(obj: Long): Unit =
      transactionalContext.locks.releaseExclusiveRelationshipLock(obj)

    override def getTxStateProperty(relId: Long, propertyKeyId: Int): Value =
      getTxStateRelationshipPropertyOrNull(relId, propertyKeyId)

    override def hasTxStatePropertyForCachedProperty(relId: Long, propertyKeyId: Int): Option[Boolean] = {
      if (isDeletedInThisTx(relId)) {
        // Relationship deleted in TxState
        Some(false)
      } else {
        val relPropertyInTx = reads().relationshipPropertyChangeInBatchOrNull(relId, propertyKeyId)
        relPropertyInTx match {
          case null        => None // no changes in TxState.
          case IsNoValue() => Some(false) // property removed in TxState
          case _           => Some(true) // property changed in TxState
        }
      }
    }
  }

  override def getAllIndexes(): Map[IndexDescriptor, IndexInfo] = {
    val schemaRead: SchemaReadCore = transactionalContext.schemaRead.snapshot()
    val indexes = schemaRead.indexesGetAll().asScala.toList

    indexes.foldLeft(Map[IndexDescriptor, IndexInfo]()) {
      (map, index) =>
        val indexStatus = getIndexStatus(schemaRead, index)
        val schema = index.schema
        val labelsOrTypes = tokenRead.entityTokensGetNames(schema.entityType(), schema.getEntityTokenIds).toList
        val properties = schema.getPropertyIds.map(id => tokenRead.propertyKeyGetName(id)).toList
        map + (index -> runtime.IndexInfo(indexStatus, labelsOrTypes, properties))
    }
  }

  private def getIndexStatus(schemaRead: SchemaReadCore, index: IndexDescriptor): IndexStatus = {
    val (
      state: String,
      failureMessage: String,
      populationProgress: Double,
      maybeConstraint: Option[ConstraintDescriptor]
    ) =
      try {
        val internalIndexState: InternalIndexState = schemaRead.indexGetState(index)
        val progress =
          schemaRead.indexGetPopulationProgress(index).toIndexPopulationProgress.getCompletedPercentage.toDouble
        val message: String =
          if (internalIndexState == InternalIndexState.FAILED) schemaRead.indexGetFailure(index) else ""
        val constraint: ConstraintDescriptor = schemaRead.constraintGetForName(index.getName)
        (internalIndexState.toString, message, progress, Option(constraint))
      } catch {
        case _: IndexNotFoundKernelException =>
          val errorMessage = "Index not found. It might have been concurrently dropped."
          ("NOT FOUND", errorMessage, 0.0, None)
      }
    IndexStatus(state, failureMessage, populationProgress, maybeConstraint)
  }

  override def getIndexUsageStatistics(index: IndexDescriptor): IndexUsageStats =
    transactionalContext.schemaRead.indexUsageStats(index)

  override def getIndexInformation(name: String): IndexInformation =
    getIndexInformation(transactionalContext.schemaRead.indexGetForName(name))

  override def getIndexInformation(index: IndexDescriptor): IndexInformation = {
    val name = index.getName
    val indexType = index.getIndexType

    val schema = index.schema()
    val entityType = schema.entityType()
    val (entities, props) = indexType match {
      case IndexType.LOOKUP =>
        (List.empty, List.empty)
      case _ =>
        val entities = schema.entityType() match {
          case EntityType.NODE =>
            schema.getEntityTokenIds.map(tokenNameLookup.labelGetName).toList
          case EntityType.RELATIONSHIP =>
            schema.getEntityTokenIds.map(tokenNameLookup.relationshipTypeGetName).toList
        }
        val props = schema.getPropertyIds.map(tokenNameLookup.propertyKeyGetName).toList
        (entities, props)
    }

    IndexInformation(entityType == EntityType.NODE, indexType, name, entities, props)
  }

  override def indexExists(name: String): Boolean =
    transactionalContext.schemaRead.indexGetForName(name) != IndexDescriptor.NO_INDEX

  override def constraintExists(name: String): Boolean =
    transactionalContext.schemaRead.constraintGetForName(name) != null

  override def constraintExists(matchFn: ConstraintDescriptor => Boolean, entityId: Int, properties: Int*): Boolean =
    transactionalContext.schemaRead.constraintsGetForSchema(
      SchemaDescriptors.forLabel(entityId, properties: _*)
    ).asScala.exists(matchFn) ||
      transactionalContext.schemaRead.constraintsGetForSchema(
        SchemaDescriptors.forRelType(entityId, properties: _*)
      ).asScala.exists(matchFn)

  override def getConstraintInformation(name: String): ConstraintInformation =
    getConstraintInfo(transactionalContext.schemaRead.constraintGetForName(name))

  override def getConstraintInformation(
    matchFn: ConstraintDescriptor => Boolean,
    entityId: Int,
    properties: Int*
  ): ConstraintInformation = {
    val nodeConstraints = transactionalContext.schemaRead.constraintsGetForSchema(
      SchemaDescriptors.forLabel(entityId, properties: _*)
    ).asScala.filter(matchFn)
    val relationshipConstraints = transactionalContext.schemaRead.constraintsGetForSchema(
      SchemaDescriptors.forRelType(entityId, properties: _*)
    ).asScala.filter(matchFn)
    // Return first found that matches
    val found = (nodeConstraints ++ relationshipConstraints).next()
    getConstraintInfo(found)
  }

  private def getConstraintInfo(constraint: ConstraintDescriptor): ConstraintInformation = {
    val name = constraint.getName

    val schema = constraint.schema()
    val (entity, isNode) = schema.entityType() match {
      case EntityType.NODE         => (tokenNameLookup.labelGetName(schema.getLabelId), true)
      case EntityType.RELATIONSHIP => (tokenNameLookup.relationshipTypeGetName(schema.getRelTypeId), false)
    }
    val props = schema.getPropertyIds.map(tokenNameLookup.propertyKeyGetName).toList
    val constraintType = constraint.`type`()

    val propertyType = if (constraint.isPropertyTypeConstraint) {
      Some(constraint.asPropertyTypeConstraint().propertyType().userDescription())
    } else None

    ConstraintInformation(isNode, constraintType, name, entity, props, propertyType)
  }

  override def getAllConstraints(): Map[ConstraintDescriptor, ConstraintInfo] = {
    val schemaRead: SchemaReadCore = transactionalContext.schemaRead.snapshot()
    val constraints = schemaRead.constraintsGetAll().asScala.toList

    constraints.foldLeft(Map[ConstraintDescriptor, ConstraintInfo]()) {
      (map, constraint) =>
        val schema = constraint.schema
        val labelsOrTypes = tokenRead.entityTokensGetNames(schema.entityType(), schema.getEntityTokenIds).toList
        val properties = schema.getPropertyIds.map(id => tokenRead.propertyKeyGetName(id)).toList
        val maybeIndex =
          try {
            Some(schemaRead.indexGetForName(constraint.getName))
          } catch {
            case _: IndexNotFoundKernelException => None
          }
        map + (constraint -> runtime.ConstraintInfo(labelsOrTypes, properties, maybeIndex))
    }
  }

  private val tokenNameLookup: TokenNameLookup = new TokenNameLookup {
    def propertyKeyGetName(propertyKeyId: Int): String = getPropertyKeyName(propertyKeyId)

    def labelGetName(labelId: Int): String = getLabelName(labelId)

    def relationshipTypeGetName(relTypeId: Int): String = getRelTypeName(relTypeId)
  }

  override def getImportDataConnection(uri: URI): CharReadable = {
    transactionalContext.getImportDataConnection(uri)
  }

  override def nodeCountByCountStore(labelId: Int): Long = {
    reads().countsForNode(labelId)
  }

  override def relationshipCountByCountStore(startLabelId: Int, typeId: Int, endLabelId: Int): Long = {
    reads().countsForRelationship(startLabelId, typeId, endLabelId)
  }

  override def lockNodes(nodeIds: Long*): Unit =
    nodeIds.sorted.foreach(transactionalContext.locks.acquireExclusiveNodeLock(_))

  override def lockRelationships(relIds: Long*): Unit =
    relIds.sorted
      .foreach(transactionalContext.locks.acquireExclusiveRelationshipLock(_))

  override def callReadOnlyProcedure(
    id: Int,
    args: Array[AnyValue],
    context: ProcedureCallContext
  ): Iterator[Array[AnyValue]] =
    CallSupport.callReadOnlyProcedure(transactionalContext.procedures, id, args, context)

  override def callReadWriteProcedure(
    id: Int,
    args: Array[AnyValue],
    context: ProcedureCallContext
  ): Iterator[Array[AnyValue]] =
    CallSupport.callReadWriteProcedure(transactionalContext.procedures, id, args, context)

  override def callSchemaWriteProcedure(
    id: Int,
    args: Array[AnyValue],
    context: ProcedureCallContext
  ): Iterator[Array[AnyValue]] =
    CallSupport.callSchemaWriteProcedure(transactionalContext.procedures, id, args, context)

  override def callDbmsProcedure(
    id: Int,
    args: Array[AnyValue],
    context: ProcedureCallContext
  ): Iterator[Array[AnyValue]] =
    CallSupport.callDbmsProcedure(transactionalContext.procedures, id, args, context)

  override def callFunction(id: Int, args: Array[AnyValue], context: ProcedureCallContext): AnyValue =
    CallSupport.callFunction(transactionalContext.procedures, id, args, context)

  override def callBuiltInFunction(id: Int, args: Array[AnyValue], context: ProcedureCallContext): AnyValue =
    CallSupport.callBuiltInFunction(transactionalContext.procedures, id, args, context)

  override def aggregateFunction(id: Int, context: ProcedureCallContext): UserAggregationReducer =
    CallSupport.aggregateFunction(transactionalContext.procedures, id, context)

  override def builtInAggregateFunction(id: Int, context: ProcedureCallContext): UserAggregationReducer =
    CallSupport.builtInAggregateFunction(transactionalContext.procedures, id, context)

  override def assertShowIndexAllowed(): Unit = {
    val ktx = transactionalContext.kernelTransaction
    ktx.securityAuthorizationHandler().assertShowIndexAllowed(transactionalContext.securityContext)
  }

  override def assertShowConstraintAllowed(): Unit = {
    val ktx = transactionalContext.kernelTransaction
    ktx.securityAuthorizationHandler().assertShowConstraintAllowed(transactionalContext.securityContext)
  }

  override def systemGraph: GraphDatabaseService = {
    transactionalContext.graph.getDependencyResolver.resolveDependency(classOf[DatabaseManagementService]).database(
      SYSTEM_DATABASE_NAME
    )
  }

  override def jobScheduler: JobScheduler = {
    transactionalContext.graph.getDependencyResolver.resolveDependency(classOf[JobScheduler])
  }

  override def logProvider: InternalLogProvider = {
    transactionalContext.graph.getDependencyResolver.resolveDependency(classOf[LogService]).getInternalLogProvider
  }

  override def providedLanguageFunctions: Seq[FunctionInformation] = {
    val dependencyResolver = transactionalContext.graph.getDependencyResolver
    dependencyResolver.resolveDependency(classOf[QueryExecutionEngine]).getProvidedLanguageFunctions.asScala.toSeq
  }

  override def getConfig: Config = {
    transactionalContext.config
  }

  override def getTransactionType: KernelTransaction.Type = transactionalContext.kernelTransaction.transactionType()

  override def contextWithNewTransaction(): TransactionBoundQueryContext = {
    val newTransactionalContext = transactionalContext.contextWithNewTransaction
    // creating the resource manager may fail since it will allocate memory in the memory tracker
    val newResourceManager =
      try {
        new ResourceManager(resources.monitor, newTransactionalContext.memoryTracker)
      } catch {
        case NonFatal(e) =>
          try {
            newTransactionalContext.rollback()
          } catch {
            case NonFatal(rollbackException) =>
              e.addSuppressed(rollbackException)
          }
          newTransactionalContext.close()
          throw e
      }

    val statement = newTransactionalContext.kernelTransactionalContext.statement()
    statement.registerCloseableResource(newResourceManager)
    // NOTE: It is not safe to call unregisterCloseableResource after commit or rollback.
    //       The resource will be unregistered anyway when the statement is closed, so we do not
    //       really need to have a callback just to call unregister.

    new TransactionBoundQueryContext(
      newTransactionalContext,
      newResourceManager,
      None
    )(indexSearchMonitor)
  }

  def close(): Unit = {
    closeable.foreach(_.close())
  }

  private def allocateAndTraceNodeCursor() = {
    val cursor = transactionalContext.cursors.allocateNodeCursor(
      transactionalContext.cursorContext,
      transactionalContext.memoryTracker
    )
    resources.trace(cursor)
    cursor
  }

  private def allocateAndTraceRelationshipScanCursor() = {
    val cursor = transactionalContext.cursors.allocateRelationshipScanCursor(
      transactionalContext.cursorContext,
      transactionalContext.memoryTracker
    )
    resources.trace(cursor)
    cursor
  }

  private def allocateAndTraceNodeValueIndexCursor() = {
    val cursor = transactionalContext.cursors.allocateNodeValueIndexCursor(
      transactionalContext.cursorContext,
      transactionalContext.memoryTracker
    )
    resources.trace(cursor)
    cursor
  }

  private def allocateAndTraceRelationshipValueIndexCursor() = {
    val cursor = transactionalContext.cursors.allocateRelationshipValueIndexCursor(
      transactionalContext.cursorContext,
      transactionalContext.memoryTracker
    )
    resources.untrace(cursor)
    resources.trace(cursor)
    cursor
  }

  private def allocateAndTraceNodeLabelIndexCursor() = {
    val cursor = transactionalContext.cursors.allocateNodeLabelIndexCursor(
      transactionalContext.cursorContext,
      transactionalContext.memoryTracker
    )
    resources.trace(cursor)
    cursor
  }

  override def entityTransformer: EntityTransformer = new TransactionBoundEntityTransformer

  class TransactionBoundEntityTransformer extends EntityTransformer {

    private[this] val cache = collection.mutable.Map.empty[AnyValue, AnyValue]

    override def needsRebinding(value: AnyValue): Boolean = value match {
      case _: NodeEntityWrappingNodeValue =>
        true
      case _: RelationshipEntityWrappingValue =>
        true
      case _: PathWrappingPathValue =>
        true

      case m: MapValue if !m.isEmpty =>
        var foundValue = false
        m.foreach((_, v) => {
          if (needsRebinding(v)) {
            // We could break here if we had something like `exists` or an entrySet-iterator on MapValue,
            // and we also want to avoid using a non-local return.
            // However, the common case is when we do not need to rebind and then we need to visit every entry anyway.
            foundValue = true
          }
        })
        foundValue

      case _: IntegralRangeListValue =>
        false

      case l: ListValue if !l.isEmpty =>
        val iter = l.iterator()
        var foundValue = false
        while (iter.hasNext && !foundValue) {
          val v = iter.next()
          if (needsRebinding(v)) {
            foundValue = true
          }
        }
        foundValue

      case _ =>
        false
    }

    override def rebindEntityWrappingValue(value: AnyValue): AnyValue = value match {

      case n: NodeEntityWrappingNodeValue =>
        rebindNode(n.getEntity)

      case r: RelationshipEntityWrappingValue =>
        rebindRelationship(r.getEntity)

      case p: PathWrappingPathValue =>
        val nodeValues: util.List[VirtualNodeValue] =
          util.Arrays.asList(p.path().nodes().asScala.map(rebindNode).toArray: _*)
        val relValues = util.Arrays.asList(p.path().relationships().asScala.map(rebindRelationship).toArray: _*)
        VirtualValues.pathReference(nodeValues, relValues)

      case m: MapValue if !m.isEmpty =>
        val builder = new MapValueBuilder(m.size())
        m.foreach((k, v) => builder.add(k, rebindEntityWrappingValue(v)))
        builder.build()

      case i: IntegralRangeListValue =>
        i

      case l: ListValue if !l.isEmpty =>
        cache.getOrElseUpdate(
          l, {
            val builder = ListValueBuilder.newListBuilder(l.size())
            l.forEach(v => builder.add(rebindEntityWrappingValue(v)))
            builder.build()
          }
        )

      case other =>
        other
    }

    private def rebindNode(node: Node): VirtualNodeValue =
      VirtualValues.node(node.getId)

    private def rebindRelationship(relationship: Relationship): VirtualRelationshipValue =
      VirtualValues.relationship(relationship.getId)
  }
}

object TransactionBoundQueryContext {

  abstract class PrimitiveCursorIterator extends ClosingLongIterator {
    private var _next: Long = fetchNext()

    protected def fetchNext(): Long

    override def innerHasNext: Boolean = _next >= 0

    override def next(): Long = {
      if (!hasNext) {
        Iterator.empty.next()
      }

      val current = _next
      _next = fetchNext()
      current
    }
  }

  abstract class CursorIterator[T] extends ClosingIterator[T] {
    private var _next: T = fetchNext()

    protected def fetchNext(): T

    override def innerHasNext: Boolean = _next != null

    override def next(): T = {
      if (!hasNext) {
        Iterator.empty.next()
      }

      val current = _next
      _next = fetchNext()
      current
    }
  }

  abstract class BaseRelationshipCursorIterator extends ClosingLongIterator with RelationshipIterator {

    import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.BaseRelationshipCursorIterator.NOT_INITIALIZED
    import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.BaseRelationshipCursorIterator.NO_ID

    private var _next = NOT_INITIALIZED
    protected var relTypeId: Int = NO_ID
    protected var source: Long = NO_ID
    protected var target: Long = NO_ID

    override def relationshipVisit[EXCEPTION <: Exception](
      relationshipId: Long,
      visitor: RelationshipVisitor[EXCEPTION]
    ): Boolean = {
      visitor.visit(relationshipId, relTypeId, source, target)
      true
    }

    protected def fetchNext(): Long

    override def innerHasNext: Boolean = {
      if (_next == NOT_INITIALIZED) {
        _next = fetchNext()
      }

      _next >= 0
    }

    override def startNodeId(): Long = source

    override def endNodeId(): Long = target

    override def typeId(): Int = relTypeId

    /**
     * Store the current state in case the underlying cursor is closed when calling next.
     */
    protected def storeState(): Unit

    override def next(): Long = {
      if (!hasNext) {
        close()
        Iterator.empty.next()
      }

      val current = _next
      storeState()
      // Note that if no more elements are found cursors
      // will be closed so no need to do an extra check after fetching
      _next = fetchNext()

      current
    }

    override def close(): Unit
  }

  class RelationshipCursorIterator(
    selectionCursor: RelationshipTraversalCursor,
    traversalCursor: RelationshipTraversalCursor = null
  ) extends BaseRelationshipCursorIterator {

    override protected def fetchNext(): Long =
      if (selectionCursor.next()) selectionCursor.relationshipReference()
      else {
        -1L
      }

    override protected def storeState(): Unit = {
      relTypeId = selectionCursor.`type`()
      source = selectionCursor.sourceNodeReference()
      target = selectionCursor.targetNodeReference()
    }

    override def close(): Unit = {
      if (traversalCursor != null && !(traversalCursor eq selectionCursor)) {
        traversalCursor.close()
      }
      selectionCursor.close()
    }
  }

  class RelationshipTypeCursorIterator(
    read: Read,
    typeIndexCursor: RelationshipTypeIndexCursor
  ) extends BaseRelationshipCursorIterator {

    override def relationshipVisit[EXCEPTION <: Exception](
      relationshipId: Long,
      visitor: RelationshipVisitor[EXCEPTION]
    ): Boolean = {
      visitor.visit(relationshipId, relTypeId, source, target)
      true
    }

    override protected def fetchNext(): Long = {
      while (typeIndexCursor.next()) {
        // check that relationship was successfully retrieved from store (protect against concurrent deletes)
        if (typeIndexCursor.readFromStore()) {
          return typeIndexCursor.relationshipReference()
        }
      }
      -1L
    }

    override protected def storeState(): Unit = {
      relTypeId = typeIndexCursor.`type`()
      source = typeIndexCursor.sourceNodeReference()
      target = typeIndexCursor.targetNodeReference()
    }

    override def close(): Unit = {
      typeIndexCursor.close()
    }
  }

  object BaseRelationshipCursorIterator {
    private val NOT_INITIALIZED = -2L
    private val NO_ID = -1
  }

  trait IndexSearchMonitor {

    def indexSeek(index: IndexDescriptor, values: Seq[Any]): Unit

    def lockingUniqueIndexSeek(index: IndexDescriptor, values: Seq[Any]): Unit
  }

  object IndexSearchMonitor {

    val NOOP: IndexSearchMonitor = new IndexSearchMonitor {
      override def indexSeek(index: IndexDescriptor, values: Seq[Any]): Unit = {}

      override def lockingUniqueIndexSeek(index: IndexDescriptor, values: Seq[Any]): Unit = {}
    }
  }
}
