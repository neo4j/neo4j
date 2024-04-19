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
package org.neo4j.cypher.internal.runtime.interpreted

import org.eclipse.collections.api.map.primitive.IntObjectMap
import org.eclipse.collections.api.set.primitive.IntSet
import org.eclipse.collections.impl.factory.primitive.IntSets
import org.neo4j.common.EntityType
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.runtime
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingLongIterator
import org.neo4j.cypher.internal.runtime.ConstraintInfo
import org.neo4j.cypher.internal.runtime.EntityTransformer
import org.neo4j.cypher.internal.runtime.Expander
import org.neo4j.cypher.internal.runtime.IndexInfo
import org.neo4j.cypher.internal.runtime.IndexStatus
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.KernelAPISupport.RANGE_SEEKABLE_VALUE_GROUPS
import org.neo4j.cypher.internal.runtime.KernelAPISupport.asKernelIndexOrder
import org.neo4j.cypher.internal.runtime.KernelPredicate
import org.neo4j.cypher.internal.runtime.NodeValueHit
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.ReadQueryContext
import org.neo4j.cypher.internal.runtime.RelationshipIterator
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.UserDefinedAggregator
import org.neo4j.cypher.internal.runtime.ValuedNodeIndexCursor
import org.neo4j.cypher.internal.runtime.ValuedRelationshipIndexCursor
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.RelationshipCursorIterator
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.RelationshipTypeCursorIterator
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.DirectionConverter.toGraphDb
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.OnlyDirectionExpander
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.TypeAndDirectionExpander
import org.neo4j.cypher.operations.CursorUtils
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.dbms.database.DatabaseContext
import org.neo4j.dbms.database.DatabaseManager
import org.neo4j.exceptions.EntityNotFoundException
import org.neo4j.exceptions.FailedIndexException
import org.neo4j.graphalgo.BasicEvaluationContext
import org.neo4j.graphalgo.impl.path.ShortestPath
import org.neo4j.graphalgo.impl.path.ShortestPath.ShortestPathPredicate
import org.neo4j.graphdb.Entity
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.NotFoundException
import org.neo4j.graphdb.Path
import org.neo4j.graphdb.PathExpanderBuilder
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.security.URLAccessValidationError
import org.neo4j.internal.helpers.collection.Iterators
import org.neo4j.internal.kernel.api
import org.neo4j.internal.kernel.api.IndexQueryConstraints
import org.neo4j.internal.kernel.api.IndexQueryConstraints.ordered
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.InternalIndexState
import org.neo4j.internal.kernel.api.NodeCursor
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
import org.neo4j.internal.kernel.api.SchemaWrite
import org.neo4j.internal.kernel.api.TokenPredicate
import org.neo4j.internal.kernel.api.TokenRead
import org.neo4j.internal.kernel.api.TokenReadSession
import org.neo4j.internal.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException
import org.neo4j.internal.kernel.api.helpers.Nodes
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections.allCursor
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections.incomingCursor
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections.outgoingCursor
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.internal.schema.ConstraintType
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.internal.schema.IndexDescriptor
import org.neo4j.internal.schema.IndexPrototype
import org.neo4j.internal.schema.IndexProviderDescriptor
import org.neo4j.internal.schema.IndexType
import org.neo4j.internal.schema.SchemaDescriptor
import org.neo4j.internal.schema.SchemaDescriptors
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.StatementConstants
import org.neo4j.kernel.api.exceptions.schema.EquivalentSchemaRuleAlreadyExistsException
import org.neo4j.kernel.impl.core.TransactionalEntityFactory
import org.neo4j.kernel.impl.query.FunctionInformation
import org.neo4j.kernel.impl.query.QueryExecutionEngine
import org.neo4j.kernel.impl.util.DefaultValueMapper
import org.neo4j.kernel.impl.util.NodeEntityWrappingNodeValue
import org.neo4j.kernel.impl.util.PathWrappingPathValue
import org.neo4j.kernel.impl.util.RelationshipEntityWrappingValue
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.logging.LogProvider
import org.neo4j.logging.internal.LogService
import org.neo4j.memory.MemoryTracker
import org.neo4j.storageengine.api.RelationshipVisitor
import org.neo4j.values.AnyValue
import org.neo4j.values.ValueMapper
import org.neo4j.values.storable.FloatingPointValue
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

import java.net.URL
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal


sealed class TransactionBoundQueryContext(transactionalContext: TransactionalContextWrapper,
                                          resources: ResourceManager,
                                          closeable: Option[AutoCloseable] = None)
                                         (implicit indexSearchMonitor: IndexSearchMonitor)
  extends TransactionBoundReadQueryContext(transactionalContext, resources, closeable) with QueryContext {

  override val nodeWriteOps: NodeWriteOperations = new NodeWriteOperations
  override val relationshipWriteOps: RelationshipWriteOperations = new RelationshipWriteOperations

  private def writes() = transactionalContext.dataWrite

  private def tokenWrite = transactionalContext.tokenWrite

  override def createParallelQueryContext(): QueryContext = {
    // TODO: As an optimization, create a single-threaded copy of ResourceManager attach to the ThreadSafeResourceManager coming in
    val parallelTransactionalContext = transactionalContext.createParallelTransactionalContext()
    new ParallelTransactionBoundQueryContext(parallelTransactionalContext, resources, closeable)(indexSearchMonitor)
  }

  override def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int = labelIds.foldLeft(0) {
    case (count, labelId) => if (writes().nodeAddLabel(node, labelId)) count + 1 else count
  }

  override def createNodeId(labels: Array[Int]): Long = writes().nodeCreateWithLabels(labels)

  override def createRelationshipId(start: Long, end: Long, relType: Int): Long = writes().relationshipCreate(start, relType, end)

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

  override def addBtreeIndexRule(entityId: Int, entityType: EntityType, propertyKeyIds: Seq[Int], name: Option[String], provider: Option[String], indexConfig: IndexConfig): IndexDescriptor = {
    val indexProvider = provider.map(p => transactionalContext.schemaWrite.indexProviderByName(p))
    val (descriptor, prototype) = getIndexDescriptorAndPrototype(IndexType.BTREE, entityId, entityType, propertyKeyIds, name, indexProvider)
    val prototypeWithConfig = prototype.withIndexConfig(indexConfig)
    addIndexRule(descriptor, prototypeWithConfig)
  }

  override def addRangeIndexRule(entityId: Int, entityType: EntityType, propertyKeyIds: Seq[Int], name: Option[String], provider: Option[IndexProviderDescriptor]): IndexDescriptor = {
    val (descriptor, prototype) = getIndexDescriptorAndPrototype(IndexType.RANGE, entityId, entityType, propertyKeyIds, name, provider)
    addIndexRule(descriptor, prototype)
  }

  override def addLookupIndexRule(entityType: EntityType, name: Option[String], provider: Option[IndexProviderDescriptor]): IndexDescriptor = {
    val descriptor = SchemaDescriptors.forAnyEntityTokens(entityType)
    val prototype = provider.map(IndexPrototype.forSchema(descriptor, _)).getOrElse(IndexPrototype.forSchema(descriptor)).withIndexType(IndexType.LOOKUP)
    val namedPrototype = name.map(n => prototype.withName(n)).getOrElse(prototype)
    addIndexRule(descriptor, namedPrototype)
  }

  override def addFulltextIndexRule(entityIds: List[Int],
                                    entityType: EntityType,
                                    propertyKeyIds: Seq[Int],
                                    name: Option[String],
                                    provider: Option[IndexProviderDescriptor],
                                    indexConfig: IndexConfig): IndexDescriptor = {
    val descriptor = SchemaDescriptors.fulltext(entityType, entityIds.toArray, propertyKeyIds.toArray)
    val prototype =
      provider.map(p => IndexPrototype.forSchema(descriptor, p)).getOrElse(IndexPrototype.forSchema(descriptor))
        .withIndexType(IndexType.FULLTEXT)
        .withIndexConfig(indexConfig)
    val namedPrototype = name.map(n => prototype.withName(n)).getOrElse(prototype)
    addIndexRule(descriptor, namedPrototype)
  }

  override def addTextIndexRule(entityId: Int, entityType: EntityType, propertyKeyIds: Seq[Int], name: Option[String], provider: Option[IndexProviderDescriptor]): IndexDescriptor = {
    val (descriptor, prototype) = getIndexDescriptorAndPrototype(IndexType.TEXT, entityId, entityType, propertyKeyIds, name, provider)
    addIndexRule(descriptor, prototype)
  }

  override def addPointIndexRule(entityId: Int, entityType: EntityType, propertyKeyIds: Seq[Int], name: Option[String], provider: Option[IndexProviderDescriptor], indexConfig: IndexConfig): IndexDescriptor = {
    val (descriptor, prototype) = getIndexDescriptorAndPrototype(IndexType.POINT, entityId, entityType, propertyKeyIds, name, provider)
    val prototypeWithConfig = prototype.withIndexConfig(indexConfig)
    addIndexRule(descriptor, prototypeWithConfig)
  }

  private def getIndexDescriptorAndPrototype(indexType: IndexType,
                                             entityId: Int,
                                             entityType: EntityType,
                                             propertyKeyIds: Seq[Int],
                                             name: Option[String],
                                             provider: Option[IndexProviderDescriptor]): (SchemaDescriptor, IndexPrototype) = {
    val descriptor = entityType match {
      case EntityType.NODE         => SchemaDescriptors.forLabel(entityId, propertyKeyIds: _*)
      case EntityType.RELATIONSHIP => SchemaDescriptors.forRelType(entityId, propertyKeyIds: _*)
    }
    val prototype = provider.map(p => IndexPrototype.forSchema(descriptor, p)).getOrElse(IndexPrototype.forSchema(descriptor)).withIndexType(indexType)
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

  override def dropIndexRule(labelId: Int, propertyKeyIds: Seq[Int]): Unit =
    transactionalContext.schemaWrite
      .indexDrop(SchemaDescriptors.forLabel(labelId, propertyKeyIds: _*))

  override def dropIndexRule(name: String): Unit =
    transactionalContext.schemaWrite.indexDrop(name)

  override def createNodeKeyConstraint(labelId: Int,
                                       propertyKeyIds: Seq[Int],
                                       name: Option[String],
                                       provider: Option[String],
                                       indexConfig: IndexConfig): Unit = {
    val schemaWrite = transactionalContext.schemaWrite
    val indexPrototype = getUniqueIndexPrototype(labelId, propertyKeyIds, name, provider, indexConfig, schemaWrite)
    schemaWrite.nodeKeyConstraintCreate(indexPrototype)
  }

  override def dropNodeKeyConstraint(labelId: Int, propertyKeyIds: Seq[Int]): Unit =
    transactionalContext.schemaWrite
      .constraintDrop(SchemaDescriptors.forLabel(labelId, propertyKeyIds: _*), ConstraintType.UNIQUE_EXISTS)

  override def createUniqueConstraint(labelId: Int,
                                      propertyKeyIds: Seq[Int],
                                      name: Option[String],
                                      provider: Option[String],
                                      indexConfig: IndexConfig): Unit = {
    val schemaWrite = transactionalContext.schemaWrite
    val indexPrototype = getUniqueIndexPrototype(labelId, propertyKeyIds, name, provider, indexConfig, schemaWrite)
    schemaWrite.uniquePropertyConstraintCreate(indexPrototype)
  }

  private def getUniqueIndexPrototype(labelId: Int,
                                      propertyKeyIds: Seq[Int],
                                      name: Option[String],
                                      provider: Option[String],
                                      indexConfig: IndexConfig,
                                      schemaWrite: SchemaWrite) = {
    val descriptor = SchemaDescriptors.forLabel(labelId, propertyKeyIds: _*)
    val providerAndType = provider.map(p => (schemaWrite.indexProviderByName(p), schemaWrite.indexTypeByProviderName(p)))

    val indexPrototype = providerAndType.map { case (provider, indexType) => IndexPrototype.uniqueForSchema(descriptor, provider).withIndexType(indexType) }
      .getOrElse(IndexPrototype.uniqueForSchema(descriptor))

    indexPrototype.withName(name.orNull).withIndexConfig(indexConfig)
  }

  override def dropUniqueConstraint(labelId: Int, propertyKeyIds: Seq[Int]): Unit =
    transactionalContext.schemaWrite
      .constraintDrop(SchemaDescriptors.forLabel(labelId, propertyKeyIds: _*), ConstraintType.UNIQUE)

  override def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int, name: Option[String]): Unit =
    transactionalContext.schemaWrite.nodePropertyExistenceConstraintCreate(
      SchemaDescriptors.forLabel(labelId, propertyKeyId), name.orNull)

  override def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int): Unit =
    transactionalContext.schemaWrite
      .constraintDrop(SchemaDescriptors.forLabel(labelId, propertyKeyId), ConstraintType.EXISTS)

  override def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int,
                                                             name: Option[String]): Unit =
    transactionalContext.schemaWrite.relationshipPropertyExistenceConstraintCreate(
      SchemaDescriptors.forRelType(relTypeId, propertyKeyId), name.orNull)

  override def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int): Unit =
    transactionalContext.schemaWrite
      .constraintDrop(SchemaDescriptors.forRelType(relTypeId, propertyKeyId), ConstraintType.EXISTS)

  override def dropNamedConstraint(name: String): Unit =
    transactionalContext.schemaWrite.constraintDrop(name)

  override def detachDeleteNode(node: Long): Int = transactionalContext.dataWrite.nodeDetachDelete(node)

  override def assertSchemaWritesAllowed(): Unit =
    transactionalContext.schemaWrite

  override def getDatabaseManager: DatabaseManager[DatabaseContext] = {
    val dependencyResolver = transactionalContext.graph.getDependencyResolver
    dependencyResolver.resolveDependency(classOf[DatabaseManager[_ <: DatabaseContext]]).asInstanceOf[DatabaseManager[DatabaseContext]]
  }

  override def getConfig: Config =
    transactionalContext.graph.getDependencyResolver.resolveDependency(classOf[Config])

  override def nodeApplyChanges(node: Long,
                                addedLabels: IntSet,
                                removedLabels: IntSet,
                                properties: IntObjectMap[Value]): Unit = {
    writes().nodeApplyChanges(node, addedLabels, removedLabels, properties)
  }

  override def relationshipApplyChanges(relationship: Long,
                                        properties: IntObjectMap[Value]): Unit = {
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
        case _: api.exceptions.EntityNotFoundException => //ignore
      }
    }

    override def setProperties(obj: Long,
                               properties: IntObjectMap[Value]): Unit = {
      try {
        writes().nodeApplyChanges(obj, IntSets.immutable.empty(), IntSets.immutable.empty(), properties)
      } catch {
        case _: api.exceptions.EntityNotFoundException => //ignore
      }
    }
  }

  class RelationshipWriteOperations extends RelationshipReadOperations with org.neo4j.cypher.internal.runtime.RelationshipOperations {
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
        case _: api.exceptions.EntityNotFoundException => //ignore
      }
    }

    override def setProperties(obj: Long,
                               properties: IntObjectMap[Value]): Unit = {
      try {
        writes().relationshipApplyChanges(obj, properties)
      } catch {
        case _: api.exceptions.EntityNotFoundException => //ignore
      }
    }
  }
}

private[internal] class TransactionBoundReadQueryContext(val transactionalContext: TransactionalContextWrapper,
                                                         val resources: ResourceManager,
                                                         private val closeable: Option[AutoCloseable] = None)
                                                        (implicit indexSearchMonitor: IndexSearchMonitor)
  extends TransactionBoundReadTokenContext(transactionalContext) with ReadQueryContext {

  override val nodeReadOps: NodeReadOperations = new NodeReadOperations
  override val relationshipReadOps: RelationshipReadOperations = new RelationshipReadOperations

  // TODO: Make parallel transaction use safe. Entity values hold a reference to InternalTransaction, which is not thread-safe.
  private[internal] lazy val entityAccessor: TransactionalEntityFactory = transactionalContext.kernelTransactionalContext.transaction()
  private[internal] lazy val valueMapper: ValueMapper[java.lang.Object] = new DefaultValueMapper(transactionalContext.kernelTransactionalContext.transaction())

  //We cannot assign to value because of periodic commit
  protected def reads(): Read = transactionalContext.dataRead

  private def allocateNodeCursor() = transactionalContext.cursors.allocateNodeCursor( transactionalContext.cursorContext )

  protected def tokenRead: TokenRead = transactionalContext.tokenRead

  override def singleNode(id: Long, cursor: NodeCursor): Unit = {
    reads().singleNode(id, cursor)
  }

  override def singleRelationship(id: Long, cursor: RelationshipScanCursor): Unit = {
    reads().singleRelationship(id, cursor)
  }

  override def getLabelsForNode(node: Long, nodeCursor: NodeCursor): ListValue = {
    reads().singleNode(node, nodeCursor)
    if (!nodeCursor.next()) {
      if (nodeReadOps.isDeletedInThisTx(node))
        throw new EntityNotFoundException(s"Node with id $node has been deleted in this transaction")
      else
        VirtualValues.EMPTY_LIST
    }
    val labelSet = nodeCursor.labels()
    val labelArray = new Array[TextValue](labelSet.numberOfTokens())
    var i = 0
    while (i < labelSet.numberOfTokens()) {
      labelArray(i) = Values.stringValue(tokenRead.nodeLabelName(labelSet.token(i)))
      i += 1
    }
    VirtualValues.list(labelArray: _*)
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

  override def isAnyLabelSetOnNode(labels: Array[Int], id: Long, nodeCursor: NodeCursor): Boolean = {
    CursorUtils.nodeHasAnyLabel(reads(), nodeCursor, id, labels)
  }

  override def isTypeSetOnRelationship(typ: Int, id: Long, relationshipCursor: RelationshipScanCursor): Boolean = {
    CursorUtils.relationshipHasType(reads(), relationshipCursor, id, typ)
  }

  override def getRelationshipsForIds(node: Long, dir: SemanticDirection,
                                      types: Array[Int]): ClosingLongIterator with RelationshipIterator = {
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
          case BOTH => allCursor(cursors, cursor, types, cursorContext)
        }
        resources.trace(selectionCursor)
        new RelationshipCursorIterator(selectionCursor)
      }
    } finally {
      cursor.close()
    }
  }

  override def getRelationshipsByType(session: TokenReadSession, relType: Int, indexOrder: IndexOrder): ClosingLongIterator with RelationshipIterator = {
    val read = reads()
    val typeCursor = transactionalContext.cursors.allocateRelationshipTypeIndexCursor(transactionalContext.cursorContext)
    val relCursor = transactionalContext.cursors.allocateRelationshipScanCursor(transactionalContext.cursorContext)
    read.relationshipTypeScan(session, typeCursor, ordered(asKernelIndexOrder(indexOrder)), new TokenPredicate(relType), transactionalContext.cursorContext)
    resources.trace(typeCursor)
    resources.trace(relCursor)
    new RelationshipTypeCursorIterator(read, typeCursor, relCursor)
  }

  override def nodeCursor(): NodeCursor =
    transactionalContext.cursors.allocateNodeCursor(transactionalContext.cursorContext)

  override def traversalCursor(): RelationshipTraversalCursor =
    transactionalContext.cursors.allocateRelationshipTraversalCursor(transactionalContext.cursorContext)

  override def scanCursor(): RelationshipScanCursor =
    transactionalContext.cursors.allocateRelationshipScanCursor(transactionalContext.kernelTransaction.cursorContext)

  override def relationshipById(relationshipId: Long,
                                startNodeId: Long,
                                endNodeId: Long,
                                typeId: Int): VirtualRelationshipValue =
      VirtualValues.relationship(relationshipId, startNodeId, endNodeId, typeId)

  override def nodeIndexSeek(index: IndexReadSession,
                             needsValues: Boolean,
                             indexOrder: IndexOrder,
                             predicates: Seq[PropertyIndexQuery]): NodeValueIndexCursor = {
    val impossiblePredicate =
      predicates.exists {
        case p: PropertyIndexQuery.ExactPredicate => (p.value() eq Values.NO_VALUE) || (p.value().isInstanceOf[FloatingPointValue] && p.value().asInstanceOf[FloatingPointValue].isNaN)
        case _: PropertyIndexQuery.ExistsPredicate => predicates.length <= 1
        case p: PropertyIndexQuery.RangePredicate[_] =>
          !RANGE_SEEKABLE_VALUE_GROUPS.contains(p.valueGroup())
        case _ => false
      }

    if (impossiblePredicate) {
      NodeValueIndexCursor.EMPTY
    } else {
      innerNodeIndexSeek(index, needsValues, indexOrder, predicates: _*)
    }
  }

  override def relationshipIndexSeek(index: IndexReadSession,
                                     needsValues: Boolean,
                                     indexOrder: IndexOrder,
                                     predicates: Seq[PropertyIndexQuery]): RelationshipValueIndexCursor = {

    val impossiblePredicate =
      predicates.exists {
        case p: PropertyIndexQuery.ExactPredicate => (p.value() eq Values.NO_VALUE) || (p.value().isInstanceOf[FloatingPointValue] && p.value().asInstanceOf[FloatingPointValue].isNaN)
        case _: PropertyIndexQuery.ExistsPredicate => predicates.length <= 1
        case p: PropertyIndexQuery.RangePredicate[_] =>
          !RANGE_SEEKABLE_VALUE_GROUPS.contains(p.valueGroup())
        case _ => false
      }

    if (impossiblePredicate) {
      RelationshipValueIndexCursor.EMPTY
    } else {
      innerRelationshipIndexSeek(index, needsValues, indexOrder, predicates: _*)
    }
  }

  override def relationshipIndexSeekByContains(index: IndexReadSession,
                                               needsValues: Boolean,
                                               indexOrder: IndexOrder,
                                               value: TextValue): RelationshipValueIndexCursor =
    innerRelationshipIndexSeek(index, needsValues, indexOrder,
      PropertyIndexQuery.stringContains(index.reference().schema().getPropertyIds()(0), value))

  override def relationshipIndexSeekByEndsWith(index: IndexReadSession,
                                               needsValues: Boolean,
                                               indexOrder: IndexOrder,
                                               value: TextValue): RelationshipValueIndexCursor =
    innerRelationshipIndexSeek(index, needsValues, indexOrder,
      PropertyIndexQuery.stringSuffix(index.reference().schema().getPropertyIds()(0), value))

  override def relationshipIndexScan(index: IndexReadSession,
                                     needsValues: Boolean,
                                     indexOrder: IndexOrder): RelationshipValueIndexCursor = {
    val relCursor = allocateAndTraceRelationshipValueIndexCursor()
    reads().relationshipIndexScan(index, relCursor, IndexQueryConstraints.constrained(asKernelIndexOrder(indexOrder), needsValues))
    relCursor
  }

  override def lookupIndexReference(entityType: EntityType): IndexDescriptor = {
    val descriptor = SchemaDescriptors.forAnyEntityTokens(entityType)
    Iterators.single(transactionalContext.schemaRead.index(descriptor))
  }

  override def fulltextIndexReference(entityIds: List[Int], entityType: EntityType, properties: Int*): IndexDescriptor = {
    val descriptor = SchemaDescriptors.fulltext(entityType, entityIds.toArray, properties.toArray)
    Iterators.single(transactionalContext.schemaRead.index(descriptor))
  }

  override def indexReference(indexType: IndexType, entityId: Int, entityType: EntityType, properties: Int*): IndexDescriptor = {
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
    throw new NoSuchElementException(s"No such ${indexType.toString.toLowerCase} index exists.")
  }

  private def innerNodeIndexSeek(index: IndexReadSession,
                                 needsValues: Boolean,
                                 indexOrder: IndexOrder,
                                 queries: PropertyIndexQuery*): NodeValueIndexCursor = {

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
    reads().nodeIndexSeek(transactionalContext.kernelQueryContext, index, nodeCursor,
      IndexQueryConstraints.constrained(asKernelIndexOrder(indexOrder), needsValuesFromIndexSeek), queries: _*)
    if (needsValues && actualValues != null) {
      new ValuedNodeIndexCursor(nodeCursor, actualValues)
    } else {
      nodeCursor
    }
  }

  private def innerRelationshipIndexSeek(index: IndexReadSession,
                                         needsValues: Boolean,
                                         indexOrder: IndexOrder,
                                         queries: PropertyIndexQuery*): RelationshipValueIndexCursor = {

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
    reads().relationshipIndexSeek(transactionalContext.kernelQueryContext, index, relCursor, IndexQueryConstraints.constrained(asKernelIndexOrder(indexOrder), needsValuesFromIndexSeek), queries: _*)
    if (needsValues && actualValues != null) {
      new ValuedRelationshipIndexCursor(relCursor, actualValues)
    } else {
      relCursor
    }
  }

  override def nodeIndexScan(index: IndexReadSession,
                             needsValues: Boolean,
                             indexOrder: IndexOrder): NodeValueIndexCursor = {
    val nodeCursor = allocateAndTraceNodeValueIndexCursor()
    reads().nodeIndexScan(index, nodeCursor, IndexQueryConstraints.constrained(asKernelIndexOrder(indexOrder), needsValues))
    nodeCursor
  }

  override def nodeIndexSeekByContains(index: IndexReadSession,
                                       needsValues: Boolean,
                                       indexOrder: IndexOrder,
                                       value: TextValue): NodeValueIndexCursor =
    innerNodeIndexSeek(index, needsValues, indexOrder,
      PropertyIndexQuery.stringContains(index.reference().schema().getPropertyIds()(0), value))

  override def nodeIndexSeekByEndsWith(index: IndexReadSession,
                                       needsValues: Boolean,
                                       indexOrder: IndexOrder,
                                       value: TextValue): NodeValueIndexCursor =
    innerNodeIndexSeek(index, needsValues, indexOrder, PropertyIndexQuery.stringSuffix(index.reference().schema().getPropertyIds()(0), value))

  override def nodeLockingUniqueIndexSeek(index: IndexDescriptor,
                                          queries: Seq[PropertyIndexQuery.ExactPredicate]): NodeValueIndexCursor = {

    val cursor = transactionalContext.cursors.allocateNodeValueIndexCursor(transactionalContext.cursorContext, transactionalContext.memoryTracker)
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

  override def nodeAsMap(id: Long, nodeCursor: NodeCursor, propertyCursor: PropertyCursor): MapValue = {
    reads().singleNode(id, nodeCursor)
    if (!nodeCursor.next()) VirtualValues.EMPTY_MAP
    else {
      val tokens = tokenRead
      nodeCursor.properties(propertyCursor)
      val builder = new MapValueBuilder()
      while (propertyCursor.next()) {
        builder.add(tokens.propertyKeyName(propertyCursor.propertyKey()), propertyCursor.propertyValue())
      }
      builder.build()
    }
  }

  override def relationshipAsMap(id: Long, relationshipCursor: RelationshipScanCursor,
                                 propertyCursor: PropertyCursor): MapValue = {
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

  override def getNodesByLabel(tokenReadSession: TokenReadSession, id: Int, indexOrder: IndexOrder): ClosingLongIterator = {
    val cursor = allocateAndTraceNodeLabelIndexCursor()
    reads().nodeLabelScan(tokenReadSession, cursor, ordered(asKernelIndexOrder(indexOrder)), new TokenPredicate(id),
      transactionalContext.cursorContext)
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

  override def nodeGetOutgoingDegreeWithMax(maxDegree: Int, node: Long, relationship: Int, nodeCursor: NodeCursor): Int = {
    reads().singleNode(node, nodeCursor)
    if (!nodeCursor.next()) 0
    else {
      Nodes.countWithMax(maxDegree, nodeCursor, relationship, org.neo4j.graphdb.Direction.OUTGOING)
    }
  }

  override def nodeGetIncomingDegreeWithMax(maxDegree: Int, node: Long, relationship: Int, nodeCursor: NodeCursor): Int = {
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

  override def getTxStateNodePropertyOrNull(nodeId: Long,
                                            propertyKey: Int): Value = {
    val ops = reads()
    if (ops.nodeDeletedInTransaction(nodeId)) {
      throw new EntityNotFoundException(
        s"Node with id $nodeId has been deleted in this transaction")
    }

    ops.nodePropertyChangeInTransactionOrNull(nodeId, propertyKey)
  }

  override def getTxStateRelationshipPropertyOrNull(relId: Long,
                                                    propertyKey: Int): Value = {
    val ops = reads()
    if (ops.relationshipDeletedInTransaction(relId)) {
      throw new EntityNotFoundException(
        s"Relationship with id $relId has been deleted in this transaction")
    }

    ops.relationshipPropertyChangeInTransactionOrNull(relId, propertyKey)
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
          buffer.append(propertyCursor.propertyKey())
        }
        buffer.toArray
      }
    }

    override def getProperty(id: Long, propertyKeyId: Int, nodeCursor: NodeCursor, propertyCursor: PropertyCursor,
                             throwOnDeleted: Boolean): Value = {
      CursorUtils.nodeGetProperty(reads(), nodeCursor, id, propertyCursor, propertyKeyId, throwOnDeleted)
    }

    override def getTxStateProperty(nodeId: Long, propertyKeyId: Int): Value =
      getTxStateNodePropertyOrNull(nodeId, propertyKeyId)

    override def hasProperty(id: Long, propertyKey: Int, nodeCursor: NodeCursor,
                             propertyCursor: PropertyCursor): Boolean = {
      CursorUtils.nodeHasProperty(reads(), nodeCursor, id, propertyCursor, propertyKey)
    }

    override def hasTxStatePropertyForCachedProperty(nodeId: Long, propertyKeyId: Int): Option[Boolean] = {
      if (isDeletedInThisTx(nodeId)) {
        // Node deleted in TxState
        Some(false)
      } else {
        val nodePropertyInTx = reads().nodePropertyChangeInTransactionOrNull(nodeId, propertyKeyId)
        nodePropertyInTx match {
          case null => None // no changes in TxState.
          case IsNoValue() => Some(false) // property removed in TxState
          case _ => Some(true) // property changed in TxState
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

    override def propertyKeyIds(id: Long, relationshipScanCursor: RelationshipScanCursor,
                                propertyCursor: PropertyCursor): Array[Int] = {
      reads().singleRelationship(id, relationshipScanCursor)
      if (!relationshipScanCursor.next()) Array.empty
      else {
        val buffer = ArrayBuffer[Int]()
        relationshipScanCursor.properties(propertyCursor)
        while (propertyCursor.next()) {
          buffer.append(propertyCursor.propertyKey())
        }
        buffer.toArray
      }
    }

    override def getProperty(id: Long, propertyKeyId: Int, relationshipCursor: RelationshipScanCursor,
                             propertyCursor: PropertyCursor, throwOnDeleted: Boolean): Value = {
      CursorUtils
        .relationshipGetProperty(reads(), relationshipCursor, id, propertyCursor, propertyKeyId, throwOnDeleted)
    }

    override def hasProperty(id: Long, propertyKey: Int, relationshipCursor: RelationshipScanCursor,
                             propertyCursor: PropertyCursor): Boolean = {
      CursorUtils.relationshipHasProperty(reads(), relationshipCursor, id, propertyCursor, propertyKey)
    }

    override def getById(id: Long): VirtualRelationshipValue = try {
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
        val relPropertyInTx = reads().relationshipPropertyChangeInTransactionOrNull(relId, propertyKeyId)
        relPropertyInTx match {
          case null => None // no changes in TxState.
          case IsNoValue() => Some(false) // property removed in TxState
          case _ => Some(true) // property changed in TxState
        }
      }
    }
  }

  override def getAllIndexes(): Map[IndexDescriptor, IndexInfo] = {
    val schemaRead: SchemaReadCore = transactionalContext.schemaRead.snapshot()
    val indexes = schemaRead.indexesGetAll().asScala.toList

    indexes.foldLeft(Map[IndexDescriptor, IndexInfo]()) {
      (map, index) =>
        val indexStatus = getIndexStatus( schemaRead, index )
        val schema = index.schema
        val labelsOrTypes = tokenRead.entityTokensGetNames( schema.entityType(), schema.getEntityTokenIds).toList
        val properties = schema.getPropertyIds.map( id => tokenRead.propertyKeyGetName(id)).toList
        map + (index -> runtime.IndexInfo(indexStatus, labelsOrTypes, properties))
    }
  }

  private def getIndexStatus(schemaRead: SchemaReadCore, index: IndexDescriptor): IndexStatus = {
    val (state: String, failureMessage: String, populationProgress: Double, maybeConstraint: Option[ConstraintDescriptor]) =
    try {
      val internalIndexState: InternalIndexState = schemaRead.indexGetState(index)
      val progress = schemaRead.indexGetPopulationProgress(index).toIndexPopulationProgress.getCompletedPercentage.toDouble
      val message: String = if (internalIndexState == InternalIndexState.FAILED) schemaRead.indexGetFailure(index) else ""
      val constraint: ConstraintDescriptor = schemaRead.constraintGetForName(index.getName)
      (internalIndexState.toString, message,  progress, Option(constraint))
    } catch {
      case _: IndexNotFoundKernelException =>
        val errorMessage = "Index not found. It might have been concurrently dropped."
        ("NOT FOUND", errorMessage, 0.0, None)
    }
    IndexStatus(state, failureMessage, populationProgress, maybeConstraint)
  }

  override def indexExists(name: String): Boolean =
    transactionalContext.schemaRead.indexGetForName(name) != IndexDescriptor.NO_INDEX

  override def constraintExists(name: String): Boolean =
    transactionalContext.schemaRead.constraintGetForName(name) != null

  override def constraintExists(matchFn: ConstraintDescriptor => Boolean, entityId: Int, properties: Int*): Boolean =
    transactionalContext.schemaRead.constraintsGetForSchema(SchemaDescriptors.forLabel(entityId, properties: _*)).asScala.exists(matchFn) ||
      transactionalContext.schemaRead.constraintsGetForSchema(SchemaDescriptors.forRelType(entityId, properties: _*)).asScala.exists(matchFn)

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

  override def getImportURL(url: URL): Either[String, URL] = transactionalContext.graph match {
    case db: GraphDatabaseQueryService =>
      try {
        Right(db.validateURLAccess(url))
      } catch {
        case error: URLAccessValidationError => Left(error.getMessage)
      }
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

  override def singleShortestPath(left: Long, right: Long, depth: Int, expander: Expander,
                                  pathPredicate: KernelPredicate[Path],
                                  filters: Seq[KernelPredicate[Entity]],
                                  memoryTracker: MemoryTracker): Option[Path] = {
    val pathFinder = buildPathFinder(depth, expander, pathPredicate, filters, memoryTracker)

    //could probably do without node proxies here
    Option(pathFinder.findSinglePath(entityAccessor.newNodeEntity(left), entityAccessor.newNodeEntity(right)))
  }

  override def allShortestPath(left: Long, right: Long, depth: Int, expander: Expander,
                               pathPredicate: KernelPredicate[Path],
                               filters: Seq[KernelPredicate[Entity]], memoryTracker: MemoryTracker): ClosingIterator[Path] = {
    val pathFinder = buildPathFinder(depth, expander, pathPredicate, filters, memoryTracker)

    pathFinder.findAllPathsAutoCloseableIterator(entityAccessor.newNodeEntity(left), entityAccessor.newNodeEntity(right))
  }

  override def callReadOnlyProcedure(id: Int, args: Array[AnyValue],
                                     context: ProcedureCallContext): Iterator[Array[AnyValue]] =
    CallSupport.callReadOnlyProcedure(transactionalContext.procedures, id, args, context)

  override def callReadWriteProcedure(id: Int, args: Array[AnyValue],
                                      context: ProcedureCallContext): Iterator[Array[AnyValue]] =
    CallSupport.callReadWriteProcedure(transactionalContext.procedures, id, args, context)

  override def callSchemaWriteProcedure(id: Int, args: Array[AnyValue],
                                        context: ProcedureCallContext): Iterator[Array[AnyValue]] =
    CallSupport.callSchemaWriteProcedure(transactionalContext.procedures, id, args, context)

  override def callDbmsProcedure(id: Int, args: Array[AnyValue],
                                 context: ProcedureCallContext): Iterator[Array[AnyValue]] =
    CallSupport.callDbmsProcedure(transactionalContext.procedures, id, args, context)

  override def callFunction(id: Int, args: Array[AnyValue]): AnyValue =
    CallSupport.callFunction(transactionalContext.procedures, id, args)

  override def callBuiltInFunction(id: Int, args: Array[AnyValue]): AnyValue =
    CallSupport.callBuiltInFunction(transactionalContext.procedures, id, args)

  override def aggregateFunction(id: Int): UserDefinedAggregator =
    CallSupport.aggregateFunction(transactionalContext.procedures, id)

  override def builtInAggregateFunction(id: Int): UserDefinedAggregator =
    CallSupport.builtInAggregateFunction(transactionalContext.procedures, id)

  private def buildPathFinder(depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path],
                              filters: Seq[KernelPredicate[Entity]], memoryTracker: MemoryTracker): ShortestPath = {
    val startExpander = expander match {
      case OnlyDirectionExpander(_, _, dir) =>
        PathExpanderBuilder.allTypes(toGraphDb(dir))
      case TypeAndDirectionExpander(_, _, typDirs) =>
        typDirs.foldLeft(PathExpanderBuilder.empty()) {
          case (acc, (typ, dir)) => acc.add(RelationshipType.withName(typ), toGraphDb(dir))
        }
    }

    val expanderWithNodeFilters = expander.nodeFilters.foldLeft(startExpander) {
      case (acc, filter) => acc.addNodeFilter((t: Entity) => filter.test(t))
    }
    val expanderWithAllPredicates = expander.relFilters.foldLeft(expanderWithNodeFilters) {
      case (acc, filter) => acc.addRelationshipFilter((t: Entity) => filter.test(t))
    }
    val shortestPathPredicate = new ShortestPathPredicate {
      override def test(path: Path): Boolean = pathPredicate.test(path)
    }

    new ShortestPath(new BasicEvaluationContext(null /* ShortestPath does not need transaction */, null /* or GraphDatabaseService */),
                     depth, expanderWithAllPredicates.build(), shortestPathPredicate, memoryTracker) {
      override protected def filterNextLevelNodes(nextNode: Node): Node =
        if (filters.isEmpty) nextNode
        else if (filters.forall(filter => filter test nextNode)) nextNode
        else null
    }
  }

  override def assertShowIndexAllowed(): Unit = {
    val ktx = transactionalContext.kernelTransaction
    ktx.securityAuthorizationHandler().assertShowIndexAllowed(transactionalContext.securityContext)
  }

  override def assertShowConstraintAllowed(): Unit = {
    val ktx = transactionalContext.kernelTransaction
    ktx.securityAuthorizationHandler().assertShowConstraintAllowed(transactionalContext.securityContext)
  }

  override def systemGraph: GraphDatabaseService = {
    transactionalContext.graph.getDependencyResolver.resolveDependency(classOf[DatabaseManagementService]).database(SYSTEM_DATABASE_NAME)
  }

  override def logProvider: LogProvider  = {
    transactionalContext.graph.getDependencyResolver.resolveDependency(classOf[LogService]).getInternalLogProvider
  }

  override def providedLanguageFunctions: Seq[FunctionInformation] = {
    val dependencyResolver = transactionalContext.graph.getDependencyResolver
    dependencyResolver.resolveDependency(classOf[QueryExecutionEngine]).getProvidedLanguageFunctions.asScala
  }

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
    val closeable: AutoCloseable = () => statement.unregisterCloseableResource(newResourceManager)

      new TransactionBoundQueryContext(
        newTransactionalContext,
        newResourceManager,
        Some(closeable)
      )(indexSearchMonitor)
  }

  def close(): Unit = {
    closeable.foreach(_.close())
  }

  private def allocateAndTraceNodeCursor() = {
    val cursor = transactionalContext.cursors.allocateNodeCursor(transactionalContext.cursorContext)
    resources.trace(cursor)
    cursor
  }

  private def allocateAndTraceRelationshipScanCursor() = {
    val cursor = transactionalContext.cursors.allocateRelationshipScanCursor(transactionalContext.cursorContext)
    resources.trace(cursor)
    cursor
  }

  private def allocateAndTraceNodeValueIndexCursor() = {
    val cursor = transactionalContext.cursors.allocateNodeValueIndexCursor(transactionalContext.cursorContext, transactionalContext.memoryTracker)
    resources.trace(cursor)
    cursor
  }

  private def allocateAndTraceRelationshipValueIndexCursor() = {
    val cursor = transactionalContext.cursors.allocateRelationshipValueIndexCursor(transactionalContext.cursorContext, transactionalContext.memoryTracker)
    resources.trace(cursor)
    cursor
  }

  private def allocateAndTraceNodeLabelIndexCursor() = {
    val cursor = transactionalContext.cursors.allocateNodeLabelIndexCursor(transactionalContext.cursorContext)
    resources.trace(cursor)
    cursor
  }

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

  override def entityTransformer: EntityTransformer = new TransactionBoundEntityTransformer

  class TransactionBoundEntityTransformer extends EntityTransformer {

    private[this] val cache = collection.mutable.Map.empty[AnyValue, AnyValue]

    override def rebindEntityWrappingValue(value: AnyValue): AnyValue = value match {

      case n: NodeEntityWrappingNodeValue =>
        rebindNode(n.getEntity)

      case r: RelationshipEntityWrappingValue =>
        rebindRelationship(r.getEntity)

      case p: PathWrappingPathValue =>
        val nodeValues = p.path().nodes().asScala.map(rebindNode).toArray
        val relValues = p.path().relationships().asScala.map(rebindRelationship).toArray
        VirtualValues.pathReference(nodeValues, relValues)

      case m: MapValue if !m.isEmpty =>
        val builder = new MapValueBuilder(m.size())
        m.foreach((k, v) => builder.add(k, rebindEntityWrappingValue(v)))
        builder.build()

      case i: IntegralRangeListValue =>
        i

      case l: ListValue if !l.isEmpty => cache.getOrElseUpdate(l, {
        val builder = ListValueBuilder.newListBuilder(l.size())
        l.forEach(v => builder.add(rebindEntityWrappingValue(v)))
        builder.build()
      })

      case other =>
        other
    }

    private def rebindNode(node: Node): VirtualNodeValue =
      ValueUtils.fromNodeEntity(entityAccessor.newNodeEntity(node.getId))

    private def rebindRelationship(relationship:Relationship): VirtualRelationshipValue =
      ValueUtils.fromRelationshipEntity(entityAccessor.newRelationshipEntity(relationship.getId))
  }
}

object TransactionBoundQueryContext {

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

    override def relationshipVisit[EXCEPTION <: Exception](relationshipId: Long,
                                                           visitor: RelationshipVisitor[EXCEPTION]): Boolean = {
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

  class RelationshipCursorIterator(selectionCursor: RelationshipTraversalCursor, traversalCursor: RelationshipTraversalCursor = null) extends BaseRelationshipCursorIterator {

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

  class RelationshipTypeCursorIterator(read: Read, typeIndexCursor: RelationshipTypeIndexCursor, scanCursor: RelationshipScanCursor) extends BaseRelationshipCursorIterator {

    override def relationshipVisit[EXCEPTION <: Exception](relationshipId: Long,
                                                           visitor: RelationshipVisitor[EXCEPTION]): Boolean = {
      visitor.visit(relationshipId, relTypeId, source, target)
      true
    }

    private def nextFromRelStore(): Boolean = {
      read.singleRelationship(typeIndexCursor.relationshipReference(), scanCursor)
      scanCursor.next()
    }

    override protected def fetchNext(): Long = {
      while (typeIndexCursor.next()) {
        // check that relationship was successfully retrieved from store (protect against concurrent deletes)
        if (nextFromRelStore()) {
          return scanCursor.relationshipReference()
        }
      }
      -1L
    }

    override protected def storeState(): Unit = {
      relTypeId = scanCursor.`type`()
      source = scanCursor.sourceNodeReference()
      target = scanCursor.targetNodeReference()
    }

    override def close(): Unit = {
      typeIndexCursor.close()
      scanCursor.close()
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

      override def lockingUniqueIndexSeek(index: IndexDescriptor,
                                          values: Seq[Any]): Unit = {}
    }
  }
}
