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
package org.neo4j.cypher.internal.spi.v3_1

import java.net.URL
import java.util.function.Predicate

import org.neo4j.collection.RawIterator
import org.neo4j.collection.primitive.PrimitiveLongIterator
import org.neo4j.collection.primitive.base.Empty.EMPTY_PRIMITIVE_LONG_COLLECTION
import org.neo4j.cypher.internal.compiler.v3_1.MinMaxOrdering.{BY_NUMBER, BY_STRING, BY_VALUE}
import org.neo4j.cypher.internal.compiler.v3_1._
import org.neo4j.cypher.internal.compiler.v3_1.ast.convert.commands.DirectionConverter.toGraphDb
import org.neo4j.cypher.internal.compiler.v3_1.commands.expressions
import org.neo4j.cypher.internal.compiler.v3_1.commands.expressions.{KernelPredicate, OnlyDirectionExpander, TypeAndDirectionExpander}
import org.neo4j.cypher.internal.compiler.v3_1.helpers.JavaConversionSupport
import org.neo4j.cypher.internal.compiler.v3_1.helpers.JavaConversionSupport._
import org.neo4j.cypher.internal.compiler.v3_1.pipes.matching.PatternNode
import org.neo4j.cypher.internal.compiler.v3_1.spi._
import org.neo4j.cypher.internal.frontend.v3_1.{Bound, EntityNotFoundException, FailedIndexException, SemanticDirection}
import org.neo4j.cypher.internal.spi.BeansAPIRelationshipIterator
import org.neo4j.cypher.internal.spi.v3_1.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.cypher.{InternalException, internal}
import org.neo4j.graphalgo.impl.path.ShortestPath
import org.neo4j.graphalgo.impl.path.ShortestPath.ShortestPathPredicate
import org.neo4j.graphdb.RelationshipType._
import org.neo4j.graphdb._
import org.neo4j.graphdb.security.URLAccessValidationError
import org.neo4j.graphdb.traversal.{Evaluators, TraversalDescription, Uniqueness}
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api._
import org.neo4j.kernel.api.constraints.{NodePropertyExistenceConstraint, RelationshipPropertyExistenceConstraint, UniquenessConstraint}
import org.neo4j.kernel.api.dbms.DbmsOperations
import org.neo4j.kernel.api.exceptions.ProcedureException
import org.neo4j.kernel.api.exceptions.schema.{AlreadyConstrainedException, AlreadyIndexedException}
import org.neo4j.kernel.api.index.InternalIndexState
import org.neo4j.kernel.api.proc.{QualifiedName => KernelQualifiedName}
import org.neo4j.kernel.api.schema_new.IndexQuery
import org.neo4j.kernel.api.schema_new.SchemaDescriptorFactory
import org.neo4j.kernel.api.schema_new.constaints.{ConstraintBoundary, ConstraintDescriptorFactory}
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptorFactory
import org.neo4j.kernel.impl.core.NodeManager
import org.neo4j.kernel.impl.locking.ResourceTypes

import scala.collection.Iterator
import scala.collection.JavaConverters._

final class TransactionBoundQueryContext(txContext: TransactionalContextWrapper)(implicit indexSearchMonitor: IndexSearchMonitor)
  extends TransactionBoundTokenContext(txContext.statement) with QueryContext with IndexDescriptorCompatibility {

  type EntityAccessor = NodeManager

  val nodeOps = new NodeOperations
  val relationshipOps = new RelationshipOperations

  override lazy val entityAccessor = txContext.graph.getDependencyResolver.resolveDependency(classOf[NodeManager])

  override def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int = labelIds.foldLeft(0) {
    case (count, labelId) => if (txContext.statement.dataWriteOperations().nodeAddLabel(node, labelId)) count + 1 else count
  }

  override def withAnyOpenQueryContext[T](work: (QueryContext) => T): T = {
    if (txContext.isOpen) {
      work(this)
    } else {
      val context = txContext.getOrBeginNewIfClosed()
      var success = false
      try {
        val result = work(new TransactionBoundQueryContext(context))
        success = true
        result
      } finally {
        context.close(success)
      }
    }
  }

  override def createNode(): Node =
    txContext.graph.createNode()

  override def createRelationship(start: Node, end: Node, relType: String) =
    start.createRelationshipTo(end, withName(relType))

  override def createRelationship(start: Long, end: Long, relType: Int) = {
    val relId = txContext.statement.dataWriteOperations().relationshipCreate(relType, start, end)
    relationshipOps.getById(relId)
  }

  override def getOrCreateRelTypeId(relTypeName: String): Int =
    txContext.statement.tokenWriteOperations().relationshipTypeGetOrCreateForName(relTypeName)

  override def getLabelsForNode(node: Long) = try {
    JavaConversionSupport.asScala(txContext.statement.readOperations().nodeGetLabels(node))
  } catch {
    case e: org.neo4j.kernel.api.exceptions.EntityNotFoundException =>
      if (nodeOps.isDeletedInThisTx(node))
        throw new EntityNotFoundException(s"Node with id $node has been deleted in this transaction", e)
      else
        Iterator.empty
  }

  override def getPropertiesForNode(node: Long) =
    JavaConversionSupport.asScala(txContext.statement.readOperations().nodeGetPropertyKeys(node))

  override def getPropertiesForRelationship(relId: Long) =
    JavaConversionSupport.asScala(txContext.statement.readOperations().relationshipGetPropertyKeys(relId))

  override def isLabelSetOnNode(label: Int, node: Long) =
    txContext.statement.readOperations().nodeHasLabel(node, label)

  override def getOrCreateLabelId(labelName: String) =
    txContext.statement.tokenWriteOperations().labelGetOrCreateForName(labelName)

  def getRelationshipsForIds(node: Node, dir: SemanticDirection, types: Option[Seq[Int]]): Iterator[Relationship] = {
    val relationships = types match {
      case None =>
        txContext.statement.readOperations().nodeGetRelationships(node.getId, toGraphDb(dir))
      case Some(typeIds) =>
        txContext.statement.readOperations().nodeGetRelationships(node.getId, toGraphDb(dir), typeIds.toArray)
    }
    new BeansAPIRelationshipIterator(relationships, entityAccessor)
  }

  override def indexSeek(index: IndexDescriptor, value: Any) = {
    indexSearchMonitor.indexSeek(index, value)
    JavaConversionSupport.mapToScalaENFXSafe(txContext.statement.readOperations().indexQuery(index, IndexQuery.exact(index.property, value)))(nodeOps.getById)
  }

  override def indexSeekByRange(index: IndexDescriptor, value: Any) = value match {

    case PrefixRange(prefix: String) =>
      indexSeekByPrefixRange(index, prefix)
    case range: InequalitySeekRange[Any] =>
      indexSeekByPrefixRange(index, range)

    case range =>
      throw new InternalException(s"Unsupported index seek by range: $range")
  }

  private def indexSeekByPrefixRange(index: IndexDescriptor, range: InequalitySeekRange[Any]): scala.Iterator[Node] = {
    val groupedRanges = range.groupBy { (bound: Bound[Any]) =>
      bound.endPoint match {
        case n: Number => classOf[Number]
        case s: String => classOf[String]
        case c: Character => classOf[String]
        case _ => classOf[Any]
      }
    }

      val optNumericRange = groupedRanges.get(classOf[Number]).map(_.asInstanceOf[InequalitySeekRange[Number]])
      val optStringRange = groupedRanges.get(classOf[String]).map(_.mapBounds(_.toString))
      val anyRange = groupedRanges.get(classOf[Any])

      if (anyRange.nonEmpty) {
        // If we get back an exclusion test, the range could return values otherwise it is empty
        anyRange.get.inclusionTest[Any](BY_VALUE).map { test =>
          throw new IllegalArgumentException("Cannot compare a property against values that are neither strings nor numbers.")
        }.getOrElse(Iterator.empty)
      } else {
        (optNumericRange, optStringRange) match {
          case (Some(numericRange), None) => indexSeekByNumericalRange(index, numericRange)
          case (None, Some(stringRange)) => indexSeekByStringRange(index, stringRange)

          case (Some(numericRange), Some(stringRange)) =>
            // Consider MATCH (n:Person) WHERE n.prop < 1 AND n.prop > "London":
            // The order of predicate evaluation is unspecified, i.e.
            // LabelScan fby Filter(n.prop < 1) fby Filter(n.prop > "London") is a valid plan
            // If the first filter returns no results, the plan returns no results.
            // If the first filter returns any result, the following filter will fail since
            // comparing string against numbers throws an exception. Same for the reverse case.
            //
            // Below we simulate this behaviour:
            //
            if (indexSeekByNumericalRange( index, numericRange ).isEmpty
                || indexSeekByStringRange(index, stringRange).isEmpty) {
              Iterator.empty
            } else {
              throw new IllegalArgumentException(s"Cannot compare a property against both numbers and strings. They are incomparable.")
            }

          case (None, None) =>
            // If we get here, the non-empty list of range bounds was partitioned into two empty ones
            throw new IllegalStateException("Failed to partition range bounds")
        }
      }
  }

  private def indexSeekByPrefixRange(index: IndexDescriptor, prefix: String): scala.Iterator[Node] = {
    val indexedNodes = txContext.statement.readOperations().indexQuery(index, IndexQuery.stringPrefix(index.property, prefix))
    JavaConversionSupport.mapToScalaENFXSafe(indexedNodes)(nodeOps.getById)
  }

  private def indexSeekByNumericalRange(index: IndexDescriptor, range: InequalitySeekRange[Number]): scala.Iterator[Node] = {
    val readOps = txContext.statement.readOperations()
    val matchingNodes: PrimitiveLongIterator = (range match {

      case rangeLessThan: RangeLessThan[Number] =>
        rangeLessThan.limit(BY_NUMBER).map { limit =>
          readOps.indexQuery(index, IndexQuery.range(index.property, null, false, limit.endPoint, limit.isInclusive))
        }

      case rangeGreaterThan: RangeGreaterThan[Number] =>
        rangeGreaterThan.limit(BY_NUMBER).map { limit =>
          readOps.indexQuery(index, IndexQuery.range(index.property, limit.endPoint, limit.isInclusive, null, false))
        }

      case RangeBetween(rangeGreaterThan, rangeLessThan) =>
        rangeGreaterThan.limit(BY_NUMBER).flatMap { greaterThanLimit =>
          rangeLessThan.limit(BY_NUMBER).map { lessThanLimit =>
            readOps.indexQuery(index, IndexQuery.range(index.property, greaterThanLimit.endPoint, greaterThanLimit.isInclusive, lessThanLimit.endPoint, lessThanLimit.isInclusive))
          }
        }
    }).getOrElse(EMPTY_PRIMITIVE_LONG_COLLECTION.iterator)
    JavaConversionSupport.mapToScalaENFXSafe(matchingNodes)(nodeOps.getById)
  }

  private def indexSeekByStringRange(index: IndexDescriptor, range: InequalitySeekRange[String]): scala.Iterator[Node] = {
    val readOps = txContext.statement.readOperations()
    val matchingNodes: PrimitiveLongIterator = range match {

      case rangeLessThan: RangeLessThan[String] =>
        rangeLessThan.limit(BY_STRING).map { limit =>
          readOps.indexQuery(index, IndexQuery.range(index.property, null, false, limit.endPoint.asInstanceOf[String], limit.isInclusive))
        }.getOrElse(EMPTY_PRIMITIVE_LONG_COLLECTION.iterator)

      case rangeGreaterThan: RangeGreaterThan[String] =>
        rangeGreaterThan.limit(BY_STRING).map { limit =>
          readOps.indexQuery(index, IndexQuery.range(index.property, limit.endPoint.asInstanceOf[String], limit.isInclusive, null, false))
        }.getOrElse(EMPTY_PRIMITIVE_LONG_COLLECTION.iterator)

      case RangeBetween(rangeGreaterThan, rangeLessThan) =>
        rangeGreaterThan.limit(BY_STRING).flatMap { greaterThanLimit =>
          rangeLessThan.limit(BY_STRING).map { lessThanLimit =>
            readOps.indexQuery(index, IndexQuery.range(index.property, greaterThanLimit.endPoint.asInstanceOf[String], greaterThanLimit.isInclusive, lessThanLimit.endPoint.asInstanceOf[String], lessThanLimit.isInclusive))
          }
        }.getOrElse(EMPTY_PRIMITIVE_LONG_COLLECTION.iterator)
    }

    JavaConversionSupport.mapToScalaENFXSafe(matchingNodes)(nodeOps.getById)
  }

  override def indexScan(index: IndexDescriptor) =
    mapToScalaENFXSafe(txContext.statement.readOperations().indexQuery(index, IndexQuery.exists(index.property)))(nodeOps.getById)

  override def indexScanByContains(index: IndexDescriptor, value: String) =
    mapToScalaENFXSafe(txContext.statement.readOperations().indexQuery(index, IndexQuery.stringContains(index.property, value)))(nodeOps.getById)

  override def indexScanByEndsWith(index: IndexDescriptor, value: String) =
    mapToScalaENFXSafe(txContext.statement.readOperations().indexQuery(index, IndexQuery.stringSuffix(index.property, value)))(nodeOps.getById)

  override def lockingUniqueIndexSeek(index: IndexDescriptor, value: Any): Option[Node] = {
    indexSearchMonitor.lockingUniqueIndexSeek(index, value)
    val nodeId = txContext.statement.readOperations().nodeGetFromUniqueIndexSeek(index, IndexQuery.exact(index.property, value))
    if (StatementConstants.NO_SUCH_NODE == nodeId) None else Some(nodeOps.getById(nodeId))
  }

  override def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int = labelIds.foldLeft(0) {
    case (count, labelId) =>
      if (txContext.statement.dataWriteOperations().nodeRemoveLabel(node, labelId)) count + 1 else count
  }

  override def getNodesByLabel(id: Int): Iterator[Node] =
    JavaConversionSupport.mapToScalaENFXSafe(txContext.statement.readOperations().nodesGetForLabel(id))(nodeOps.getById)

  override def nodeGetDegree(node: Long, dir: SemanticDirection): Int =
    txContext.statement.readOperations().nodeGetDegree(node, toGraphDb(dir))

  override def nodeGetDegree(node: Long, dir: SemanticDirection, relTypeId: Int): Int =
    txContext.statement.readOperations().nodeGetDegree(node, toGraphDb(dir), relTypeId)

  override def nodeIsDense(node: Long): Boolean = txContext.statement.readOperations().nodeIsDense(node)

  class NodeOperations extends BaseOperations[Node] {
    override def delete(obj: Node) {
      try {
        txContext.statement.dataWriteOperations().nodeDelete(obj.getId)
      } catch {
        case _: exceptions.EntityNotFoundException => // node has been deleted by another transaction, oh well...
      }
    }

    override def propertyKeyIds(id: Long): Iterator[Int] = try {
      JavaConversionSupport.asScalaENFXSafe(txContext.statement.readOperations().nodeGetPropertyKeys(id))
    } catch {
      case _: exceptions.EntityNotFoundException => Iterator.empty
    }

    override def getProperty(id: Long, propertyKeyId: Int): Any = try {
      txContext.statement.readOperations().nodeGetProperty(id, propertyKeyId)
    } catch {
      case e: org.neo4j.kernel.api.exceptions.EntityNotFoundException =>
        if (isDeletedInThisTx(id))
          throw new EntityNotFoundException(s"Node with id $id has been deleted in this transaction", e)
        else
          null
    }

    override def hasProperty(id: Long, propertyKey: Int) = try {
      txContext.statement.readOperations().nodeHasProperty(id, propertyKey)
    } catch {
      case _: exceptions.EntityNotFoundException => false
    }

    override def removeProperty(id: Long, propertyKeyId: Int) {
      try {
        txContext.statement.dataWriteOperations().nodeRemoveProperty(id, propertyKeyId)
      } catch {
        case _: exceptions.EntityNotFoundException => //ignore
      }
    }

    override def setProperty(id: Long, propertyKeyId: Int, value: Any) {
      try {
        txContext.statement.dataWriteOperations().nodeSetProperty(id, properties.Property.property(propertyKeyId, value) )
      } catch {
        case _: exceptions.EntityNotFoundException => //ignore
      }
    }

    override def getById(id: Long) = try {
      txContext.graph.getNodeById(id)
    } catch {
      case e: NotFoundException => throw new EntityNotFoundException(s"Node with id $id", e)
    }

    override def all: Iterator[Node] =
      JavaConversionSupport.mapToScalaENFXSafe(txContext.statement.readOperations().nodesGetAll())(getById)

    override def indexGet(name: String, key: String, value: Any): Iterator[Node] =
      JavaConversionSupport.mapToScalaENFXSafe(txContext.statement.readOperations().nodeLegacyIndexGet(name, key, value))(getById)

    override def indexQuery(name: String, query: Any): Iterator[Node] =
      JavaConversionSupport.mapToScalaENFXSafe(txContext.statement.readOperations().nodeLegacyIndexQuery(name, query))(getById)

    override def isDeletedInThisTx(n: Node): Boolean = isDeletedInThisTx(n.getId)

    def isDeletedInThisTx(id: Long): Boolean =
      txContext.stateView.hasTxStateWithChanges && txContext.stateView.txState().nodeIsDeletedInThisTx(id)

    override def acquireExclusiveLock(obj: Long) =
      txContext.statement.readOperations().acquireExclusive(ResourceTypes.NODE, obj)

    override def releaseExclusiveLock(obj: Long) =
      txContext.statement.readOperations().releaseExclusive(ResourceTypes.NODE, obj)
  }

  class RelationshipOperations extends BaseOperations[Relationship] {

    override def delete(obj: Relationship) {
      try {
        txContext.statement.dataWriteOperations().relationshipDelete(obj.getId)
      } catch {
        case _: exceptions.EntityNotFoundException => // node has been deleted by another transaction, oh well...
      }
    }

    override def propertyKeyIds(id: Long): Iterator[Int] = try {
      asScalaENFXSafe(txContext.statement.readOperations().relationshipGetPropertyKeys(id))
    } catch {
      case _: exceptions.EntityNotFoundException => Iterator.empty
    }

    override def getProperty(id: Long, propertyKeyId: Int): Any = try {
      txContext.statement.readOperations().relationshipGetProperty(id, propertyKeyId)
    } catch {
      case e: org.neo4j.kernel.api.exceptions.EntityNotFoundException =>
        if (isDeletedInThisTx(id))
          throw new EntityNotFoundException(s"Relationship with id $id has been deleted in this transaction", e)
        else
          null
    }

    override def hasProperty(id: Long, propertyKey: Int) = try {
      txContext.statement.readOperations().relationshipHasProperty(id, propertyKey)
    } catch {
      case _: exceptions.EntityNotFoundException => false
    }

    override def removeProperty(id: Long, propertyKeyId: Int) {
      try {
        txContext.statement.dataWriteOperations().relationshipRemoveProperty(id, propertyKeyId)
      } catch {
        case _: exceptions.EntityNotFoundException => //ignore
      }
    }

    override def setProperty(id: Long, propertyKeyId: Int, value: Any) {
      try {
        txContext.statement.dataWriteOperations().relationshipSetProperty(id, properties.Property.property(propertyKeyId, value))
      } catch {
        case _: exceptions.EntityNotFoundException => //ignore
      }
    }

    override def getById(id: Long) = try {
      txContext.graph.getRelationshipById(id)
    } catch {
      case e: NotFoundException => throw new EntityNotFoundException(s"Relationship with id $id", e)
    }

    override def all: Iterator[Relationship] = {
      JavaConversionSupport.mapToScalaENFXSafe(txContext.statement.readOperations().relationshipsGetAll())(getById)
    }

    override def indexGet(name: String, key: String, value: Any): Iterator[Relationship] =
      JavaConversionSupport.mapToScalaENFXSafe(txContext.statement.readOperations().relationshipLegacyIndexGet(name, key, value, -1, -1))(getById)

    override def indexQuery(name: String, query: Any): Iterator[Relationship] =
      JavaConversionSupport.mapToScalaENFXSafe(txContext.statement.readOperations().relationshipLegacyIndexQuery(name, query, -1, -1))(getById)

    override def isDeletedInThisTx(r: Relationship): Boolean =
      isDeletedInThisTx(r.getId)

    def isDeletedInThisTx(id: Long): Boolean =
      txContext.stateView.hasTxStateWithChanges && txContext.stateView.txState().relationshipIsDeletedInThisTx(id)

    override def acquireExclusiveLock(obj: Long) =
      txContext.statement.readOperations().acquireExclusive(ResourceTypes.RELATIONSHIP, obj)

    override def releaseExclusiveLock(obj: Long) =
      txContext.statement.readOperations().acquireExclusive(ResourceTypes.RELATIONSHIP, obj)
  }

  override def getOrCreatePropertyKeyId(propertyKey: String) =
    txContext.statement.tokenWriteOperations().propertyKeyGetOrCreateForName(propertyKey)

  abstract class BaseOperations[T <: PropertyContainer] extends Operations[T] {
    def primitiveLongIteratorToScalaIterator(primitiveIterator: PrimitiveLongIterator): Iterator[Long] =
      new Iterator[Long] {
        override def hasNext: Boolean = primitiveIterator.hasNext

        override def next(): Long = primitiveIterator.next
      }
  }

  override def getOrCreateFromSchemaState[K, V](key: K, creator: => V) = {
    val javaCreator = new java.util.function.Function[K, V]() {
      override def apply(key: K) = creator
    }
    txContext.statement.readOperations().schemaStateGetOrCreate(key, javaCreator)
  }

  override def addIndexRule(labelId: Int, propertyKeyId: Int): IdempotentResult[IndexDescriptor] = try {
    IdempotentResult(txContext.statement.schemaWriteOperations().indexCreate( SchemaDescriptorFactory.forLabel(labelId, propertyKeyId)))
  } catch {
    case _: AlreadyIndexedException =>
      val indexDescriptor = txContext.statement.readOperations().indexGetForLabelAndPropertyKey(SchemaDescriptorFactory.forLabel(labelId, propertyKeyId))
      if(txContext.statement.readOperations().indexGetState(indexDescriptor) == InternalIndexState.FAILED)
        throw new FailedIndexException(indexDescriptor.userDescription(tokenNameLookup))
     IdempotentResult(indexDescriptor, wasCreated = false)
  }

  override def dropIndexRule(labelId: Int, propertyKeyId: Int) =
    txContext.statement.schemaWriteOperations().indexDrop(NewIndexDescriptorFactory.forLabel( labelId, propertyKeyId ))

  override def createUniqueConstraint(labelId: Int, propertyKeyId: Int): IdempotentResult[UniquenessConstraint] = try {
    IdempotentResult(ConstraintBoundary.mapUnique(
      txContext.statement.schemaWriteOperations().uniquePropertyConstraintCreate(
        SchemaDescriptorFactory.forLabel(labelId, propertyKeyId)
      )))
  } catch {
    case existing: AlreadyConstrainedException =>
      IdempotentResult(existing.constraint().asInstanceOf[UniquenessConstraint], wasCreated = false)
  }

  override def dropUniqueConstraint(labelId: Int, propertyKeyId: Int) =
    txContext.statement.schemaWriteOperations().constraintDrop(ConstraintDescriptorFactory.uniqueForLabel(labelId, propertyKeyId))

  override def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int): IdempotentResult[NodePropertyExistenceConstraint] =
    try {
      IdempotentResult(ConstraintBoundary.mapNode(
        txContext.statement.schemaWriteOperations().nodePropertyExistenceConstraintCreate(
          SchemaDescriptorFactory.forLabel(labelId, propertyKeyId)
        )).asInstanceOf[NodePropertyExistenceConstraint]) // this cast can be resolved with 3.1.2
    } catch {
      case existing: AlreadyConstrainedException =>
        IdempotentResult(existing.constraint().asInstanceOf[NodePropertyExistenceConstraint], wasCreated = false)
    }

  override def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int) =
    txContext.statement.schemaWriteOperations().constraintDrop(ConstraintDescriptorFactory.existsForLabel(labelId, propertyKeyId))

  override def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int): IdempotentResult[RelationshipPropertyExistenceConstraint] =
    try {
      IdempotentResult(ConstraintBoundary.mapRelationship(
        txContext.statement.schemaWriteOperations().relationshipPropertyExistenceConstraintCreate(
          SchemaDescriptorFactory.forRelType(relTypeId, propertyKeyId)
        )).asInstanceOf[RelationshipPropertyExistenceConstraint])
    } catch {
      case existing: AlreadyConstrainedException =>
        IdempotentResult(existing.constraint().asInstanceOf[RelationshipPropertyExistenceConstraint], wasCreated = false)
    }

  override def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int) =
    txContext.statement.schemaWriteOperations().constraintDrop(ConstraintDescriptorFactory.existsForRelType(relTypeId,propertyKeyId))

  override def getImportURL(url: URL): Either[String,URL] = txContext.graph match {
    case db: GraphDatabaseQueryService =>
      try {
        Right(db.validateURLAccess(url))
      } catch {
        case error: URLAccessValidationError => Left(error.getMessage)
      }
  }

  override def relationshipStartNode(rel: Relationship) = rel.getStartNode

  override def relationshipEndNode(rel: Relationship) = rel.getEndNode

  private lazy val tokenNameLookup = new StatementTokenNameLookup(txContext.statement.readOperations())

  // Legacy dependency between kernel and compiler
  override def variableLengthPathExpand(node: PatternNode,
                                        realNode: Node,
                                        minHops: Option[Int],
                                        maxHops: Option[Int],
                                        direction: SemanticDirection,
                                        relTypes: Seq[String]): Iterator[Path] = {
    val depthEval = (minHops, maxHops) match {
      case (None, None) => Evaluators.fromDepth(1)
      case (Some(min), None) => Evaluators.fromDepth(min)
      case (None, Some(max)) => Evaluators.includingDepths(1, max)
      case (Some(min), Some(max)) => Evaluators.includingDepths(min, max)
    }

    // The RULE compiler makes use of older kernel API capabilities for variable length expanding
    // TODO: Consider re-writing this using similar code to the COST var-length expand
    val baseTraversalDescription: TraversalDescription = txContext.graph.asInstanceOf[GraphDatabaseCypherService]
      .getGraphDatabaseService
      .traversalDescription()
      .evaluator(depthEval)
      .uniqueness(Uniqueness.RELATIONSHIP_PATH)

    val traversalDescription = if (relTypes.isEmpty) {
      baseTraversalDescription.expand(PathExpanderBuilder.allTypes(toGraphDb(direction)).build())
    } else {
      val emptyExpander = PathExpanderBuilder.empty()
      val expander = relTypes.foldLeft(emptyExpander) {
        case (e, t) => e.add(RelationshipType.withName(t), toGraphDb(direction))
      }
      baseTraversalDescription.expand(expander.build())
    }
    traversalDescription.traverse(realNode).iterator().asScala
  }

  override def nodeCountByCountStore(labelId: Int): Long = {
    txContext.statement.readOperations().countsForNode(labelId)
  }

  override def relationshipCountByCountStore(startLabelId: Int, typeId: Int, endLabelId: Int): Long = {
    txContext.statement.readOperations().countsForRelationship(startLabelId, typeId, endLabelId)
  }

  override def lockNodes(nodeIds: Long*) =
    nodeIds.sorted.foreach(txContext.statement.readOperations().acquireExclusive(ResourceTypes.NODE, _))

  override def lockRelationships(relIds: Long*) =
    relIds.sorted.foreach(txContext.statement.readOperations().acquireExclusive(ResourceTypes.RELATIONSHIP, _))

  override def singleShortestPath(left: Node, right: Node, depth: Int, expander: expressions.Expander,
                                  pathPredicate: KernelPredicate[Path],
                                  filters: Seq[KernelPredicate[PropertyContainer]]): Option[Path] = {
    val pathFinder = buildPathFinder(depth, expander, pathPredicate, filters)

    Option(pathFinder.findSinglePath(left, right))
  }

  override def allShortestPath(left: Node, right: Node, depth: Int, expander: expressions.Expander,
                               pathPredicate: KernelPredicate[Path],
                               filters: Seq[KernelPredicate[PropertyContainer]]): scala.Iterator[Path] = {
    val pathFinder = buildPathFinder(depth, expander, pathPredicate, filters)

    pathFinder.findAllPaths(left, right).iterator().asScala
  }

  type KernelProcedureCall = (KernelQualifiedName, Array[AnyRef]) => RawIterator[Array[AnyRef], ProcedureException]
  type KernelFunctionCall = (KernelQualifiedName, Array[AnyRef]) => AnyRef

  private def shouldElevate(allowed: Array[String]): Boolean = {
    // We have to be careful with elevation, since we cannot elevate permissions in a nested procedure call
    // above the original allowed procedure mode. We enforce this by checking if mode is already an overridden mode.
    val accessMode = txContext.securityContext.mode()
    allowed.nonEmpty && !accessMode.isOverridden && accessMode.allowsProcedureWith(allowed)
  }

  override def callReadOnlyProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) = {
    val call: KernelProcedureCall =
      if (shouldElevate(allowed))
        txContext.statement.procedureCallOperations.procedureCallReadOverride(_, _)
      else
        txContext.statement.procedureCallOperations.procedureCallRead(_, _)
    callProcedure(name, args, call)
  }

  override def callReadWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) = {
    val call: KernelProcedureCall =
      if (shouldElevate(allowed))
        txContext.statement.procedureCallOperations.procedureCallWriteOverride(_, _)
      else
        txContext.statement.procedureCallOperations.procedureCallWrite(_, _)
    callProcedure(name, args, call)
  }

  override def callSchemaWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) = {
    val call: KernelProcedureCall =
      if (shouldElevate(allowed))
        txContext.statement.procedureCallOperations.procedureCallSchemaOverride(_, _)
      else
        txContext.statement.procedureCallOperations.procedureCallSchema(_, _)
    callProcedure(name, args, call)
  }

  override def callDbmsProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) = {
    callProcedure(name, args, txContext.dbmsOperations.procedureCallDbms(_,_,txContext.securityContext))
  }

  private def callProcedure(name: QualifiedName, args: Seq[Any], call: KernelProcedureCall) = {
    val kn = new KernelQualifiedName(name.namespace.asJava, name.name)
    val toArray = args.map(_.asInstanceOf[AnyRef]).toArray
    val read = call(kn, toArray)
    new scala.Iterator[Array[AnyRef]] {
      override def hasNext: Boolean = read.hasNext
      override def next(): Array[AnyRef] = read.next
    }
  }

  override def callFunction(name: QualifiedName, args: Seq[Any], allowed: Array[String]) = {
    val call: KernelFunctionCall =
      if (shouldElevate(allowed))
        txContext.statement.procedureCallOperations.functionCallOverride(_, _)
      else
        txContext.statement.procedureCallOperations.functionCall(_, _)
    callFunction(name, args, call)
  }

  private def callFunction(name: QualifiedName, args: Seq[Any],
                           call: KernelFunctionCall) = {
    val kn = new KernelQualifiedName(name.namespace.asJava, name.name)
    val toArray = args.map(_.asInstanceOf[AnyRef]).toArray
    call(kn, toArray)
  }

  override def isGraphKernelResultValue(v: Any): Boolean = internal.isGraphKernelResultValue(v)

  private def buildPathFinder(depth: Int, expander: expressions.Expander, pathPredicate: KernelPredicate[Path],
                              filters: Seq[KernelPredicate[PropertyContainer]]): ShortestPath = {
    val startExpander = expander match {
      case OnlyDirectionExpander(_, _, dir) =>
        PathExpanderBuilder.allTypes(toGraphDb(dir))
      case TypeAndDirectionExpander(_,_,typDirs) =>
        typDirs.foldLeft(PathExpanderBuilder.empty()) {
          case (acc, (typ, dir)) => acc.add(RelationshipType.withName(typ), toGraphDb(dir))
        }
    }

    val expanderWithNodeFilters = expander.nodeFilters.foldLeft(startExpander) {
      case (acc, filter) => acc.addNodeFilter(new Predicate[PropertyContainer] {
        override def test(t: PropertyContainer): Boolean = filter.test(t)
      })
    }
    val expanderWithAllPredicates = expander.relFilters.foldLeft(expanderWithNodeFilters) {
      case (acc, filter) => acc.addRelationshipFilter(new Predicate[PropertyContainer] {
        override def test(t: PropertyContainer): Boolean = filter.test(t)
      })
    }
    val shortestPathPredicate = new ShortestPathPredicate {
      override def test(path: Path): Boolean = pathPredicate.test(path)
    }

    new ShortestPath(depth, expanderWithAllPredicates.build(), shortestPathPredicate) {
      override protected def filterNextLevelNodes(nextNode: Node): Node =
        if (filters.isEmpty) nextNode
        else if (filters.forall(filter => filter test nextNode)) nextNode
        else null
    }
  }

  override def detachDeleteNode(node: Node): Int = {
    try {
      txContext.statement.dataWriteOperations().nodeDetachDelete(node.getId)
    } catch {
      case _: exceptions.EntityNotFoundException => 0 // node has been deleted by another transaction, oh well...
    }
  }

  override def assertSchemaWritesAllowed(): Unit =
    txContext.statement.schemaWriteOperations()

  override def transactionalContext: QueryTransactionalContext = new QueryTransactionalContext {
    override type ReadOps = ReadOperations

    override type DbmsOps = DbmsOperations

    override def commitAndRestartTx(): Unit = txContext.commitAndRestartTx()

    override def readOperations: ReadOps = txContext.tc.readOperations()

    override def isTopLevelTx: Boolean = txContext.isTopLevelTx

    override def dbmsOperations: DbmsOperations = txContext.tc.dbmsOperations()

    override def close(success: Boolean): Unit = txContext.tc.close(success)
  }
}

object TransactionBoundQueryContext {
  trait IndexSearchMonitor {
    def indexSeek(index: IndexDescriptor, value: Any): Unit

    def lockingUniqueIndexSeek(index: IndexDescriptor, value: Any): Unit
  }
}
