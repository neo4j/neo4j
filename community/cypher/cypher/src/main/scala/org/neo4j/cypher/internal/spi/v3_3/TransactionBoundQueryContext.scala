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
package org.neo4j.cypher.internal.spi.v3_3

import java.net.URL
import java.util.function.Predicate

import org.neo4j.collection.RawIterator
import org.neo4j.collection.primitive.PrimitiveLongIterator
import org.neo4j.collection.primitive.base.Empty.EMPTY_PRIMITIVE_LONG_COLLECTION
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.convert.DirectionConverter.toGraphDb
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.{KernelPredicate, OnlyDirectionExpander, TypeAndDirectionExpander, UserDefinedAggregator}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers.JavaConversionSupport
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers.JavaConversionSupport._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.matching.PatternNode
import org.neo4j.cypher.internal.compiler.v3_3.MinMaxOrdering._
import org.neo4j.cypher.internal.compiler.v3_3.spi.QualifiedName
import org.neo4j.cypher.internal.compiler.v3_3.{IndexDescriptor, _}
import org.neo4j.cypher.internal.frontend.v3_3._
import org.neo4j.cypher.internal.spi.BeansAPIRelationshipIterator
import org.neo4j.cypher.internal.spi.v3_3.TransactionBoundQueryContext.IndexSearchMonitor
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
import org.neo4j.kernel.api.exceptions.ProcedureException
import org.neo4j.kernel.api.exceptions.schema.{AlreadyConstrainedException, AlreadyIndexedException}
import org.neo4j.kernel.api.index.InternalIndexState
import org.neo4j.kernel.api.proc.CallableUserAggregationFunction.Aggregator
import org.neo4j.kernel.api.proc.{QualifiedName => KernelQualifiedName}
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory
import org.neo4j.kernel.api.schema.{IndexQuery, SchemaDescriptorFactory}
import org.neo4j.kernel.impl.core.NodeManager
import org.neo4j.kernel.impl.locking.ResourceTypes

import scala.collection.Iterator
import scala.collection.JavaConverters._

final class TransactionBoundQueryContext(val transactionalContext: TransactionalContextWrapper)(implicit indexSearchMonitor: IndexSearchMonitor)
  extends TransactionBoundTokenContext(transactionalContext.statement) with QueryContext with IndexDescriptorCompatibility {

  override type EntityAccessor = NodeManager

  override val nodeOps = new NodeOperations
  override val relationshipOps = new RelationshipOperations
  override lazy val entityAccessor: NodeManager =
    transactionalContext.graph.getDependencyResolver.resolveDependency(classOf[NodeManager])

  override def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int = labelIds.foldLeft(0) {
    case (count, labelId) => if (transactionalContext.statement.dataWriteOperations().nodeAddLabel(node, labelId)) count + 1 else count
  }

  override def withAnyOpenQueryContext[T](work: (QueryContext) => T): T = {
    if (transactionalContext.isOpen) {
      work(this)
    } else {
      val context = transactionalContext.getOrBeginNewIfClosed()
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
    transactionalContext.graph.createNode()

  override def createRelationship(start: Node, end: Node, relType: String) =
    start.createRelationshipTo(end, withName(relType))

  override def createRelationship(start: Long, end: Long, relType: Int) = {
    val relId = transactionalContext.statement.dataWriteOperations().relationshipCreate(relType, start, end)
    relationshipOps.getById(relId)
  }

  override def getOrCreateRelTypeId(relTypeName: String): Int =
    transactionalContext.statement.tokenWriteOperations().relationshipTypeGetOrCreateForName(relTypeName)

  override def getLabelsForNode(node: Long) = try {
    JavaConversionSupport.asScala(transactionalContext.statement.readOperations().nodeGetLabels(node))
  } catch {
    case e: org.neo4j.kernel.api.exceptions.EntityNotFoundException =>
      if (nodeOps.isDeletedInThisTx(node))
        throw new EntityNotFoundException(s"Node with id $node has been deleted in this transaction", e)
      else
        Iterator.empty
  }

  override def getPropertiesForNode(node: Long) =
    JavaConversionSupport.asScala(transactionalContext.statement.readOperations().nodeGetPropertyKeys(node))

  override def getPropertiesForRelationship(relId: Long) =
    JavaConversionSupport.asScala(transactionalContext.statement.readOperations().relationshipGetPropertyKeys(relId))

  override def isLabelSetOnNode(label: Int, node: Long) =
    transactionalContext.statement.readOperations().nodeHasLabel(node, label)

  override def getOrCreateLabelId(labelName: String) =
    transactionalContext.statement.tokenWriteOperations().labelGetOrCreateForName(labelName)

  def getRelationshipsForIds(node: Node, dir: SemanticDirection, types: Option[Seq[Int]]): Iterator[Relationship] = {
    val relationships = types match {
      case None =>
        transactionalContext.statement.readOperations().nodeGetRelationships(node.getId, toGraphDb(dir))
      case Some(typeIds) =>
        transactionalContext.statement.readOperations().nodeGetRelationships(node.getId, toGraphDb(dir), typeIds.toArray)
    }
    new BeansAPIRelationshipIterator(relationships, entityAccessor)
  }

  override def indexSeek(index: IndexDescriptor, values: Seq[Any]) = {
    indexSearchMonitor.indexSeek(index, values)
    val predicates = index.properties.zip(values).map(p => IndexQuery.exact(p._1, p._2))
    val indexResult = transactionalContext.statement.readOperations().indexQuery(index, predicates: _*)
    JavaConversionSupport.mapToScalaENFXSafe(indexResult)(nodeOps.getById)
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
    val indexedNodes = transactionalContext.statement.readOperations().indexQuery(index, IndexQuery.stringPrefix(index.property, prefix))
    JavaConversionSupport.mapToScalaENFXSafe(indexedNodes)(nodeOps.getById)
  }

  private def indexSeekByNumericalRange(index: IndexDescriptor, range: InequalitySeekRange[Number]): scala.Iterator[Node] = {
    val readOps = transactionalContext.statement.readOperations()
    val matchingNodes: PrimitiveLongIterator = (range match {

      case rangeLessThan: RangeLessThan[Number] =>
        rangeLessThan.limit(BY_NUMBER).map { limit =>
          val rangePredicate = IndexQuery.range(index.property, null, false, limit.endPoint, limit.isInclusive)
          readOps.indexQuery(index, rangePredicate)
        }

      case rangeGreaterThan: RangeGreaterThan[Number] =>
        rangeGreaterThan.limit(BY_NUMBER).map { limit =>
          val rangePredicate = IndexQuery.range(index.property, limit.endPoint, limit.isInclusive, null, false)
          readOps.indexQuery(index, rangePredicate)
        }

      case RangeBetween(rangeGreaterThan, rangeLessThan) =>
        rangeGreaterThan.limit(BY_NUMBER).flatMap { greaterThanLimit =>
          rangeLessThan.limit(BY_NUMBER).map { lessThanLimit =>
            val rangePredicate = IndexQuery.range(index.property, greaterThanLimit.endPoint, greaterThanLimit.isInclusive, lessThanLimit.endPoint, lessThanLimit.isInclusive)
            readOps.indexQuery(index, rangePredicate)
          }
        }
    }).getOrElse(EMPTY_PRIMITIVE_LONG_COLLECTION.iterator)
    JavaConversionSupport.mapToScalaENFXSafe(matchingNodes)(nodeOps.getById)
  }

  private def indexSeekByStringRange(index: IndexDescriptor, range: InequalitySeekRange[String]): scala.Iterator[Node] = {
    val readOps = transactionalContext.statement.readOperations()
    val matchingNodes: PrimitiveLongIterator = range match {

      case rangeLessThan: RangeLessThan[String] =>
        rangeLessThan.limit(BY_STRING).map { limit =>
          val rangePredicate = IndexQuery.range(index.property, null, false, limit.endPoint.asInstanceOf[String], limit.isInclusive)
          readOps.indexQuery(index, rangePredicate)
        }.getOrElse(EMPTY_PRIMITIVE_LONG_COLLECTION.iterator)

      case rangeGreaterThan: RangeGreaterThan[String] =>
        rangeGreaterThan.limit(BY_STRING).map { limit =>
          val rangePredicate = IndexQuery.range(index.property, limit.endPoint.asInstanceOf[String], limit.isInclusive, null, false);
          readOps.indexQuery(index, rangePredicate)
        }.getOrElse(EMPTY_PRIMITIVE_LONG_COLLECTION.iterator)

      case RangeBetween(rangeGreaterThan, rangeLessThan) =>
        rangeGreaterThan.limit(BY_STRING).flatMap { greaterThanLimit =>
          rangeLessThan.limit(BY_STRING).map { lessThanLimit =>
            val rangePredicate = IndexQuery.range(index.property, greaterThanLimit.endPoint.asInstanceOf[String], greaterThanLimit.isInclusive, lessThanLimit.endPoint.asInstanceOf[String], lessThanLimit.isInclusive)
            readOps.indexQuery(index, rangePredicate)
          }
        }.getOrElse(EMPTY_PRIMITIVE_LONG_COLLECTION.iterator)
    }

    JavaConversionSupport.mapToScalaENFXSafe(matchingNodes)(nodeOps.getById)
  }

  override def indexScan(index: IndexDescriptor) =
    mapToScalaENFXSafe(transactionalContext.statement.readOperations().indexQuery(index, IndexQuery.exists(index.property)))(nodeOps.getById)

  override def indexScanByContains(index: IndexDescriptor, value: String) =
    mapToScalaENFXSafe(transactionalContext.statement.readOperations().indexQuery(index, IndexQuery.stringContains(index.property, value)))(nodeOps.getById)

  override def indexScanByEndsWith(index: IndexDescriptor, value: String) =
    mapToScalaENFXSafe(transactionalContext.statement.readOperations().indexQuery(index, IndexQuery.stringSuffix(index.property, value)))(nodeOps.getById)

  override def lockingUniqueIndexSeek(index: IndexDescriptor, values: Seq[Any]): Option[Node] = {
    indexSearchMonitor.lockingUniqueIndexSeek(index, values)
    val predicates = index.properties.zip(values).map(p => IndexQuery.exact(p._1, p._2))
    val nodeId = transactionalContext.statement.readOperations().nodeGetFromUniqueIndexSeek(index, predicates: _*)
    if (StatementConstants.NO_SUCH_NODE == nodeId) None else Some(nodeOps.getById(nodeId))
  }

  override def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int = labelIds.foldLeft(0) {
    case (count, labelId) =>
      if (transactionalContext.statement.dataWriteOperations().nodeRemoveLabel(node, labelId)) count + 1 else count
  }

  override def getNodesByLabel(id: Int): Iterator[Node] =
    JavaConversionSupport.mapToScalaENFXSafe(transactionalContext.statement.readOperations().nodesGetForLabel(id))(nodeOps.getById)

  override def nodeGetDegree(node: Long, dir: SemanticDirection): Int =
    transactionalContext.statement.readOperations().nodeGetDegree(node, toGraphDb(dir))

  override def nodeGetDegree(node: Long, dir: SemanticDirection, relTypeId: Int): Int =
    transactionalContext.statement.readOperations().nodeGetDegree(node, toGraphDb(dir), relTypeId)

  override def nodeIsDense(node: Long): Boolean = transactionalContext.statement.readOperations().nodeIsDense(node)

  class NodeOperations extends BaseOperations[Node] {
    override def delete(obj: Node) {
      try {
        transactionalContext.statement.dataWriteOperations().nodeDelete(obj.getId)
      } catch {
        case _: exceptions.EntityNotFoundException => // node has been deleted by another transaction, oh well...
      }
    }

    override def propertyKeyIds(id: Long): Iterator[Int] = try {
      JavaConversionSupport.asScalaENFXSafe(transactionalContext.statement.readOperations().nodeGetPropertyKeys(id))
    } catch {
      case _: exceptions.EntityNotFoundException => Iterator.empty
    }

    override def getProperty(id: Long, propertyKeyId: Int): Any = try {
      transactionalContext.statement.readOperations().nodeGetProperty(id, propertyKeyId)
    } catch {
      case e: org.neo4j.kernel.api.exceptions.EntityNotFoundException =>
        if (isDeletedInThisTx(id))
          throw new EntityNotFoundException(s"Node with id $id has been deleted in this transaction", e)
        else
          null
    }

    override def hasProperty(id: Long, propertyKey: Int) = try {
      transactionalContext.statement.readOperations().nodeHasProperty(id, propertyKey)
    } catch {
      case _: exceptions.EntityNotFoundException => false
    }

    override def removeProperty(id: Long, propertyKeyId: Int) {
      try {
        transactionalContext.statement.dataWriteOperations().nodeRemoveProperty(id, propertyKeyId)
      } catch {
        case _: exceptions.EntityNotFoundException => //ignore
      }
    }

    override def setProperty(id: Long, propertyKeyId: Int, value: Any) {
      try {
        transactionalContext.statement.dataWriteOperations().nodeSetProperty(id, properties.Property.property(propertyKeyId, value) )
      } catch {
        case _: exceptions.EntityNotFoundException => //ignore
      }
    }

    override def getById(id: Long) = try {
      transactionalContext.graph.getNodeById(id)
    } catch {
      case e: NotFoundException => throw new EntityNotFoundException(s"Node with id $id", e)
    }

    override def all: Iterator[Node] =
      JavaConversionSupport.mapToScalaENFXSafe(transactionalContext.statement.readOperations().nodesGetAll())(getById)

    override def indexGet(name: String, key: String, value: Any): Iterator[Node] =
      JavaConversionSupport.mapToScalaENFXSafe(transactionalContext.statement.readOperations().nodeLegacyIndexGet(name, key, value))(getById)

    override def indexQuery(name: String, query: Any): Iterator[Node] =
      JavaConversionSupport.mapToScalaENFXSafe(transactionalContext.statement.readOperations().nodeLegacyIndexQuery(name, query))(getById)

    override def isDeletedInThisTx(n: Node): Boolean = isDeletedInThisTx(n.getId)

    def isDeletedInThisTx(id: Long): Boolean = transactionalContext.stateView.hasTxStateWithChanges && transactionalContext.stateView.txState().nodeIsDeletedInThisTx(id)

    override def acquireExclusiveLock(obj: Long) =
      transactionalContext.statement.readOperations().acquireExclusive(ResourceTypes.NODE, obj)

    override def releaseExclusiveLock(obj: Long) =
      transactionalContext.statement.readOperations().releaseExclusive(ResourceTypes.NODE, obj)
  }

  class RelationshipOperations extends BaseOperations[Relationship] {

    override def delete(obj: Relationship) {
      try {
        transactionalContext.statement.dataWriteOperations().relationshipDelete(obj.getId)
      } catch {
        case _: exceptions.EntityNotFoundException => // node has been deleted by another transaction, oh well...
      }
    }

    override def propertyKeyIds(id: Long): Iterator[Int] = try {
      asScalaENFXSafe(transactionalContext.statement.readOperations().relationshipGetPropertyKeys(id))
    } catch {
      case _: exceptions.EntityNotFoundException => Iterator.empty
    }

    override def getProperty(id: Long, propertyKeyId: Int): Any = try {
      transactionalContext.statement.readOperations().relationshipGetProperty(id, propertyKeyId)
    } catch {
      case e: org.neo4j.kernel.api.exceptions.EntityNotFoundException =>
        if (isDeletedInThisTx(id))
          throw new EntityNotFoundException(s"Relationship with id $id has been deleted in this transaction", e)
        else
          null
    }

    override def hasProperty(id: Long, propertyKey: Int) = try {
      transactionalContext.statement.readOperations().relationshipHasProperty(id, propertyKey)
    } catch {
      case _: exceptions.EntityNotFoundException => false
    }

    override def removeProperty(id: Long, propertyKeyId: Int) {
      try {
        transactionalContext.statement.dataWriteOperations().relationshipRemoveProperty(id, propertyKeyId)
      } catch {
        case _: exceptions.EntityNotFoundException => //ignore
      }
    }

    override def setProperty(id: Long, propertyKeyId: Int, value: Any) {
      try {
        transactionalContext.statement.dataWriteOperations().relationshipSetProperty(id, properties.Property.property(propertyKeyId, value))
      } catch {
        case _: exceptions.EntityNotFoundException => //ignore
      }
    }

    override def getById(id: Long) = try {
      transactionalContext.graph.getRelationshipById(id)
    } catch {
      case e: NotFoundException => throw new EntityNotFoundException(s"Relationship with id $id", e)
    }

    override def all: Iterator[Relationship] = {
      JavaConversionSupport.mapToScalaENFXSafe(transactionalContext.statement.readOperations().relationshipsGetAll())(getById)
    }

    override def indexGet(name: String, key: String, value: Any): Iterator[Relationship] =
      JavaConversionSupport.mapToScalaENFXSafe(transactionalContext.statement.readOperations().relationshipLegacyIndexGet(name, key, value, -1, -1))(getById)

    override def indexQuery(name: String, query: Any): Iterator[Relationship] =
      JavaConversionSupport.mapToScalaENFXSafe(transactionalContext.statement.readOperations().relationshipLegacyIndexQuery(name, query, -1, -1))(getById)

    override def isDeletedInThisTx(r: Relationship): Boolean =
      isDeletedInThisTx(r.getId)

    def isDeletedInThisTx(id: Long): Boolean =
      transactionalContext.stateView.hasTxStateWithChanges && transactionalContext.stateView.txState().relationshipIsDeletedInThisTx(id)

    override def acquireExclusiveLock(obj: Long) =
      transactionalContext.statement.readOperations().acquireExclusive(ResourceTypes.RELATIONSHIP, obj)

    override def releaseExclusiveLock(obj: Long) =
      transactionalContext.statement.readOperations().acquireExclusive(ResourceTypes.RELATIONSHIP, obj)
  }

  override def getOrCreatePropertyKeyId(propertyKey: String) =
    transactionalContext.statement.tokenWriteOperations().propertyKeyGetOrCreateForName(propertyKey)

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
    transactionalContext.statement.readOperations().schemaStateGetOrCreate(key, javaCreator)
  }

  override def addIndexRule(descriptor: IndexDescriptor): IdempotentResult[IndexDescriptor] = {
    try {
      IdempotentResult(transactionalContext.statement.schemaWriteOperations().indexCreate( descriptor ))
    } catch {
      case _: AlreadyIndexedException =>
        val indexDescriptor = transactionalContext.statement.readOperations().indexGetForSchema (
          SchemaDescriptorFactory.forLabel(descriptor.getLabelId, descriptor.getPropertyIds:_*))
        if(transactionalContext.statement.readOperations().indexGetState(indexDescriptor) == InternalIndexState.FAILED)
          throw new FailedIndexException(indexDescriptor.userDescription(tokenNameLookup))
        IdempotentResult(indexDescriptor, wasCreated = false)
    }
  }

  override def dropIndexRule(descriptor: IndexDescriptor) =
    transactionalContext.statement.schemaWriteOperations().indexDrop(descriptor)

  override def createNodeKeyConstraint(descriptor: IndexDescriptor): Boolean = try {
    transactionalContext.statement.schemaWriteOperations().nodeKeyConstraintCreate(descriptor)
    true
  } catch {
    case existing: AlreadyConstrainedException => false
  }

  override def dropNodeKeyConstraint(descriptor: IndexDescriptor) =
    transactionalContext.statement.schemaWriteOperations().constraintDrop(ConstraintDescriptorFactory.nodeKeyForSchema(descriptor))

  override def createUniqueConstraint(descriptor: IndexDescriptor): Boolean = try {
    transactionalContext.statement.schemaWriteOperations().uniquePropertyConstraintCreate(descriptor)
    true
  } catch {
    case existing: AlreadyConstrainedException => false
  }

  override def dropUniqueConstraint(descriptor: IndexDescriptor) =
    transactionalContext.statement.schemaWriteOperations().constraintDrop(ConstraintDescriptorFactory.uniqueForSchema(descriptor))

  override def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int): Boolean =
    try {
      transactionalContext.statement.schemaWriteOperations().nodePropertyExistenceConstraintCreate(
        SchemaDescriptorFactory.forLabel(labelId, propertyKeyId))
      true
    } catch {
      case existing: AlreadyConstrainedException => false
    }

  override def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int) =
    transactionalContext.statement.schemaWriteOperations().constraintDrop(ConstraintDescriptorFactory.existsForLabel(labelId, propertyKeyId))

  override def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int): Boolean =
    try {
      transactionalContext.statement.schemaWriteOperations().relationshipPropertyExistenceConstraintCreate(
        SchemaDescriptorFactory.forRelType(relTypeId, propertyKeyId))
      true
    } catch {
      case existing: AlreadyConstrainedException => false
    }

  override def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int) =
    transactionalContext.statement.schemaWriteOperations().constraintDrop(ConstraintDescriptorFactory.existsForRelType(relTypeId, propertyKeyId))

  override def getImportURL(url: URL): Either[String,URL] = transactionalContext.graph match {
    case db: GraphDatabaseQueryService =>
      try {
        Right(db.validateURLAccess(url))
      } catch {
        case error: URLAccessValidationError => Left(error.getMessage)
      }
  }

  override def relationshipStartNode(rel: Relationship) = rel.getStartNode

  override def relationshipEndNode(rel: Relationship) = rel.getEndNode

  private lazy val tokenNameLookup = new StatementTokenNameLookup(transactionalContext.statement.readOperations())

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
    val baseTraversalDescription: TraversalDescription = transactionalContext.graph.asInstanceOf[GraphDatabaseCypherService]
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
    transactionalContext.statement.readOperations().countsForNode(labelId)
  }

  override def relationshipCountByCountStore(startLabelId: Int, typeId: Int, endLabelId: Int): Long = {
    transactionalContext.statement.readOperations().countsForRelationship(startLabelId, typeId, endLabelId)
  }

  override def lockNodes(nodeIds: Long*) =
    nodeIds.sorted.foreach(transactionalContext.statement.readOperations().acquireExclusive(ResourceTypes.NODE, _))

  override def lockRelationships(relIds: Long*) =
    relIds.sorted.foreach(transactionalContext.statement.readOperations().acquireExclusive(ResourceTypes.RELATIONSHIP, _))

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
  type KernelAggregationFunctionCall = (KernelQualifiedName) => Aggregator

  private def shouldElevate(allowed: Array[String]): Boolean = {
    // We have to be careful with elevation, since we cannot elevate permissions in a nested procedure call
    // above the original allowed procedure mode. We enforce this by checking if mode is already an overridden mode.
    val accessMode = transactionalContext.securityContext.mode()
    allowed.nonEmpty && !accessMode.isOverridden && accessMode.allowsProcedureWith(allowed)
  }

  override def callReadOnlyProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) = {
    val call: KernelProcedureCall =
      if (shouldElevate(allowed))
        transactionalContext.statement.procedureCallOperations.procedureCallReadOverride(_, _)
      else
        transactionalContext.statement.procedureCallOperations.procedureCallRead(_, _)
    callProcedure(name, args, call)
  }

  override def callReadWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) = {
    val call: KernelProcedureCall =
      if (shouldElevate(allowed))
        transactionalContext.statement.procedureCallOperations.procedureCallWriteOverride(_, _)
      else
        transactionalContext.statement.procedureCallOperations.procedureCallWrite(_, _)
    callProcedure(name, args, call)
  }

  override def callSchemaWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) = {
    val call: KernelProcedureCall =
      if (shouldElevate(allowed))
        transactionalContext.statement.procedureCallOperations.procedureCallSchemaOverride(_, _)
      else
        transactionalContext.statement.procedureCallOperations.procedureCallSchema(_, _)
    callProcedure(name, args, call)
  }

  override def callDbmsProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) = {
    callProcedure(name, args, transactionalContext.dbmsOperations.procedureCallDbms(_,_,transactionalContext.securityContext))
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
        transactionalContext.statement.procedureCallOperations.functionCallOverride(_, _)
      else
        transactionalContext.statement.procedureCallOperations.functionCall(_, _)
    callFunction(name, args, call)
  }

  override def aggregateFunction(name: QualifiedName, allowed: Array[String]) = {
    val call: KernelAggregationFunctionCall =
      if (shouldElevate(allowed))
        transactionalContext.statement.procedureCallOperations.aggregationFunctionOverride(_)
      else
        transactionalContext.statement.procedureCallOperations.aggregationFunction(_)
    callAggregationFunction(name, call)
  }

  private def callFunction(name: QualifiedName, args: Seq[Any],
                           call: KernelFunctionCall) = {
    val kn = new KernelQualifiedName(name.namespace.asJava, name.name)
    val toArray = args.map(_.asInstanceOf[AnyRef]).toArray
    call(kn, toArray)
  }

  private def callAggregationFunction(name: QualifiedName,
                           call: KernelAggregationFunctionCall) = {
    val kn = new KernelQualifiedName(name.namespace.asJava, name.name)
    val aggregator = call(kn)
    new UserDefinedAggregator {
      override def result = aggregator.result()

      override def update(args: IndexedSeq[Any]) = {
        val toArray = args.map(_.asInstanceOf[AnyRef]).toArray
        aggregator.update(toArray)
      }
    }
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
      transactionalContext.statement.dataWriteOperations().nodeDetachDelete(node.getId)
    } catch {
      case _: exceptions.EntityNotFoundException => 0 // node has been deleted by another transaction, oh well...
    }
  }

  override def assertSchemaWritesAllowed(): Unit =
    transactionalContext.statement.schemaWriteOperations()

}

object TransactionBoundQueryContext {
  trait IndexSearchMonitor {
    def indexSeek(index: IndexDescriptor, values: Seq[Any]): Unit

    def lockingUniqueIndexSeek(index: IndexDescriptor, values: Seq[Any]): Unit
  }
}
