/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.spi.v3_0

import java.net.URL
import java.util.function.Predicate

import org.neo4j.collection.RawIterator
import org.neo4j.collection.primitive.PrimitiveLongIterator
import org.neo4j.collection.primitive.base.Empty.EMPTY_PRIMITIVE_LONG_COLLECTION
import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compiler.v3_0.MinMaxOrdering.{BY_NUMBER, BY_STRING, BY_VALUE}
import org.neo4j.cypher.internal.compiler.v3_0._
import org.neo4j.cypher.internal.compiler.v3_0.ast.convert.commands.DirectionConverter.toGraphDb
import org.neo4j.cypher.internal.compiler.v3_0.commands.expressions
import org.neo4j.cypher.internal.compiler.v3_0.commands.expressions.{KernelPredicate, OnlyDirectionExpander, TypeAndDirectionExpander}
import org.neo4j.cypher.internal.compiler.v3_0.helpers.JavaConversionSupport._
import org.neo4j.cypher.internal.compiler.v3_0.helpers.{BeansAPIRelationshipIterator, JavaConversionSupport}
import org.neo4j.cypher.internal.compiler.v3_0.pipes.matching.PatternNode
import org.neo4j.cypher.internal.compiler.v3_0.spi._
import org.neo4j.cypher.internal.frontend.v3_0.{Bound, EntityNotFoundException, FailedIndexException, SemanticDirection}
import org.neo4j.cypher.internal.spi.v3_0.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.graphalgo.impl.path.ShortestPath
import org.neo4j.graphalgo.impl.path.ShortestPath.ShortestPathPredicate
import org.neo4j.graphdb.RelationshipType._
import org.neo4j.graphdb._
import org.neo4j.graphdb.traversal.{Evaluators, TraversalDescription, Uniqueness}
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.api._
import org.neo4j.kernel.api.constraints.{NodePropertyExistenceConstraint, RelationshipPropertyExistenceConstraint, UniquenessConstraint}
import org.neo4j.kernel.api.exceptions.ProcedureException
import org.neo4j.kernel.api.exceptions.schema.{AlreadyConstrainedException, AlreadyIndexedException}
import org.neo4j.kernel.api.index.{IndexDescriptor, InternalIndexState}
import org.neo4j.kernel.impl.api.{KernelStatement => InternalKernelStatement}
import org.neo4j.kernel.impl.core.{NodeManager, RelationshipProxy, ThreadToStatementContextBridge}
import org.neo4j.kernel.impl.locking.ResourceTypes
import org.neo4j.graphdb.security.URLAccessValidationError

import scala.collection.Iterator
import scala.collection.JavaConverters._

final class TransactionBoundQueryContext(graph: GraphDatabaseAPI,
                                         var tx: Transaction,
                                         val isTopLevelTx: Boolean,
                                         initialStatement: Statement)(implicit indexSearchMonitor: IndexSearchMonitor)
  extends TransactionBoundTokenContext(initialStatement) with QueryContext {

  type KernelStatement = Statement

  type EntityAccessor = NodeManager

  private var open = true
  private val txBridge = graph.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge])
  val nodeOps = new NodeOperations
  val relationshipOps = new RelationshipOperations
  val relationshipActions = graph.getDependencyResolver.resolveDependency(classOf[RelationshipProxy.RelationshipActions])

  override def statement = _statement

  override def entityAccessor = graph.getDependencyResolver.resolveDependency(classOf[NodeManager])

  override def isOpen = open

  override def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int = labelIds.foldLeft(0) {
    case (count, labelId) => if (_statement.dataWriteOperations().nodeAddLabel(node, labelId)) count + 1 else count
  }

  override def close(success: Boolean) {
    try {
      _statement.close()

      if (success)
        tx.success()
      else
        tx.failure()
      tx.close()
    }
    finally {
      open = false
    }
  }

  override def withAnyOpenQueryContext[T](work: (QueryContext) => T): T = {
    if (open) {
      work(this)
    }
    else {
      val isTopLevelTx = !txBridge.hasTransaction
      val tx = graph.beginTx()
      try {
        val otherStatement = txBridge.get()
        val result = try {
          work(new TransactionBoundQueryContext(graph, tx, isTopLevelTx, otherStatement))
        }
        finally {
          otherStatement.close()
        }
        tx.success()
        result
      }
      finally {
        tx.close()
      }
    }
  }

  override def createNode(): Node =
    graph.createNode()

  override def createRelationship(start: Node, end: Node, relType: String) =
    start.createRelationshipTo(end, withName(relType))

  override def createRelationship(start: Long, end: Long, relType: Int) = {
    val relId = _statement.dataWriteOperations().relationshipCreate(relType, start, end)
    relationshipOps.getById(relId)
  }

  override def getOrCreateRelTypeId(relTypeName: String): Int =
    _statement.tokenWriteOperations().relationshipTypeGetOrCreateForName(relTypeName)

  override def getLabelsForNode(node: Long) = try {
    JavaConversionSupport.asScala(_statement.readOperations().nodeGetLabels(node))
  } catch {
    case e: org.neo4j.kernel.api.exceptions.EntityNotFoundException =>
      if (nodeOps.isDeletedInThisTx(node))
        throw new EntityNotFoundException(s"Node with id $node has been deleted in this transaction", e)
      else
        null
  }

  override def getPropertiesForNode(node: Long) =
    JavaConversionSupport.asScala(_statement.readOperations().nodeGetPropertyKeys(node))

  override def getPropertiesForRelationship(relId: Long) =
    JavaConversionSupport.asScala(_statement.readOperations().relationshipGetPropertyKeys(relId))

  override def isLabelSetOnNode(label: Int, node: Long) =
    _statement.readOperations().nodeHasLabel(node, label)

  override def getOrCreateLabelId(labelName: String) =
    _statement.tokenWriteOperations().labelGetOrCreateForName(labelName)

  override def getRelationshipsForIds(node: Node, dir: SemanticDirection, types: Option[Seq[Int]]): Iterator[Relationship] = types match {
    case None =>
      val relationships = _statement.readOperations().nodeGetRelationships(node.getId, toGraphDb(dir))
      new BeansAPIRelationshipIterator(relationships, relationshipActions)
    case Some(typeIds) =>
      val relationships = _statement.readOperations().nodeGetRelationships(node.getId, toGraphDb(dir), typeIds: _*)
      new BeansAPIRelationshipIterator(relationships, relationshipActions)
  }

  override def indexSeek(index: IndexDescriptor, value: Any) = {
    indexSearchMonitor.indexSeek(index, value)
    JavaConversionSupport.mapToScalaENFXSafe(_statement.readOperations().nodesGetFromIndexSeek(index, value))(nodeOps.getById)
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
    val indexedNodes = _statement.readOperations().nodesGetFromIndexRangeSeekByPrefix(index, prefix)
    JavaConversionSupport.mapToScalaENFXSafe(indexedNodes)(nodeOps.getById)
  }

  private def indexSeekByNumericalRange(index: IndexDescriptor, range: InequalitySeekRange[Number]): scala.Iterator[Node] = {
    val readOps = _statement.readOperations()
    val matchingNodes: PrimitiveLongIterator = (range match {

      case rangeLessThan: RangeLessThan[Number] =>
        rangeLessThan.limit(BY_NUMBER).map { limit =>
          readOps.nodesGetFromIndexRangeSeekByNumber( index, null, false, limit.endPoint, limit.isInclusive )
        }

      case rangeGreaterThan: RangeGreaterThan[Number] =>
        rangeGreaterThan.limit(BY_NUMBER).map { limit =>
          readOps.nodesGetFromIndexRangeSeekByNumber( index, limit.endPoint, limit.isInclusive, null, false )
        }

      case RangeBetween(rangeGreaterThan, rangeLessThan) =>
        rangeGreaterThan.limit(BY_NUMBER).flatMap { greaterThanLimit =>
          rangeLessThan.limit(BY_NUMBER).map { lessThanLimit =>
            readOps.nodesGetFromIndexRangeSeekByNumber(
              index,
              greaterThanLimit.endPoint, greaterThanLimit.isInclusive,
              lessThanLimit.endPoint, lessThanLimit.isInclusive )
          }
        }
    }).getOrElse(EMPTY_PRIMITIVE_LONG_COLLECTION.iterator)
    JavaConversionSupport.mapToScalaENFXSafe(matchingNodes)(nodeOps.getById)
  }

  private def indexSeekByStringRange(index: IndexDescriptor, range: InequalitySeekRange[String]): scala.Iterator[Node] = {
    val readOps = _statement.readOperations()
    val propertyKeyId = index.getPropertyKeyId
    val matchingNodes: PrimitiveLongIterator = range match {

      case rangeLessThan: RangeLessThan[String] =>
        rangeLessThan.limit(BY_STRING).map { limit =>
          readOps.nodesGetFromIndexRangeSeekByString( index, null, false, limit.endPoint.asInstanceOf[String], limit.isInclusive )
        }.getOrElse(EMPTY_PRIMITIVE_LONG_COLLECTION.iterator)

      case rangeGreaterThan: RangeGreaterThan[String] =>
        rangeGreaterThan.limit(BY_STRING).map { limit =>
          readOps.nodesGetFromIndexRangeSeekByString( index, limit.endPoint.asInstanceOf[String], limit.isInclusive, null, false )
        }.getOrElse(EMPTY_PRIMITIVE_LONG_COLLECTION.iterator)

      case RangeBetween(rangeGreaterThan, rangeLessThan) =>
        rangeGreaterThan.limit(BY_STRING).flatMap { greaterThanLimit =>
          rangeLessThan.limit(BY_STRING).map { lessThanLimit =>
            readOps.nodesGetFromIndexRangeSeekByString(
              index,
              greaterThanLimit.endPoint.asInstanceOf[String], greaterThanLimit.isInclusive,
              lessThanLimit.endPoint.asInstanceOf[String], lessThanLimit.isInclusive )
          }
        }.getOrElse(EMPTY_PRIMITIVE_LONG_COLLECTION.iterator)
    }

    JavaConversionSupport.mapToScalaENFXSafe(matchingNodes)(nodeOps.getById)
  }

  override def indexScan(index: IndexDescriptor) =
    mapToScalaENFXSafe(_statement.readOperations().nodesGetFromIndexScan(index))(nodeOps.getById)

  override def indexSeekByContains(index: IndexDescriptor, value: String) =
    mapToScalaENFXSafe(_statement.readOperations().nodesGetFromIndexContainsScan(index, value))(nodeOps.getById)

  override def lockingUniqueIndexSeek(index: IndexDescriptor, value: Any): Option[Node] = {
    indexSearchMonitor.lockingUniqueIndexSeek(index, value)
    val nodeId = _statement.readOperations().nodeGetFromUniqueIndexSeek(index, value)
    if (StatementConstants.NO_SUCH_NODE == nodeId) None else Some(nodeOps.getById(nodeId))
  }

  override def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int = labelIds.foldLeft(0) {
    case (count, labelId) =>
      if (_statement.dataWriteOperations().nodeRemoveLabel(node, labelId)) count + 1 else count
  }

  override def getNodesByLabel(id: Int): Iterator[Node] =
    JavaConversionSupport.mapToScalaENFXSafe(_statement.readOperations().nodesGetForLabel(id))(nodeOps.getById)

  override def nodeGetDegree(node: Long, dir: SemanticDirection): Int =
    _statement.readOperations().nodeGetDegree(node, toGraphDb(dir))

  override def nodeGetDegree(node: Long, dir: SemanticDirection, relTypeId: Int): Int =
    _statement.readOperations().nodeGetDegree(node, toGraphDb(dir), relTypeId)

  override def nodeIsDense(node: Long): Boolean = _statement.readOperations().nodeIsDense(node)

  private def kernelStatement: InternalKernelStatement =
    txBridge
      .getKernelTransactionBoundToThisThread(true)
      .acquireStatement()
      .asInstanceOf[InternalKernelStatement]

  class NodeOperations extends BaseOperations[Node] {
    override def delete(obj: Node) {
      _statement.dataWriteOperations().nodeDelete(obj.getId)
    }

    override def propertyKeyIds(id: Long): Iterator[Int] =
      JavaConversionSupport.asScala(_statement.readOperations().nodeGetPropertyKeys(id))

    override def getProperty(id: Long, propertyKeyId: Int): Any = try {
      _statement.readOperations().nodeGetProperty(id, propertyKeyId)
    } catch {
      case e: org.neo4j.kernel.api.exceptions.EntityNotFoundException =>
        if (isDeletedInThisTx(id))
          throw new EntityNotFoundException(s"Node with id $id has been deleted in this transaction", e)
        else
          null
    }

    override def hasProperty(id: Long, propertyKey: Int) =
      _statement.readOperations().nodeHasProperty(id, propertyKey)

    override def removeProperty(id: Long, propertyKeyId: Int) {
      _statement.dataWriteOperations().nodeRemoveProperty(id, propertyKeyId)
    }

    override def setProperty(id: Long, propertyKeyId: Int, value: Any) {
      _statement.dataWriteOperations().nodeSetProperty(id, properties.Property.property(propertyKeyId, value) )
    }

    override def getById(id: Long) = try {
      graph.getNodeById(id)
    } catch {
      case e: NotFoundException => throw new EntityNotFoundException(s"Node with id $id", e)
    }

    override def all: Iterator[Node] = graph.getAllNodes.iterator().asScala

    override def indexGet(name: String, key: String, value: Any): Iterator[Node] =
      graph.index.forNodes(name).get(key, value).iterator().asScala

    override def indexQuery(name: String, query: Any): Iterator[Node] =
      graph.index.forNodes(name).query(query).iterator().asScala

    override def isDeletedInThisTx(n: Node): Boolean = isDeletedInThisTx(n.getId)

    def isDeletedInThisTx(id: Long): Boolean =
      kernelStatement.hasTxStateWithChanges && kernelStatement.txState().nodeIsDeletedInThisTx(id)

    override def acquireExclusiveLock(obj: Long) =
      _statement.readOperations().acquireExclusive(ResourceTypes.NODE, obj)

    override def releaseExclusiveLock(obj: Long) =
      _statement.readOperations().releaseExclusive(ResourceTypes.NODE, obj)

    override def exists(n: Node) = _statement.readOperations().nodeExists(n.getId)
  }

  class RelationshipOperations extends BaseOperations[Relationship] {

    override def delete(obj: Relationship) {
      _statement.dataWriteOperations().relationshipDelete(obj.getId)
    }

    override def propertyKeyIds(id: Long): Iterator[Int] =
      asScala(_statement.readOperations().relationshipGetPropertyKeys(id))

    override def getProperty(id: Long, propertyKeyId: Int): Any = try {
      _statement.readOperations().relationshipGetProperty(id, propertyKeyId)
    } catch {
      case e: org.neo4j.kernel.api.exceptions.EntityNotFoundException =>
        if (isDeletedInThisTx(id))
          throw new EntityNotFoundException(s"Relationship with id $id has been deleted in this transaction", e)
        else
          null
    }

    override def hasProperty(id: Long, propertyKey: Int) =
      _statement.readOperations().relationshipHasProperty(id, propertyKey)

    override def removeProperty(id: Long, propertyKeyId: Int) {
      _statement.dataWriteOperations().relationshipRemoveProperty(id, propertyKeyId)
    }

    override def setProperty(id: Long, propertyKeyId: Int, value: Any) {
      _statement.dataWriteOperations().relationshipSetProperty(id, properties.Property.property(propertyKeyId, value) )
    }

    override def getById(id: Long) = try {
      graph.getRelationshipById(id)
    } catch {
      case e: NotFoundException => throw new EntityNotFoundException(s"Relationship with id $id", e)
    }

    override def all: Iterator[Relationship] = graph.getAllRelationships.iterator().asScala

    override def indexGet(name: String, key: String, value: Any): Iterator[Relationship] =
      graph.index.forRelationships(name).get(key, value).iterator().asScala

    override def indexQuery(name: String, query: Any): Iterator[Relationship] =
      graph.index.forRelationships(name).query(query).iterator().asScala

    override def isDeletedInThisTx(r: Relationship): Boolean =
      isDeletedInThisTx(r.getId)

    def isDeletedInThisTx(id: Long): Boolean =
      kernelStatement.hasTxStateWithChanges && kernelStatement.txState().relationshipIsDeletedInThisTx(id)

    override def acquireExclusiveLock(obj: Long) =
      _statement.readOperations().acquireExclusive(ResourceTypes.RELATIONSHIP, obj)

    override def releaseExclusiveLock(obj: Long) =
      _statement.readOperations().acquireExclusive(ResourceTypes.RELATIONSHIP, obj)

    override def exists(r: Relationship) = _statement.readOperations().relationshipExists(r.getId)
  }

  override def getOrCreatePropertyKeyId(propertyKey: String) =
    _statement.tokenWriteOperations().propertyKeyGetOrCreateForName(propertyKey)

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
    _statement.readOperations().schemaStateGetOrCreate(key, javaCreator)
  }

  override def addIndexRule(labelId: Int, propertyKeyId: Int): IdempotentResult[IndexDescriptor] = try {
    IdempotentResult(_statement.schemaWriteOperations().indexCreate(labelId, propertyKeyId))
  } catch {
    case _: AlreadyIndexedException =>
      val indexDescriptor = _statement.readOperations().indexGetForLabelAndPropertyKey(labelId, propertyKeyId)
      if(_statement.readOperations().indexGetState(indexDescriptor) == InternalIndexState.FAILED)
        throw new FailedIndexException(indexDescriptor.userDescription(tokenNameLookup))
     IdempotentResult(indexDescriptor, wasCreated = false)
  }

  override def dropIndexRule(labelId: Int, propertyKeyId: Int) =
    _statement.schemaWriteOperations().indexDrop(new IndexDescriptor(labelId, propertyKeyId))

  override def createUniqueConstraint(labelId: Int, propertyKeyId: Int): IdempotentResult[UniquenessConstraint] = try {
    IdempotentResult(_statement.schemaWriteOperations().uniquePropertyConstraintCreate(labelId, propertyKeyId))
  } catch {
    case existing: AlreadyConstrainedException =>
      IdempotentResult(existing.constraint().asInstanceOf[UniquenessConstraint], wasCreated = false)
  }

  override def dropUniqueConstraint(labelId: Int, propertyKeyId: Int) =
    _statement.schemaWriteOperations().constraintDrop(new UniquenessConstraint(labelId, propertyKeyId))

  override def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int): IdempotentResult[NodePropertyExistenceConstraint] =
    try {
      IdempotentResult(_statement.schemaWriteOperations().nodePropertyExistenceConstraintCreate(labelId, propertyKeyId))
    } catch {
      case existing: AlreadyConstrainedException =>
        IdempotentResult(existing.constraint().asInstanceOf[NodePropertyExistenceConstraint], wasCreated = false)
    }

  override def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int) =
    _statement.schemaWriteOperations().constraintDrop(new NodePropertyExistenceConstraint(labelId, propertyKeyId))

  override def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int): IdempotentResult[RelationshipPropertyExistenceConstraint] =
    try {
      IdempotentResult(_statement.schemaWriteOperations().relationshipPropertyExistenceConstraintCreate(relTypeId, propertyKeyId))
    } catch {
      case existing: AlreadyConstrainedException =>
        IdempotentResult(existing.constraint().asInstanceOf[RelationshipPropertyExistenceConstraint], wasCreated = false)
    }

  override def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int) =
    _statement.schemaWriteOperations().constraintDrop(new RelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId))

  override def getImportURL(url: URL): Either[String,URL] = graph match {
    case db: GraphDatabaseAPI =>
      try {
        Right(db.validateURLAccess(url))
      } catch {
        case error: URLAccessValidationError => Left(error.getMessage)
      }
  }

  override def relationshipStartNode(rel: Relationship) = rel.getStartNode

  override def relationshipEndNode(rel: Relationship) = rel.getEndNode

  private val tokenNameLookup = new StatementTokenNameLookup(_statement.readOperations())

  override def commitAndRestartTx() {
    tx.success()
    tx.close()

    tx = graph.beginTx()
    _statement = txBridge.get()
  }

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

    val baseTraversalDescription: TraversalDescription = graph.traversalDescription()
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
    _statement.readOperations().countsForNode(labelId)
  }

  override def relationshipCountByCountStore(startLabelId: Int, typeId: Int, endLabelId: Int): Long = {
    _statement.readOperations().countsForRelationship(startLabelId, typeId, endLabelId)
  }

  override def lockNodes(nodeIds: Long*) =
    nodeIds.sorted.foreach(_statement.readOperations().acquireExclusive(ResourceTypes.NODE, _))

  override def lockRelationships(relIds: Long*) =
    relIds.sorted.foreach(_statement.readOperations().acquireExclusive(ResourceTypes.RELATIONSHIP, _))

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

  override def callReadOnlyProcedure(signature: ProcedureSignature, args: Seq[Any]) =
    callProcedure(signature, args, statement.readOperations().procedureCallRead )

  override def callReadWriteProcedure(signature: ProcedureSignature, args: Seq[Any]) =
    callProcedure(signature, args, statement.dataWriteOperations().procedureCallWrite )

  private def callProcedure(signature: ProcedureSignature, args: Seq[Any],
                            call: (proc.ProcedureSignature.ProcedureName, Array[AnyRef]) => RawIterator[Array[AnyRef], ProcedureException]) = {
    val kn = new proc.ProcedureSignature.ProcedureName(signature.name.namespace.asJava, signature.name.name)
    val toArray = args.map(_.asInstanceOf[AnyRef]).toArray
    val read: RawIterator[Array[AnyRef], ProcedureException] = call(kn, toArray)
    new scala.Iterator[Array[AnyRef]] {
      override def hasNext: Boolean = read.hasNext
      override def next(): Array[AnyRef] = read.next
    }
  }

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
}

object TransactionBoundQueryContext {
  trait IndexSearchMonitor {
    def indexSeek(index: IndexDescriptor, value: Any): Unit

    def lockingUniqueIndexSeek(index: IndexDescriptor, value: Any): Unit
  }
}
