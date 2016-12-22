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
package org.neo4j.cypher.internal.spi.v2_3

import java.net.URL
import java.util.function.Predicate

import org.neo4j.collection.primitive.PrimitiveLongIterator
import org.neo4j.collection.primitive.base.Empty.EMPTY_PRIMITIVE_LONG_COLLECTION
import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compiler.v2_3.MinMaxOrdering.{BY_NUMBER, BY_STRING, BY_VALUE}
import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.ast.convert.commands.DirectionConverter.toGraphDb
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{KernelPredicate, OnlyDirectionExpander, TypeAndDirectionExpander}
import org.neo4j.cypher.internal.compiler.v2_3.helpers.JavaConversionSupport
import org.neo4j.cypher.internal.compiler.v2_3.helpers.JavaConversionSupport._
import org.neo4j.cypher.internal.compiler.v2_3.pipes.matching.PatternNode
import org.neo4j.cypher.internal.compiler.v2_3.spi._
import org.neo4j.cypher.internal.frontend.v2_3.{Bound, EntityNotFoundException, FailedIndexException, SemanticDirection}
import org.neo4j.cypher.internal.spi.{BeansAPIRelationshipIterator, IndexDescriptorCompatibility}
import org.neo4j.cypher.internal.spi.v3_2.TransactionalContextWrapper
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.graphalgo.impl.path.ShortestPath
import org.neo4j.graphalgo.impl.path.ShortestPath.ShortestPathPredicate
import org.neo4j.graphdb.RelationshipType._
import org.neo4j.graphdb._
import org.neo4j.graphdb.security.URLAccessValidationError
import org.neo4j.graphdb.traversal.{Evaluators, TraversalDescription, Uniqueness}
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.constraints.{NodePropertyExistenceConstraint, RelationshipPropertyExistenceConstraint, UniquenessConstraint}
import org.neo4j.kernel.api.exceptions.schema.{AlreadyConstrainedException, AlreadyIndexedException}
import org.neo4j.kernel.api.index.{IndexDescriptor, InternalIndexState}
import org.neo4j.kernel.api.schema.{IndexDescriptorFactory, NodePropertyDescriptor, RelationshipPropertyDescriptor}
import org.neo4j.kernel.api.{exceptions, _}
import org.neo4j.kernel.impl.core.NodeManager

import scala.collection.JavaConverters._
import scala.collection.{Iterator, mutable}

final class TransactionBoundQueryContext(tc: TransactionalContextWrapper)
  extends TransactionBoundTokenContext(tc.statement) with QueryContext with IndexDescriptorCompatibility {

  override val nodeOps = new NodeOperations
  override val relationshipOps = new RelationshipOperations
  private val nodeManager = tc.graph.getDependencyResolver.resolveDependency(classOf[NodeManager])

  def isOpen = tc.isOpen

  def isTopLevelTx: Boolean = tc.isTopLevelTx

  def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int = labelIds.foldLeft(0) {
    case (count, labelId) => if (tc.statement.dataWriteOperations().nodeAddLabel(node, labelId)) count + 1 else count
  }

  def close(success: Boolean) { tc.close(success) }

  override def withAnyOpenQueryContext[T](work: (QueryContext) => T): T = {
    if (tc.isOpen) {
      work(this)
    } else {
      val context = tc.getOrBeginNewIfClosed()
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
    tc.graph.createNode()

  override def createRelationship(start: Node, end: Node, relType: String) =
    start.createRelationshipTo(end, withName(relType))

  def createRelationship(start: Long, end: Long, relType: Int) = {
    val relId = tc.statement.dataWriteOperations().relationshipCreate(relType, start, end)
    relationshipOps.getById(relId)
  }

  def getOrCreateRelTypeId(relTypeName: String): Int =
    tc.statement.tokenWriteOperations().relationshipTypeGetOrCreateForName(relTypeName)

  def getLabelsForNode(node: Long) =
    JavaConversionSupport.asScala(tc.statement.readOperations().nodeGetLabels(node))

  def getPropertiesForNode(node: Long) =
    JavaConversionSupport.asScala(tc.statement.readOperations().nodeGetPropertyKeys(node))

  def getPropertiesForRelationship(relId: Long) =
    JavaConversionSupport.asScala(tc.statement.readOperations().relationshipGetPropertyKeys(relId))

  override def isLabelSetOnNode(label: Int, node: Long) =
    tc.statement.readOperations().nodeHasLabel(node, label)

  def getOrCreateLabelId(labelName: String) =
    tc.statement.tokenWriteOperations().labelGetOrCreateForName(labelName)

  def getRelationshipsForIds(node: Node, dir: SemanticDirection, types: Option[Seq[Int]]): Iterator[Relationship] = {
    val relationships = types match {
      case None =>
        tc.statement.readOperations().nodeGetRelationships(node.getId, toGraphDb(dir))
      case Some(typeIds) =>
        tc.statement.readOperations().nodeGetRelationships(node.getId, toGraphDb(dir), typeIds: _*)
    }
    new BeansAPIRelationshipIterator(relationships, nodeManager)
  }

  def indexSeek(index: IndexDescriptor, value: Any) =
    JavaConversionSupport.mapToScalaENFXSafe(tc.statement.readOperations().nodesGetFromIndexSeek(index, value))(nodeOps.getById)

  def indexSeekByRange(index: IndexDescriptor, value: Any) = value match {

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
          if (indexSeekByNumericalRange(index, numericRange).isEmpty
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
    val indexedNodes = tc.statement.readOperations().nodesGetFromIndexRangeSeekByPrefix(index, prefix)
    JavaConversionSupport.mapToScalaENFXSafe(indexedNodes)(nodeOps.getById)
  }

  private def indexSeekByNumericalRange(index: IndexDescriptor, range: InequalitySeekRange[Number]): scala.Iterator[Node] = {
    val readOps = tc.statement.readOperations()
    val matchingNodes: PrimitiveLongIterator = (range match {

      case rangeLessThan: RangeLessThan[Number] =>
        rangeLessThan.limit(BY_NUMBER).map { limit =>
          readOps.nodesGetFromIndexRangeSeekByNumber(index, null, false, limit.endPoint, limit.isInclusive)
        }

      case rangeGreaterThan: RangeGreaterThan[Number] =>
        rangeGreaterThan.limit(BY_NUMBER).map { limit =>
          readOps.nodesGetFromIndexRangeSeekByNumber(index, limit.endPoint, limit.isInclusive, null, false)
        }

      case RangeBetween(rangeGreaterThan, rangeLessThan) =>
        rangeGreaterThan.limit(BY_NUMBER).flatMap { greaterThanLimit =>
          rangeLessThan.limit(BY_NUMBER).map { lessThanLimit =>
            readOps.nodesGetFromIndexRangeSeekByNumber(
              index,
              greaterThanLimit.endPoint, greaterThanLimit.isInclusive,
              lessThanLimit.endPoint, lessThanLimit.isInclusive)
          }
        }
    }).getOrElse(EMPTY_PRIMITIVE_LONG_COLLECTION.iterator)
    JavaConversionSupport.mapToScalaENFXSafe(matchingNodes)(nodeOps.getById)
  }

  private def indexSeekByStringRange(index: IndexDescriptor, range: InequalitySeekRange[String]): scala.Iterator[Node] = {
    val readOps = tc.statement.readOperations()
    val matchingNodes: PrimitiveLongIterator = range match {

      case rangeLessThan: RangeLessThan[String] =>
        rangeLessThan.limit(BY_STRING).map { limit =>
          readOps.nodesGetFromIndexRangeSeekByString(index, null, false, limit.endPoint.asInstanceOf[String], limit.isInclusive)
        }.getOrElse(EMPTY_PRIMITIVE_LONG_COLLECTION.iterator)

      case rangeGreaterThan: RangeGreaterThan[String] =>
        rangeGreaterThan.limit(BY_STRING).map { limit =>
          readOps.nodesGetFromIndexRangeSeekByString(index, limit.endPoint.asInstanceOf[String], limit.isInclusive, null, false)
        }.getOrElse(EMPTY_PRIMITIVE_LONG_COLLECTION.iterator)

      case RangeBetween(rangeGreaterThan, rangeLessThan) =>
        rangeGreaterThan.limit(BY_STRING).flatMap { greaterThanLimit =>
          rangeLessThan.limit(BY_STRING).map { lessThanLimit =>
            readOps.nodesGetFromIndexRangeSeekByString(
              index,
              greaterThanLimit.endPoint.asInstanceOf[String], greaterThanLimit.isInclusive,
              lessThanLimit.endPoint.asInstanceOf[String], lessThanLimit.isInclusive)
          }
        }.getOrElse(EMPTY_PRIMITIVE_LONG_COLLECTION.iterator)
    }

    JavaConversionSupport.mapToScalaENFXSafe(matchingNodes)(nodeOps.getById)
  }

  def indexScan(index: IndexDescriptor) =
    mapToScalaENFXSafe(tc.statement.readOperations().nodesGetFromIndexScan(index))(nodeOps.getById)

  override def lockingExactUniqueIndexSearch(index: IndexDescriptor, value: Any): Option[Node] = {
    val nodeId: Long = tc.statement.readOperations().nodeGetFromUniqueIndexSeek(index, value)
    if (StatementConstants.NO_SUCH_NODE == nodeId) None else Some(nodeOps.getById(nodeId))
  }

  def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int = labelIds.foldLeft(0) {
    case (count, labelId) =>
      if (tc.statement.dataWriteOperations().nodeRemoveLabel(node, labelId)) count + 1 else count
  }

  def getNodesByLabel(id: Int): Iterator[Node] =
    JavaConversionSupport.mapToScalaENFXSafe(tc.statement.readOperations().nodesGetForLabel(id))(nodeOps.getById)

  def nodeGetDegree(node: Long, dir: SemanticDirection): Int =
    tc.statement.readOperations().nodeGetDegree(node, toGraphDb(dir))

  def nodeGetDegree(node: Long, dir: SemanticDirection, relTypeId: Int): Int =
    tc.statement.readOperations().nodeGetDegree(node, toGraphDb(dir), relTypeId)

  override def nodeIsDense(node: Long): Boolean = tc.statement.readOperations().nodeIsDense(node)

  class NodeOperations extends BaseOperations[Node] {
    def delete(obj: Node) {
      try {
        tc.statement.dataWriteOperations().nodeDelete(obj.getId)
      } catch {
        case _: exceptions.EntityNotFoundException => // node has been deleted by another transaction, oh well...
      }
    }

    def detachDelete(obj: Node): Int = {
      try {
        tc.statement.dataWriteOperations().nodeDetachDelete(obj.getId)
      } catch {
        case _: exceptions.EntityNotFoundException => // the node has been deleted by another transaction, oh well...
          0
      }
    }

    def propertyKeyIds(id: Long): Iterator[Int] = try {
      // use the following when bumping 2.3.x dependency
      // JavaConversionSupport.asScalaENFXSafe(tc.statement.readOperations().nodeGetPropertyKeys(id))
      new Iterator[Int] {
        val inner = tc.statement.readOperations().nodeGetPropertyKeys(id)

        override def hasNext: Boolean = inner.hasNext

        override def next(): Int = try {
          inner.next()
        } catch {
          case _: org.neo4j.kernel.api.exceptions.EntityNotFoundException => null.asInstanceOf[Int]
        }
      }
    } catch {
      case _: exceptions.EntityNotFoundException => Iterator.empty
    }

    def getProperty(id: Long, propertyKeyId: Int): Any = try {
      tc.statement.readOperations().nodeGetProperty(id, propertyKeyId)
    } catch {
      case _: org.neo4j.kernel.api.exceptions.EntityNotFoundException => null.asInstanceOf[Int]
    }

    def hasProperty(id: Long, propertyKey: Int) = try {
      tc.statement.readOperations().nodeHasProperty(id, propertyKey)
    } catch {
      case _: org.neo4j.kernel.api.exceptions.EntityNotFoundException => false
    }

    def removeProperty(id: Long, propertyKeyId: Int) = try {
      tc.statement.dataWriteOperations().nodeRemoveProperty(id, propertyKeyId)
    } catch {
      case _: org.neo4j.kernel.api.exceptions.EntityNotFoundException => //ignore
    }

    def setProperty(id: Long, propertyKeyId: Int, value: Any) = try {
      tc.statement.dataWriteOperations().nodeSetProperty(id, properties.Property.property(propertyKeyId, value) )
    } catch {
      case _: org.neo4j.kernel.api.exceptions.EntityNotFoundException => //ignore
    }

    override def getById(id: Long) = try {
      tc.graph.getNodeById(id)
    } catch {
      case e: NotFoundException => throw new EntityNotFoundException(s"Node with id $id", e)
    }

    def all: Iterator[Node] =
      JavaConversionSupport.mapToScalaENFXSafe(tc.statement.readOperations().nodesGetAll())(getById)

    def indexGet(name: String, key: String, value: Any): Iterator[Node] =
      JavaConversionSupport.mapToScalaENFXSafe(tc.statement.readOperations().nodeLegacyIndexGet(name, key, value))(getById)

    def indexQuery(name: String, query: Any): Iterator[Node] =
      JavaConversionSupport.mapToScalaENFXSafe(tc.statement.readOperations().nodeLegacyIndexQuery(name, query))(getById)

    def isDeleted(n: Node): Boolean =
      tc.stateView.hasTxStateWithChanges && tc.stateView.txState().nodeIsDeletedInThisTx(n.getId)
  }

  class RelationshipOperations extends BaseOperations[Relationship] {
    override def delete(obj: Relationship) {
      try {
        tc.statement.dataWriteOperations().relationshipDelete(obj.getId)
      } catch {
        case _: exceptions.EntityNotFoundException => // node has been deleted by another transaction, oh well...
      }
    }

    override def propertyKeyIds(id: Long): Iterator[Int] = try {
      // use the following when bumping the cypher 2.3.x version
      //JavaConversionSupport.asScalaENFXSafe(statement.readOperations().relationshipGetPropertyKeys(id))
        new Iterator[Int] {
          val inner = tc.statement.readOperations().relationshipGetPropertyKeys(id)

          override def hasNext: Boolean = inner.hasNext

          override def next(): Int = try {
            inner.next()
          } catch {
            case _: org.neo4j.kernel.api.exceptions.EntityNotFoundException => null.asInstanceOf[Int]
          }
        }
      } catch {
        case _: exceptions.EntityNotFoundException => Iterator.empty
      }

    override def getProperty(id: Long, propertyKeyId: Int): Any = try {
      tc.statement.readOperations().relationshipGetProperty(id, propertyKeyId)
    } catch {
      case _: exceptions.EntityNotFoundException => null
    }

    override def hasProperty(id: Long, propertyKey: Int) = try {
      tc.statement.readOperations().relationshipHasProperty(id, propertyKey)
    } catch {
      case _: exceptions.EntityNotFoundException => false
    }

    override def removeProperty(id: Long, propertyKeyId: Int) = try {
      tc.statement.dataWriteOperations().relationshipRemoveProperty(id, propertyKeyId)
    } catch {
      case _: exceptions.EntityNotFoundException => //ignore
    }

    override def setProperty(id: Long, propertyKeyId: Int, value: Any) = try {
      tc.statement.dataWriteOperations().relationshipSetProperty(id, properties.Property.property(propertyKeyId, value))
    } catch {
      case _: exceptions.EntityNotFoundException => //ignore
    }

    override def getById(id: Long) = try {
      tc.graph.getRelationshipById(id)
    } catch {
      case e: NotFoundException => throw new EntityNotFoundException(s"Relationship with id $id", e)
    }

    override def all: Iterator[Relationship] = {
      JavaConversionSupport.mapToScalaENFXSafe(tc.statement.readOperations().relationshipsGetAll())(getById)
    }

    override def indexGet(name: String, key: String, value: Any): Iterator[Relationship] =
      JavaConversionSupport.mapToScalaENFXSafe(tc.statement.readOperations().relationshipLegacyIndexGet(name, key, value, -1, -1))(getById)

    override def indexQuery(name: String, query: Any): Iterator[Relationship] =
      JavaConversionSupport.mapToScalaENFXSafe(tc.statement.readOperations().relationshipLegacyIndexQuery(name, query, -1, -1))(getById)

    override def isDeleted(r: Relationship): Boolean =
      tc.stateView.hasTxStateWithChanges && tc.stateView.txState().relationshipIsDeletedInThisTx(r.getId)
  }

  override def getOrCreatePropertyKeyId(propertyKey: String) =
    tc.statement.tokenWriteOperations().propertyKeyGetOrCreateForName(propertyKey)

  override def upgrade(context: QueryContext): LockingQueryContext = new RepeatableReadQueryContext(context, new Locker {
    private val locks = new mutable.ListBuffer[Lock]

    def releaseAllLocks() {
      locks.foreach(_.release())
    }

    def acquireLock(p: PropertyContainer) {
      locks += tc.acquireWriteLock(p)
    }
  })

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
    tc.statement.readOperations().schemaStateGetOrCreate(key, javaCreator)
  }

  def addIndexRule(labelId: Int, propertyKeyId: Int): IdempotentResult[IndexDescriptor] = try {
    IdempotentResult(tc.statement.schemaWriteOperations().indexCreate(new NodePropertyDescriptor(labelId, propertyKeyId)))
  } catch {
    case _: AlreadyIndexedException =>

      val indexDescriptor = tc.statement.readOperations().indexGetForLabelAndPropertyKey(new NodePropertyDescriptor(labelId, propertyKeyId))

      if (tc.statement.readOperations().indexGetState(indexDescriptor) == InternalIndexState.FAILED)
        throw new FailedIndexException(indexDescriptor.userDescription(tokenNameLookup))
      IdempotentResult(indexDescriptor, wasCreated = false)
  }

  def dropIndexRule(labelId: Int, propertyKeyId: Int) =
    tc.statement.schemaWriteOperations().indexDrop(IndexDescriptorFactory.from(new NodePropertyDescriptor(labelId, propertyKeyId)))

  def createUniqueConstraint(labelId: Int, propertyKeyId: Int): IdempotentResult[UniquenessConstraint] = try {
    IdempotentResult(tc.statement.schemaWriteOperations().uniquePropertyConstraintCreate(new NodePropertyDescriptor(labelId,
      propertyKeyId)))
  } catch {
    case existing: AlreadyConstrainedException =>
      IdempotentResult(existing.constraint().asInstanceOf[UniquenessConstraint], wasCreated = false)
  }

  def dropUniqueConstraint(labelId: Int, propertyKeyId: Int) =
    tc.statement.schemaWriteOperations().constraintDrop(new UniquenessConstraint(new NodePropertyDescriptor(labelId,
      propertyKeyId)))

  def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int): IdempotentResult[NodePropertyExistenceConstraint] =
    try {
      IdempotentResult(tc.statement.schemaWriteOperations().nodePropertyExistenceConstraintCreate(new NodePropertyDescriptor(labelId,
        propertyKeyId)))
    } catch {
      case existing: AlreadyConstrainedException =>
        IdempotentResult(existing.constraint().asInstanceOf[NodePropertyExistenceConstraint], wasCreated = false)
    }

  def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int) =
    tc.statement.schemaWriteOperations().constraintDrop(new NodePropertyExistenceConstraint(new NodePropertyDescriptor(labelId,
      propertyKeyId)))

  def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int): IdempotentResult[RelationshipPropertyExistenceConstraint] =
    try {
      IdempotentResult(tc.statement.schemaWriteOperations().relationshipPropertyExistenceConstraintCreate(new RelationshipPropertyDescriptor(relTypeId, propertyKeyId)))
    } catch {
      case existing: AlreadyConstrainedException =>
        IdempotentResult(existing.constraint().asInstanceOf[RelationshipPropertyExistenceConstraint], wasCreated = false)
    }

  def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int) =
    tc.statement.schemaWriteOperations().constraintDrop(new RelationshipPropertyExistenceConstraint(new
        RelationshipPropertyDescriptor(relTypeId,propertyKeyId)))

  override def getImportURL(url: URL): Either[String,URL] = tc.graph match {
    case db: GraphDatabaseQueryService =>
      try {
        Right(db.validateURLAccess(url))
      } catch {
        case error: URLAccessValidationError => Left(error.getMessage)
      }
  }

  def relationshipStartNode(rel: Relationship) = rel.getStartNode

  def relationshipEndNode(rel: Relationship) = rel.getEndNode

  private val tokenNameLookup = new StatementTokenNameLookup(tc.statement.readOperations())

  override def commitAndRestartTx() { tc.commitAndRestartTx() }

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
    val baseTraversalDescription: TraversalDescription = tc.graph.asInstanceOf[GraphDatabaseCypherService]
      .getGraphDatabaseService.traversalDescription()
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

  override def singleShortestPath(left: Node, right: Node, depth: Int, expander: expressions.Expander, pathPredicate: KernelPredicate[Path],
                                  filters: Seq[KernelPredicate[PropertyContainer]]): Option[Path] = {
    val pathFinder = buildPathFinder(depth, expander, pathPredicate, filters)

    Option(pathFinder.findSinglePath(left, right))
  }

  private def buildPathFinder(depth: Int, expander: expressions.Expander, pathPredicate: KernelPredicate[Path],
                              filters: Seq[KernelPredicate[PropertyContainer]]): ShortestPath = {
    val startExpander = expander match {
      case OnlyDirectionExpander(_, _, dir) =>
        PathExpanderBuilder.allTypes(toGraphDb(dir))
      case TypeAndDirectionExpander(_, _, typDirs) =>
        typDirs.foldLeft(PathExpanderBuilder.empty()) {
          case (acc, (typ, dir)) => acc.add(DynamicRelationshipType.withName(typ), toGraphDb(dir))
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

  override def allShortestPath(left: Node, right: Node, depth: Int, expander: expressions.Expander, pathPredicate: KernelPredicate[Path],
                               filters: Seq[KernelPredicate[PropertyContainer]]): scala.Iterator[Path] = {
    val pathFinder = buildPathFinder(depth, expander, pathPredicate, filters)

    pathFinder.findAllPaths(left, right).iterator().asScala
  }

  def nodeCountByCountStore(labelId: Int): Long = {
    tc.statement.readOperations().countsForNode(labelId)
  }

  def relationshipCountByCountStore(startLabelId: Int, typeId: Int, endLabelId: Int): Long = {
    tc.statement.readOperations().countsForRelationship(startLabelId, typeId, endLabelId)
  }

  override def detachDeleteNode(node: Node): Int = {
    try {
      tc.statement.dataWriteOperations().nodeDetachDelete(node.getId)
    } catch {
      case _: exceptions.EntityNotFoundException => // the node has been deleted by another transaction, oh well...
        0
    }
  }
}
