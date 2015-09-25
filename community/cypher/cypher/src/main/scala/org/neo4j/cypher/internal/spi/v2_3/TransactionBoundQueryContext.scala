/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.collection.primitive.PrimitiveLongIterator
import org.neo4j.collection.primitive.base.Empty.EMPTY_PRIMITIVE_LONG_COLLECTION
import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compiler.v2_3.MinMaxOrdering.{BY_NUMBER, BY_STRING, BY_VALUE}
import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.ast.convert.commands.DirectionConverter.toGraphDb
import org.neo4j.cypher.internal.compiler.v2_3.helpers.JavaConversionSupport._
import org.neo4j.cypher.internal.compiler.v2_3.helpers.{BeansAPIRelationshipIterator, JavaConversionSupport}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.matching.PatternNode
import org.neo4j.cypher.internal.compiler.v2_3.spi._
import org.neo4j.cypher.internal.frontend.v2_3.{SemanticDirection, Bound, EntityNotFoundException, FailedIndexException}
import org.neo4j.graphdb.DynamicRelationshipType._
import org.neo4j.graphdb._
import org.neo4j.graphdb.traversal.{TraversalDescription, Evaluators}
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.kernel.security.URLAccessValidationError
import org.neo4j.kernel.{Uniqueness, Traversal, GraphDatabaseAPI}
import org.neo4j.kernel.api._
import org.neo4j.kernel.api.constraints.{NodePropertyExistenceConstraint, RelationshipPropertyExistenceConstraint, UniquenessConstraint}
import org.neo4j.kernel.api.exceptions.schema.{AlreadyConstrainedException, AlreadyIndexedException}
import org.neo4j.kernel.api.index.{IndexDescriptor, InternalIndexState}
import org.neo4j.kernel.impl.api.KernelStatement
import org.neo4j.kernel.impl.core.{RelationshipProxy, ThreadToStatementContextBridge}
import org.neo4j.tooling.GlobalGraphOperations

import scala.collection.JavaConverters._
import scala.collection.{Iterator, mutable}

final class TransactionBoundQueryContext(graph: GraphDatabaseAPI,
                                         var tx: Transaction,
                                         val isTopLevelTx: Boolean,
                                         initialStatement: Statement)
  extends TransactionBoundTokenContext(initialStatement) with QueryContext {

  private var open = true
  private val txBridge = graph.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge])

  val nodeOps = new NodeOperations
  val relationshipOps = new RelationshipOperations
  val relationshipActions = graph.getDependencyResolver.resolveDependency(classOf[RelationshipProxy.RelationshipActions])

  def isOpen = open

  private val protocolWhiteList: Seq[String] = Seq("file", "http", "https", "ftp")

  def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int = labelIds.foldLeft(0) {
    case (count, labelId) => if (statement.dataWriteOperations().nodeAddLabel(node, labelId)) count + 1 else count
  }

  def close(success: Boolean) {
    try {
      statement.close()

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

  def withAnyOpenQueryContext[T](work: (QueryContext) => T): T = {
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

  def createNode(): Node =
    graph.createNode()

  def createRelationship(start: Node, end: Node, relType: String) =
    start.createRelationshipTo(end, withName(relType))

  def createRelationship(start: Long, end: Long, relType: Int) = {
    val relId = statement.dataWriteOperations().relationshipCreate(relType, start, end)
    relationshipOps.getById(relId)
  }

  def getOrCreateRelTypeId(relTypeName: String): Int =
    statement.tokenWriteOperations().relationshipTypeGetOrCreateForName(relTypeName)

  def getLabelsForNode(node: Long) =
    JavaConversionSupport.asScala(statement.readOperations().nodeGetLabels(node))

  def getPropertiesForNode(node: Long) =
    JavaConversionSupport.asScala(statement.readOperations().nodeGetPropertyKeys(node))

  def getPropertiesForRelationship(relId: Long) =
    JavaConversionSupport.asScala(statement.readOperations().relationshipGetPropertyKeys(relId))

  override def isLabelSetOnNode(label: Int, node: Long) =
    statement.readOperations().nodeHasLabel(node, label)

  def getOrCreateLabelId(labelName: String) =
    statement.tokenWriteOperations().labelGetOrCreateForName(labelName)

  def getRelationshipsForIds(node: Node, dir: SemanticDirection, types: Option[Seq[Int]]): Iterator[Relationship] = types match {
    case None =>
      val relationships = statement.readOperations().nodeGetRelationships(node.getId, toGraphDb(dir))
      new BeansAPIRelationshipIterator(relationships, relationshipActions)
    case Some(typeIds) =>
      val relationships = statement.readOperations().nodeGetRelationships(node.getId, toGraphDb(dir), typeIds: _*)
      new BeansAPIRelationshipIterator(relationships, relationshipActions)
  }

  def indexSeek(index: IndexDescriptor, value: Any) =
    JavaConversionSupport.mapToScalaENFXSafe(statement.readOperations().nodesGetFromIndexSeek(index, value))(nodeOps.getById)

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
          throw new ThisShouldNotHappenError("Stefan", "Failed to partition range bounds")
      }
    }
  }

  private def indexSeekByPrefixRange(index: IndexDescriptor, prefix: String): scala.Iterator[Node] = {
    val indexedNodes = statement.readOperations().nodesGetFromIndexRangeSeekByPrefix(index, prefix)
    JavaConversionSupport.mapToScalaENFXSafe(indexedNodes)(nodeOps.getById)
  }

  private def indexSeekByNumericalRange(index: IndexDescriptor, range: InequalitySeekRange[Number]): scala.Iterator[Node] = {
    val readOps = statement.readOperations()
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
    val readOps = statement.readOperations()
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

  def indexScan(index: IndexDescriptor) =
    mapToScalaENFXSafe(statement.readOperations().nodesGetFromIndexScan(index))(nodeOps.getById)

  def uniqueIndexSeek(index: IndexDescriptor, value: Any): Option[Node] = {
    val nodeId: Long = statement.readOperations().nodeGetFromUniqueIndexSeek(index, value)
    if (StatementConstants.NO_SUCH_NODE == nodeId) None else Some(nodeOps.getById(nodeId))
  }

  def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int = labelIds.foldLeft(0) {
    case (count, labelId) =>
      if (statement.dataWriteOperations().nodeRemoveLabel(node, labelId)) count + 1 else count
  }

  def getNodesByLabel(id: Int): Iterator[Node] =
    JavaConversionSupport.mapToScalaENFXSafe(statement.readOperations().nodesGetForLabel(id))(nodeOps.getById)

  def nodeGetDegree(node: Long, dir: SemanticDirection): Int =
    statement.readOperations().nodeGetDegree(node, toGraphDb(dir))

  def nodeGetDegree(node: Long, dir: SemanticDirection, relTypeId: Int): Int =
    statement.readOperations().nodeGetDegree(node, toGraphDb(dir), relTypeId)

  override def nodeIsDense(node: Long): Boolean = statement.readOperations().nodeIsDense(node)

  private def kernelStatement: KernelStatement =
    txBridge
      .getKernelTransactionBoundToThisThread(true)
      .acquireStatement()
      .asInstanceOf[KernelStatement]

  class NodeOperations extends BaseOperations[Node] {
    def delete(obj: Node) {
      statement.dataWriteOperations().nodeDelete(obj.getId)
    }

    def propertyKeyIds(id: Long): Iterator[Int] =
      JavaConversionSupport.asScala(statement.readOperations().nodeGetPropertyKeys(id))

    def getProperty(id: Long, propertyKeyId: Int): Any = try {
      statement.readOperations().nodeGetProperty(id, propertyKeyId)
    } catch {
      case _: org.neo4j.kernel.api.exceptions.EntityNotFoundException => null
    }

    def hasProperty(id: Long, propertyKey: Int) =
      statement.readOperations().nodeHasProperty(id, propertyKey)

    def removeProperty(id: Long, propertyKeyId: Int) {
      statement.dataWriteOperations().nodeRemoveProperty(id, propertyKeyId)
    }

    def setProperty(id: Long, propertyKeyId: Int, value: Any) {
      statement.dataWriteOperations().nodeSetProperty(id, properties.Property.property(propertyKeyId, value) )
    }

    def getById(id: Long) = try {
      graph.getNodeById(id)
    } catch {
      case e: NotFoundException => throw new EntityNotFoundException(s"Node with id $id", e)
    }

    def all: Iterator[Node] = GlobalGraphOperations.at(graph).getAllNodes.iterator().asScala

    def indexGet(name: String, key: String, value: Any): Iterator[Node] =
      graph.index.forNodes(name).get(key, value).iterator().asScala

    def indexQuery(name: String, query: Any): Iterator[Node] =
      graph.index.forNodes(name).query(query).iterator().asScala

    def isDeleted(n: Node): Boolean =
      kernelStatement.txState().nodeIsDeletedInThisTx(n.getId)
  }

  class RelationshipOperations extends BaseOperations[Relationship] {
    def delete(obj: Relationship) {
      statement.dataWriteOperations().relationshipDelete(obj.getId)
    }

    def propertyKeyIds(id: Long): Iterator[Int] =
      asScala(statement.readOperations().relationshipGetPropertyKeys(id))

    def getProperty(id: Long, propertyKeyId: Int): Any =
      statement.readOperations().relationshipGetProperty(id, propertyKeyId)

    def hasProperty(id: Long, propertyKey: Int) =
      statement.readOperations().relationshipHasProperty(id, propertyKey)

    def removeProperty(id: Long, propertyKeyId: Int) {
      statement.dataWriteOperations().relationshipRemoveProperty(id, propertyKeyId)
    }

    def setProperty(id: Long, propertyKeyId: Int, value: Any) {
      statement.dataWriteOperations().relationshipSetProperty(id, properties.Property.property(propertyKeyId, value) )
    }

    def getById(id: Long) = try {
      graph.getRelationshipById(id)
    } catch {
      case e: NotFoundException => throw new EntityNotFoundException(s"Relationship with id $id", e)
    }

    def all: Iterator[Relationship] =
      GlobalGraphOperations.at(graph).getAllRelationships.iterator().asScala

    def indexGet(name: String, key: String, value: Any): Iterator[Relationship] =
      graph.index.forRelationships(name).get(key, value).iterator().asScala

    def indexQuery(name: String, query: Any): Iterator[Relationship] =
      graph.index.forRelationships(name).query(query).iterator().asScala

    def isDeleted(r: Relationship): Boolean =
      kernelStatement.txState().relationshipIsDeletedInThisTx(r.getId)
  }

  def getOrCreatePropertyKeyId(propertyKey: String) =
    statement.tokenWriteOperations().propertyKeyGetOrCreateForName(propertyKey)

  def upgrade(context: QueryContext): LockingQueryContext = new RepeatableReadQueryContext(context, new Locker {
    private val locks = new mutable.ListBuffer[Lock]

    def releaseAllLocks() {
      locks.foreach(_.release())
    }

    def acquireLock(p: PropertyContainer) {
      locks += tx.acquireWriteLock(p)
    }
  })

  abstract class BaseOperations[T <: PropertyContainer] extends Operations[T] {
    def primitiveLongIteratorToScalaIterator(primitiveIterator: PrimitiveLongIterator): Iterator[Long] =
      new Iterator[Long] {
        def hasNext: Boolean = primitiveIterator.hasNext

        def next(): Long = primitiveIterator.next
      }
  }

  def getOrCreateFromSchemaState[K, V](key: K, creator: => V) = {
    val javaCreator = new org.neo4j.function.Function[K, V]() {
      def apply(key: K) = creator
    }
    statement.readOperations().schemaStateGetOrCreate(key, javaCreator)
  }

  def addIndexRule(labelId: Int, propertyKeyId: Int): IdempotentResult[IndexDescriptor] = try {
    IdempotentResult(statement.schemaWriteOperations().indexCreate(labelId, propertyKeyId))
  } catch {
    case _: AlreadyIndexedException =>
      val indexDescriptor = statement.readOperations().indexesGetForLabelAndPropertyKey(labelId, propertyKeyId)
      if(statement.readOperations().indexGetState(indexDescriptor) == InternalIndexState.FAILED)
        throw new FailedIndexException(indexDescriptor.userDescription(tokenNameLookup))
     IdempotentResult(indexDescriptor, wasCreated = false)
  }

  def dropIndexRule(labelId: Int, propertyKeyId: Int) =
    statement.schemaWriteOperations().indexDrop(new IndexDescriptor(labelId, propertyKeyId))

  def createUniqueConstraint(labelId: Int, propertyKeyId: Int): IdempotentResult[UniquenessConstraint] = try {
    IdempotentResult(statement.schemaWriteOperations().uniquePropertyConstraintCreate(labelId, propertyKeyId))
  } catch {
    case existing: AlreadyConstrainedException =>
      IdempotentResult(existing.constraint().asInstanceOf[UniquenessConstraint], wasCreated = false)
  }

  def dropUniqueConstraint(labelId: Int, propertyKeyId: Int) =
    statement.schemaWriteOperations().constraintDrop(new UniquenessConstraint(labelId, propertyKeyId))

  def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int): IdempotentResult[NodePropertyExistenceConstraint] =
    try {
      IdempotentResult(statement.schemaWriteOperations().nodePropertyExistenceConstraintCreate(labelId, propertyKeyId))
    } catch {
      case existing: AlreadyConstrainedException =>
        IdempotentResult(existing.constraint().asInstanceOf[NodePropertyExistenceConstraint], wasCreated = false)
    }

  def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int) =
    statement.schemaWriteOperations().constraintDrop(new NodePropertyExistenceConstraint(labelId, propertyKeyId))

  def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int): IdempotentResult[RelationshipPropertyExistenceConstraint] =
    try {
      IdempotentResult(statement.schemaWriteOperations().relationshipPropertyExistenceConstraintCreate(relTypeId, propertyKeyId))
    } catch {
      case existing: AlreadyConstrainedException =>
        IdempotentResult(existing.constraint().asInstanceOf[RelationshipPropertyExistenceConstraint], wasCreated = false)
    }

  def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int) =
    statement.schemaWriteOperations().constraintDrop(new RelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId))

  override def getImportURL(url: URL): Either[String,URL] = graph match {
    case db: GraphDatabaseAPI =>
      try {
        Right(db.validateURLAccess(url))
      } catch {
        case error: URLAccessValidationError => Left(error.getMessage)
      }
  }

  def relationshipStartNode(rel: Relationship) = rel.getStartNode

  def relationshipEndNode(rel: Relationship) = rel.getEndNode

  private val tokenNameLookup = new StatementTokenNameLookup(statement.readOperations())

  override def commitAndRestartTx() {
    tx.success()
    tx.close()

    tx = graph.beginTx()
    statement = txBridge.get()
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

    val baseTraversalDescription: TraversalDescription = Traversal.description()
      .evaluator(depthEval)
      .uniqueness(Uniqueness.RELATIONSHIP_PATH)

    val traversalDescription = if (relTypes.isEmpty) {
      baseTraversalDescription.expand(Traversal.expanderForAllTypes(toGraphDb(direction)))
    } else {
      val emptyExpander = Traversal.emptyExpander()
      val expander = relTypes.foldLeft(emptyExpander) {
        case (e, t) => e.add(DynamicRelationshipType.withName(t), toGraphDb(direction))
      }
      baseTraversalDescription.expand(expander)
    }
    traversalDescription.traverse(realNode).iterator().asScala
  }
}
