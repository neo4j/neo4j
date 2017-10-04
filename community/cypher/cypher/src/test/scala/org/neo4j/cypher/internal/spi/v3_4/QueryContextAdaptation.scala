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
package org.neo4j.cypher.internal.spi.v3_4

import java.net.URL

import org.neo4j.collection.primitive.PrimitiveLongIterator
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions.{Expander, KernelPredicate, UserDefinedAggregator}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes.matching.PatternNode
import org.neo4j.cypher.internal.compiler.v3_4.IndexDescriptor
import org.neo4j.cypher.internal.v3_4.logical.plans.QualifiedName
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection
import org.neo4j.graphdb.{Node, Path, PropertyContainer, Relationship}
import org.neo4j.kernel.impl.api.store.RelationshipIterator
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.{EdgeValue, NodeValue}

trait QueryContextAdaptation {
  self: QueryContext =>

  override type EntityAccessor = this.type

  override def indexScanByContains(index: IndexDescriptor, value: String): scala.Iterator[Node] = ???

  override def indexScanByEndsWith(index: IndexDescriptor, value: String): Iterator[Node] = ???

  override def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int): Boolean = ???

  override def createNode(): Node = ???

  override def createNodeId(): Long = ???

  override def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int): Unit = ???

  override def createUniqueConstraint(descriptor: IndexDescriptor): Boolean = ???

  override def createNodeKeyConstraint(descriptor: IndexDescriptor): Boolean = ???

  override def getOrCreateRelTypeId(relTypeName: String): Int = ???

  override def getPropertiesForRelationship(relId: Long): scala.Iterator[Int] = ???

  override def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int): Unit = ???

  override def singleShortestPath(left: Long, right: Long, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path], filters: Seq[KernelPredicate[PropertyContainer]]): Option[Path] = ???

  override def asObject(value: AnyValue): AnyRef = ???

  override def edgeGetStartNode(edge: EdgeValue): NodeValue = ???

  override def edgeGetEndNode(edge: EdgeValue): NodeValue = ???

  /**
    * This should not be used. We'll remove sooner (or later). Don't do it.
    */
  override def withAnyOpenQueryContext[T](work: (QueryContext) => T): T = ???

  // Legacy dependency between kernel and compiler
  override def variableLengthPathExpand(node: PatternNode, realNode: Long, minHops: Option[Int], maxHops: Option[Int], direction: SemanticDirection, relTypes: Seq[String]): scala.Iterator[Path] = ???

  override def nodeGetDegree(node: Long, dir: SemanticDirection): Int = ???

  override def nodeGetDegree(node: Long, dir: SemanticDirection, relTypeId: Int): Int = ???

  override def entityAccessor: EntityAccessor = ???

  override def getOrCreatePropertyKeyId(propertyKey: String): Int = ???

  override def isLabelSetOnNode(label: Int, node: Long): Boolean = ???

  override def indexSeek(index: IndexDescriptor, value: Seq[Any]): scala.Iterator[Node] = ???

  override def getRelationshipsForIds(node: Long, dir: SemanticDirection, types: Option[Array[Int]]): scala.Iterator[Relationship] = ???

  override def getRelationshipsForIdsPrimitive(node: Long, dir: SemanticDirection, types: Option[Array[Int]]): RelationshipIterator = ???

  override def getRelationshipFor(relationshipId: Long, typeId: Int, startNodeId: Long, endNodeId: Long): Relationship = ???

  override def getLabelsForNode(node: Long): scala.Iterator[Int] = ???

  override def dropUniqueConstraint(descriptor: IndexDescriptor): Unit = ???

  override def dropNodeKeyConstraint(descriptor: IndexDescriptor): Unit = ???

  // Check if a runtime value is a node, relationship, path or some such value returned from
  override def isGraphKernelResultValue(v: Any): Boolean = ???

  override def transactionalContext: QueryTransactionalContext = ???

  override def allShortestPath(left: Long, right: Long, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path], filters: Seq[KernelPredicate[PropertyContainer]]): scala.Iterator[Path] = ???

  override def nodeOps: Operations[Node] = ???

  override def lockRelationships(relIds: Long*): Unit = ???

  override def getOrCreateLabelId(labelName: String): Int = ???

  override def indexScan(index: IndexDescriptor): scala.Iterator[Node] = ???

  override def indexScanPrimitive(index: IndexDescriptor): PrimitiveLongIterator = ???

  override def getPropertiesForNode(node: Long): scala.Iterator[Int] = ???

  override def getImportURL(url: URL): Either[String, URL] = ???

  override def relationshipCountByCountStore(startLabelId: Int, typeId: Int, endLabelId: Int): Long = ???

  override def nodeIsDense(node: Long): Boolean = ???

  override def indexSeekByRange(index: IndexDescriptor, value: Any): scala.Iterator[Node] = ???

  override def setLabelsOnNode(node: Long, labelIds: scala.Iterator[Int]): Int = ???

  override def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int): Boolean = ???

  override def dropIndexRule(descriptor: IndexDescriptor): Unit = ???

  override def lockNodes(nodeIds: Long*): Unit = ???

  override def relationshipOps: Operations[Relationship] = ???

  override def getNodesByLabel(id: Int): scala.Iterator[Node] = ???

  override def getNodesByLabelPrimitive(id: Int): PrimitiveLongIterator = ???

  override def lockingUniqueIndexSeek(index: IndexDescriptor, values: Seq[Any]): Option[Node] = ???

  override def callReadOnlyProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): scala.Iterator[Array[AnyRef]] = ???

  override def callReadWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): scala.Iterator[Array[AnyRef]] = ???

  override def callSchemaWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] = ???

  override def callDbmsProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] = ???

  override def callFunction(name: QualifiedName, args: Seq[Any], allowed: Array[String]): AnyRef = ???

  override def aggregateFunction(name: QualifiedName,
                                 allowed: Array[String]): UserDefinedAggregator = ???

  override def getOrCreateFromSchemaState[K, V](key: K, creator: => V): V = ???

  override def removeLabelsFromNode(node: Long, labelIds: scala.Iterator[Int]): Int = ???

  override def createRelationship(start: Node, end: Node, relType: String): Relationship = ???

  override def createRelationship(start: Long, end: Long, relType: Int): Relationship = ???

  override def nodeCountByCountStore(labelId: Int): Long = ???

  override def addIndexRule(descriptor: IndexDescriptor): IdempotentResult[IndexDescriptor] = ???

  override def getOptRelTypeId(relType: String): Option[Int] = ???

  override def getRelTypeName(id: Int): String = ???

  override def getRelTypeId(relType: String): Int = ???

  override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] = ???

  override def getLabelName(id: Int): String = ???

  override def getOptLabelId(labelName: String): Option[Int] = ???

  override def getPropertyKeyId(propertyKeyName: String): Int = ???

  override def getPropertyKeyName(id: Int): String = ???

  override def getLabelId(labelName: String): Int = ???

  override def detachDeleteNode(node: Long): Int = ???

  override def assertSchemaWritesAllowed(): Unit = ???
}
