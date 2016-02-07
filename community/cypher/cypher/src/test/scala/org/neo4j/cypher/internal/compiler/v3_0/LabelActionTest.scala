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
package org.neo4j.cypher.internal.compiler.v3_0

import java.net.URL

import org.neo4j.cypher.GraphDatabaseFunSuite
import org.neo4j.cypher.internal.compiler.v3_0.commands.expressions.{KernelPredicate, Expander, Literal}
import org.neo4j.cypher.internal.compiler.v3_0.commands.values.{KeyToken, TokenType}
import org.neo4j.cypher.internal.compiler.v3_0.commands.{LabelAction, LabelSetOp}
import org.neo4j.cypher.internal.compiler.v3_0.pipes.matching.PatternNode
import org.neo4j.cypher.internal.compiler.v3_0.spi.{ProcedureName, IdempotentResult, ProcedureSignature, QueryContext}
import org.neo4j.cypher.internal.frontend.v3_0.SemanticDirection
import org.neo4j.graphdb.{PropertyContainer, Node, Path, Relationship}
import org.neo4j.kernel.api.constraints.{NodePropertyExistenceConstraint, UniquenessConstraint}
import org.neo4j.kernel.api.index.IndexDescriptor

import scala.collection.Iterator

class LabelActionTest extends GraphDatabaseFunSuite {
  val queryContext = new SnitchingQueryContext
  val state = QueryStateHelper.newWith(query = queryContext)
  val ctx = ExecutionContext()

  test("set single label on node") {
    //GIVEN
    val n = createNode()
    val given = LabelAction(Literal(n), LabelSetOp, Seq(KeyToken.Resolved("green", 12, TokenType.Label)))

    //WHEN
    val result = given.exec(ctx, state)

    //THEN
    queryContext.node should equal(n.getId)
    queryContext.ids should equal(Seq(12))
    result.toList should equal(List(ctx))
  }

  test("set two labels on node") {
    //GIVEN
    val n = createNode()
    val given = LabelAction(Literal(n), LabelSetOp, Seq(KeyToken.Resolved("green", 12, TokenType.Label),
      KeyToken.Resolved("blue", 42, TokenType.Label)))

    //WHEN
    val result = given.exec(ctx, state)

    //THEN
    queryContext.node should equal(n.getId)
    queryContext.ids should equal(Seq(12, 42))
    result.toList should equal(List(ctx))
  }
}

class SnitchingQueryContext extends QueryContext {

  var node: Long = -666
  var ids: Seq[Int] = null

  var highLabelId: Int = 0
  var labels: Map[String, Int] = Map("green" -> 12, "blue" -> 42)

  override def setLabelsOnNode(n: Long, input: Iterator[Int]): Int = {
    node = n
    ids = input.toSeq
    ids.size
  }

  override def isOpen: Boolean = ???

  override def isTopLevelTx: Boolean = ???

  override def getOrCreateRelTypeId(relTypeName: String) = ???

  override def getOrCreateLabelId(labelName: String) = labels(labelName)

  override def close(success: Boolean) {???}

  override def createNode() = ???

  override def createRelationship(start: Node, end: Node, relType: String) = ???

  override def getLabelName(id: Int) = ???

  override def getLabelsForNode(node: Long) = ???

  override def nodeOps = ???

  override def relationshipOps = ???

  override def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int = {???}

  override def getPropertiesForNode(node: Long) = ???

  override def getPropertiesForRelationship(relId: Long) = ???

  override def getOrCreatePropertyKeyId(propertyKey: String) = ???

  override def getOptPropertyKeyId(propertyKey: String): Option[Int] = ???

  override def getPropertyKeyId(propertyKey: String) = ???

  override def addIndexRule(labelId: Int, propertyKeyId: Int): IdempotentResult[IndexDescriptor] = ???

  override def dropIndexRule(labelId: Int, propertyKeyId: Int) = ???

  override def indexSeek(index: IndexDescriptor, value: Any): Iterator[Node] = ???

  override def indexSeekByRange(index: IndexDescriptor, value: Any): Iterator[Node] = ???

  override def indexScan(index: IndexDescriptor): Iterator[Node] = ???

  override def lockingUniqueIndexSeek(index: IndexDescriptor, value: Any): Option[Node] = ???

  override def getNodesByLabel(id: Int): Iterator[Node] = ???

  override def getOrCreateFromSchemaState[K, V](key: K, creator: => V): V = ???

  override def getOptLabelId(labelName: String): Option[Int] = labels.get(labelName)

  override def createUniqueConstraint(labelId: Int, propertyKeyId: Int): IdempotentResult[UniquenessConstraint] = ???

  override def dropUniqueConstraint(labelId: Int, propertyKeyId: Int) = ???

  override def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int): IdempotentResult[NodePropertyExistenceConstraint] = ???

  override def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int) = ???

  override def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int) = ???

  override def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int) = ???

  override def getLabelId(labelName: String): Int = ???

  override def getPropertyKeyName(id: Int): String = ???

  override def withAnyOpenQueryContext[T](work: (QueryContext) => T): T = ???

  override def commitAndRestartTx() { ??? }

  override def getRelTypeId(relType: String): Int = ???

  override def getOptRelTypeId(relType: String): Option[Int] = ???

  override def getRelTypeName(id: Int): String = ???

  override def relationshipStartNode(rel: Relationship) = ???

  override def relationshipEndNode(rel: Relationship) = ???

  override def getRelationshipsForIds(node: Node, dir: SemanticDirection, types: Option[Seq[Int]]): Iterator[Relationship] = ???

  override def nodeGetDegree(node: Long, dir: SemanticDirection): Int = ???

  override def nodeGetDegree(node: Long, dir: SemanticDirection, relTypeId: Int): Int = ???

  override def nodeIsDense(node: Long): Boolean = ???

  // Legacy dependency between kernel and compiler
  override def variableLengthPathExpand(node: PatternNode, realNode: Node, minHops: Option[Int], maxHops: Option[Int], direction: SemanticDirection, relTypes: Seq[String]): Iterator[Path] = ???

  override def getImportURL(url: URL): Either[String,URL] = ???

  override def createRelationship(start: Long, end: Long, relType: Int) = ???

  override def isLabelSetOnNode(label: Int, node: Long): Boolean = ???

  override def nodeCountByCountStore(labelId: Int): Long = ???

  override def relationshipCountByCountStore(startLabelId: Int, typeId: Int, endLabelId: Int): Long = ???

  override def lockNodes(nodeIds: Long*): Unit = ???

  override def lockRelationships(relIds: Long*): Unit = ???

  override type KernelStatement = this.type

  override def statement: KernelStatement = ???

  override type EntityAccessor = this.type

  override def entityAccessor: EntityAccessor = ???

  override def singleShortestPath(left: Node, right: Node, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path], filters: Seq[KernelPredicate[PropertyContainer]]): Option[Path] = ???

  override def allShortestPath(left: Node, right: Node, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path], filters: Seq[KernelPredicate[PropertyContainer]]): Iterator[Path] = ???

  override def callReadOnlyProcedure(name: ProcedureName, args: Seq[Any]): Iterator[Array[AnyRef]] = ???

  override def indexSeekByContains(index: IndexDescriptor, value: String): scala.Iterator[Node] = ???

  override def callReadWriteProcedure(name: ProcedureName, args: Seq[Any]): Iterator[Array[AnyRef]] = ???
}
