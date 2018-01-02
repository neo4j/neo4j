/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3

import java.net.URL

import org.neo4j.cypher.GraphDatabaseFunSuite
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Expander, KernelPredicate, Literal}
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.{KeyToken, TokenType}
import org.neo4j.cypher.internal.compiler.v2_3.commands.{LabelAction, LabelSetOp}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.matching.PatternNode
import org.neo4j.cypher.internal.compiler.v2_3.spi.SchemaTypes.{IndexDescriptor, NodePropertyExistenceConstraint, UniquenessConstraint}
import org.neo4j.cypher.internal.compiler.v2_3.spi.{IdempotentResult, LockingQueryContext, QueryContext}
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.graphdb.{Node, Path, PropertyContainer, Relationship}

class LabelActionTest extends GraphDatabaseFunSuite {
  val queryContext = new SnitchingQueryContext
  val state = QueryStateHelper.emptyWith(query = queryContext)
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

  def isOpen: Boolean = ???

  def isTopLevelTx: Boolean = ???

  def getOrCreateRelTypeId(relTypeName: String) = ???

  def getOrCreateLabelId(labelName: String) = labels(labelName)

  def getLabelsForNode(node: Node) = Seq(12L)

  def close(success: Boolean) {???}

  def createNode() = ???

  def createRelationship(start: Node, end: Node, relType: String) = ???

  def getLabelName(id: Int) = ???

  def getLabelsForNode(node: Long) = ???

  def getRelationshipsFor(node: Node, dir: SemanticDirection, types: Seq[String]) = ???

  def nodeOps = ???

  def relationshipOps = ???

  def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int = {???}

  def getTransaction = ???

  def getPropertiesForNode(node: Long) = ???

  def getPropertiesForRelationship(relId: Long) = ???

  def getOrCreatePropertyKeyId(propertyKey: String) = ???

  def getOptPropertyKeyId(propertyKey: String): Option[Int] = ???

  def getPropertyKeyId(propertyKey: String) = ???

  def addIndexRule(labelId: Int, propertyKeyId: Int): IdempotentResult[IndexDescriptor] = ???

  def dropIndexRule(labelId: Int, propertyKeyId: Int) = ???

  def indexSeek(index: IndexDescriptor, value: Any): Iterator[Node] = ???

  def indexSeekByRange(index: IndexDescriptor, value: Any): Iterator[Node] = ???

  def indexScan(index: IndexDescriptor): Iterator[Node] = ???

  def getNodesByLabel(id: Int): Iterator[Node] = ???

  def upgrade(context: QueryContext): LockingQueryContext = ???

  def getOrCreateFromSchemaState[K, V](key: K, creator: => V): V = ???

  def schemaStateContains(key: String) = ???

  def getOptLabelId(labelName: String): Option[Int] = labels.get(labelName)

  def createUniqueConstraint(labelId: Int, propertyKeyId: Int): IdempotentResult[UniquenessConstraint] = ???

  def dropUniqueConstraint(labelId: Int, propertyKeyId: Int) = ???

  def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int): IdempotentResult[NodePropertyExistenceConstraint] = ???

  def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int) = ???

  def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int) = ???

  def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int) = ???

  def getLabelId(labelName: String): Int = ???

  def getPropertyKeyName(id: Int): String = ???

  def withAnyOpenQueryContext[T](work: (QueryContext) => T): T = ???

  def lockingExactUniqueIndexSearch(index: IndexDescriptor, value: Any): Option[Node] = ???

  def commitAndRestartTx() { ??? }

  def getRelTypeId(relType: String): Int = ???

  def getOptRelTypeId(relType: String): Option[Int] = ???

  def getRelTypeName(id: Int): String = ???

  def relationshipStartNode(rel: Relationship) = ???

  def relationshipEndNode(rel: Relationship) = ???

  def getRelationshipsForIds(node: Node, dir: SemanticDirection, types: Option[Seq[Int]]): Iterator[Relationship] = ???

  def nodeGetDegree(node: Long, dir: SemanticDirection): Int = ???

  def nodeGetDegree(node: Long, dir: SemanticDirection, relTypeId: Int): Int = ???

  def nodeIsDense(node: Long): Boolean = ???

  // Legacy dependency between kernel and compiler
  override def variableLengthPathExpand(node: PatternNode, realNode: Node, minHops: Option[Int], maxHops: Option[Int], direction: SemanticDirection, relTypes: Seq[String]): Iterator[Path] = ???

  def getImportURL(url: URL): Either[String,URL] = ???
  override def createRelationship(start: Long, end: Long, relType: Int) = ???

  override def isLabelSetOnNode(label: Int, node: Long): Boolean = ???

  override def singleShortestPath(left: Node, right: Node, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path], filters: Seq[KernelPredicate[PropertyContainer]]): Option[Path] = ???

  override def allShortestPath(left: Node, right: Node, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path], filters: Seq[KernelPredicate[PropertyContainer]]): Iterator[Path] = ???

  override def detachDeleteNode(node: Node): Int = ???
}
