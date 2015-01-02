/**
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
package org.neo4j.cypher.internal.compiler.v2_0

import pipes.{NullDecorator, QueryState}
import org.neo4j.cypher.internal.compiler.v2_0.spi.{IdempotentResult, LockingQueryContext, QueryContext}
import org.neo4j.graphdb.{Direction, Node}
import org.junit.Test
import org.neo4j.kernel.api.constraints.UniquenessConstraint
import org.neo4j.cypher.GraphDatabaseJUnitSuite
import org.neo4j.cypher.internal.compiler.v2_0.commands.{LabelSetOp, LabelAction}
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Literal
import org.neo4j.cypher.internal.compiler.v2_0.commands.values.{TokenType, KeyToken}
import org.neo4j.kernel.api.index.IndexDescriptor
import java.net.URL

class LabelActionTest extends GraphDatabaseJUnitSuite {
  val queryContext = new SnitchingQueryContext
  val state = new QueryState(null, queryContext, Map.empty, NullDecorator)
  val ctx = ExecutionContext()

  @Test
  def set_single_label_on_node() {
    //GIVEN
    val n = createNode()
    val given = LabelAction(Literal(n), LabelSetOp, Seq(KeyToken.Resolved("green", 12, TokenType.Label)))

    //WHEN
    val result = given.exec(ctx, state)

    //THEN
    assert(queryContext.node === n.getId)
    assert(queryContext.ids === Seq(12))
    assert(result.toList === List(ctx))
  }

  @Test
  def set_two_labels_on_node() {
    //GIVEN
    val n = createNode()
    val given = LabelAction(Literal(n), LabelSetOp, Seq(KeyToken.Resolved("green", 12, TokenType.Label),
      KeyToken.Resolved("blue", 42, TokenType.Label)))

    //WHEN
    val result = given.exec(ctx, state)

    //THEN
    assert(queryContext.node === n.getId)
    assert(queryContext.ids === Seq(12, 42))
    assert(result.toList === List(ctx))
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

  def getOrCreateLabelId(labelName: String) = labels(labelName)

  def getLabelsForNode(node: Node) = Seq(12L)

  def close(success: Boolean) {???}

  def createNode() = ???

  def createRelationship(start: Node, end: Node, relType: String) = ???

  def getLabelName(id: Int) = ???

  def getLabelsForNode(node: Long) = ???

  def getRelationshipsFor(node: Node, dir: Direction, types: Seq[String]) = ???

  def nodeOps = ???

  def relationshipOps = ???

  def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int = {???}

  def getTransaction = ???

  def getOrCreatePropertyKeyId(propertyKey: String) = ???

  def getOptPropertyKeyId(propertyKey: String): Option[Int] = ???

  def getPropertyKeyId(propertyKey: String) = ???

  def addIndexRule(labelId: Int, propertyKeyId: Int): IdempotentResult[IndexDescriptor] = ???

  def dropIndexRule(labelId: Int, propertyKeyId: Int) = ???

  def exactIndexSearch(index: IndexDescriptor, value: Any): Iterator[Node] = ???

  def getNodesByLabel(id: Int): Iterator[Node] = ???

  def upgrade(context: QueryContext): LockingQueryContext = ???

  def getOrCreateFromSchemaState[K, V](key: K, creator: => V): V = ???

  def schemaStateContains(key: String) = ???

  def getOptLabelId(labelName: String): Option[Int] = labels.get(labelName)

  def createUniqueConstraint(labelId: Int, propertyKeyId: Int): IdempotentResult[UniquenessConstraint] = ???

  def dropUniqueConstraint(labelId: Int, propertyKeyId: Int) {???}

  def getLabelId(labelName: String): Int = ???

  def getPropertyKeyName(id: Int): String = ???

  def withAnyOpenQueryContext[T](work: (QueryContext) => T): T = ???

  def exactUniqueIndexSearch(index: IndexDescriptor, value: Any): Option[Node] = ???

  def getCsvIterator(url: URL): Iterator[Array[String]] = ???
}
