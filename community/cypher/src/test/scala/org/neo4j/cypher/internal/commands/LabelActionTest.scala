/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.commands

import expressions.Literal
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.GraphDatabaseTestBase
import org.scalatest.Assertions
import org.neo4j.cypher.internal.spi.{LockingQueryContext, QueryContext}
import org.neo4j.graphdb.{Direction, Node}
import org.neo4j.cypher.internal.pipes.{NullDecorator, QueryState}
import org.junit.Test
import values.ResolvedLabel

class LabelActionTest extends GraphDatabaseTestBase with Assertions {
  val queryContext = new SnitchingQueryContext
  val state = new QueryState(graph, queryContext, Map.empty, NullDecorator)
  val ctx = ExecutionContext()

  @Test
  def set_single_label_on_node() {
    //GIVEN
    val n = createNode()
    val given = LabelAction(Literal(n), LabelSetOp, Seq(ResolvedLabel("green", 12)))

    //WHEN
    val result = given.exec(ctx, state)

    //THEN
    assert(queryContext.node === n.getId)
    assert(queryContext.ids === Seq(12))
    assert(result === Stream(ctx))
  }

  @Test
  def set_two_labels_on_node() {
    //GIVEN
    val n = createNode()
    val given = LabelAction(Literal(n), LabelSetOp, Seq(ResolvedLabel("green", 12), ResolvedLabel("blue", 42)))

    //WHEN
    val result = given.exec(ctx, state)

    //THEN
    assert(queryContext.node === n.getId)
    assert(queryContext.ids === Seq(12, 42))
    assert(result === Stream(ctx))
  }
}

class SnitchingQueryContext extends QueryContext {

  var node: Long = -666
  var ids: Seq[Long] = null

  var highLabelId: Long = 0
  var labels: Map[String, Long] = Map("green" -> 12, "blue" -> 42)


  override def setLabelsOnNode(n: Long, input: Iterable[Long]): Int = {
    node = n
    ids = input.toSeq
    ids.size
  }

  def getOrCreateLabelId(labelName: String) = labels(labelName)

  def getLabelsForNode(node: Node) = Seq(12L)

  def close(success: Boolean) {???}

  def createNode() = ???

  def createRelationship(start: Node, end: Node, relType: String) = ???

  def getLabelName(id: Long) = ???

  def getLabelsForNode(node: Long) = ???

  def getRelationshipsFor(node: Node, dir: Direction, types: Seq[String]) = ???

  def nodeOps = ???

  def relationshipOps = ???

  def removeLabelsFromNode(node: Long, labelIds: Iterable[Long]): Int = {???}

  def getTransaction = ???

  def getOrCreatePropertyKeyId(propertyKey: String) = ???

  def getPropertyKeyId(propertyKey: String) = ???

  def addIndexRule(labelIds: Long, propertyKeyId: Long) = ???

  def dropIndexRule(labelIds: Long, propertyKeyId: Long) = ???

  def exactIndexSearch(id: Long, value: Any): Iterator[Node] = ???

  def getNodesByLabel(id: Long): Iterator[Node] = ???

  def upgrade(context: QueryContext): LockingQueryContext = ???

  def getOrCreateFromSchemaState[K, V](key: K, creator: => V): V = ???

  def schemaStateContains(key: String) = ???
}