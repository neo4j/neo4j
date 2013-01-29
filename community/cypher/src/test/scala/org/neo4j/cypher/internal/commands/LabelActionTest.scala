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
import org.neo4j.cypher.{CypherTypeException, GraphDatabaseTestBase}
import org.scalatest.Assertions
import org.neo4j.cypher.internal.spi.QueryContext
import org.neo4j.graphdb.{Direction, Node}
import org.neo4j.cypher.internal.pipes.QueryState
import org.junit.Test
import values.{ResolvedLabel, LabelName}

class LabelActionTest extends GraphDatabaseTestBase with Assertions {
  val queryContext = new SnitchingQueryContext
  val state = new QueryState(graph, queryContext, Map.empty)
  val ctx = ExecutionContext(state = state)

  @Test
  def set_single_label_on_node() {
    //GIVEN
    val n = createNode()
    val given = LabelAction(Literal(n), LabelAdd, Literal(resolvedLabel(12, "green")))

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
    val given = LabelAction(Literal(n), LabelAdd, Literal(Seq(resolvedLabel(12, "green"), resolvedLabel(42, "blue"))))

    //WHEN
    val result = given.exec(ctx, state)

    //THEN
    assert(queryContext.node === n.getId)
    assert(queryContext.ids === Seq(12, 42))
    assert(result === Stream(ctx))
  }

  private def label(v: String) = LabelName(v)

  private def resolvedLabel(id: Long, v: String) = ResolvedLabel(id, v)

  @Test
  def set_invalid_label_set_on_node() {
    //GIVEN
    val n = createNode()
    val given = LabelAction(Literal(n), LabelAdd, Literal(Seq(label("green"), "blue")))

    intercept[CypherTypeException](given.exec(ctx, state))
  }
}

class SnitchingQueryContext extends QueryContext {

  var node: Long = -666
  var ids: Seq[Long] = null

  var highLabelId: Long = 0
  var labels: Map[String, Long] = Map("green" -> 12, "blue" -> 42)


  override def addLabelsToNode(n: Long, input: Iterable[Long]): Int = {
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

  def replaceLabelsOfNode(node: Long, labelIds: Iterable[Long]) {???}

  def getTransaction = ???
}