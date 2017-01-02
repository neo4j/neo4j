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
package org.neo4j.cypher.internal.compiler.v3_1

import org.neo4j.cypher.GraphDatabaseFunSuite
import org.neo4j.cypher.internal.compiler.v3_1.commands.expressions.Literal
import org.neo4j.cypher.internal.compiler.v3_1.commands.values.{KeyToken, TokenType}
import org.neo4j.cypher.internal.compiler.v3_1.commands.{LabelAction, LabelSetOp}
import org.neo4j.cypher.internal.compiler.v3_1.spi.{QueryContext, _}

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

class SnitchingQueryContext extends QueryContext with QueryContextAdaptation {

  var node: Long = -666
  var ids: Seq[Int] = null

  var highLabelId: Int = 0
  var labels: Map[String, Int] = Map("green" -> 12, "blue" -> 42)

  override def setLabelsOnNode(n: Long, input: Iterator[Int]): Int = {
    node = n
    ids = input.toSeq
    ids.size
  }

  override def getOrCreateLabelId(labelName: String) = labels(labelName)

  override def getOptLabelId(labelName: String): Option[Int] = labels.get(labelName)
}
