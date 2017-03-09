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
package org.neo4j.cypher.internal.compiler.v3_2.executionplan.builders

import java.util

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v3_2.commands._
import org.neo4j.cypher.internal.compiler.v3_2.commands.expressions.Literal
import org.neo4j.cypher.internal.compiler.v3_2.pipes.QueryStateHelper
import org.neo4j.cypher.internal.compiler.v3_2.spi.{PlanContext, QueryContext, QueryContextAdaptation}
import org.neo4j.cypher.internal.compiler.v3_2.{ExecutionContext, IndexDescriptor}
import org.neo4j.cypher.internal.frontend.v3_2.IndexHintException
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Node

class EntityProducerFactoryTest extends CypherFunSuite {
  var planContext: PlanContext = null
  var factory: EntityProducerFactory = null
  val context = ExecutionContext.empty

  override def beforeEach() {
    super.beforeEach()
    planContext = mock[PlanContext]
    factory = new EntityProducerFactory
  }

  test("throws error when index is missing") {
    //GIVEN
    val label: String = "label"
    val prop: String = "prop"
    when(planContext.indexGet(label, Seq(prop))).thenReturn(None)

    //WHEN
    intercept[IndexHintException](factory.nodeByIndexHint(readOnly = true)(planContext -> SchemaIndex("id", label, Seq(prop), AnyIndex, None)))
  }

  test("calls the right methods") {
    //GIVEN
    val label: String = "label"
    val prop: String = "prop"
    val index: IndexDescriptor = IndexDescriptor(123, 456)
    val value = 42
    val queryContext: QueryContext = mock[QueryContext]
    when(planContext.indexGet(label, Seq(prop))).thenReturn(Some(index))
    val indexResult = Iterator(null)
    when(queryContext.indexSeek(index, Seq(value))).thenReturn(indexResult)
    val state = QueryStateHelper.emptyWith(query = queryContext)

    //WHEN
    val func = factory.nodeByIndexHint(readOnly = true)(planContext -> SchemaIndex("id", label, Seq(prop), AnyIndex, Some(SingleQueryExpression(Literal(value)))))
    func(context, state) should equal(indexResult)
  }

  test("should translate values to neo4j") {
    //GIVEN
    val labelName = "Label"
    val propertyKey = "prop"
    val index: IndexDescriptor = IndexDescriptor(123, 456)
    when(planContext.indexGet(labelName, Seq(propertyKey))).thenReturn(Some(index))
    val producer = factory.nodeByIndexHint(readOnly = true)(planContext -> SchemaIndex("x", labelName, Seq(propertyKey), AnyIndex, Some(SingleQueryExpression(Literal(Seq(1,2,3))))))


    var seenValues: Seq[Any] = null

    val queryContext: QueryContext = new QueryContext with QueryContextAdaptation {
      override def indexSeek(index: IndexDescriptor, values: Seq[Any]): Iterator[Node] = {
        seenValues = values
        Iterator.empty
      }
    }
    val state = QueryStateHelper.emptyWith(query = queryContext)

    //WHEN
    producer.apply(context, state)

    //THEN
    util.Arrays.equals(seenValues.head.asInstanceOf[Array[Int]], Array(1,2,3))
  }
}
