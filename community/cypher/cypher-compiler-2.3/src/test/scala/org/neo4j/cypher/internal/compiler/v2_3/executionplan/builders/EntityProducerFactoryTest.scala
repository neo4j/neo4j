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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.commands._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Literal
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryStateHelper
import org.neo4j.cypher.internal.compiler.v2_3.spi.SchemaTypes.IndexDescriptor
import org.neo4j.cypher.internal.compiler.v2_3.spi.{PlanContext, QueryContext}
import org.neo4j.cypher.internal.frontend.v2_3.IndexHintException
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class EntityProducerFactoryTest extends CypherFunSuite {
  var planContext: PlanContext = null
  var factory: EntityProducerFactory = null
  val context = ExecutionContext.empty

  override def beforeEach() {
    super.beforeEach()
    planContext = mock[PlanContext]
    factory = new EntityProducerFactory
  }

  test("throws_error_when_index_is_missing") {
    //GIVEN
    val label: String = "label"
    val prop: String = "prop"
    when(planContext.getIndexRule(label, prop)).thenReturn(None)

    //WHEN
    intercept[IndexHintException](factory.nodeByIndexHint(readOnly = true)(planContext -> SchemaIndex("id", label, prop, AnyIndex, None)))
  }

  test("calls_the_right_methods") {
    //GIVEN
    val label: String = "label"
    val prop: String = "prop"
    val index: IndexDescriptor = IndexDescriptor(123,456)
    val value = 42
    val queryContext: QueryContext = mock[QueryContext]
    when(planContext.getIndexRule(label, prop)).thenReturn(Some(index))
    val indexResult = Iterator(null)
    when(queryContext.indexSeek(index, value)).thenReturn(indexResult)
    val state = QueryStateHelper.emptyWith(query = queryContext)

    //WHEN
    val func = factory.nodeByIndexHint(readOnly = true)(planContext -> SchemaIndex("id", label, prop, AnyIndex, Some(SingleQueryExpression(Literal(value)))))
    func(context, state) should equal(indexResult)
  }

  test("retries_every_time_if_the_label_did_not_exist_at_plan_building") {
    // given
    val label: String = "label"
    val queryContext: QueryContext = mock[QueryContext]
    when(planContext.getOptLabelId(label)).thenReturn(None)
    when(queryContext.getOptLabelId(label)).thenReturn(None)
    val state = QueryStateHelper.emptyWith(query = queryContext)

    // when
    val func = factory.nodeByLabel(planContext -> NodeByLabel("id", label))
    func(context, state) should equal(Iterator.empty)

    // then
    verify(queryContext, times(1)).getOptLabelId(label)
  }

  test("should_translate_values_to_neo4j") {
    //GIVEN
    val labelName = "Label"
    val propertyKey = "prop"
    val index: IndexDescriptor = IndexDescriptor(123, 456)
    when(planContext.getIndexRule(labelName, propertyKey)).thenReturn(Some(index))
    val producer = factory.nodeByIndexHint(readOnly = true)(planContext -> SchemaIndex("x", labelName, propertyKey, AnyIndex, Some(SingleQueryExpression(Literal(Seq(1,2,3))))))
    val queryContext: QueryContext = mock[QueryContext]
    val state = QueryStateHelper.emptyWith(query = queryContext)
    when(queryContext.indexSeek(index, Array(1,2,3))).thenReturn(Iterator.empty)

    //WHEN
    producer.apply(context, state)

    //THEN
    verify(queryContext, times(1)).indexSeek(index, Array(1,2,3))
  }
}
