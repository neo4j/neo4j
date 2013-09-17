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
package org.neo4j.cypher.internal.executionplan.builders

import org.neo4j.cypher.internal.spi.{QueryContext, PlanContext}
import org.scalatest.mock.MockitoSugar
import org.junit.{Before, Test}
import org.mockito.Mockito._
import org.neo4j.cypher.internal.commands.{AnyIndex, NodeByLabel, SchemaIndex}
import org.neo4j.cypher.IndexHintException
import org.scalatest.Assertions
import org.neo4j.cypher.internal.commands.expressions.Literal
import org.neo4j.cypher.internal.pipes.QueryStateHelper
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.kernel.impl.api.index.IndexDescriptor

class EntityProducerFactoryTest extends MockitoSugar with Assertions {
  var planContext: PlanContext = null
  var factory: EntityProducerFactory = null
  val context = ExecutionContext.empty

  @Before
  def init() {
    planContext = mock[PlanContext]
    factory = new EntityProducerFactory
  }

  @Test
  def throws_error_when_index_is_missing() {
    //GIVEN
    val label: String = "label"
    val prop: String = "prop"
    when(planContext.getIndexRule(label, prop)).thenReturn(None)

    //WHEN
    intercept[IndexHintException](factory.nodeByIndexHint(planContext, SchemaIndex("id", label, prop, AnyIndex, None)))
  }

  @Test
  def calls_the_right_methods() {
    //GIVEN
    val label: String = "label"
    val prop: String = "prop"
    val index: IndexDescriptor = new IndexDescriptor(123,456)
    val value = 42
    val queryContext: QueryContext = mock[QueryContext]
    when(planContext.getIndexRule(label, prop)).thenReturn(Some(index))
    val indexResult = Iterator(null)
    when(queryContext.exactIndexSearch(index, value)).thenReturn(indexResult)
    val state = QueryStateHelper.empty.copy(inner = queryContext)

    //WHEN
    val func = factory.nodeByIndexHint(planContext, SchemaIndex("id", label, prop, AnyIndex, Some(Literal(value))))
    assert(func(context, state) === indexResult)
  }

  @Test
  def retries_every_time_if_the_label_did_not_exist_at_plan_building() {
    // given
    val label: String = "label"
    val queryContext: QueryContext = mock[QueryContext]
    when(planContext.getOptLabelId(label)).thenReturn(None)
    when(queryContext.getOptLabelId(label)).thenReturn(None)
    val state = QueryStateHelper.empty.copy(inner = queryContext)

    // when
    val func = factory.nodeByLabel(planContext, NodeByLabel("id", label))
    assert(func(context, state) === Iterator.empty)

    // then
    verify(queryContext, times(1)).getOptLabelId(label)
  }

  @Test
  def should_translate_values_to_neo4j() {
    //GIVEN
    val labelName = "Label"
    val propertyKey = "prop"
    val index: IndexDescriptor = new IndexDescriptor(123, 456)
    when(planContext.getIndexRule(labelName, propertyKey)).thenReturn(Some(index))
    val producer = factory.nodeByIndexHint(planContext, SchemaIndex("x", labelName, propertyKey, AnyIndex, Some(Literal(Seq(1,2,3)))))
    val queryContext: QueryContext = mock[QueryContext]
    val state = QueryStateHelper.empty.copy(inner = queryContext)

    //WHEN
    producer.apply(context, state)

    //THEN
    verify(queryContext, times(1)).exactIndexSearch(index, Array(1,2,3))
  }
}
