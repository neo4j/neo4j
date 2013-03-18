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
import org.neo4j.cypher.internal.commands.SchemaIndex
import org.neo4j.cypher.IndexHintException
import org.scalatest.Assertions
import org.neo4j.cypher.internal.commands.expressions.Literal
import org.neo4j.cypher.internal.pipes.{QueryStateHelper, QueryState}


class EntityProducerFactoryTest extends MockitoSugar with Assertions {
  var planContext: PlanContext = null
  var factory: EntityProducerFactory = null

  @Before
  def init() {
    planContext = mock[PlanContext]
    factory = new EntityProducerFactory(planContext)
  }

  @Test
  def throws_error_when_index_is_missing() {
    //GIVEN
    val label: String = "label"
    val prop: String = "prop"
    when(planContext.getIndexRuleId(label, prop)).thenReturn(None)

    //WHEN
    intercept[IndexHintException](factory.nodeByIndexHint(SchemaIndex("id", label, prop, None)))
  }

  @Test
  def calls_the_right_methods() {
    //GIVEN
    val label: String = "label"
    val prop: String = "prop"
    val indexId: Long = 1L
    val value = 42
    val queryContext: QueryContext = mock[QueryContext]
    when(planContext.getIndexRuleId(label, prop)).thenReturn(Some(indexId))
    val indexResult = Iterator(null)
    when(queryContext.exactIndexSearch(indexId, value)).thenReturn(indexResult)
    val state = QueryStateHelper.empty.copy(inner = queryContext)

    //WHEN
    val func = factory.nodeByIndexHint(SchemaIndex("id", label, prop, Some(Literal(value))))
    assert(func(null, state) === indexResult)
  }
}
