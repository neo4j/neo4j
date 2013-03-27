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
package org.neo4j.cypher.internal.pipes

import org.junit.{Before, Test}
import org.neo4j.cypher.internal.spi.PlanContext
import org.mockito.Mockito._
import org.neo4j.cypher.internal.commands._
import org.neo4j.cypher.internal.commands.expressions.Literal
import org.scalatest.mock.MockitoSugar
import org.neo4j.cypher.internal.executionplan.builders.EntityProducerFactory
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers._
import org.neo4j.cypher.PlanDescription
import org.neo4j.cypher.internal.commands.SchemaIndex
import scala.Some
import org.neo4j.cypher.internal.commands.NodeByIndex


class StartPipePlanDescriptionTest extends MockitoSugar {

  var planContext: PlanContext = null
  var factory: EntityProducerFactory = null
  val label: String = "label"
  val prop: String = "prop"
  val value = 42

  @Before
  def init() {
    planContext = mock[PlanContext]
    factory = new EntityProducerFactory
    when(planContext.getIndexRuleId(label, prop)).thenReturn(Some(1L))
    when(planContext.getLabelId(label)).thenReturn(Some(1L))
  }

  @Test
  def schema_index() {
    //GIVEN
    val planDescription = createPlanDescription(SchemaIndex("n", label, prop, Some(Literal(value))))

    //WHEN
    val result = planDescription.toString

    //THEN
    assertThat(result, containsString(label))
    assertThat(result, containsString("SchemaIndex"))
    assertThat(result, containsString(prop))
    assertThat(result, containsString(value.toString))
  }

  @Test
  def node_by_id() {
    //GIVEN
    val planDescription = createPlanDescription(NodeById("n", 0, 1))

    //WHEN
    val result = planDescription.toString

    //THEN
    assertThat(result, containsString("NodeById"))
    assertThat(result, containsString("0"))
    assertThat(result, containsString("1"))
  }

  @Test
  def node_by_index() {
    //GIVEN
    val planDescription = createPlanDescription(NodeByIndex("n", "indexName", Literal("key"), Literal("value")))

    //WHEN
    val result = planDescription.toString

    //THEN
    assertThat(result, containsString("NodeByIndex"))
    assertThat(result, containsString("indexName"))
    assertThat(result, containsString("key"))
    assertThat(result, containsString("value"))
  }

  @Test
  def node_by_index_query() {
    //GIVEN
    val planDescription = createPlanDescription(NodeByIndexQuery("n", "indexName", Literal("awesomeIndexQuery")))

    //WHEN
    val result = planDescription.toString

    //THEN
    assertThat(result, containsString("NodeByIndex"))
    assertThat(result, containsString("indexName"))
    assertThat(result, containsString("awesomeIndexQuery"))
  }

  @Test
  def node_by_label() {
    //GIVEN
    val planDescription = createPlanDescription(NodeByLabel("n", label))

    //WHEN
    val result = planDescription.toString

    //THEN
    assertThat(result, containsString("NodeByLabel"))
    assertThat(result, containsString(label))
  }

  private def createPlanDescription(startItem: StartItem): PlanDescription = {
    val producer = factory.nodeStartItems((planContext, startItem))
    val pipe = new NodeStartPipe(NullPipe, "n", producer)
    pipe.executionPlanDescription
  }
}