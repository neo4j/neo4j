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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_3.commands._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Literal
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v2_3.spi.SchemaTypes.IndexDescriptor
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class StartPipePlanDescriptionTest extends CypherFunSuite {

  private implicit val monitor = mock[PipeMonitor]
  private var planContext: PlanContext = null
  private var factory: EntityProducerFactory = null
  private val label: String = "label"
  private val prop: String = "prop"
  private val v42 = 42

  override def beforeEach() {
    super.beforeEach()
    planContext = mock[PlanContext]
    factory = new EntityProducerFactory
    when(planContext.getIndexRule(label, prop)).thenReturn(Some(IndexDescriptor(123,456)))
    when(planContext.getOptLabelId(label)).thenReturn(Some(1))
  }

  test("schema_index") {
    //GIVEN
    val planDescription = createPlanDescription(SchemaIndex("n", label, prop, AnyIndex, Some(SingleQueryExpression(Literal(v42)))))

    //WHEN
    val result = planDescription.toString

    //THEN
    result should include(label)
    result should include("SchemaIndex")
    result should include(prop)
    result should include(v42.toString)
  }

  test("node_by_id") {
    //GIVEN
    val planDescription = createPlanDescription(NodeById("n", 0, 1))

    //WHEN
    val result = planDescription.toString

    //THEN
    result should include("NodeById")
    result should include("0")
    result should include("1")
  }

  test("node_by_index") {
    //GIVEN
    val planDescription = createPlanDescription(NodeByIndex("n", "indexName", Literal("key"), Literal("value")))

    //WHEN
    val result = planDescription.toString

    //THEN
    result should include("NodeByIndex")
    result should include("indexName")
    result should include("key")
    result should include("value")
  }

  test("node_by_index_query") {
    //GIVEN
    val planDescription = createPlanDescription(NodeByIndexQuery("n", "indexName", Literal("awesomeIndexQuery")))

    //WHEN
    val result = planDescription.toString

    //THEN
    result should include("NodeByIndex")
    result should include("indexName")
    result should include("awesomeIndexQuery")
  }

  test("node_by_label") {
    //GIVEN
    val planDescription = createPlanDescription(NodeByLabel("n", label))

    //WHEN
    val result = planDescription.toString

    //THEN
    result should include("NodeByLabel")
    result should include(label)
  }

  private def createPlanDescription(startItem: StartItem): InternalPlanDescription = {
    val producer = factory.readNodeStartItems((planContext, startItem))
    val pipe = new NodeStartPipe(SingleRowPipe(), "n", producer)()
    pipe.planDescription
  }
}
