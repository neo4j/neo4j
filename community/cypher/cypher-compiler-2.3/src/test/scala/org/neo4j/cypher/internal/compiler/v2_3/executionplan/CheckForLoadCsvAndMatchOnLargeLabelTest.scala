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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan

import org.mockito.Mockito._
import org.mockito.Matchers._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compiler.v2_3.commands.NodeByLabel
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Literal
import org.neo4j.cypher.internal.compiler.v2_3.pipes.{LazyLabel, NodeByLabelScanPipe, SingleRowPipe, NodeByLabelEntityProducer, NodeStartPipe, PipeMonitor, HasHeaders, AllNodesScanPipe, LoadCSVPipe, EagerPipe}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Cardinality
import org.neo4j.cypher.internal.compiler.v2_3.spi.{GraphStatistics, PlanContext}
import org.neo4j.cypher.internal.frontend.v2_3.LabelId
import org.neo4j.cypher.internal.frontend.v2_3.notification.LargeLabelWithLoadCsvNotification
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
class CheckForLoadCsvAndMatchOnLargeLabelTest extends CypherFunSuite {
  private val THRESHOLD = 100
  private val labelOverThreshold = "A"
  private val labelUnderThrehsold = "B"
  private val indexFor= Map(labelOverThreshold -> 1, labelUnderThrehsold -> 2)
  private implicit val monitor = mock[PipeMonitor]
  private val planContext = mock[PlanContext]
  when(planContext.getOptLabelId(anyString)).thenAnswer(new Answer[Option[Int]] {
    override def answer(invocationOnMock: InvocationOnMock): Option[Int] = {
     val label = invocationOnMock.getArguments()(0).asInstanceOf[String]
      indexFor.get(label)
    }
  })
  private val statistics = mock[GraphStatistics]
  when(statistics.nodesWithLabelCardinality(Some(LabelId(1)))).thenReturn(Cardinality(101))
  when(statistics.nodesWithLabelCardinality(Some(LabelId(2)))).thenReturn(Cardinality(99))
  when(planContext.statistics).thenReturn(statistics)
  private val checker = CheckForLoadCsvAndMatchOnLargeLabel(planContext, THRESHOLD)

  test("should notify when doing LoadCsv on top of large label scan") {
    val loadCsvPipe = LoadCSVPipe(SingleRowPipe(), HasHeaders, Literal("foo"), "bar", None)
    val pipe = NodeStartPipe(loadCsvPipe, "foo",
      NodeByLabelEntityProducer(NodeByLabel("bar", labelOverThreshold), indexFor(labelOverThreshold)))()

    checker(pipe) should equal(Some(LargeLabelWithLoadCsvNotification))
  }

  test("should not notify when doing LoadCsv on top of a large label scan") {
    val loadCsvPipe = LoadCSVPipe(SingleRowPipe(), HasHeaders, Literal("foo"), "bar", None)
    val pipe = NodeStartPipe(loadCsvPipe, "foo",
      NodeByLabelEntityProducer(NodeByLabel("bar", labelUnderThrehsold), indexFor(labelUnderThrehsold)))()

    checker(pipe) should equal(None)
  }

  test("should not notify when doing LoadCsv on top of large label scan") {
    val startPipe = NodeStartPipe(SingleRowPipe(), "foo", NodeByLabelEntityProducer(NodeByLabel("bar", labelOverThreshold), indexFor(labelOverThreshold)))()
    val pipe = LoadCSVPipe(startPipe, HasHeaders, Literal("foo"), "bar", None)

    checker(pipe) should equal(None)
  }
}
