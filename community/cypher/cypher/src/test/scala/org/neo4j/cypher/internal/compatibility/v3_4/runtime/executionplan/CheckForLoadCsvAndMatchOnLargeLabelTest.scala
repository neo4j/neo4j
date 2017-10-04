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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.NodeByLabel
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions.Literal
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes._
import org.neo4j.cypher.internal.compiler.v3_4.spi.{GraphStatistics, PlanContext}
import org.neo4j.cypher.internal.frontend.v3_4.LabelId
import org.neo4j.cypher.internal.frontend.v3_4.notification.LargeLabelWithLoadCsvNotification
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_4.{Cardinality, HasHeaders}

class CheckForLoadCsvAndMatchOnLargeLabelTest extends CypherFunSuite {
  private val THRESHOLD = 100
  private val labelOverThreshold = "A"
  private val labelUnderThrehsold = "B"
  private val indexFor= Map(labelOverThreshold -> 1, labelUnderThrehsold -> 2)
  private val planContext = mock[PlanContext]
  when(planContext.getOptLabelId(anyString)).thenAnswer(new Answer[Option[Int]] {
    override def answer(invocationOnMock: InvocationOnMock): Option[Int] = {
      val label: String = invocationOnMock.getArgument(0)
      indexFor.get(label)
    }
  })
  private val statistics = mock[GraphStatistics]
  when(statistics.nodesWithLabelCardinality(Some(LabelId(1)))).thenReturn(Cardinality(101))
  when(statistics.nodesWithLabelCardinality(Some(LabelId(2)))).thenReturn(Cardinality(99))
  when(planContext.statistics).thenReturn(statistics)
  private val checker = CheckForLoadCsvAndMatchOnLargeLabel(planContext, THRESHOLD)

  test("should notify when doing LoadCsv on top of large label scan") {
    val loadCsvPipe = LoadCSVPipe(SingleRowPipe()(), HasHeaders, Literal("foo"), "bar", None, false)()
    val pipe = NodeStartPipe(loadCsvPipe, "foo",
      NodeByLabelEntityProducer(NodeByLabel("bar", labelOverThreshold), indexFor(labelOverThreshold)))()

    checker(pipe) should equal(Some(LargeLabelWithLoadCsvNotification))
  }

  test("should not notify when doing LoadCsv on top of a large label scan") {
    val loadCsvPipe = LoadCSVPipe(SingleRowPipe()(), HasHeaders, Literal("foo"), "bar", None, false)()
    val pipe = NodeStartPipe(loadCsvPipe, "foo",
      NodeByLabelEntityProducer(NodeByLabel("bar", labelUnderThrehsold), indexFor(labelUnderThrehsold)))()

    checker(pipe) should equal(None)
  }

  test("should not notify when doing LoadCsv on top of large label scan") {
    val startPipe = NodeStartPipe(SingleRowPipe()(), "foo", NodeByLabelEntityProducer(NodeByLabel("bar", labelOverThreshold), indexFor(labelOverThreshold)))()
    val pipe = LoadCSVPipe(startPipe, HasHeaders, Literal("foo"), "bar", None, false)()

    checker(pipe) should equal(None)
  }
}
