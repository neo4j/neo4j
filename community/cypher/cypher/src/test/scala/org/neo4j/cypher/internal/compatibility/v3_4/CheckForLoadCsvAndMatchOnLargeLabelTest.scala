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
package org.neo4j.cypher.internal.compatibility.v3_5

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.frontend.v3_5.notification.LargeLabelWithLoadCsvNotification
import org.neo4j.cypher.internal.ir.v3_5.HasHeaders
import org.neo4j.cypher.internal.planner.v3_5.spi.{GraphStatistics, PlanContext}
import org.neo4j.cypher.internal.util.v3_5.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.v3_5.{Cardinality, LabelId}
import org.neo4j.cypher.internal.v3_5.expressions.{LabelName, StringLiteral}
import org.neo4j.cypher.internal.v3_5.logical.plans._

class CheckForLoadCsvAndMatchOnLargeLabelTest
    extends CypherFunSuite
    with LogicalPlanningTestSupport {

  private val url = StringLiteral("file:///tmp/foo.csv")(pos)

  private val THRESHOLD = 100
  private val labelOverThreshold = "A"
  private val labelUnderThreshold = "B"
  private val indexFor = Map(labelOverThreshold -> 1, labelUnderThreshold -> 2)
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
    val loadCsv =
      LoadCSV(
        Argument(),
        url,
        "foo",
        HasHeaders,
        None,
        legacyCsvQuoteEscaping = false
      )

    val plan = CartesianProduct(
      loadCsv,
      NodeByLabelScan("bar", LabelName(labelOverThreshold)(pos), Set.empty)
    )

    checker(plan) should equal(Some(LargeLabelWithLoadCsvNotification))
  }

  test("should not notify when doing LoadCsv on top of a small label scan") {
    val loadCsv =
      LoadCSV(
        Argument(),
        url,
        "foo",
        HasHeaders,
        None,
        legacyCsvQuoteEscaping = false
      )

    val plan =
      CartesianProduct(
        loadCsv,
        NodeByLabelScan("bar", LabelName(labelUnderThreshold)(pos), Set.empty)
      )

    checker(plan) should equal(None)
  }

  test("should not notify when doing large label scan on top of LoadCSV") {
    val start = NodeByLabelScan("bar", LabelName(labelOverThreshold)(pos), Set.empty)
    val plan =
      LoadCSV(start, url, "foo", HasHeaders, None, legacyCsvQuoteEscaping = false)

    checker(plan) should equal(None)
  }
}
