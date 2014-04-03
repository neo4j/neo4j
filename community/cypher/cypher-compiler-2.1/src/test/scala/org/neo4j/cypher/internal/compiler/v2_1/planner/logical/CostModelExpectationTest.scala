/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.compiler.v2_1.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Selection
import org.neo4j.cypher.internal.compiler.v2_1.ast.Equals
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.compiler.v2_1.ast.Identifier
import org.neo4j.cypher.internal.compiler.v2_1.ast.PropertyKeyName
import org.neo4j.cypher.internal.compiler.v2_1.ast.StringLiteral
import org.neo4j.cypher.internal.compiler.v2_1.ast.Property
import org.neo4j.graphdb.Direction
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_1.RelTypeId

class CostModelExpectationTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("select(label scan) < select(all nodes scan)") {
    val statistics = newMockedStatistics
    when(statistics.nodesCardinality).thenReturn(1000)
    val cost = newMetricsFactory.newMetrics(statistics).cost

    val cost1 = cost(
      Selection(
        Seq(Equals(Property(Identifier("a")_, PropertyKeyName("name")()_)_, StringLiteral("Andres")_)_),
        NodeByLabelScan("a", Left("Label"))()
      )
    )

    val cost2 = cost(
      Selection(
        Seq(
          Equals(Property(Identifier("a")_, PropertyKeyName("name")()_)_, StringLiteral("Andres")_)_,
          HasLabels(Identifier("a")_, Seq(LabelName("Label")()_))_
        ),
        AllNodesScan("a")
      )
    )

    cost1 should be < cost2
  }

  test("label scan < select(all nodes scan)") {
    val statistics = newMockedStatistics
    when(statistics.nodesCardinality).thenReturn(1000)
    val cost = newMetricsFactory.newMetrics(statistics).cost

    val cost1 = cost(
      NodeByLabelScan("a", Left("Label"))()
    )

    val cost2 = cost(
      Selection(
        Seq(
          Equals(Property(Identifier("b")_, PropertyKeyName("name")()_)_, StringLiteral("Andres")_)_,
          Equals(Property(Identifier("b")_, PropertyKeyName("age")()_)_, SignedIntegerLiteral("12")_)_
        ),
        AllNodesScan("b")
      )
    )

    cost1 should be < cost2
  }

  test("all node scan < select(all nodes scan)") {
    val statistics = newMockedStatistics
    when(statistics.nodesCardinality).thenReturn(1000)
    val cost = newMetricsFactory.newMetrics(statistics).cost

    val cost1 = cost(
      AllNodesScan("a")
    )

    val cost2 = cost(
      Selection(
        Seq(
          Equals(Property(Identifier("b")_, PropertyKeyName("name")()_)_, StringLiteral("Andres")_)_,
          Equals(Property(Identifier("b")_, PropertyKeyName("age")()_)_, SignedIntegerLiteral("12")_)_
        ),
        AllNodesScan("b")
      )
    )


    cost1 should be < cost2
  }

  test("expand(select(all nodes scan)) < expand(all node scan)") {
    val statistics = newMockedStatistics
    when(statistics.nodesCardinality).thenReturn(1000.0)
    val cost = newMetricsFactory.newMetrics(statistics).cost

    val cost1 = cost(
      Expand(
        Selection(
          Seq(
            Equals(Property(Identifier("b")_, PropertyKeyName("name")()_)_, StringLiteral("Andres")_)_,
            Equals(Property(Identifier("b")_, PropertyKeyName("age")()_)_, SignedIntegerLiteral("12")_)_
          ),
          AllNodesScan("b")
        ),
        "b", Direction.OUTGOING, Seq.empty, "c", "r1"
      )
    )

    val cost2 = cost(
      Expand(
        AllNodesScan("a"),
        "a", Direction.OUTGOING, Seq.empty, "d", "r2"
      )
    )

    cost1 should be < cost2
  }

  test("expand(select(all nodes scan)) < select(expand(all node scan))") {
    val statistics = newMockedStatistics
    when(statistics.nodesCardinality).thenReturn(1000)
    when(statistics.degreeByLabelTypeAndDirection(RelTypeId(12), Direction.BOTH)).thenReturn(2.1)
    val cost = newMetricsFactory.newMetrics(statistics).cost

    val cost1 = cost(
      Expand(
        Selection(
          Seq(Equals(Property(Identifier("a") _, PropertyKeyName("name")() _) _, StringLiteral("Andres") _) _),
          AllNodesScan("a")
        ),
        "a", Direction.BOTH, Seq(RelTypeName("x")(Some(RelTypeId(12)))_), "start", "rel"
      )
    )

    val cost2 = cost(
      Selection(
        Seq(Equals(Property(Identifier("a")_, PropertyKeyName("name")()_)_, StringLiteral("Andres")_)_),
        Expand(
          AllNodesScan("start"),
          "start", Direction.BOTH, Seq(RelTypeName("x")(Some(RelTypeId(12))) _), "a", "rel"
        )
      )
    )

    cost1 should be < cost2
  }
}
