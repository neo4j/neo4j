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

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.mockito.Mockito._
import org.neo4j.kernel.api.index.IndexDescriptor
import org.mockito.Matchers._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v2_1.PropertyKeyId
import org.neo4j.cypher.internal.compiler.v2_1.LabelId

class LeafPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("should build plans for all nodes scans") {
    implicit val planContext = newMockedPlanContext
    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: AllNodesScan => 1
      case _               => Double.MaxValue
    })
    implicit val planner = newPlanner(factory)

    produceLogicalPlan("MATCH (n) RETURN n") should equal(
      AllNodesScan("n")
    )
  }

  test("should build plans for label scans without compile-time label id") {
    implicit val planContext = newMockedPlanContext
    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: AllNodesScan    => 1000
      case _: NodeByIdSeek    => 2
      case _: NodeByLabelScan => 1
      case _                  => Double.MaxValue
    })
    implicit val planner = newPlanner(factory)

    when(planContext.getOptLabelId("Awesome")).thenReturn(None)

    produceLogicalPlan("MATCH (n:Awesome) RETURN n") should equal(
      NodeByLabelScan("n", Left("Awesome"))
    )
  }

  test("should build plans for label scans with compile-time label id") {
    implicit val planContext = newMockedPlanContext
    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: AllNodesScan    => 1000
      case _: NodeByIdSeek    => 2
      case _: NodeByLabelScan => 1
      case _                  => Double.MaxValue
    })
    implicit val planner = newPlanner(factory)
    when(planContext.getOptLabelId("Awesome")).thenReturn(Some(12))

    produceLogicalPlan("MATCH (n:Awesome) RETURN n") should equal(
      NodeByLabelScan("n", Right(LabelId(12)))
    )
  }

  test("should build plans for index scan when there is an index on the property") {
    implicit val planContext = newMockedPlanContext
    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: AllNodesScan        => 1000
      case _: NodeIndexSeek       => 1
      case _: NodeIndexUniqueSeek => 2
      case _: NodeByLabelScan     => 100
      case _                      => Double.MaxValue
    })
    implicit val planner = newPlanner(factory)

    when(planContext.getOptLabelId("Awesome")).thenReturn(Some(12))
    when(planContext.getOptPropertyKeyId("prop")).thenReturn(Some(15))
    when(planContext.getIndexRule("Awesome", "prop")).thenReturn(Some(new IndexDescriptor(12, 15)))
    when(planContext.getUniqueIndexRule("Awesome", "prop")).thenReturn(None)

    produceLogicalPlan("MATCH (n:Awesome) WHERE n.prop = 42 RETURN n") should equal(
      NodeIndexSeek("n", LabelId(12), PropertyKeyId(15), SignedIntegerLiteral("42")_)
    )
  }

  test("should build plans for index seek when there is an index on the property") {
    implicit val planContext = newMockedPlanContext
    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: AllNodesScan        => 1000
      case _: NodeIndexSeek       => 2
      case _: NodeIndexUniqueSeek => 1
      case _: NodeByLabelScan     => 100
      case _                      => Double.MaxValue
    })
    implicit val planner = newPlanner(factory)

    when(planContext.getOptLabelId("Awesome")).thenReturn(Some(12))
    when(planContext.getOptPropertyKeyId("prop")).thenReturn(Some(15))
    when(planContext.getIndexRule("Awesome", "prop")).thenReturn(None)
    when(planContext.getUniqueIndexRule("Awesome", "prop")).thenReturn(Some(new IndexDescriptor(12, 15)))

    produceLogicalPlan("MATCH (n:Awesome) WHERE n.prop = 42 RETURN n") should equal(
      NodeIndexUniqueSeek("n", LabelId(12), PropertyKeyId(15), SignedIntegerLiteral("42")_)
    )
  }

  test("should build plans for node by ID mixed with label scan when node by ID is cheaper") {
    implicit val planContext = newMockedPlanContext
    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: AllNodesScan    => 1000
      case _: NodeByIdSeek    => 1
      case _: NodeByLabelScan => 100
      case _                  => Double.MaxValue
    })
    implicit val planner = newPlanner(factory)

    when(planContext.getOptLabelId("Awesome")).thenReturn(Some(12))

    produceLogicalPlan("MATCH (n:Awesome) WHERE id(n) = 42 RETURN n") should equal(
      Selection(
        List(HasLabels(Identifier("n")_, Seq(LabelName("Awesome")_))_),
        NodeByIdSeek("n", Seq(SignedIntegerLiteral("42")_))
      )
    )
  }

  test("should build plans for node by ID mixed with label scan when label scan is cheaper") {
    implicit val planContext = newMockedPlanContext
    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: AllNodesScan    => 1000
      case _: NodeByIdSeek    => 10
      case _: NodeByLabelScan => 1
      case _                  => Double.MaxValue
    })
    implicit val planner = newPlanner(factory)

    when(planContext.getOptLabelId("Awesome")).thenReturn(Some(12))

    produceLogicalPlan("MATCH (n:Awesome) WHERE id(n) = 42 RETURN n") should equal(
      Selection(
        List(Equals(
          FunctionInvocation(FunctionName("id")_, Identifier("n")_)_,
          SignedIntegerLiteral("42")_
        )_),
        NodeByLabelScan("n", Right(LabelId(12)))
      )
    )
  }

  test("should use node by id when possible") {
    val factory: SpyableMetricsFactory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: AllNodesScan => 1000
      case _: NodeByLabelScan => 100
      case _: NodeIndexSeek => 10
      case _: NodeByIdSeek => 1
      case _: Expand => 100.0
      case _ => Double.MaxValue
    })
    implicit val planner = newPlanner(factory)
    implicit val planContext = newMockedPlanContext

    produceLogicalPlan("MATCH n WHERE ID(n) = 0 RETURN n") should equal(
      NodeByIdSeek(IdName("n"), Seq(SignedIntegerLiteral("0") _))
    )

    produceLogicalPlan("MATCH n WHERE id(n) = 0 RETURN n") should equal(
      NodeByIdSeek(IdName("n"), Seq(SignedIntegerLiteral("0") _))
    )
  }
}
