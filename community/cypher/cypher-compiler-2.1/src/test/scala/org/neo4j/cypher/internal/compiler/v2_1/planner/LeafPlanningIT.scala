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
package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.CardinalityEstimator
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.mockito.Mockito._
import org.mockito.stubbing.Answer
import org.neo4j.kernel.api.index.IndexDescriptor
import org.mockito.invocation.InvocationOnMock
import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.AllNodesScan
import scala.Some
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Selection
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeIndexUniqueSeek
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_1.planner.Selections
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.AllNodesScan
import scala.Some
import scala.Equals
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Selection
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeIndexUniqueSeek
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_1.planner.Selections
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.AllNodesScan
import scala.Some
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Selection
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeIndexUniqueSeek
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.ast.FunctionName
import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_1.planner.Selections
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.compiler.v2_1.ast.Identifier
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.compiler.v2_1.ast.SignedIntegerLiteral
import scala.Some
import org.neo4j.cypher.internal.compiler.v2_1.ast.LabelName
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.compiler.v2_1.ast.HasLabels
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Selection
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeIndexUniqueSeek
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.ast.FunctionName
import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_1.planner.Selections
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.compiler.v2_1.ast.Identifier
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.compiler.v2_1.ast.SignedIntegerLiteral
import scala.Some
import org.neo4j.cypher.internal.compiler.v2_1.ast.LabelName
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.compiler.v2_1.ast.HasLabels
import org.neo4j.cypher.internal.compiler.v2_1.PropertyKeyId
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Selection
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeIndexUniqueSeek
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.ast.FunctionName
import org.neo4j.cypher.internal.compiler.v2_1.ExpressionTypeInfo
import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_1.planner.Selections
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.compiler.v2_1.ast.Identifier
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.compiler.v2_1.ast.SignedIntegerLiteral
import scala.Some
import org.neo4j.cypher.internal.compiler.v2_1.LabelId
import org.neo4j.cypher.internal.compiler.v2_1.ast.LabelName
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.compiler.v2_1.ast.HasLabels

class LeafPlanningIT extends CypherFunSuite with LogicalPlanningTestSupport {

  test("should build plans for all nodes scans") {
    implicit val planner = newStubbedPlanner(CardinalityEstimator.lift {
      case _: AllNodesScan => 1
      case _               => 100
    })

    produceLogicalPlan("MATCH (n) RETURN n") should equal(
      AllNodesScan("n")
    )
  }

  test("should build plans for label scans without compile-time label id") {
    implicit val planner = newStubbedPlanner(CardinalityEstimator.lift {
      case _: AllNodesScan    => 1000
      case _: NodeByIdSeek    => 2
      case _: NodeByLabelScan => 1
    })

    implicit val planContext = newMockedPlanContext
    when(planContext.getOptLabelId("Awesome")).thenReturn(None)

    produceLogicalPlan("MATCH (n:Awesome) RETURN n") should equal(
      NodeByLabelScan("n", Left("Awesome"))()
    )
  }

  test("should build plans for label scans with compile-time label id") {
    implicit val planner = newStubbedPlanner(CardinalityEstimator.lift {
      case _: AllNodesScan    => 1000
      case _: NodeByIdSeek    => 2
      case _: NodeByLabelScan => 1
    })

    implicit val planContext = newMockedPlanContext
    when(planContext.getOptLabelId("Awesome")).thenReturn(Some(12))

    produceLogicalPlan("MATCH (n:Awesome) RETURN n") should equal(
      NodeByLabelScan("n", Right(LabelId(12)))()
    )
  }

  test("should build plans for index scan when there is an index on the property") {
    implicit val planner = newStubbedPlanner(CardinalityEstimator.lift {
      case _: AllNodesScan         => 1000
      case _: NodeIndexSeek        => 1
      case _: NodeIndexUniqueSeek  => 2
      case _: NodeByLabelScan      => 100
    })

    implicit val planContext = newMockedPlanContext
    when(planContext.getOptLabelId("Awesome")).thenReturn(Some(12))
    when(planContext.getOptPropertyKeyId("prop")).thenReturn(Some(15))
    when(planContext.indexesGetForLabel(12)).thenAnswer(new Answer[Iterator[IndexDescriptor]] {
      def answer(invocation: InvocationOnMock) = Iterator(new IndexDescriptor(12, 15))
    })
    when(planContext.uniqueIndexesGetForLabel(12)).thenReturn(Iterator())

    produceLogicalPlan("MATCH (n:Awesome) WHERE n.prop = 42 RETURN n") should equal(
      NodeIndexSeek("n", LabelId(12), PropertyKeyId(15), SignedIntegerLiteral("42")_)()
    )
  }

  test("should build plans for index seek when there is an index on the property") {
    implicit val planner = newStubbedPlanner(CardinalityEstimator.lift {
      case _: AllNodesScan         => 1000
      case _: NodeIndexSeek        => 2
      case _: NodeIndexUniqueSeek  => 1
      case _: NodeByLabelScan      => 100
    })

    implicit val planContext = newMockedPlanContext
    when(planContext.getOptLabelId("Awesome")).thenReturn(Some(12))
    when(planContext.getOptPropertyKeyId("prop")).thenReturn(Some(15))
    when(planContext.indexesGetForLabel(12)).thenReturn(Iterator())
    when(planContext.uniqueIndexesGetForLabel(12)).thenAnswer(new Answer[Iterator[IndexDescriptor]] {
      def answer(invocation: InvocationOnMock) = Iterator(new IndexDescriptor(12, 15))
    })

    produceLogicalPlan("MATCH (n:Awesome) WHERE n.prop = 42 RETURN n") should equal(
      NodeIndexUniqueSeek("n", LabelId(12), PropertyKeyId(15), SignedIntegerLiteral("42")_)()
    )
  }

  test("should build plans for node by ID mixed with label scan when node by ID is cheaper") {
    implicit val planner = newStubbedPlanner(CardinalityEstimator.lift {
      case _: AllNodesScan    => 1000
      case _: NodeByIdSeek    => 1
      case _: NodeByLabelScan => 100
    })

    implicit val planContext = newMockedPlanContext
    when(planContext.getOptLabelId("Awesome")).thenReturn(Some(12))

    produceLogicalPlan("MATCH (n:Awesome) WHERE id(n) = 42 RETURN n") should equal(
      Selection(
        List(HasLabels(Identifier("n")_, Seq(LabelName("Awesome")(None)_))_),
        NodeByIdSeek("n", Seq(SignedIntegerLiteral("42")_))()
      )
    )
  }

  test("should build plans for node by ID mixed with label scan when label scan is cheaper") {
    implicit val planner = newStubbedPlanner(CardinalityEstimator.lift {
      case _: AllNodesScan    => 1000
      case _: NodeByIdSeek    => 10
      case _: NodeByLabelScan => 1
    })

    implicit val planContext = newMockedPlanContext
    when(planContext.getOptLabelId("Awesome")).thenReturn(Some(12))

    produceLogicalPlan("MATCH (n:Awesome) WHERE id(n) = 42 RETURN n") should equal(
      Selection(
        List(Equals(
          FunctionInvocation(FunctionName("id")_, Identifier("n")_)_,
          SignedIntegerLiteral("42")_
        )_),
        NodeByLabelScan("n", Right(LabelId(12)))()
      )
    )
  }
}
