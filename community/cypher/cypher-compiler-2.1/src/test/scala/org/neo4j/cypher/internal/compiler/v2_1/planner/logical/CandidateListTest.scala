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
import org.neo4j.cypher.internal.compiler.v2_1.planner.{QueryGraph, LogicalPlanningTestSupport}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{QueryPlan, LogicalPlan, IdName}
import org.mockito.Matchers._
import org.mockito.Mockito._

class CandidateListTest extends CypherFunSuite with LogicalPlanningTestSupport {
  implicit val semanticTable = newMockedSemanticTable
  implicit val planContext = newMockedPlanContext
  implicit val context = newMockedLogicalPlanContext(planContext)

  val x = newMockedQueryPlan("x")
  val y = newMockedQueryPlan("y")
  val xAndY = newMockedQueryPlan("x", "y")

  test("prune with no overlaps returns the same candidates") {
    val candidates = CandidateList(Seq(x, y))
    candidates.pruned should equal(candidates)
  }

  test("prune with overlaps returns the first ones") {
    val candidates = CandidateList(Seq(x, xAndY))

    candidates.pruned should equal(CandidateList(Seq(x)))
  }

  test("empty prune is legal") {
    val candidates = CandidateList(Seq())

    candidates.pruned should equal(CandidateList(Seq()))
  }

  test("picks the right plan by cost, no matter the cardinality") {
    val a = newMockedQueryPlanWithProjections("a")
    val b = newMockedQueryPlanWithProjections("b")

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(any())).thenReturn((plan: LogicalPlan) => plan match {
      case p if p eq a.plan => 100
      case p if p eq b.plan => 50
      case _                => Double.MaxValue
    })

    assertTopPlan(winner = b, a, b)(factory)
  }

  test("picks the right plan by cost, no matter the size of the covered ids") {
    val ab = QueryPlan( newMockedLogicalPlanWithPatterns(Set(IdName("a"), IdName("b"))) )
    val b = newMockedQueryPlanWithProjections("b")

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(any())).thenReturn((plan: LogicalPlan) => plan match {
      case p if p eq ab.plan => 100
      case p if p eq b.plan  => 50
      case _                 => Double.MaxValue
    })

    assertTopPlan(winner = b, ab, b)(factory)
  }

  test("picks the right plan by cost and secondly by the covered ids") {
    val ab = newMockedQueryPlanWithPatterns(Set(IdName("a"), IdName("b")))
    val c = newMockedQueryPlanWithProjections("c")

    val factory = newMockedMetricsFactory
    when(factory.newCostModel(any())).thenReturn((plan: LogicalPlan) => plan match {
      case p if p eq ab.plan => 50
      case p if p eq c.plan  => 50
      case _                 => Double.MaxValue
    })

    assertTopPlan(winner = ab, ab, c)(factory)
  }

  private def assertTopPlan(winner: QueryPlan, candidates: QueryPlan*)(metrics: MetricsFactory) {
    val costs = metrics.newMetrics(context.statistics, semanticTable).cost
    CandidateList(candidates).bestPlan(costs) should equal(Some(winner))
    CandidateList(candidates.reverse).bestPlan(costs) should equal(Some(winner))
  }
}


