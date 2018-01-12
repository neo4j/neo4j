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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical

import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.steps.{LogicalPlanProducer, pickBestPlanUsingHintsAndCost}
import org.neo4j.cypher.internal.frontend.v3_4.ast.UsingIndexHint
import org.neo4j.cypher.internal.frontend.v3_4.phases.devNullLogger
import org.neo4j.cypher.internal.ir.v3_4.{CardinalityEstimation, PlannerQuery, RegularPlannerQuery}
import org.neo4j.cypher.internal.util.v3_4.Cost
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.v3_4.expressions.{LabelName, PropertyKeyName}

class PickBestPlanUsingHintsAndCostTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  val GIVEN_FIXED_COST = new given {
    cost = {
      case _ => Cost(100)
    }
  }

  val hint1: UsingIndexHint = UsingIndexHint(varFor("n"), LabelName("Person")_, Seq(PropertyKeyName("name")_))_
  val hint2: UsingIndexHint = UsingIndexHint(varFor("n"), LabelName("Person")_, Seq(PropertyKeyName("age")_))_
  val hint3: UsingIndexHint = UsingIndexHint(varFor("n"), LabelName("Person")_, Seq(PropertyKeyName("income")_))_

  test("picks the right plan by cost, no matter the cardinality") {
    val solveds = new Solveds
    val cardinalities = new Cardinalities

    val a = fakeLogicalPlanFor(solveds, cardinalities, "a")
    val b = fakeLogicalPlanFor(solveds, cardinalities, "b")

    assertTopPlan(winner = b, solveds, cardinalities, a, b)(new given {
      cost = {
        case (p, _, _) if p == a => Cost(100)
        case (p, _, _) if p == b => Cost(50)
      }
    })
  }

  test("picks the right plan by cost, no matter the size of the covered ids") {
    val solveds = new Solveds
    val cardinalities = new Cardinalities

    val ab = fakeLogicalPlanFor(solveds, cardinalities, "a", "b")
    val b = fakeLogicalPlanFor(solveds, cardinalities, "b")

    val GIVEN = new given {
      cost = {
        case (p, _, _) if p == ab => Cost(100)
        case (p, _, _) if p == b => Cost(50)
      }
    }

    assertTopPlan(winner = b, solveds, cardinalities, ab, b)(GIVEN)
  }

  test("picks the right plan by cost and secondly by the covered ids") {
    val solveds = new Solveds
    val cardinalities = new Cardinalities

    val ab = fakeLogicalPlanFor(solveds, cardinalities, "a", "b")
    val c = fakeLogicalPlanFor(solveds, cardinalities, "c")

    assertTopPlan(winner = ab, solveds, cardinalities, ab, c)(GIVEN_FIXED_COST)
  }

  test("Prefers plans that solves a hint over plan that solves no hint") {
    val solveds = new Solveds
    val cardinalities = new Cardinalities

    val a = fakeLogicalPlanFor(solveds, cardinalities, "a")
    solveds.set(a.id, PlannerQuery.empty.amendQueryGraph(_.addHints(Some(hint1))))
    val b = fakeLogicalPlanFor(solveds, cardinalities, "a")

    assertTopPlan(winner = a, solveds, cardinalities, a, b)(GIVEN_FIXED_COST)
  }

  test("Prefers plans that solve more hints") {
    val solveds = new Solveds
    val cardinalities = new Cardinalities

    val f: PlannerQuery => PlannerQuery = (query: PlannerQuery) => query.amendQueryGraph(_.addHints(Some(hint1)))
    val a = fakeLogicalPlanFor(solveds, cardinalities, "a")
    solveds.set(a.id, PlannerQuery.empty.amendQueryGraph(_.addHints(Some(hint1))))
    val g: PlannerQuery => PlannerQuery = (query: PlannerQuery) => query.amendQueryGraph(_.addHints(Seq(hint1, hint2)))
    val b = fakeLogicalPlanFor(solveds, cardinalities, "a")
    solveds.set(b.id, PlannerQuery.empty.amendQueryGraph(_.addHints(Seq(hint1, hint2))))

    assertTopPlan(winner = b, solveds, cardinalities, a, b)(GIVEN_FIXED_COST)
  }

  test("Prefers plans that solve more hints in tails") {
    val solveds = new Solveds
    val cardinalities = new Cardinalities

    val f: PlannerQuery => PlannerQuery = (query: PlannerQuery) => query.amendQueryGraph(_.addHints(Some(hint1)))
    val a = fakeLogicalPlanFor(solveds, cardinalities, "a")
    solveds.set(a.id, PlannerQuery.empty.amendQueryGraph(_.addHints(Some(hint1))))
    val g: PlannerQuery => PlannerQuery = (query: PlannerQuery) => query.withTail(PlannerQuery.empty.amendQueryGraph(_.addHints(Seq(hint1, hint2))))
    val b = fakeLogicalPlanFor(solveds, cardinalities, "a")
    solveds.set(b.id, PlannerQuery.empty.withTail(PlannerQuery.empty.amendQueryGraph(_.addHints(Seq(hint1, hint2)))))

    assertTopPlan(winner = b, solveds, cardinalities, a, b)(GIVEN_FIXED_COST)
  }

  private def assertTopPlan(winner: LogicalPlan, solveds: Solveds, cardinalities: Cardinalities, candidates: LogicalPlan*)(GIVEN: given) {
    val environment = LogicalPlanningEnvironment(GIVEN)
    val metrics: Metrics = environment.metricsFactory.newMetrics(GIVEN.graphStatistics, GIVEN.expressionEvaluator)
    val context = LogicalPlanningContext(null, LogicalPlanProducer(metrics.cardinality, LogicalPlan.LOWEST_TX_LAYER, solveds, cardinalities, idGen), metrics, null, null, notificationLogger = devNullLogger)
    pickBestPlanUsingHintsAndCost(context, solveds, cardinalities)(candidates) should equal(Some(winner))
    pickBestPlanUsingHintsAndCost(context, solveds, cardinalities)(candidates.reverse) should equal(Some(winner))
  }

  private implicit def lift(f: PlannerQuery => PlannerQuery): PlannerQuery with CardinalityEstimation => PlannerQuery with CardinalityEstimation =
    (solved: PlannerQuery with CardinalityEstimation) => CardinalityEstimation.lift(f(solved), solved.estimatedCardinality)
}


