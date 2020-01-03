/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.steps.{LogicalPlanProducer, devNullListener, pickBestPlanUsingHintsAndCost}
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.v4_0.ast.UsingIndexHint
import org.neo4j.cypher.internal.v4_0.expressions.PropertyKeyName
import org.neo4j.cypher.internal.v4_0.frontend.phases.devNullLogger
import org.neo4j.cypher.internal.v4_0.util.Cost
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class PickBestPlanUsingHintsAndCostTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  private val GIVEN_FIXED_COST = new given {
    cost = {
      case _ => Cost(100)
    }
  }

  private val hint1: UsingIndexHint = UsingIndexHint(varFor("n"), labelName("Person"), Seq(PropertyKeyName("name")_))_
  private val hint2: UsingIndexHint = UsingIndexHint(varFor("n"), labelName("Person"), Seq(PropertyKeyName("age")_))_

  test("picks the right plan by cost, no matter the cardinality") {
    val a = fakeLogicalPlanFor("a")
    val b = fakeLogicalPlanFor("b")

    assertTopPlan(winner = b, PlanningAttributes(new StubSolveds, new StubCardinalities, new StubProvidedOrders), a, b)(new given {
      cost = {
        case (p, _, _) if p == a => Cost(100)
        case (p, _, _) if p == b => Cost(50)
      }
    })
  }

  test("Prefers plans that solves a hint over plan that solves no hint") {
    val solveds = new Solveds
    val a = fakeLogicalPlanFor("a")
    solveds.set(a.id, SinglePlannerQuery.empty.amendQueryGraph(_.addHints(Some(hint1))))
    val b = fakeLogicalPlanFor("a")
    solveds.set(b.id, SinglePlannerQuery.empty)

    assertTopPlan(winner = a, PlanningAttributes(solveds, new StubCardinalities, new StubProvidedOrders), a, b)(GIVEN_FIXED_COST)
  }

  test("Prefers plans that solve more hints") {
    val solveds = new Solveds
    val a = fakeLogicalPlanFor("a")
    solveds.set(a.id, SinglePlannerQuery.empty.amendQueryGraph(_.addHints(Some(hint1))))
    val b = fakeLogicalPlanFor("a")
    solveds.set(b.id, SinglePlannerQuery.empty.amendQueryGraph(_.addHints(Set(hint1, hint2))))

    assertTopPlan(winner = b, PlanningAttributes(solveds, new StubCardinalities, new StubProvidedOrders), a, b)(GIVEN_FIXED_COST)
  }

  test("Prefers plans that solve more hints in tails") {
    val solveds = new Solveds
    val a = fakeLogicalPlanFor("a")
    solveds.set(a.id, SinglePlannerQuery.empty.amendQueryGraph(_.addHints(Some(hint1))))
    val b = fakeLogicalPlanFor("a")
    solveds.set(b.id, SinglePlannerQuery.empty.withTail(SinglePlannerQuery.empty.amendQueryGraph(_.addHints(Set(hint1, hint2)))))

    assertTopPlan(winner = b, PlanningAttributes(solveds, new StubCardinalities, new StubProvidedOrders), a, b)(GIVEN_FIXED_COST)
  }

  private def assertTopPlan(winner: LogicalPlan, planningAttributes: PlanningAttributes, candidates: LogicalPlan*)(GIVEN: given): Unit = {
    val environment = LogicalPlanningEnvironment(GIVEN)
    val metrics: Metrics = environment.metricsFactory.newMetrics(GIVEN.statistics, GIVEN.expressionEvaluator, cypherCompilerConfig)
    val producer = LogicalPlanProducer(metrics.cardinality, planningAttributes, idGen)
    val context = LogicalPlanningContext(null,
      producer,
      metrics,
      null,
      null,
      notificationLogger = devNullLogger,
      costComparisonListener = devNullListener,
      planningAttributes = planningAttributes,
      innerVariableNamer = innerVariableNamer,
      idGen = idGen
    )
    pickBestPlanUsingHintsAndCost(context)(candidates).get shouldBe theSameInstanceAs(winner)
    pickBestPlanUsingHintsAndCost(context)(candidates.reverse).get shouldBe theSameInstanceAs(winner)
  }
}


