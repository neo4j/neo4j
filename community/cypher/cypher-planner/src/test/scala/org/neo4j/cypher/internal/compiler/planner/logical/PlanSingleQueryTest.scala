/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.idp.BestResults
import org.neo4j.cypher.internal.compiler.planner.logical.steps.BestPlans
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryPagination
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.mutable.ArrayBuffer

class PlanSingleQueryTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  private type LogEntry = (PlannerType, Selectivity)

  test("should use Selectivity.ONE in the absence of LIMIT") {
    // MATCH (n) RETURN n
    val q = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n"))
    )

    planSingleQuery(q) shouldEqual Vector(
      (PlannerType.Match, Selectivity.ONE),
      (PlannerType.Horizon, Selectivity.ONE)
    )
  }

  test("horizon with LIMIT should not reduce its own cardinality") {
    // MATCH (n) RETURN n LIMIT 1000
    val q = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      horizon = RegularQueryProjection(
        queryPagination = QueryPagination(
          limit = Some(literalInt(1000))
        )
      )
    )

    planSingleQuery(q) shouldEqual Vector(
      (PlannerType.Match, Selectivity.of(0.1).get),
      (PlannerType.Horizon, Selectivity.ONE)
    )
  }

  test("horizon with LIMIT should not reduce its own cardinality, with tail") {
    // MATCH (n) WITH n AS m LIMIT 1000 RETURN m
    val q = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      horizon = RegularQueryProjection(
        projections = Map(v"m" -> varFor("n")),
        queryPagination = QueryPagination(
          limit = Some(literalInt(1000))
        )
      ),
      tail = Some(RegularSinglePlannerQuery(
        queryGraph = QueryGraph(
          patternNodes = Set("m"),
          argumentIds = Set("m")
        )
      ))
    )

    planSingleQuery(q) shouldEqual Vector(
      (PlannerType.Match, Selectivity.of(0.1).get),
      (PlannerType.Horizon, Selectivity.ONE),
      (PlannerType.Match, Selectivity.ONE),
      (PlannerType.Horizon, Selectivity.ONE)
    )
  }

  test("horizon that contains LIMIT should not reduce its own cardinality, but should affect earlier horizons") {
    // MATCH (n) WITH n AS m LIMIT 1000 RETURN m LIMIT 500
    val q = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      horizon = RegularQueryProjection(
        projections = Map(v"m" -> varFor("n")),
        queryPagination = QueryPagination(
          limit = Some(literalInt(1000))
        )
      ),
      tail = Some(RegularSinglePlannerQuery(
        queryGraph = QueryGraph(
          patternNodes = Set("m"),
          argumentIds = Set("m")
        ),
        horizon = RegularQueryProjection(
          queryPagination = QueryPagination(
            limit = Some(literalInt(500))
          )
        )
      ))
    )

    planSingleQuery(q) shouldEqual Vector(
      (PlannerType.Match, Selectivity.of(0.1 * 0.5).get),
      (PlannerType.Horizon, Selectivity.of(0.5).get),
      (PlannerType.Match, Selectivity.of(0.5).get),
      (PlannerType.Horizon, Selectivity.ONE)
    )
  }

  private def planSingleQuery(query: SinglePlannerQuery): Vector[LogEntry] = {
    val log = ArrayBuffer[LogEntry]()
    val recordingPlanMatch = new RecordingMatchPlanner(log)
    val recordingPlanHorizon = new RecordingHorizonPlanner(log)

    val planner = PlanSingleQuery(
      PlanHead(matchPlanner = recordingPlanMatch, eventHorizonPlanner = recordingPlanHorizon),
      PlanWithTail(matchPlanner = recordingPlanMatch, eventHorizonPlanner = recordingPlanHorizon)
    )

    new givenConfig().withLogicalPlanningContext { (_, context) => planner.plan(query, context) }
    log.toVector
  }

  private class RecordingMatchPlanner(log: ArrayBuffer[LogEntry]) extends MatchPlanner {

    override protected def doPlan(
      query: SinglePlannerQuery,
      context: LogicalPlanningContext,
      rhsPart: Boolean
    ): BestPlans = {
      log += ((context.plannerState.input.activePlanner, context.plannerState.input.limitSelectivity))
      planMatch.plan(query, context, rhsPart)
    }
  }

  private class RecordingHorizonPlanner(log: ArrayBuffer[LogEntry]) extends EventHorizonPlanner {

    override protected def doPlanHorizon(
      plannerQuery: SinglePlannerQuery,
      incomingPlans: BestResults[LogicalPlan],
      prevInterestingOrder: Option[InterestingOrder],
      context: LogicalPlanningContext
    ): BestResults[LogicalPlan] = {
      log += ((context.plannerState.input.activePlanner, context.plannerState.input.limitSelectivity))
      PlanEventHorizon.planHorizon(plannerQuery, incomingPlans, prevInterestingOrder, context)
    }
  }
}
