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
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality

import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfiguration
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningSupport
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.EffectiveCardinality
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

trait CardinalityIntegrationTestSupport extends StatisticsBackedLogicalPlanningSupport {
  self: CypherFunSuite =>

  val DEFAULT_PREDICATE_SELECTIVITY: Double = PlannerDefaults.DEFAULT_PREDICATE_SELECTIVITY.factor
  val DEFAULT_PROPERTY_SELECTIVITY: Double = PlannerDefaults.DEFAULT_PROPERTY_SELECTIVITY.factor
  val DEFAULT_EQUALITY_SELECTIVITY: Double = PlannerDefaults.DEFAULT_EQUALITY_SELECTIVITY.factor
  val DEFAULT_RANGE_SELECTIVITY: Double = PlannerDefaults.DEFAULT_RANGE_SELECTIVITY.factor
  val DEFAULT_REL_UNIQUENESS_SELECTIVITY: Double = PlannerDefaults.DEFAULT_REL_UNIQUENESS_SELECTIVITY.factor
  val DEFAULT_RANGE_SEEK_FACTOR: Double = PlannerDefaults.DEFAULT_RANGE_SEEK_FACTOR
  val DEFAULT_LIST_CARDINALITY: Int = PlannerDefaults.DEFAULT_LIST_CARDINALITY.amount.toInt
  val DEFAULT_LIMIT_CARDINALITY: Int = PlannerDefaults.DEFAULT_LIMIT_CARDINALITY.amount.toInt
  val DEFAULT_DISTINCT_SELECTIVITY: Double = PlannerDefaults.DEFAULT_DISTINCT_SELECTIVITY.factor
  val DEFAULT_MULTIPLIER: Double = PlannerDefaults.DEFAULT_MULTIPLIER.coefficient

  private val combiner: SelectivityCombiner = IndependenceCombiner

  def and(numbers: Double*): Double =
    combiner.andTogetherSelectivities(numbers.map(Selectivity.of(_).getOrElse(Selectivity.ONE))).get.factor

  def or(numbers: Double*): Double =
    combiner.orTogetherSelectivities(numbers.map(Selectivity.of(_).getOrElse(Selectivity.ONE))).get.factor

  def queryShouldHaveCardinality(
    config: StatisticsBackedLogicalPlanningConfiguration,
    query: String,
    expectedCardinality: Double
  ): Unit = {
    planShouldHaveCardinality(
      config,
      query,
      {
        case _: ProduceResult => true
      },
      expectedCardinality
    )
  }

  def planShouldHaveCardinality(
    config: StatisticsBackedLogicalPlanningConfiguration,
    query: String,
    findPlanId: PartialFunction[LogicalPlan, Boolean],
    expectedCardinality: Double
  ): Unit = {
    val planState = config.planState(s"$query RETURN 1 AS result")

    val planId = planState.logicalPlan.flatten(CancellationChecker.neverCancelled()).collectFirst {
      case lp if findPlanId.applyOrElse(lp, (_: LogicalPlan) => false) => lp.id
    }.get
    val actualCardinality = planState.planningAttributes.effectiveCardinalities.get(planId)

    // used to handle double rounding errors in assertion
    import org.neo4j.cypher.internal.compiler.planner.logical.CardinalitySupport.EffectiveCardinalityEquality
    actualCardinality should equal(EffectiveCardinality(expectedCardinality))(EffectiveCardinalityEquality)
  }

  def queryShouldHaveCardinality(query: String, expectedCardinality: Double): Unit = {
    queryShouldHaveCardinality(
      plannerBuilder()
        .setExecutionModel(ExecutionModel.Volcano)
        .build(),
      query,
      expectedCardinality
    )
  }

  def planShouldHaveCardinality(
    query: String,
    findPlanId: PartialFunction[LogicalPlan, Boolean],
    expectedCardinality: Double
  ): Unit = {
    planShouldHaveCardinality(
      plannerBuilder()
        .setExecutionModel(ExecutionModel.Volcano)
        .build(),
      query,
      findPlanId,
      expectedCardinality
    )
  }
}
