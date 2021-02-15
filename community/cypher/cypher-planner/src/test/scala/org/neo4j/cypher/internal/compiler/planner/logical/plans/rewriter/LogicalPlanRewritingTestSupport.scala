/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.util.attribution.Id
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

trait LogicalPlanRewritingTestSupport {

  private val precision = 0.00001

  def haveSameEffectiveCardinalitiesAs(expected: (LogicalPlan, EffectiveCardinalities)): Matcher[(LogicalPlan, EffectiveCardinalities)] =
    (actual: (LogicalPlan, EffectiveCardinalities)) => {
      val (actPlan, actCards) = actual
      val (expPlan, expCards) = expected

      planAndCardinalitiesMatchResult(actPlan, expPlan, x => actCards(x).amount, x => expCards(x).amount)
    }

  def haveSameCardinalitiesAs(expected: (LogicalPlan, Cardinalities)): Matcher[(LogicalPlan, Cardinalities)] =
    (actual: (LogicalPlan, Cardinalities)) => {
      val (actPlan, actCards) = actual
      val (expPlan, expCards) = expected

      planAndCardinalitiesMatchResult(actPlan, expPlan, x => actCards(x).amount, x => expCards(x).amount)
    }

  private def planAndCardinalitiesMatchResult(actPlan: LogicalPlan,
                                              expPlan: LogicalPlan,
                                              getActualCardinality: Id => Double,
                                              getExpectedCardinality: Id => Double,
                                             ): MatchResult = {
    val planPairs = actPlan.flatten.zip(expPlan.flatten)

    val planMismatch =
      if (actPlan != expPlan) {
        val actPlanString = LogicalPlanToPlanBuilderString(actPlan)
        val expPlanString = LogicalPlanToPlanBuilderString(expPlan)
        Some(MatchResult(
          matches = false,
          rawFailureMessage = s"Expected same plan but actual contained:\n$actPlanString\nand expected contained:\n$expPlanString",
          rawNegatedFailureMessage = ""))
      } else {
        None
      }

    val results = planPairs.map {
      case (act, exp) =>
        val actCard = getActualCardinality(act.id)
        val expCard = getExpectedCardinality(exp.id)
        val actPlanString = LogicalPlanToPlanBuilderString(act)
        MatchResult(
          matches = (actCard - expCard).abs < precision,
          rawFailureMessage = s"Expected cardinality $expCard but was $actCard for plan:\n$actPlanString",
          rawNegatedFailureMessage = "")
    }

    val cardinalityMismatch = results.find(!_.matches)
    val ok = MatchResult(
      matches = true,
      rawFailureMessage = "",
      rawNegatedFailureMessage = "")

    (planMismatch orElse cardinalityMismatch) getOrElse ok
  }
}
