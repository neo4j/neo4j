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
package org.neo4j.cypher.internal.compiler.planner

import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.EffectiveCardinality
import org.neo4j.cypher.internal.util.attribution.Attribute
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

trait LogicalPlanningAttributesTestSupport {

  private val precision = 0.00001

  def haveSamePlanAndEffectiveCardinalitiesAsBuilder(
    expected: LogicalPlanBuilder,
    attributeComparisonStrategy: AttributeComparisonStrategy = AttributeComparisonStrategy.ComparingAllAttributes
  ): Matcher[LogicalPlanState] =
    matchPlanAndAttributesUsing(
      _.effectiveCardinalities,
      expected,
      _.effectiveCardinalities,
      attributeComparisonStrategy
    )(isEffectiveCardinalityWithinBound)

  def haveSamePlanAndEffectiveCardinalitiesAs(
    expected: (LogicalPlan, EffectiveCardinalities),
    attributeComparisonStrategy: AttributeComparisonStrategy = AttributeComparisonStrategy.ComparingAllAttributes
  ): Matcher[(LogicalPlan, EffectiveCardinalities)] =
    matchPlanAndAttributes(expected, attributeComparisonStrategy)(isEffectiveCardinalityWithinBound)

  private def isEffectiveCardinalityWithinBound(actual: EffectiveCardinality, expected: EffectiveCardinality): Boolean =
    (actual.amount - expected.amount).abs < precision

  def haveSamePlanAndCardinalitiesAsBuilder(
    expected: LogicalPlanBuilder,
    attributeComparisonStrategy: AttributeComparisonStrategy = AttributeComparisonStrategy.ComparingAllAttributes
  ): Matcher[LogicalPlanState] =
    matchPlanAndAttributesUsing(_.cardinalities, expected, _.cardinalities, attributeComparisonStrategy)(
      isCardinalityWithinBound
    )

  def haveSamePlanAndCardinalitiesAs(
    expected: (LogicalPlan, Cardinalities),
    attributeComparisonStrategy: AttributeComparisonStrategy = AttributeComparisonStrategy.ComparingAllAttributes
  ): Matcher[(LogicalPlan, Cardinalities)] =
    matchPlanAndAttributes(expected, attributeComparisonStrategy)(isCardinalityWithinBound)

  private def isCardinalityWithinBound(actual: Cardinality, expected: Cardinality): Boolean =
    (actual.amount - expected.amount).abs < precision

  def haveSamePlanAndProvidedOrdersAsBuilder(
    expected: LogicalPlanBuilder,
    attributeComparisonStrategy: AttributeComparisonStrategy = AttributeComparisonStrategy.ComparingAllAttributes
  ): Matcher[LogicalPlanState] =
    matchPlanAndAttributesUsing(_.providedOrders, expected, _.providedOrders, attributeComparisonStrategy)(_ == _)

  def haveSamePlanAndProvidedOrdersAs(
    expected: (LogicalPlan, ProvidedOrders),
    attributeComparisonStrategy: AttributeComparisonStrategy = AttributeComparisonStrategy.ComparingAllAttributes
  ): Matcher[(LogicalPlan, ProvidedOrders)] =
    matchPlanAndAttributes(expected, attributeComparisonStrategy)(_ == _)

  private def matchPlanAndAttributesUsing[A](
    getActualAttributes: PlanningAttributes => Attribute[LogicalPlan, A],
    expectedPlanBuilder: LogicalPlanBuilder,
    getExpectedAttributes: LogicalPlanBuilder => Attribute[LogicalPlan, A],
    attributeComparisonStrategy: AttributeComparisonStrategy
  )(
    checkMatches: (A, A) => Boolean
  ): Matcher[LogicalPlanState] = actualPlanState =>
    matchPlanAndAttributes(
      (expectedPlanBuilder.build(), getExpectedAttributes(expectedPlanBuilder)),
      attributeComparisonStrategy
    )(checkMatches)
      .apply((actualPlanState.logicalPlan, getActualAttributes(actualPlanState.planningAttributes)))

  private def matchPlanAndAttributes[A](
    expected: (LogicalPlan, Attribute[LogicalPlan, A]),
    attributeComparisonStrategy: AttributeComparisonStrategy
  )(
    checkMatches: (A, A) => Boolean
  ): Matcher[(LogicalPlan, Attribute[LogicalPlan, A])] = actual => {
    val actualPlan = actual._1
    val actualAttributes = actual._2
    val expectedPlan = expected._1
    val expectedAttributes = expected._2

    val planMismatch =
      if (actualPlan != expectedPlan) {
        val actualPlanString = actualPlan.toString
        val expectedPlanString = expectedPlan.toString
        Some(MatchResult(
          matches = false,
          rawFailureMessage =
            s"Expected same plan but actual contained:\n$actualPlanString\nand expected contained:\n$expectedPlanString",
          rawNegatedFailureMessage = ""
        ))
      } else {
        None
      }

    val results = for {
      (actualOperator, expectedOperator) <- actualPlan.flatten(CancellationChecker.neverCancelled()).zip(
        expectedPlan.flatten(CancellationChecker.neverCancelled())
      )
      expectedAttribute <- attributeComparisonStrategy match {
        case AttributeComparisonStrategy.ComparingAllAttributes => Some(expectedAttributes.get(expectedOperator.id))
        case AttributeComparisonStrategy.ComparingProvidedAttributesOnly =>
          expectedAttributes.getOption(expectedOperator.id)
      }
      actualAttribute = actualAttributes.get(actualOperator.id)
      actualPlanString = LogicalPlanToPlanBuilderString(actualOperator)
      matches = checkMatches(actualAttribute, expectedAttribute)
      failureMessage = s"Expected $expectedAttribute but was $actualAttribute for plan:\n$actualPlanString"
    } yield MatchResult(matches, failureMessage, "")

    val attributeMismatch = results.find(!_.matches)
    val ok = MatchResult(
      matches = true,
      rawFailureMessage = "",
      rawNegatedFailureMessage = ""
    )

    (planMismatch orElse attributeMismatch) getOrElse ok
  }
}

/** Strategy used to compare attributes, such as cardinalities or provided orders, between two plans.
 *
 *  When comparing attributes between two plans:
 *  - [[AttributeComparisonStrategy.ComparingAllAttributes]] will compare every attribute that is either explicitly defined or with a default value (provided by [[org.neo4j.cypher.internal.util.attribution.Default]]).
 *  - [[AttributeComparisonStrategy.ComparingProvidedAttributesOnly]] will only compare explicitly provided attributes, ignoring default values.
 */
sealed trait AttributeComparisonStrategy

object AttributeComparisonStrategy {
  case object ComparingAllAttributes extends AttributeComparisonStrategy
  case object ComparingProvidedAttributesOnly extends AttributeComparisonStrategy
}
