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
package org.neo4j.cypher.planmatching

import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrder
import org.neo4j.cypher.internal.plandescription.Arguments.Order
import org.neo4j.cypher.internal.plandescription.InternalPlanDescription
import org.neo4j.cypher.internal.plandescription.asPrettyString
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

/**
 * Asserts that the Order argument of a PlanDescription contains the expected provided order.
 */
case class OrderArgumentMatcher(expected: ProvidedOrder) extends Matcher[InternalPlanDescription] {

  override def apply(plan: InternalPlanDescription): MatchResult = {
    val orderArgs = plan.arguments.collect { case o: Order => o }
    val expectedAsOrder = asPrettyString.order(expected)
    MatchResult(
      matches = orderArgs.contains(expectedAsOrder),
      rawFailureMessage = s"Expected ${plan.name} to have order $expectedAsOrder but got $orderArgs.",
      rawNegatedFailureMessage = s"Expected ${plan.name} not to have order $expectedAsOrder."
    )
  }
}
