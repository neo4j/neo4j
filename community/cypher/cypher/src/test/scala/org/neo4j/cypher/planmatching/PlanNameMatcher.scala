/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.planmatching

import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.util.matching.Regex

/**
  * Match on the name of a plan.
  */
trait PlanNameMatcher extends Matcher[InternalPlanDescription] {
  val expectedName: String
}

/**
  * Match the name exactly
  */
case class PlanExactNameMatcher(expectedName: String) extends PlanNameMatcher {
  override def apply(plan: InternalPlanDescription): MatchResult = {
    MatchResult(
      matches = plan.name == expectedName,
      rawFailureMessage = s"Expected a plan with name $expectedName but got ${plan.name}.",
      rawNegatedFailureMessage = s"Expected no plan with name $expectedName."
    )
  }
}

/**
  * Match the name by Regex
  */
case class PlanRegexNameMatcher(expectedNameRegex: Regex) extends PlanNameMatcher {
  override val expectedName: String = expectedNameRegex.toString()

  override def apply(plan: InternalPlanDescription): MatchResult = {
    MatchResult(
      matches = expectedNameRegex.pattern.matcher(plan.name).matches(),
      rawFailureMessage = s"Expected a plan with name matched by the Regex $expectedName but got ${plan.name}.",
      rawNegatedFailureMessage = s"Expected no plan with name matched by the Regex $expectedName."
    )
  }
}
