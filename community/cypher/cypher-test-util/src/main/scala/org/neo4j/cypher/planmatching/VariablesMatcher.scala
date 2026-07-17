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

import org.neo4j.cypher.internal.plandescription.InternalPlanDescription
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

import scala.util.matching.Regex

/**
 * Asserts that a plan has certain variables
 */
trait VariablesMatcher extends Matcher[InternalPlanDescription] {
  val expected: Set[String]
  def planVars(plan: InternalPlanDescription): Set[String] = plan.variables.map(_.prettifiedString)
}

/**
 * Asserts that a plan has exactly the provided variables.
 */
case class ExactVariablesMatcher(expected: Set[String]) extends VariablesMatcher {

  override def apply(plan: InternalPlanDescription): MatchResult = {
    val vars = planVars(plan)
    MatchResult(
      matches = vars == expected,
      rawFailureMessage = s"Expected ${plan.name} to have variables $expected but got $vars.",
      rawNegatedFailureMessage = s"Expected ${plan.name} not to have variables $expected."
    )
  }
}

/**
 * Asserts that a plan contains the provided variables (among others).
 */
case class ContainsVariablesMatcher(expected: Set[String]) extends VariablesMatcher {

  override def apply(plan: InternalPlanDescription): MatchResult = {
    val vars = planVars(plan)
    MatchResult(
      matches = expected.subsetOf(vars),
      rawFailureMessage = s"Expected ${plan.name} to contain variables $expected but got $vars.",
      rawNegatedFailureMessage = s"Expected ${plan.name} not to contain variables $expected."
    )
  }
}

/**
 * Asserts that a plan contains variables matching the provided regex (among others).
 */
case class ContainsRegexVariablesMatcher(expectedRegexes: Set[Regex]) extends VariablesMatcher {
  override val expected: Set[String] = expectedRegexes.map(_.toString())

  override def apply(plan: InternalPlanDescription): MatchResult = {
    val vars = planVars(plan)
    MatchResult(
      matches = expectedRegexes.forall(regex => vars.exists(variable => regex.pattern.matcher(variable).matches())),
      rawFailureMessage = s"Expected ${plan.name} to contain variables matching $expected but got $vars.",
      rawNegatedFailureMessage = s"Expected ${plan.name} not to contain variables matching $expected."
    )
  }
}
