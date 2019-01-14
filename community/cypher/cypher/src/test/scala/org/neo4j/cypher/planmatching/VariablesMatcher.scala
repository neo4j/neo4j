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

/**
  * Asserts that a plan has certain variables
  */
trait VariablesMatcher extends Matcher[InternalPlanDescription] {
  val expected: Set[String]
}

/**
  * Asserts that a plan has exactly the provided variables.
  */
case class ExactVariablesMatcher(expected: Set[String]) extends VariablesMatcher {
  override def apply(plan: InternalPlanDescription): MatchResult = {
    MatchResult(
      matches = plan.variables == expected,
      rawFailureMessage = s"Expected ${plan.name} to have variables $expected but got ${plan.variables}.",
      rawNegatedFailureMessage = s"Expected ${plan.name} not to have variables $expected."
    )
  }
}

/**
  * Asserts that a plan contains the provided variables (among others).
  */
case class ContainsVariablesMatcher(expected: Set[String]) extends VariablesMatcher {
  override def apply(plan: InternalPlanDescription): MatchResult = {
    MatchResult(
      matches = expected.subsetOf(plan.variables),
      rawFailureMessage = s"Expected ${plan.name} to contain variables $expected but got ${plan.variables}.",
      rawNegatedFailureMessage = s"Expected ${plan.name} not to contain variables $expected."
    )
  }
}
