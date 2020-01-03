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
package org.neo4j.cypher.planmatching

import org.neo4j.cypher.internal.runtime.planDescription.renderAsTreeTable.UNNAMED_PATTERN
import org.neo4j.cypher.internal.runtime.planDescription.{Argument, InternalPlanDescription, PlanDescriptionArgumentSerializer}
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.util.matching.Regex

/**
  * Asserts that the arguments of a PlanDescription (of which some are printed in the "Other" column, while some have their own columns)
  * contain some arguments, as serialized with [[PlanDescriptionArgumentSerializer.serialize()]]
  */
trait StringArgumentsMatcher extends Matcher[InternalPlanDescription] {
  val expected: Set[String]
  def planArgs(plan: InternalPlanDescription): Set[String] = plan.arguments.toSet.map { arg: Argument =>
    PlanDescriptionArgumentSerializer.serialize(arg).toString.replaceAll(UNNAMED_PATTERN, "")
  }
}

/**
  * Asserts that the arguments contain all of what is provided as `expected`
  */
case class ContainsExactStringArgumentsMatcher(expected: Set[String]) extends StringArgumentsMatcher {
  override def apply(plan: InternalPlanDescription): MatchResult = {
    val args = planArgs(plan)
    MatchResult(
      matches = expected.subsetOf(args),
      rawFailureMessage = s"Expected ${plan.name} to contain arguments $expected but got $args.",
      rawNegatedFailureMessage = s"Expected ${plan.name} not to contain arguments $expected."
    )
  }
}

/**
  * Asserts that the arguments contain all of what is provided as `expectedRegexes` by regex matching.
  */
case class ContainsRegexStringArgumentsMatcher(expectedRegexes: Set[Regex]) extends StringArgumentsMatcher {
  override val expected: Set[String] = expectedRegexes.map(_.toString())

  override def apply(plan: InternalPlanDescription): MatchResult = {
    val args = planArgs(plan)
    MatchResult(
      matches = expectedRegexes.forall(regex => args.exists(arg => regex.pattern.matcher(arg).matches())),
      rawFailureMessage = s"Expected ${plan.name} to contain arguments $expected but got $args.",
      rawNegatedFailureMessage = s"Expected ${plan.name} not to contain arguments $expected."
    )
  }
}
