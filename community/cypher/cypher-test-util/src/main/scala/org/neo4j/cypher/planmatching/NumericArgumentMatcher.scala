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

import org.neo4j.cypher.internal.plandescription.Arguments.DbHits
import org.neo4j.cypher.internal.plandescription.Arguments.EstimatedRows
import org.neo4j.cypher.internal.plandescription.Arguments.GlobalMemory
import org.neo4j.cypher.internal.plandescription.Arguments.Memory
import org.neo4j.cypher.internal.plandescription.Arguments.PageCacheHits
import org.neo4j.cypher.internal.plandescription.Arguments.PageCacheMisses
import org.neo4j.cypher.internal.plandescription.Arguments.Rows
import org.neo4j.cypher.internal.plandescription.Arguments.Time
import org.neo4j.cypher.internal.plandescription.InternalPlanDescription
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

/**
 * Matches a numeric argument from a PlanDescription, e.g. EstimatedRows.
 *
 * There are two types of inheriting types:
 * - abstract classes that specify how to match
 * - traits that specify what to match.
 *
 * They need to mixed together to get a fully functional matcher, for example
 * `new ExactArgumentMatcher(rows) with ActualRowsMatcher`
 */
trait NumericArgumentMatcher extends Matcher[InternalPlanDescription] {

  /**
   * Obtains the value of the argument from the plan description, if it exists
   */
  def maybeMatchingArgument(plan: InternalPlanDescription): Option[Long]

  /**
   * A string of the argument name to build cool error messages
   */
  val argString: String

  /**
   * A string of the expected value to build cool error messages
   */
  val argMatcherDesc: String

  /**
   * @param actualValue the actual value from the argument
   * @return if it matches the expected value
   */
  def matches(actualValue: Long): Boolean

  /**
   * Just for re-creating the PlanDescription for toString
   */
  val expectedValue: Long

  override def apply(plan: InternalPlanDescription): MatchResult = {
    maybeMatchingArgument(plan) match {
      case None => MatchResult(
          matches = false,
          rawFailureMessage = s"No $argString found.",
          rawNegatedFailureMessage = ""
        )
      case Some(amount) => MatchResult(
          matches = matches(amount),
          rawFailureMessage = s"Expected ${plan.name} to have $argMatcherDesc $argString but had $amount $argString.",
          rawNegatedFailureMessage = s"Expected ${plan.name} not to have $argMatcherDesc $argString."
        )
    }
  }
}

/**
 * Matches an Argument by exact comparison
 */
abstract class ExactArgumentMatcher(override val expectedValue: Long) extends NumericArgumentMatcher {
  override def matches(amount: Long): Boolean = expectedValue == amount

  override val argMatcherDesc: String = expectedValue.toString
}

/**
 * Matches an Argument by range between two values
 */
abstract class RangeArgumentMatcher(expectedAmountMin: Long, expectedAmountMax: Long) extends NumericArgumentMatcher {
  override val expectedValue: Long = expectedAmountMin

  override def matches(amount: Long): Boolean = expectedAmountMin <= amount && amount <= expectedAmountMax

  override val argMatcherDesc: String =
    if (expectedAmountMax == Long.MaxValue) {
      s"$expectedAmountMin or more"
    } else {
      s"between $expectedAmountMin and $expectedAmountMax"
    }
}

/**
 * Matches the actual rows from profiling
 */
trait ActualRowsMatcher extends NumericArgumentMatcher {
  override val argString: String = "rows"

  override def maybeMatchingArgument(plan: InternalPlanDescription): Option[Long] =
    plan.arguments.collectFirst {
      case Rows(value) => value
    }
}

/**
 * Macthes the estimated rows from planning
 */
trait EstimatedRowsMatcher extends NumericArgumentMatcher {
  override val argString: String = "estimated rows"

  override def maybeMatchingArgument(plan: InternalPlanDescription): Option[Long] =
    plan.arguments.collectFirst {
      case EstimatedRows(effectiveCardinality, _) => math.round(effectiveCardinality)
    }
}

/**
 * Matches the DB hits from profiling
 */
trait DBHitsMatcher extends NumericArgumentMatcher {
  override val argString: String = "DB hits"

  override def maybeMatchingArgument(plan: InternalPlanDescription): Option[Long] =
    plan.arguments.collectFirst {
      case DbHits(value) => value
    }
}

/**
 * Matches the Page cache hits from profiling
 */
trait PageCacheHitsMatcher extends NumericArgumentMatcher {
  override val argString: String = "Page Cache Hits"

  override def maybeMatchingArgument(plan: InternalPlanDescription): Option[Long] =
    plan.arguments.collectFirst {
      case PageCacheHits(value) => value
    }
}

/**
 * Matches the Page cache misses from profiling
 */
trait PageCacheMissesMatcher extends NumericArgumentMatcher {
  override val argString: String = "Page Cache Misses"

  override def maybeMatchingArgument(plan: InternalPlanDescription): Option[Long] =
    plan.arguments.collectFirst {
      case PageCacheMisses(value) => value
    }
}

/**
 * Matches the Time from profiling
 */
trait TimeMatcher extends NumericArgumentMatcher {
  override val argString: String = "Time"

  override def maybeMatchingArgument(plan: InternalPlanDescription): Option[Long] =
    plan.arguments.collectFirst {
      case Time(value) => value
    }
}

/**
 * Matches the Memory from profiling
 */
trait MemoryMatcher extends NumericArgumentMatcher {
  override val argString: String = "Memory"

  override def maybeMatchingArgument(plan: InternalPlanDescription): Option[Long] =
    plan.arguments.collectFirst {
      case Memory(value) => value
    }
}

/**
 * Matches the Global Memory from profiling
 */
trait GlobalMemoryMatcher extends NumericArgumentMatcher {
  override val argString: String = "GlobalMemory"

  override def maybeMatchingArgument(plan: InternalPlanDescription): Option[Long] =
    plan.arguments.collectFirst {
      case GlobalMemory(value) => value
    }
}
