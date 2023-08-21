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
package org.neo4j.cypher.internal.runtime.interpreted.commands

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper.literal
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.ComparablePredicate
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.GreaterThan
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.GreaterThanOrEqual
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.LessThan
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.LessThanOrEqual
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.AnyValues
import org.neo4j.values.storable.Values
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

class ComparablePredicateTest extends CypherFunSuite {

  private val numericalValues: Seq[AnyRef] = Seq[Number](
    Double.NegativeInfinity,
    Double.MinValue,
    Long.MinValue,
    -1,
    -0.5,
    0,
    Double.MinPositiveValue,
    0.5,
    1,
    10.00,
    10.33,
    10.66,
    11.00,
    Math.PI,
    Long.MaxValue,
    Double.MaxValue,
    Double.PositiveInfinity,
    null
  ).flatMap {
    case null                               => Seq(null)
    case v: Number if v.doubleValue().isNaN => Seq[Number](v.doubleValue(), v.floatValue(), v)
    case v: Number =>
      Seq[Number](v.doubleValue(), v.floatValue(), v.longValue(), v.intValue(), v.shortValue(), v.byteValue(), v)
  }

  private val textualValues: Seq[String] = Seq(
    "",
    "Hal",
    s"Hal${Character.MIN_VALUE}",
    "Hallo",
    "Hallo!",
    "Hello",
    "Hullo",
    null,
    "\uD801\uDC37"
  ).flatMap {
    case null      => Seq(null)
    case v: String => Seq(v, v.toUpperCase, v.toLowerCase, reverse(v))
  }

  private val allValues = numericalValues ++ textualValues

  test("should compare values using <") {
    for (left <- allValues)
      for (right <- allValues)
        LessThan(literal(left), literal(right)) should compareUsingLessThan(left, right)
  }

  test("should compare values using <=") {
    for (left <- allValues)
      for (right <- allValues)
        LessThanOrEqual(literal(left), literal(right)) should compareUsingLessThanOrEqual(left, right)
  }

  test("should compare values using >") {
    for (left <- allValues)
      for (right <- allValues)
        GreaterThan(literal(left), literal(right)) should compareUsingGreaterThan(left, right)
  }

  test("should compare values using >=") {
    for (left <- allValues)
      for (right <- allValues)
        GreaterThanOrEqual(literal(left), literal(right)) should compareUsingGreaterThanOrEqual(left, right)
  }

  private def reverse(s: String) = new StringBuilder(s).reverse.toString()

  case class compareUsingLessThan(left: Any, right: Any) extends compareUsing(left, right, "<")

  case class compareUsingLessThanOrEqual(left: Any, right: Any) extends compareUsing(left, right, "<=")

  case class compareUsingGreaterThanOrEqual(left: Any, right: Any) extends compareUsing(left, right, ">=")

  case class compareUsingGreaterThan(left: Any, right: Any) extends compareUsing(left, right, ">")

  class compareUsing(left: Any, right: Any, operator: String) extends Matcher[ComparablePredicate] {

    def apply(predicate: ComparablePredicate): MatchResult = {
      val actual = predicate.isMatch(CypherRow.empty, QueryStateHelper.empty)

      if (isIncomparable(left, right))
        buildResult(!actual.isKnown, "null", actual)
      else {
        assert(actual.isKnown, s"$left $operator $right")
        val expected = AnyValues.COMPARATOR.compare(Values.of(left), Values.of(right))
        val result = operator match {
          case "<"  => (expected < 0) == actual.asBoolean
          case "<=" => (expected <= 0) == actual.asBoolean
          case ">=" => (expected >= 0) == actual.asBoolean
          case ">"  => (expected > 0) == actual.asBoolean
        }
        buildResult(result, expected, actual)
      }
    }

    def isIncomparable(left: Any, right: Any): Boolean = {
      left == null || (left.isInstanceOf[Number] && left.asInstanceOf[Number].doubleValue().isNaN) ||
      right == null || (right.isInstanceOf[Number] && right.asInstanceOf[Number].doubleValue().isNaN) ||
      left.isInstanceOf[Number] && right.isInstanceOf[String] ||
      left.isInstanceOf[String] && right.isInstanceOf[Number]
    }

    def buildResult(result: Boolean, expected: Any, actual: Any) = {
      MatchResult(
        result,
        s"Expected $left $operator $right to compare as $expected but it was $actual",
        s"Expected $left $operator $right to not compare as $expected but it was $actual"
      )
    }
  }

}
