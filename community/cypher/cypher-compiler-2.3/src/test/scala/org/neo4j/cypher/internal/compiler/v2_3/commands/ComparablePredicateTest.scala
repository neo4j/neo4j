/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_3.commands

import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates._
import org.neo4j.cypher.internal.compiler.v2_3.{CypherOrdering, ExecutionContext}
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Literal
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryStateHelper
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class ComparablePredicateTest extends CypherFunSuite {

  val numericalValues = Seq[Number](
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
    Double.NaN
//    null TODO
  ).flatMap {
    case v: Number => if (v == null) Seq(v) else Seq[Number](v.doubleValue(), v.floatValue(), v.longValue(), v.intValue(), v.shortValue(), v.byteValue(), v)
  }

  val textualValues = Seq(
    "",
    "Hal",
    s"Hal${Character.MIN_VALUE}",
    "Hallo",
    "Hallo!",
    "Hello",
    "Hullo",
//    null, TODO
    "\uD801\uDC37"
  ).flatMap {
    case v: String => if (v == null) Seq(v) else Seq(v, v.toUpperCase, v.toLowerCase, reverse(v))
  }

  test("should compare numerical values using <") {
    for (left <- numericalValues)
      for (right <- numericalValues)
        actualLessThan(left, right) should equal(CypherOrdering.DEFAULT.compare(left, right) < 0)
  }

  test("should compare numerical values using <=") {
    for (left <- numericalValues)
      for (right <- numericalValues)
        actualLessThanOrEqual(left, right) should equal(CypherOrdering.DEFAULT.compare(left, right) <= 0)
  }

  test("should compare numerical values using >") {
    for (left <- numericalValues)
      for (right <- numericalValues)
        actualGreaterThan(left, right) should equal(CypherOrdering.DEFAULT.compare(left, right) > 0)
  }

  test("should compare numerical values using >=") {
    for (left <- numericalValues)
      for (right <- numericalValues)
        actualGreaterThanOrEqual(left, right) should equal(CypherOrdering.DEFAULT.compare(left, right) >= 0)
  }

  test("should compare textual values using <") {
    for (left <- textualValues)
      for (right <- textualValues)
        actualLessThan(left, right) should equal(CypherOrdering.DEFAULT.compare(left, right) < 0)
  }

  test("should compare textual values using <=") {
    for (left <- textualValues)
      for (right <- textualValues)
        actualLessThanOrEqual(left, right) should equal(CypherOrdering.DEFAULT.compare(left, right) <= 0)
  }

  test("should compare textual values using >") {
    for (left <- textualValues)
      for (right <- textualValues)
        actualGreaterThan(left, right) should equal(CypherOrdering.DEFAULT.compare(left, right) > 0)
  }

  test("should compare textual values using >=") {
    for (left <- textualValues)
      for (right <- textualValues)
        actualGreaterThanOrEqual(left, right) should equal(CypherOrdering.DEFAULT.compare(left, right) >= 0)
  }

  private def actualLessThan(left: Any, right: Any) =
    LessThan(Literal(left), Literal(right)).isTrue(ExecutionContext.empty)(QueryStateHelper.empty)

  private def actualLessThanOrEqual(left: Any, right: Any) =
    LessThanOrEqual(Literal(left), Literal(right)).isTrue(ExecutionContext.empty)(QueryStateHelper.empty)

  private def actualEqual(left: Any, right: Any) =
    Equals(Literal(left), Literal(right)).isTrue(ExecutionContext.empty)(QueryStateHelper.empty)

  private def actualGreaterThan(left: Any, right: Any) =
    GreaterThan(Literal(left), Literal(right)).isTrue(ExecutionContext.empty)(QueryStateHelper.empty)

  private def actualGreaterThanOrEqual(left: Any, right: Any) =
    GreaterThanOrEqual(Literal(left), Literal(right)).isTrue(ExecutionContext.empty)(QueryStateHelper.empty)

  private def reverse(s: String) = new StringBuilder(s).reverse.toString()
}




