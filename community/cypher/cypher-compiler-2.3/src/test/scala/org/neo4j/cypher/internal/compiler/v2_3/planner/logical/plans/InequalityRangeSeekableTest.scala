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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.helpers.NonEmptyList
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{Bound, ExclusiveBound, InclusiveBound}

class InequalityRangeSeekableTest extends CypherFunSuite with AstConstructionTestSupport {

  val identifier = ident("n")
  val propertyKeyName: PropertyKeyName = PropertyKeyName("prop")_
  val property: Property = Property(identifier, propertyKeyName)_

  test("Constructs RangeLessThan") {
    valueRangeSeekable(lessThan(1)).range should equal(RangeLessThan(NonEmptyList(exclusive(1))))
    valueRangeSeekable(lessThanOrEqual(3)).range should equal(RangeLessThan(NonEmptyList(inclusive(3))))
    valueRangeSeekable(lessThan(1), lessThan(2), lessThanOrEqual(3)).range should equal(RangeLessThan(NonEmptyList(exclusive(1), exclusive(2), inclusive(3))))
  }

  test("Constructs RangeGreaterThan") {
    valueRangeSeekable(greaterThan(1)).range should equal(RangeGreaterThan(NonEmptyList(exclusive(1))))
    valueRangeSeekable(greaterThanOrEqual(3)).range should equal(RangeGreaterThan(NonEmptyList(inclusive(3))))
    valueRangeSeekable(greaterThan(1), greaterThan(2), greaterThanOrEqual(3)).range should equal(RangeGreaterThan(NonEmptyList(exclusive(1), exclusive(2), inclusive(3))))
  }

  test("Constructs RangeBetween") {
    valueRangeSeekable(lessThan(1), greaterThan(1)).range should equal(RangeBetween(RangeGreaterThan(NonEmptyList(exclusive(1))), RangeLessThan(NonEmptyList(exclusive(1)))))
    valueRangeSeekable(greaterThan(1), lessThan(1)).range should equal(RangeBetween(RangeGreaterThan(NonEmptyList(exclusive(1))), RangeLessThan(NonEmptyList(exclusive(1)))))

    valueRangeSeekable(lessThanOrEqual(1), greaterThanOrEqual(1)).range should equal(RangeBetween(RangeGreaterThan(NonEmptyList(inclusive(1))), RangeLessThan(NonEmptyList(inclusive(1)))))
    valueRangeSeekable(greaterThanOrEqual(1), lessThanOrEqual(1)).range should equal(RangeBetween(RangeGreaterThan(NonEmptyList(inclusive(1))), RangeLessThan(NonEmptyList(inclusive(1)))))
  }

  private def inclusive(v: Int): Bound[Expression] =
    InclusiveBound(SignedDecimalIntegerLiteral(v.toString)_)

  private def exclusive(v: Int): Bound[Expression] =
    ExclusiveBound(SignedDecimalIntegerLiteral(v.toString)_)

  private def lessThan(v: Int): InequalityExpression =
    LessThan(property, SignedDecimalIntegerLiteral(v.toString)_)(pos)

  private def lessThanOrEqual(v: Int): InequalityExpression =
    LessThanOrEqual(property, SignedDecimalIntegerLiteral(v.toString)_)(pos)

  private def greaterThan(v: Int): InequalityExpression =
    GreaterThan(property, SignedDecimalIntegerLiteral(v.toString)_)(pos)

  private def greaterThanOrEqual(v: Int): InequalityExpression =
    GreaterThanOrEqual(property, SignedDecimalIntegerLiteral(v.toString)_)(pos)

  private def valueRangeSeekable(first: InequalityExpression, others: InequalityExpression*) = {
    val inequalities = NonEmptyList(first, others: _*)
    InequalityRangeSeekable(
      identifier,
      propertyKeyName,
      AndedPropertyInequalities(identifier, property, inequalities)
    )
  }
}
