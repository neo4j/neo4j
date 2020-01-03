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
package org.neo4j.cypher.internal.compiler.planner.logical.plans

import org.neo4j.cypher.internal.v4_0.util.NonEmptyList
import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.logical.plans._

class InequalityRangeSeekableTest extends CypherFunSuite with AstConstructionTestSupport {
  test("Constructs RangeLessThan") {
    valueRangeSeekable(
      propLessThan("n", "prop", 1)
    ).range should equal(RangeLessThan(NonEmptyList(exclusive(1))))

    valueRangeSeekable(
      lessThanOrEqual(prop("n", "prop"), literalInt(3))
    ).range should equal(RangeLessThan(NonEmptyList(inclusive(3))))

    valueRangeSeekable(
      propLessThan("n", "prop", 1),
      propLessThan("n", "prop", 2),
      lessThanOrEqual(prop("n", "prop"), literalInt(3))
    ).range should equal(RangeLessThan(NonEmptyList(exclusive(1), exclusive(2), inclusive(3))))
  }

  test("Constructs RangeGreaterThan") {
    valueRangeSeekable(
      propGreaterThan("n", "prop", 1)
    ).range should equal(RangeGreaterThan(NonEmptyList(exclusive(1))))

    valueRangeSeekable(
      greaterThanOrEqual(prop("n", "prop"), literalInt(3))
    ).range should equal(RangeGreaterThan(NonEmptyList(inclusive(3))))

    valueRangeSeekable(
      propGreaterThan("n", "prop", 1),
      propGreaterThan("n", "prop", 2),
      greaterThanOrEqual(prop("n", "prop"), literalInt(3))
    ).range should equal(RangeGreaterThan(NonEmptyList(exclusive(1), exclusive(2), inclusive(3))))
  }

  test("Constructs RangeBetween") {
    valueRangeSeekable(
      propLessThan("n", "prop", 1),
      propGreaterThan("n", "prop", 1)
    ).range should equal(RangeBetween(
      RangeGreaterThan(NonEmptyList(exclusive(1))),
      RangeLessThan(NonEmptyList(exclusive(1))))
    )

    valueRangeSeekable(
      propGreaterThan("n", "prop", 1),
      propLessThan("n", "prop", 1)
    ).range should equal(RangeBetween(
      RangeGreaterThan(NonEmptyList(exclusive(1))),
      RangeLessThan(NonEmptyList(exclusive(1))))
    )

    valueRangeSeekable(
      lessThanOrEqual(prop("n", "prop"), literalInt(1)),
      greaterThanOrEqual(prop("n", "prop"), literalInt(1))
    ).range should equal(RangeBetween(
      RangeGreaterThan(NonEmptyList(inclusive(1))),
      RangeLessThan(NonEmptyList(inclusive(1))))
    )

    valueRangeSeekable(
      greaterThanOrEqual(prop("n", "prop"), literalInt(1)),
      lessThanOrEqual(prop("n", "prop"), literalInt(1))
    ).range should equal(RangeBetween(
      RangeGreaterThan(NonEmptyList(inclusive(1))),
      RangeLessThan(NonEmptyList(inclusive(1))))
    )
  }

  private def inclusive(v: Int): Bound[Expression] = InclusiveBound(literalInt(v))

  private def exclusive(v: Int): Bound[Expression] = ExclusiveBound(literalInt(v))

  private def valueRangeSeekable(first: InequalityExpression, others: InequalityExpression*) = {
    val inequalities = NonEmptyList(first, others: _*)
    InequalityRangeSeekable(
      varFor("n"),
      PropertyKeyName("prop")_,
      AndedPropertyInequalities(varFor("n"), prop("n", "prop"), inequalities)
    )
  }
}
