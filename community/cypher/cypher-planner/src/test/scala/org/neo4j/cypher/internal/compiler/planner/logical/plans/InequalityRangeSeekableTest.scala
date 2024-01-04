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
package org.neo4j.cypher.internal.compiler.planner.logical.plans

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.expressions.AndedPropertyInequalities
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.InequalityExpression
import org.neo4j.cypher.internal.logical.plans.Bound
import org.neo4j.cypher.internal.logical.plans.ExclusiveBound
import org.neo4j.cypher.internal.logical.plans.InclusiveBound
import org.neo4j.cypher.internal.logical.plans.RangeBetween
import org.neo4j.cypher.internal.logical.plans.RangeGreaterThan
import org.neo4j.cypher.internal.logical.plans.RangeLessThan
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

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
      RangeLessThan(NonEmptyList(exclusive(1)))
    ))

    valueRangeSeekable(
      propGreaterThan("n", "prop", 1),
      propLessThan("n", "prop", 1)
    ).range should equal(RangeBetween(
      RangeGreaterThan(NonEmptyList(exclusive(1))),
      RangeLessThan(NonEmptyList(exclusive(1)))
    ))

    valueRangeSeekable(
      lessThanOrEqual(prop("n", "prop"), literalInt(1)),
      greaterThanOrEqual(prop("n", "prop"), literalInt(1))
    ).range should equal(RangeBetween(
      RangeGreaterThan(NonEmptyList(inclusive(1))),
      RangeLessThan(NonEmptyList(inclusive(1)))
    ))

    valueRangeSeekable(
      greaterThanOrEqual(prop("n", "prop"), literalInt(1)),
      lessThanOrEqual(prop("n", "prop"), literalInt(1))
    ).range should equal(RangeBetween(
      RangeGreaterThan(NonEmptyList(inclusive(1))),
      RangeLessThan(NonEmptyList(inclusive(1)))
    ))
  }

  private def inclusive(v: Int): Bound[Expression] = InclusiveBound(literalInt(v))

  private def exclusive(v: Int): Bound[Expression] = ExclusiveBound(literalInt(v))

  private def valueRangeSeekable(first: InequalityExpression, others: InequalityExpression*) = {
    val inequalities = NonEmptyList(first, others: _*)
    InequalityRangeSeekable(
      v"n",
      prop("n", "prop"),
      AndedPropertyInequalities(v"n", prop("n", "prop"), inequalities)
    )
  }
}
