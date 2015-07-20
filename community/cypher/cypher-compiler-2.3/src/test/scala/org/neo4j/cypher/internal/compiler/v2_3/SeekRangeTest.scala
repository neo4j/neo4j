/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.internal.compiler.v2_3.helpers.NonEmptyList
import org.neo4j.cypher.internal.compiler.v2_3.test_helpers.CypherFunSuite

class SeekRangeTest extends CypherFunSuite {

  implicit val ordering = MinMaxOrdering.BY_NUMBER

  test("Computes correct limit for less than") {
    RangeLessThan[Number](NonEmptyList(InclusiveBound(3), InclusiveBound(4))).limit should equal(Some(InclusiveBound(3)))
    RangeLessThan[Number](NonEmptyList(InclusiveBound(3), ExclusiveBound(4))).limit should equal(Some(InclusiveBound(3)))
    RangeLessThan[Number](NonEmptyList(ExclusiveBound(3), InclusiveBound(4))).limit should equal(Some(ExclusiveBound(3)))
    RangeLessThan[Number](NonEmptyList(ExclusiveBound(3), InclusiveBound(3))).limit should equal(Some(ExclusiveBound(3)))
    RangeLessThan[Number](NonEmptyList(ExclusiveBound(3), InclusiveBound(null))).limit should equal(None)
    RangeLessThan[Number](NonEmptyList(ExclusiveBound(null), InclusiveBound(null))).limit should equal(None)
  }

  test("Computes inclusion for less than") {
    RangeLessThan[Number](NonEmptyList(InclusiveBound(3))).includes[Number](2) should be(right = true)
    RangeLessThan[Number](NonEmptyList(ExclusiveBound(3))).includes[Number](2) should be(right = true)
    RangeLessThan[Number](NonEmptyList(InclusiveBound(3))).includes[Number](3) should be(right = true)
    RangeLessThan[Number](NonEmptyList(ExclusiveBound(3))).includes[Number](3) should be(right = false)
    RangeLessThan[Number](NonEmptyList(InclusiveBound(3))).includes[Number](4) should be(right = false)
    RangeLessThan[Number](NonEmptyList(ExclusiveBound(3))).includes[Number](4) should be(right = false)

    RangeLessThan[Number](NonEmptyList(InclusiveBound(null))).includes[Number](null) should be(right = false)
    RangeLessThan[Number](NonEmptyList(ExclusiveBound(null))).includes[Number](null) should be(right = false)
    RangeLessThan[Number](NonEmptyList(InclusiveBound(null))).includes[Number](3) should be(right = false)
    RangeLessThan[Number](NonEmptyList(ExclusiveBound(null))).includes[Number](3) should be(right = false)
    RangeLessThan[Number](NonEmptyList(InclusiveBound(3))).includes[Number](null) should be(right = false)
    RangeLessThan[Number](NonEmptyList(ExclusiveBound(3))).includes[Number](null) should be(right = false)
  }

  test("Computes correct limit for greater than") {
    RangeGreaterThan[Number](NonEmptyList(InclusiveBound(3), InclusiveBound(4))).limit should equal(Some(InclusiveBound(4)))
    RangeGreaterThan[Number](NonEmptyList(InclusiveBound(3), ExclusiveBound(4))).limit should equal(Some(ExclusiveBound(4)))
    RangeGreaterThan[Number](NonEmptyList(ExclusiveBound(3), InclusiveBound(4))).limit should equal(Some(InclusiveBound(4)))
    RangeGreaterThan[Number](NonEmptyList(ExclusiveBound(3), InclusiveBound(3))).limit should equal(Some(ExclusiveBound(3)))
    RangeGreaterThan[Number](NonEmptyList(ExclusiveBound(3), InclusiveBound(null))).limit should equal(None)
    RangeGreaterThan[Number](NonEmptyList(ExclusiveBound(null), InclusiveBound(null))).limit should equal(None)
  }

  test("Computes inclusion for greater than") {
    RangeGreaterThan[Number](NonEmptyList(InclusiveBound(3))).includes[Number](2) should be(right = false)
    RangeGreaterThan[Number](NonEmptyList(ExclusiveBound(3))).includes[Number](2) should be(right = false)
    RangeGreaterThan[Number](NonEmptyList(InclusiveBound(3))).includes[Number](3) should be(right = true)
    RangeGreaterThan[Number](NonEmptyList(ExclusiveBound(3))).includes[Number](3) should be(right = false)
    RangeGreaterThan[Number](NonEmptyList(InclusiveBound(3))).includes[Number](4) should be(right = true)
    RangeGreaterThan[Number](NonEmptyList(ExclusiveBound(3))).includes[Number](4) should be(right = true)

    RangeGreaterThan[Number](NonEmptyList(InclusiveBound(null))).includes[Number](null) should be(right = false)
    RangeGreaterThan[Number](NonEmptyList(ExclusiveBound(null))).includes[Number](null) should be(right = false)
    RangeGreaterThan[Number](NonEmptyList(InclusiveBound(null))).includes[Number](3) should be(right = false)
    RangeGreaterThan[Number](NonEmptyList(ExclusiveBound(null))).includes[Number](3) should be(right = false)
    RangeGreaterThan[Number](NonEmptyList(InclusiveBound(3))).includes[Number](null) should be(right = false)
    RangeGreaterThan[Number](NonEmptyList(ExclusiveBound(3))).includes[Number](null) should be(right = false)
  }

  test("Computes inclusion for range between") {
    val range = RangeBetween(
      RangeGreaterThan[Number](NonEmptyList(InclusiveBound(3))),
      RangeLessThan[Number](NonEmptyList(ExclusiveBound(5)))
    )

    range.includes[Number](2) should be(right = false)
    range.includes[Number](3) should be(right = true)
    range.includes[Number](4) should be(right = true)
    range.includes[Number](5) should be(right = false)
    range.includes[Number](6) should be(right = false)

    RangeBetween(
      RangeGreaterThan[Number](NonEmptyList(InclusiveBound(null))),
      RangeLessThan[Number](NonEmptyList(ExclusiveBound(5)))
    ).includes[Number](4) should be(right = false)

    RangeBetween(
      RangeGreaterThan[Number](NonEmptyList(InclusiveBound(3))),
      RangeLessThan[Number](NonEmptyList(ExclusiveBound(null)))
    ).includes[Number](4) should be(right = false)

    range.includes[Number](null) should be(right = false)
  }
}
