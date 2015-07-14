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

  implicit val ordering = CypherValueOrdering

  test("Computes correct limit for less than") {
    RangeLessThan(NonEmptyList(InclusiveBound(3), InclusiveBound(4))).limit should equal(Some(InclusiveBound(3)))
    RangeLessThan(NonEmptyList(InclusiveBound(3), ExclusiveBound(4))).limit should equal(Some(InclusiveBound(3)))
    RangeLessThan(NonEmptyList(ExclusiveBound(3), InclusiveBound(4))).limit should equal(Some(ExclusiveBound(3)))
    RangeLessThan(NonEmptyList(ExclusiveBound(3), InclusiveBound(3))).limit should equal(Some(ExclusiveBound(3)))
    RangeLessThan(NonEmptyList(ExclusiveBound(3), InclusiveBound(null))).limit should equal(None)
    RangeLessThan(NonEmptyList(ExclusiveBound(null), InclusiveBound(null))).limit should equal(None)
  }

  test("Computes inclusion for less than") {
    RangeLessThan(NonEmptyList(InclusiveBound(3))).includes(2) should be(right = true)
    RangeLessThan(NonEmptyList(ExclusiveBound(3))).includes(2) should be(right = true)
    RangeLessThan(NonEmptyList(InclusiveBound(3))).includes(3) should be(right = true)
    RangeLessThan(NonEmptyList(ExclusiveBound(3))).includes(3) should be(right = false)
    RangeLessThan(NonEmptyList(InclusiveBound(3))).includes(4) should be(right = false)
    RangeLessThan(NonEmptyList(ExclusiveBound(3))).includes(4) should be(right = false)

    RangeLessThan(NonEmptyList(InclusiveBound(null))).includes(null) should be(right = false)
    RangeLessThan(NonEmptyList(ExclusiveBound(null))).includes(null) should be(right = false)
    RangeLessThan(NonEmptyList(InclusiveBound(null))).includes(3) should be(right = false)
    RangeLessThan(NonEmptyList(ExclusiveBound(null))).includes(3) should be(right = false)
    RangeLessThan(NonEmptyList(InclusiveBound(3))).includes(null) should be(right = false)
    RangeLessThan(NonEmptyList(ExclusiveBound(3))).includes(null) should be(right = false)
  }

  test("Computes correct limit for greater than") {
    RangeGreaterThan(NonEmptyList(InclusiveBound(3), InclusiveBound(4))).limit should equal(Some(InclusiveBound(4)))
    RangeGreaterThan(NonEmptyList(InclusiveBound(3), ExclusiveBound(4))).limit should equal(Some(ExclusiveBound(4)))
    RangeGreaterThan(NonEmptyList(ExclusiveBound(3), InclusiveBound(4))).limit should equal(Some(InclusiveBound(4)))
    RangeGreaterThan(NonEmptyList(ExclusiveBound(3), InclusiveBound(3))).limit should equal(Some(ExclusiveBound(3)))
    RangeGreaterThan(NonEmptyList(ExclusiveBound(3), InclusiveBound(null))).limit should equal(None)
    RangeGreaterThan(NonEmptyList(ExclusiveBound(null), InclusiveBound(null))).limit should equal(None)
  }

  test("Computes inclusion for greater than") {
    RangeGreaterThan(NonEmptyList(InclusiveBound(3))).includes(2) should be(right = false)
    RangeGreaterThan(NonEmptyList(ExclusiveBound(3))).includes(2) should be(right = false)
    RangeGreaterThan(NonEmptyList(InclusiveBound(3))).includes(3) should be(right = true)
    RangeGreaterThan(NonEmptyList(ExclusiveBound(3))).includes(3) should be(right = false)
    RangeGreaterThan(NonEmptyList(InclusiveBound(3))).includes(4) should be(right = true)
    RangeGreaterThan(NonEmptyList(ExclusiveBound(3))).includes(4) should be(right = true)

    RangeGreaterThan(NonEmptyList(InclusiveBound(null))).includes(null) should be(right = false)
    RangeGreaterThan(NonEmptyList(ExclusiveBound(null))).includes(null) should be(right = false)
    RangeGreaterThan(NonEmptyList(InclusiveBound(null))).includes(3) should be(right = false)
    RangeGreaterThan(NonEmptyList(ExclusiveBound(null))).includes(3) should be(right = false)
    RangeGreaterThan(NonEmptyList(InclusiveBound(3))).includes(null) should be(right = false)
    RangeGreaterThan(NonEmptyList(ExclusiveBound(3))).includes(null) should be(right = false)
  }

  test("Computes inclusion for range between") {
    val range = RangeBetween(
      RangeGreaterThan(NonEmptyList(InclusiveBound(3))),
      RangeLessThan(NonEmptyList(ExclusiveBound(5)))
    )

    range.includes(2) should be(right = false)
    range.includes(3) should be(right = true)
    range.includes(4) should be(right = true)
    range.includes(5) should be(right = false)
    range.includes(6) should be(right = false)

    RangeBetween(
      RangeGreaterThan(NonEmptyList(InclusiveBound(null))),
      RangeLessThan(NonEmptyList(ExclusiveBound(5)))
    ).includes(4) should be(right = false)

    RangeBetween(
      RangeGreaterThan(NonEmptyList(InclusiveBound(3))),
      RangeLessThan(NonEmptyList(ExclusiveBound(null)))
    ).includes(4) should be(right = false)

    range.includes(null) should be(right = false)
  }
}
