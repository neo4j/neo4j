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
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.cypher.internal.logical.plans.ExclusiveBound
import org.neo4j.cypher.internal.logical.plans.InclusiveBound
import org.neo4j.cypher.internal.logical.plans.MinMaxOrdering
import org.neo4j.cypher.internal.logical.plans.RangeGreaterThan
import org.neo4j.cypher.internal.logical.plans.RangeLessThan
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values

class SeekRangeTest extends CypherFunSuite {

  implicit private val BY_VALUE: MinMaxOrdering[Value] =
    MinMaxOrdering(Ordering.comparatorToOrdering(Values.COMPARATOR))

  private val numA = Values.longValue(3)
  private val numB = Values.longValue(4)
  private val strEmpty = Values.stringValue("")
  private val strA = Values.stringValue("3")
  private val strB = Values.stringValue("4")

  test("Computes correct limit for numerical less than") {
    RangeLessThan[Value](NonEmptyList(InclusiveBound(numA), InclusiveBound(numB))).limit should equal(
      Some(InclusiveBound(numA))
    )
    RangeLessThan[Value](NonEmptyList(InclusiveBound(numA), ExclusiveBound(numB))).limit should equal(
      Some(InclusiveBound(numA))
    )
    RangeLessThan[Value](NonEmptyList(ExclusiveBound(numA), InclusiveBound(numB))).limit should equal(
      Some(ExclusiveBound(numA))
    )
    RangeLessThan[Value](NonEmptyList(ExclusiveBound(numA), InclusiveBound(numA))).limit should equal(
      Some(ExclusiveBound(numA))
    )
    RangeLessThan[Value](NonEmptyList(ExclusiveBound(numA), InclusiveBound(null))).limit should equal(None)
    RangeLessThan[Value](NonEmptyList(ExclusiveBound(null), InclusiveBound(null))).limit should equal(None)
  }

  test("Computes correct limit for numerical greater than") {
    RangeGreaterThan[Value](NonEmptyList(InclusiveBound(numA), InclusiveBound(numB))).limit should equal(
      Some(InclusiveBound(numB))
    )
    RangeGreaterThan[Value](NonEmptyList(InclusiveBound(numA), ExclusiveBound(numB))).limit should equal(
      Some(ExclusiveBound(numB))
    )
    RangeGreaterThan[Value](NonEmptyList(ExclusiveBound(numA), InclusiveBound(numB))).limit should equal(
      Some(InclusiveBound(numB))
    )
    RangeGreaterThan[Value](NonEmptyList(ExclusiveBound(numA), InclusiveBound(numA))).limit should equal(
      Some(ExclusiveBound(numA))
    )
    RangeGreaterThan[Value](NonEmptyList(ExclusiveBound(numA), InclusiveBound(null))).limit should equal(None)
    RangeGreaterThan[Value](NonEmptyList(ExclusiveBound(null), InclusiveBound(null))).limit should equal(None)
  }

  test("Computes correct limit for string less than") {
    RangeLessThan[Value](NonEmptyList(InclusiveBound(strEmpty), InclusiveBound(strB))).limit should equal(
      Some(InclusiveBound(strEmpty))
    )
    RangeLessThan[Value](NonEmptyList(ExclusiveBound(strEmpty), InclusiveBound(strB))).limit should equal(
      Some(ExclusiveBound(strEmpty))
    )
    RangeLessThan[Value](NonEmptyList(InclusiveBound(strA), InclusiveBound(strB))).limit should equal(
      Some(InclusiveBound(strA))
    )
    RangeLessThan[Value](NonEmptyList(InclusiveBound(strA), ExclusiveBound(strB))).limit should equal(
      Some(InclusiveBound(strA))
    )
    RangeLessThan[Value](NonEmptyList(ExclusiveBound(strA), InclusiveBound(strB))).limit should equal(
      Some(ExclusiveBound(strA))
    )
    RangeLessThan[Value](NonEmptyList(ExclusiveBound(strA), InclusiveBound(strA))).limit should equal(
      Some(ExclusiveBound(strA))
    )
    RangeLessThan[Value](NonEmptyList(ExclusiveBound(strA), InclusiveBound(null))).limit should equal(None)
    RangeLessThan[Value](NonEmptyList(ExclusiveBound(null), InclusiveBound(null))).limit should equal(None)
  }

  test("Computes correct limit for string greater than") {
    RangeGreaterThan[Value](NonEmptyList(InclusiveBound(strEmpty), InclusiveBound(strB))).limit should equal(
      Some(InclusiveBound(strB))
    )
    RangeGreaterThan[Value](NonEmptyList(ExclusiveBound(strEmpty), InclusiveBound(strB))).limit should equal(
      Some(InclusiveBound(strB))
    )
    RangeGreaterThan[Value](NonEmptyList(InclusiveBound(strA), InclusiveBound(strB))).limit should equal(
      Some(InclusiveBound(strB))
    )
    RangeGreaterThan[Value](NonEmptyList(InclusiveBound(strA), ExclusiveBound(strB))).limit should equal(
      Some(ExclusiveBound(strB))
    )
    RangeGreaterThan[Value](NonEmptyList(ExclusiveBound(strA), InclusiveBound(strB))).limit should equal(
      Some(InclusiveBound(strB))
    )
    RangeGreaterThan[Value](NonEmptyList(ExclusiveBound(strA), InclusiveBound(strA))).limit should equal(
      Some(ExclusiveBound(strA))
    )
    RangeGreaterThan[Value](NonEmptyList(ExclusiveBound(strA), InclusiveBound(null))).limit should equal(None)
    RangeGreaterThan[Value](NonEmptyList(ExclusiveBound(null), InclusiveBound(null))).limit should equal(None)
  }
}
