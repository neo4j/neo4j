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
package org.neo4j.cypher.internal.frontend.v2_3.symbols

import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class CypherTypeTest extends CypherFunSuite {
  test("parents should be full path up type tree branch") {
    CTInteger.parents should equal(Seq(CTNumber, CTAny))
    CTNumber.parents should equal(Seq(CTAny))
    CTAny.parents should equal(Seq())
    CTCollection(CTString).parents should equal(Seq(CTCollection(CTAny), CTAny))
  }

  test("should be assignable from sub-type") {
    CTNumber.isAssignableFrom(CTInteger) should equal(true)
    CTAny.isAssignableFrom(CTString) should equal(true)
    CTCollection(CTString).isAssignableFrom(CTCollection(CTString)) should equal(true)
    CTCollection(CTNumber).isAssignableFrom(CTCollection(CTInteger)) should equal(true)
    CTInteger.isAssignableFrom(CTNumber) should equal(false)
    CTCollection(CTInteger).isAssignableFrom(CTCollection(CTString)) should equal(false)
  }

  test("should find leastUpperBound") {
    assertLeastUpperBound(CTNumber, CTNumber, CTNumber)
    assertLeastUpperBound(CTNumber, CTAny, CTAny)
    assertLeastUpperBound(CTNumber, CTString, CTAny)
    assertLeastUpperBound(CTNumber, CTCollection(CTAny), CTAny)
    assertLeastUpperBound(CTInteger, CTFloat, CTNumber)
    assertLeastUpperBound(CTMap, CTFloat, CTAny)
  }

  private def assertLeastUpperBound(a: CypherType, b: CypherType, result: CypherType) {
    val simpleMergedType: CypherType = a leastUpperBound b
    simpleMergedType should equal(result)
    val collectionMergedType: CypherType = CTCollection(a) leastUpperBound CTCollection(b)
    collectionMergedType should equal(CTCollection(result))
  }

  test("should find greatestLowerBound") {
    assertGreatestLowerBound(CTNumber, CTNumber, Some(CTNumber))
    assertGreatestLowerBound(CTNumber, CTAny, Some(CTNumber))
    assertGreatestLowerBound(CTCollection(CTNumber), CTCollection(CTInteger), Some(CTCollection(CTInteger)))
    assertGreatestLowerBound(CTNumber, CTString, None)
    assertGreatestLowerBound(CTNumber, CTCollection(CTAny), None)
    assertGreatestLowerBound(CTInteger, CTFloat, None)
    assertGreatestLowerBound(CTMap, CTFloat, None)
    assertGreatestLowerBound(CTBoolean, CTCollection(CTAny), None)
  }

  private def assertGreatestLowerBound(a: CypherType, b: CypherType, result: Option[CypherType]) {
    val simpleMergedType: Option[CypherType] = a greatestLowerBound b
    simpleMergedType should equal(result)
    val collectionMergedType: Option[CypherType] = CTCollection(a) greatestLowerBound CTCollection(b)
    collectionMergedType should equal(for (t <- result) yield CTCollection(t))
  }
}
