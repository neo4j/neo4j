/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_3.symbols

import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite

class CypherTypeTest extends CypherFunSuite {
  test("parents should be full path up type tree branch") {
    CTInteger.parents should equal(Seq(CTNumber, CTAny))
    CTNumber.parents should equal(Seq(CTAny))
    CTAny.parents should equal(Seq())
    CTList(CTString).parents should equal(Seq(CTList(CTAny), CTAny))
  }

  test("foo") {
    val covariant = CTGraphRef.covariant
    val s = covariant.toString
    covariant should not be empty
  }

  test("should be assignable from sub-type") {
    CTNumber.isAssignableFrom(CTInteger) should equal(true)
    CTAny.isAssignableFrom(CTString) should equal(true)
    CTList(CTString).isAssignableFrom(CTList(CTString)) should equal(true)
    CTList(CTNumber).isAssignableFrom(CTList(CTInteger)) should equal(true)
    CTInteger.isAssignableFrom(CTNumber) should equal(false)
    CTList(CTInteger).isAssignableFrom(CTList(CTString)) should equal(false)
  }

  test("should find leastUpperBound") {
    assertLeastUpperBound(CTNumber, CTNumber, CTNumber)
    assertLeastUpperBound(CTNumber, CTAny, CTAny)
    assertLeastUpperBound(CTNumber, CTString, CTAny)
    assertLeastUpperBound(CTNumber, CTList(CTAny), CTAny)
    assertLeastUpperBound(CTInteger, CTFloat, CTNumber)
    assertLeastUpperBound(CTMap, CTFloat, CTAny)
  }

  private def assertLeastUpperBound(a: CypherType, b: CypherType, result: CypherType) {
    val simpleMergedType: CypherType = a leastUpperBound b
    simpleMergedType should equal(result)
    val listMergedType: CypherType = CTList(a) leastUpperBound CTList(b)
    listMergedType should equal(CTList(result))
  }

  test("should find greatestLowerBound") {
    assertGreatestLowerBound(CTNumber, CTNumber, Some(CTNumber))
    assertGreatestLowerBound(CTNumber, CTAny, Some(CTNumber))
    assertGreatestLowerBound(CTList(CTNumber), CTList(CTInteger), Some(CTList(CTInteger)))
    assertGreatestLowerBound(CTNumber, CTString, None)
    assertGreatestLowerBound(CTNumber, CTList(CTAny), None)
    assertGreatestLowerBound(CTInteger, CTFloat, None)
    assertGreatestLowerBound(CTMap, CTFloat, None)
    assertGreatestLowerBound(CTBoolean, CTList(CTAny), None)
  }

  private def assertGreatestLowerBound(a: CypherType, b: CypherType, result: Option[CypherType]) {
    val simpleMergedType: Option[CypherType] = a greatestLowerBound b
    simpleMergedType should equal(result)
    val listMergedType: Option[CypherType] = CTList(a) greatestLowerBound CTList(b)
    listMergedType should equal(for (t <- result) yield CTList(t))
  }
}
