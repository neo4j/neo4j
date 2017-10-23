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

class TypeRangeTest extends CypherFunSuite {

  test("TypeRange of single type should contain only that type") {
    val rangeOfInteger = TypeRange(CTInteger, CTInteger)
    rangeOfInteger.contains(CTInteger) should equal(true)
    rangeOfInteger.contains(CTNumber) should equal(false)
    rangeOfInteger.contains(CTFloat) should equal(false)
    rangeOfInteger.contains(CTString) should equal(false)
    rangeOfInteger.contains(CTAny) should equal(false)

    val rangeOfNumber = TypeRange(CTNumber, CTNumber)
    rangeOfNumber.contains(CTInteger) should equal(false)
    rangeOfNumber.contains(CTNumber) should equal(true)
    rangeOfNumber.contains(CTFloat) should equal(false)
    rangeOfNumber.contains(CTString) should equal(false)
    rangeOfNumber.contains(CTAny) should equal(false)

    val rangeOfListAny = TypeRange(CTList(CTAny), CTList(CTAny))
    rangeOfListAny.contains(CTInteger) should equal(false)
    rangeOfListAny.contains(CTNumber) should equal(false)
    rangeOfListAny.contains(CTString) should equal(false)
    rangeOfListAny.contains(CTList(CTString)) should equal(false)
    rangeOfListAny.contains(CTList(CTNumber)) should equal(false)
    rangeOfListAny.contains(CTList(CTAny)) should equal(true)
    rangeOfListAny.contains(CTAny) should equal(false)
  }

  test("unbounded TypeRange rooted at CTAny should contain all") {
    val rangeRootedAtAny = TypeRange(CTAny, None)
    rangeRootedAtAny.contains(CTAny) should equal(true)
    rangeRootedAtAny.contains(CTString) should equal(true)
    rangeRootedAtAny.contains(CTNumber) should equal(true)
    rangeRootedAtAny.contains(CTInteger) should equal(true)
    rangeRootedAtAny.contains(CTFloat) should equal(true)
    rangeRootedAtAny.contains(CTNode) should equal(true)
    rangeRootedAtAny.contains(CTList(CTAny)) should equal(true)
    rangeRootedAtAny.contains(CTList(CTFloat)) should equal(true)
    rangeRootedAtAny.contains(CTList(CTList(CTFloat))) should equal(true)
  }

  test("unbounded TypeRange rooted at leaf type should contain leaf") {
    val rangeRootedAtInteger = TypeRange(CTInteger, None)
    rangeRootedAtInteger.contains(CTInteger) should equal(true)
    rangeRootedAtInteger.contains(CTNumber) should equal(false)
    rangeRootedAtInteger.contains(CTFloat) should equal(false)
    rangeRootedAtInteger.contains(CTAny) should equal(false)

    val rangeRootedAtListOfNumber = TypeRange(CTList(CTNumber), None)
    rangeRootedAtListOfNumber.contains(CTList(CTInteger)) should equal(true)
    rangeRootedAtListOfNumber.contains(CTList(CTFloat)) should equal(true)
    rangeRootedAtListOfNumber.contains(CTList(CTNumber)) should equal(true)
    rangeRootedAtListOfNumber.contains(CTList(CTString)) should equal(false)
    rangeRootedAtListOfNumber.contains(CTAny) should equal(false)
  }

  test("unbounded TypeRange rooted at branch type should contain all more specific types") {
    val rangeRootedAtInteger = TypeRange(CTNumber, None)
    rangeRootedAtInteger.contains(CTInteger) should equal(true)
    rangeRootedAtInteger.contains(CTFloat) should equal(true)
    rangeRootedAtInteger.contains(CTNumber) should equal(true)
    rangeRootedAtInteger.contains(CTString) should equal(false)
    rangeRootedAtInteger.contains(CTAny) should equal(false)

    val rangeRootedAtListAny = TypeRange(CTList(CTAny), None)
    rangeRootedAtListAny.contains(CTList(CTString)) should equal(true)
    rangeRootedAtListAny.contains(CTList(CTInteger)) should equal(true)
    rangeRootedAtListAny.contains(CTList(CTAny)) should equal(true)
    rangeRootedAtListAny.contains(CTList(CTList(CTInteger))) should equal(true)
    rangeRootedAtListAny.contains(CTBoolean) should equal(false)
    rangeRootedAtListAny.contains(CTAny) should equal(false)
  }

  test("should contain overlapping range") {
    val rangeRootedAtNumber = TypeRange(CTNumber, None)
    val rangeRootedAtInteger = TypeRange(CTInteger, None)
    rangeRootedAtNumber.contains(rangeRootedAtInteger) should equal(true)

    val rangeOfNumberToDouble = TypeRange(CTNumber, CTFloat)
    rangeOfNumberToDouble.contains(rangeRootedAtInteger) should equal(false)
    rangeOfNumberToDouble.contains(rangeRootedAtNumber) should equal(false)

    val rangeOfDouble = TypeRange(CTFloat, CTFloat)
    val rangeOfNumber = TypeRange(CTNumber, CTNumber)
    val rangeOfInteger = TypeRange(CTInteger, CTInteger)
    rangeOfNumberToDouble.contains(rangeOfDouble) should equal(true)
    rangeOfNumberToDouble.contains(rangeOfNumber) should equal(true)
    rangeOfNumberToDouble.contains(rangeOfInteger) should equal(false)

    val rangeRootedAtDouble = TypeRange(CTFloat, None)
    rangeOfNumberToDouble.contains(rangeRootedAtDouble) should equal(false)
    rangeRootedAtDouble.contains(rangeOfDouble) should equal(true)

    rangeRootedAtInteger.contains(rangeRootedAtDouble) should equal(false)
  }

  test("intersection of range with overlapping range should not change range") {
    val rangeRootedAtInteger = TypeRange(CTInteger, None)
    rangeRootedAtInteger & TypeRange(CTNumber, None) should equal(Some(rangeRootedAtInteger))

    val rangeOfInteger = TypeRange(CTInteger, CTInteger)
    rangeOfInteger & TypeRange(CTNumber, None) should equal(Some(rangeOfInteger))

    val rangeOfNumber = TypeRange(CTNumber, CTNumber)
    rangeOfNumber & TypeRange(CTNumber, None) should equal(Some(rangeOfNumber))
  }

  test("intersection of range with intersecting range should return intersection") {
    val rangeOfNumber = TypeRange(CTNumber, None)
    rangeOfNumber & TypeRange(CTAny, CTNumber) should equal(Some(TypeRange(CTNumber, CTNumber)))

    val rangeToNumber = TypeRange(CTAny, CTNumber)
    rangeToNumber & TypeRange(CTNumber, None) should equal(Some(TypeRange(CTNumber, CTNumber)))
  }

  test("intersection of range to sub range should return sub range") {
    val rangeOfAll = TypeRange(CTAny, None)
    rangeOfAll & TypeRange(CTAny, CTNumber) should equal(Some(TypeRange(CTAny, CTNumber)))
    rangeOfAll & TypeRange(CTNumber, CTNumber) should equal(Some(TypeRange(CTNumber, CTNumber)))
    rangeOfAll & TypeRange(CTNumber, CTInteger) should equal(Some(TypeRange(CTNumber, CTInteger)))

    val rangeOfNumberToInteger = TypeRange(CTNumber, CTInteger)
    rangeOfNumberToInteger & TypeRange(CTNumber, CTNumber) should equal(Some(TypeRange(CTNumber, CTNumber)))
    rangeOfNumberToInteger & TypeRange(CTInteger, CTInteger) should equal(Some(TypeRange(CTInteger, CTInteger)))
  }

  test("intersection of range within list") {
    val rangeFromListAny = TypeRange(CTList(CTAny), None)
    rangeFromListAny & TypeRange(CTList(CTString), None) should equal(Some(TypeRange(CTList(CTString), None)))
    rangeFromListAny & TypeRange(CTList(CTString), CTList(CTString)) should equal(Some(TypeRange(CTList(CTString), CTList(CTString))))
  }

  test("intersection of range with non overlapping range should return none") {
    val rangeFromNumber = TypeRange(CTNumber, None)
    rangeFromNumber & TypeRange(CTString, None) should equal(None)

    val rangeOfNumber = TypeRange(CTNumber, CTNumber)
    rangeOfNumber & TypeRange(CTString, None) should equal(None)
    rangeOfNumber & TypeRange(CTBoolean, CTBoolean) should equal(None)

    val rangeOfAny = TypeRange(CTAny, CTAny)
    rangeOfAny & rangeFromNumber should equal(None)
    rangeOfAny & rangeOfNumber should equal(None)
    rangeFromNumber & rangeOfAny should equal(None)
    rangeOfNumber & rangeOfAny should equal(None)
  }

  test("leastUpperBound with super type") {
    val rangeFromAny = TypeRange(CTAny, None)
    val rangeOfAny = TypeRange(CTAny, CTAny)
    (rangeFromAny leastUpperBounds rangeOfAny) should equal(Seq(rangeOfAny))

    val rangeOfInteger = TypeRange(CTInteger, None)
    (rangeOfInteger leastUpperBounds rangeOfAny) should equal(Seq(rangeOfAny))

    val rangeOfNumber = TypeRange(CTNumber, CTNumber)
    (rangeOfInteger leastUpperBounds rangeOfNumber) should equal(Seq(rangeOfNumber))
  }

  test("leastUpperBound with sub type") {
    val rangeFromListAny = TypeRange(CTList(CTAny), None)
    val rangeOfListAny = TypeRange(CTList(CTAny), CTList(CTAny))
    (rangeFromListAny leastUpperBounds rangeOfListAny) should equal(Seq(rangeOfListAny))

    val rangeFromListString = TypeRange(CTList(CTString), None)
    (rangeFromListAny leastUpperBounds rangeFromListString) should equal(Seq(TypeRange(CTList(CTAny), CTList(CTString)), TypeRange(CTList(CTString), None)))
  }

  test("should have indefinite size when allowing unbound any at any depth") {
    TypeRange(CTAny, None).hasDefiniteSize should equal(false)
    TypeRange(CTList(CTAny), None).hasDefiniteSize should equal(false)

    TypeRange(CTString, None).hasDefiniteSize should equal(true)
    TypeRange(CTNumber, None).hasDefiniteSize should equal(true)

    TypeRange(CTAny, CTInteger).hasDefiniteSize should equal(true)

    TypeRange(CTList(CTList(CTAny)), None).hasDefiniteSize should equal(false)
    TypeRange(CTList(CTList(CTString)), None).hasDefiniteSize should equal(true)
  }

  test("should reparent into list") {
    TypeRange(CTString, None).reparent(CTList) should equal(TypeRange(CTList(CTString), None))
    TypeRange(CTAny, CTNumber).reparent(CTList) should equal(TypeRange(CTList(CTAny), CTList(CTNumber)))
  }

  test("without") {
    TypeRange(CTAny, CTInteger).without(CTNumber) should equal(Some(TypeRange(CTAny, CTNumber.parentType)))
    TypeRange(CTAny, CTNumber).without(CTInteger) should equal(Some(TypeRange(CTAny, CTNumber)))
    TypeRange(CTAny, None).without(CTInteger) should equal(Some(TypeRange(CTAny, CTInteger.parentType)))
    TypeRange(CTInteger, None).without(CTNumber) should equal(None)
    TypeRange(CTInteger, None).without(CTString) should equal(Some(TypeRange(CTInteger, None)))
    TypeRange(CTAny, CTNumber).without(CTString) should equal(Some(TypeRange(CTAny, CTNumber)))
  }
}
