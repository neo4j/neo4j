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

    val rangeOfCollectionAny = TypeRange(CTCollection(CTAny), CTCollection(CTAny))
    rangeOfCollectionAny.contains(CTInteger) should equal(false)
    rangeOfCollectionAny.contains(CTNumber) should equal(false)
    rangeOfCollectionAny.contains(CTString) should equal(false)
    rangeOfCollectionAny.contains(CTCollection(CTString)) should equal(false)
    rangeOfCollectionAny.contains(CTCollection(CTNumber)) should equal(false)
    rangeOfCollectionAny.contains(CTCollection(CTAny)) should equal(true)
    rangeOfCollectionAny.contains(CTAny) should equal(false)
  }

  test("unbounded TypeRange rooted at CTAny should contain all") {
    val rangeRootedAtAny = TypeRange(CTAny, None)
    rangeRootedAtAny.contains(CTAny) should equal(true)
    rangeRootedAtAny.contains(CTString) should equal(true)
    rangeRootedAtAny.contains(CTNumber) should equal(true)
    rangeRootedAtAny.contains(CTInteger) should equal(true)
    rangeRootedAtAny.contains(CTFloat) should equal(true)
    rangeRootedAtAny.contains(CTNode) should equal(true)
    rangeRootedAtAny.contains(CTCollection(CTAny)) should equal(true)
    rangeRootedAtAny.contains(CTCollection(CTFloat)) should equal(true)
    rangeRootedAtAny.contains(CTCollection(CTCollection(CTFloat))) should equal(true)
  }

  test("unbounded TypeRange rooted at leaf type should contain leaf") {
    val rangeRootedAtInteger = TypeRange(CTInteger, None)
    rangeRootedAtInteger.contains(CTInteger) should equal(true)
    rangeRootedAtInteger.contains(CTNumber) should equal(false)
    rangeRootedAtInteger.contains(CTFloat) should equal(false)
    rangeRootedAtInteger.contains(CTAny) should equal(false)

    val rangeRootedAtCollectionOfNumber = TypeRange(CTCollection(CTNumber), None)
    rangeRootedAtCollectionOfNumber.contains(CTCollection(CTInteger)) should equal(true)
    rangeRootedAtCollectionOfNumber.contains(CTCollection(CTFloat)) should equal(true)
    rangeRootedAtCollectionOfNumber.contains(CTCollection(CTNumber)) should equal(true)
    rangeRootedAtCollectionOfNumber.contains(CTCollection(CTString)) should equal(false)
    rangeRootedAtCollectionOfNumber.contains(CTAny) should equal(false)
  }

  test("unbounded TypeRange rooted at branch type should contain all more specific types") {
    val rangeRootedAtInteger = TypeRange(CTNumber, None)
    rangeRootedAtInteger.contains(CTInteger) should equal(true)
    rangeRootedAtInteger.contains(CTFloat) should equal(true)
    rangeRootedAtInteger.contains(CTNumber) should equal(true)
    rangeRootedAtInteger.contains(CTString) should equal(false)
    rangeRootedAtInteger.contains(CTAny) should equal(false)

    val rangeRootedAtCollectionAny = TypeRange(CTCollection(CTAny), None)
    rangeRootedAtCollectionAny.contains(CTCollection(CTString)) should equal(true)
    rangeRootedAtCollectionAny.contains(CTCollection(CTInteger)) should equal(true)
    rangeRootedAtCollectionAny.contains(CTCollection(CTAny)) should equal(true)
    rangeRootedAtCollectionAny.contains(CTCollection(CTCollection(CTInteger))) should equal(true)
    rangeRootedAtCollectionAny.contains(CTBoolean) should equal(false)
    rangeRootedAtCollectionAny.contains(CTAny) should equal(false)
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

  test("intersection of range within collection") {
    val rangeFromCollectionAny = TypeRange(CTCollection(CTAny), None)
    rangeFromCollectionAny & TypeRange(CTCollection(CTString), None) should equal(Some(TypeRange(CTCollection(CTString), None)))
    rangeFromCollectionAny & TypeRange(CTCollection(CTString), CTCollection(CTString)) should equal(Some(TypeRange(CTCollection(CTString), CTCollection(CTString))))
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
    val rangeFromCollectionAny = TypeRange(CTCollection(CTAny), None)
    val rangeOfCollectionAny = TypeRange(CTCollection(CTAny), CTCollection(CTAny))
    (rangeFromCollectionAny leastUpperBounds rangeOfCollectionAny) should equal(Seq(rangeOfCollectionAny))

    val rangeFromCollectionString = TypeRange(CTCollection(CTString), None)
    (rangeFromCollectionAny leastUpperBounds rangeFromCollectionString) should equal(Seq(TypeRange(CTCollection(CTAny), CTCollection(CTString)), TypeRange(CTCollection(CTString), None)))
  }

  test("should have indefinite size when allowing unbound any at any depth") {
    TypeRange(CTAny, None).hasDefiniteSize should equal(false)
    TypeRange(CTCollection(CTAny), None).hasDefiniteSize should equal(false)

    TypeRange(CTString, None).hasDefiniteSize should equal(true)
    TypeRange(CTNumber, None).hasDefiniteSize should equal(true)

    TypeRange(CTAny, CTInteger).hasDefiniteSize should equal(true)

    TypeRange(CTCollection(CTCollection(CTAny)), None).hasDefiniteSize should equal(false)
    TypeRange(CTCollection(CTCollection(CTString)), None).hasDefiniteSize should equal(true)
  }

  test("should reparent into collection") {
    TypeRange(CTString, None).reparent(CTCollection) should equal(TypeRange(CTCollection(CTString), None))
    TypeRange(CTAny, CTNumber).reparent(CTCollection) should equal(TypeRange(CTCollection(CTAny), CTCollection(CTNumber)))
  }
}
