/**
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
package org.neo4j.cypher.internal.compiler.v2_0.symbols

import org.junit.Assert._
import org.neo4j.cypher.internal.commons.CypherFunSuite

class TypeRangeTest extends CypherFunSuite {
  test("TypeRange of single type should contain only that type") {
    val rangeOfInteger = TypeRange(CTInteger, CTInteger)
    assertTrue(rangeOfInteger.contains(CTInteger))
    assertFalse(rangeOfInteger.contains(CTNumber))
    assertFalse(rangeOfInteger.contains(CTFloat))
    assertFalse(rangeOfInteger.contains(CTString))
    assertFalse(rangeOfInteger.contains(CTAny))

    val rangeOfNumber = TypeRange(CTNumber, CTNumber)
    assertFalse(rangeOfNumber.contains(CTInteger))
    assertTrue(rangeOfNumber.contains(CTNumber))
    assertFalse(rangeOfNumber.contains(CTFloat))
    assertFalse(rangeOfNumber.contains(CTString))
    assertFalse(rangeOfNumber.contains(CTAny))

    val rangeOfCollectionAny = TypeRange(CTCollection(CTAny), CTCollection(CTAny))
    assertFalse(rangeOfCollectionAny.contains(CTInteger))
    assertFalse(rangeOfCollectionAny.contains(CTNumber))
    assertFalse(rangeOfCollectionAny.contains(CTString))
    assertFalse(rangeOfCollectionAny.contains(CTCollection(CTString)))
    assertFalse(rangeOfCollectionAny.contains(CTCollection(CTNumber)))
    assertTrue(rangeOfCollectionAny.contains(CTCollection(CTAny)))
    assertFalse(rangeOfCollectionAny.contains(CTAny))
  }

  test("unbounded TypeRange rooted at CTAny should contain all") {
    val rangeRootedAtAny = TypeRange(CTAny, None)
    assertTrue(rangeRootedAtAny.contains(CTAny))
    assertTrue(rangeRootedAtAny.contains(CTString))
    assertTrue(rangeRootedAtAny.contains(CTNumber))
    assertTrue(rangeRootedAtAny.contains(CTInteger))
    assertTrue(rangeRootedAtAny.contains(CTFloat))
    assertTrue(rangeRootedAtAny.contains(CTNode))
    assertTrue(rangeRootedAtAny.contains(CTCollection(CTAny)))
    assertTrue(rangeRootedAtAny.contains(CTCollection(CTFloat)))
    assertTrue(rangeRootedAtAny.contains(CTCollection(CTCollection(CTFloat))))
  }

  test("unbounded TypeRange rooted at leaf type should contain leaf") {
    val rangeRootedAtInteger = TypeRange(CTInteger, None)
    assertTrue(rangeRootedAtInteger.contains(CTInteger))
    assertFalse(rangeRootedAtInteger.contains(CTNumber))
    assertFalse(rangeRootedAtInteger.contains(CTFloat))
    assertFalse(rangeRootedAtInteger.contains(CTAny))

    val rangeRootedAtCollectionOfNumber = TypeRange(CTCollection(CTNumber), None)
    assertTrue(rangeRootedAtCollectionOfNumber.contains(CTCollection(CTInteger)))
    assertTrue(rangeRootedAtCollectionOfNumber.contains(CTCollection(CTFloat)))
    assertTrue(rangeRootedAtCollectionOfNumber.contains(CTCollection(CTNumber)))
    assertFalse(rangeRootedAtCollectionOfNumber.contains(CTCollection(CTString)))
    assertFalse(rangeRootedAtCollectionOfNumber.contains(CTAny))
  }

  test("unbounded TypeRange rooted at branch type should contain all more specific types") {
    val rangeRootedAtInteger = TypeRange(CTNumber, None)
    assertTrue(rangeRootedAtInteger.contains(CTInteger))
    assertTrue(rangeRootedAtInteger.contains(CTFloat))
    assertTrue(rangeRootedAtInteger.contains(CTNumber))
    assertFalse(rangeRootedAtInteger.contains(CTString))
    assertFalse(rangeRootedAtInteger.contains(CTAny))

    val rangeRootedAtCollectionAny = TypeRange(CTCollection(CTAny), None)
    assertTrue(rangeRootedAtCollectionAny.contains(CTCollection(CTString)))
    assertTrue(rangeRootedAtCollectionAny.contains(CTCollection(CTInteger)))
    assertTrue(rangeRootedAtCollectionAny.contains(CTCollection(CTAny)))
    assertTrue(rangeRootedAtCollectionAny.contains(CTCollection(CTCollection(CTInteger))))
    assertFalse(rangeRootedAtCollectionAny.contains(CTBoolean))
    assertFalse(rangeRootedAtCollectionAny.contains(CTAny))
  }

  test("should contain overlapping range") {
    val rangeRootedAtNumber = TypeRange(CTNumber, None)
    val rangeRootedAtInteger = TypeRange(CTInteger, None)
    assertTrue(rangeRootedAtNumber.contains(rangeRootedAtInteger))

    val rangeOfNumberToDouble = TypeRange(CTNumber, CTFloat)
    assertFalse(rangeOfNumberToDouble.contains(rangeRootedAtInteger))
    assertFalse(rangeOfNumberToDouble.contains(rangeRootedAtNumber))

    val rangeOfDouble = TypeRange(CTFloat, CTFloat)
    val rangeOfNumber = TypeRange(CTNumber, CTNumber)
    val rangeOfInteger = TypeRange(CTInteger, CTInteger)
    assertTrue(rangeOfNumberToDouble.contains(rangeOfDouble))
    assertTrue(rangeOfNumberToDouble.contains(rangeOfNumber))
    assertFalse(rangeOfNumberToDouble.contains(rangeOfInteger))

    val rangeRootedAtDouble = TypeRange(CTFloat, None)
    assertFalse(rangeOfNumberToDouble.contains(rangeRootedAtDouble))
    assertTrue(rangeRootedAtDouble.contains(rangeOfDouble))

    assertFalse(rangeRootedAtInteger.contains(rangeRootedAtDouble))
  }

  test("intersection of range with overlapping range should not change range") {
    val rangeRootedAtInteger = TypeRange(CTInteger, None)
    assertEquals(Some(rangeRootedAtInteger), rangeRootedAtInteger & TypeRange(CTNumber, None))

    val rangeOfInteger = TypeRange(CTInteger, CTInteger)
    assertEquals(Some(rangeOfInteger), rangeOfInteger & TypeRange(CTNumber, None))

    val rangeOfNumber = TypeRange(CTNumber, CTNumber)
    assertEquals(Some(rangeOfNumber), rangeOfNumber & TypeRange(CTNumber, None))
  }

  test("intersection of range with intersecting range should return intersection") {
    val rangeOfNumber = TypeRange(CTNumber, None)
    assertEquals(Some(TypeRange(CTNumber, CTNumber)), rangeOfNumber & TypeRange(CTAny, CTNumber))

    val rangeToNumber = TypeRange(CTAny, CTNumber)
    assertEquals(Some(TypeRange(CTNumber, CTNumber)), rangeToNumber & TypeRange(CTNumber, None))
  }

  test("intersection of range to sub range should return sub range") {
    val rangeOfAll = TypeRange(CTAny, None)
    assertEquals(Some(TypeRange(CTAny, CTNumber)), rangeOfAll & TypeRange(CTAny, CTNumber))
    assertEquals(Some(TypeRange(CTNumber, CTNumber)), rangeOfAll & TypeRange(CTNumber, CTNumber))
    assertEquals(Some(TypeRange(CTNumber, CTInteger)), rangeOfAll & TypeRange(CTNumber, CTInteger))

    val rangeOfNumberToInteger = TypeRange(CTNumber, CTInteger)
    assertEquals(Some(TypeRange(CTNumber, CTNumber)), rangeOfNumberToInteger & TypeRange(CTNumber, CTNumber))
    assertEquals(Some(TypeRange(CTInteger, CTInteger)), rangeOfNumberToInteger & TypeRange(CTInteger, CTInteger))
  }

  test("intersection of range within collection") {
    val rangeFromCollectionAny = TypeRange(CTCollection(CTAny), None)
    assertEquals(Some(TypeRange(CTCollection(CTString), None)), rangeFromCollectionAny & TypeRange(CTCollection(CTString), None))
    assertEquals(Some(TypeRange(CTCollection(CTString), CTCollection(CTString))), rangeFromCollectionAny & TypeRange(CTCollection(CTString), CTCollection(CTString)))
  }

  test("intersection of range with non overlapping range should return none") {
    val rangeFromNumber = TypeRange(CTNumber, None)
    assertEquals(None, rangeFromNumber & TypeRange(CTString, None))

    val rangeOfNumber = TypeRange(CTNumber, CTNumber)
    assertEquals(None, rangeOfNumber & TypeRange(CTString, None))
    assertEquals(None, rangeOfNumber & TypeRange(CTBoolean, CTBoolean))

    val rangeOfAny = TypeRange(CTAny, CTAny)
    assertEquals(None, rangeOfAny & rangeFromNumber)
    assertEquals(None, rangeOfAny & rangeOfNumber)
    assertEquals(None, rangeFromNumber & rangeOfAny)
    assertEquals(None, rangeOfNumber & rangeOfAny)
  }

  test("leastUpperBounds with super types") {
    val rangeFromAny = TypeRange(CTAny, None)
    val rangeOfAny = TypeRange(CTAny, CTAny)
    assertEquals(Seq(rangeOfAny), rangeFromAny leastUpperBounds rangeOfAny)

    val rangeFromInteger = TypeRange(CTInteger, None)
    assertEquals(Seq(rangeOfAny), rangeFromInteger leastUpperBounds rangeOfAny)

    val rangeOfNumber = TypeRange(CTNumber, CTNumber)
    assertEquals(Seq(rangeOfNumber), rangeFromInteger leastUpperBounds rangeOfNumber)
  }

  test("leastUpperBounds with sub types") {
    val rangeFromCollectionAny = TypeRange(CTCollection(CTAny), None)
    val rangeOfCollectionAny = TypeRange(CTCollection(CTAny), CTCollection(CTAny))
    assertEquals(Seq(rangeOfCollectionAny), rangeFromCollectionAny leastUpperBounds rangeOfCollectionAny)

    val rangeFromCollectionString = TypeRange(CTCollection(CTString), None)
    assertEquals(Seq(TypeRange(CTCollection(CTAny), CTCollection(CTString)), TypeRange(CTCollection(CTString), None)),
      rangeFromCollectionAny leastUpperBounds rangeFromCollectionString)
  }

  test("should have indefinite size when allowing unbound any at any depth") {
    assertFalse(TypeRange(CTAny, None).hasDefiniteSize)
    assertFalse(TypeRange(CTCollection(CTAny), None).hasDefiniteSize)

    assertTrue(TypeRange(CTString, None).hasDefiniteSize)
    assertTrue(TypeRange(CTNumber, None).hasDefiniteSize)

    assertTrue(TypeRange(CTAny, CTInteger).hasDefiniteSize)

    assertFalse(TypeRange(CTCollection(CTCollection(CTAny)), None).hasDefiniteSize)
    assertTrue(TypeRange(CTCollection(CTCollection(CTString)), None).hasDefiniteSize)
  }

  test("should reparent into collection") {
    assertEquals(TypeRange(CTCollection(CTString), None), TypeRange(CTString, None).reparent(CTCollection))
    assertEquals(TypeRange(CTCollection(CTAny), CTCollection(CTNumber)), TypeRange(CTAny, CTNumber).reparent(CTCollection))
  }
}
