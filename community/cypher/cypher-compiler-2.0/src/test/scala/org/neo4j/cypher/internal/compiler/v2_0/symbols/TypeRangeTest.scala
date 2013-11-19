/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import org.junit.Test
import org.scalatest.Assertions

class TypeRangeTest extends Assertions {
  @Test
  def typeRangeOfSingleTypeShouldContainOnlyThatType() {
    val rangeOfInteger = TypeRange(IntegerType(), IntegerType())
    assertTrue(rangeOfInteger.contains(IntegerType()))
    assertFalse(rangeOfInteger.contains(NumberType()))
    assertFalse(rangeOfInteger.contains(DoubleType()))
    assertFalse(rangeOfInteger.contains(StringType()))
    assertFalse(rangeOfInteger.contains(AnyType()))

    val rangeOfNumber = TypeRange(NumberType(), NumberType())
    assertFalse(rangeOfNumber.contains(IntegerType()))
    assertTrue(rangeOfNumber.contains(NumberType()))
    assertFalse(rangeOfNumber.contains(DoubleType()))
    assertFalse(rangeOfNumber.contains(StringType()))
    assertFalse(rangeOfNumber.contains(AnyType()))

    val rangeOfCollectionAny = TypeRange(CollectionType(AnyType()), CollectionType(AnyType()))
    assertFalse(rangeOfCollectionAny.contains(IntegerType()))
    assertFalse(rangeOfCollectionAny.contains(NumberType()))
    assertFalse(rangeOfCollectionAny.contains(StringType()))
    assertFalse(rangeOfCollectionAny.contains(CollectionType(StringType())))
    assertFalse(rangeOfCollectionAny.contains(CollectionType(NumberType())))
    assertTrue(rangeOfCollectionAny.contains(CollectionType(AnyType())))
    assertFalse(rangeOfCollectionAny.contains(AnyType()))
  }

  @Test
  def unboundedTypeRangeRootedAtAnyShouldContainAll() {
    val rangeRootedAtAny = TypeRange(AnyType(), None)
    assertTrue(rangeRootedAtAny.contains(AnyType()))
    assertTrue(rangeRootedAtAny.contains(StringType()))
    assertTrue(rangeRootedAtAny.contains(NumberType()))
    assertTrue(rangeRootedAtAny.contains(IntegerType()))
    assertTrue(rangeRootedAtAny.contains(DoubleType()))
    assertTrue(rangeRootedAtAny.contains(NodeType()))
    assertTrue(rangeRootedAtAny.contains(CollectionType(AnyType())))
    assertTrue(rangeRootedAtAny.contains(CollectionType(DoubleType())))
    assertTrue(rangeRootedAtAny.contains(CollectionType(CollectionType(DoubleType()))))
  }

  @Test
  def unboundedTypeRangeRootedAtLeafTypeShouldContainLeaf() {
    val rangeRootedAtInteger = TypeRange(IntegerType(), None)
    assertTrue(rangeRootedAtInteger.contains(IntegerType()))
    assertFalse(rangeRootedAtInteger.contains(NumberType()))
    assertFalse(rangeRootedAtInteger.contains(DoubleType()))
    assertFalse(rangeRootedAtInteger.contains(AnyType()))

    val rangeRootedAtCollectionOfNumber = TypeRange(CollectionType(NumberType()), None)
    assertTrue(rangeRootedAtCollectionOfNumber.contains(CollectionType(IntegerType())))
    assertTrue(rangeRootedAtCollectionOfNumber.contains(CollectionType(DoubleType())))
    assertTrue(rangeRootedAtCollectionOfNumber.contains(CollectionType(NumberType())))
    assertFalse(rangeRootedAtCollectionOfNumber.contains(CollectionType(StringType())))
    assertFalse(rangeRootedAtCollectionOfNumber.contains(AnyType()))
  }

  @Test
  def unboundedTypeRangeRootedAtBranchTypeShouldContainAllMoreSpecificTypes() {
    val rangeRootedAtInteger = TypeRange(NumberType(), None)
    assertTrue(rangeRootedAtInteger.contains(IntegerType()))
    assertTrue(rangeRootedAtInteger.contains(DoubleType()))
    assertTrue(rangeRootedAtInteger.contains(NumberType()))
    assertFalse(rangeRootedAtInteger.contains(StringType()))
    assertFalse(rangeRootedAtInteger.contains(AnyType()))

    val rangeRootedAtCollectionAny = TypeRange(CollectionType(AnyType()), None)
    assertTrue(rangeRootedAtCollectionAny.contains(CollectionType(StringType())))
    assertTrue(rangeRootedAtCollectionAny.contains(CollectionType(IntegerType())))
    assertTrue(rangeRootedAtCollectionAny.contains(CollectionType(AnyType())))
    assertTrue(rangeRootedAtCollectionAny.contains(CollectionType(CollectionType(IntegerType()))))
    assertFalse(rangeRootedAtCollectionAny.contains(BooleanType()))
    assertFalse(rangeRootedAtCollectionAny.contains(AnyType()))
  }

  @Test
  def typeRangeShouldContainOverlappingRange() {
    val rangeRootedAtNumber = TypeRange(NumberType(), None)
    val rangeRootedAtInteger = TypeRange(IntegerType(), None)
    assertTrue(rangeRootedAtNumber.contains(rangeRootedAtInteger))

    val rangeOfNumberToDouble = TypeRange(NumberType(), DoubleType())
    assertFalse(rangeOfNumberToDouble.contains(rangeRootedAtInteger))
    assertFalse(rangeOfNumberToDouble.contains(rangeRootedAtNumber))

    val rangeOfDouble = TypeRange(DoubleType(), DoubleType())
    val rangeOfNumber = TypeRange(NumberType(), NumberType())
    val rangeOfInteger = TypeRange(IntegerType(), IntegerType())
    assertTrue(rangeOfNumberToDouble.contains(rangeOfDouble))
    assertTrue(rangeOfNumberToDouble.contains(rangeOfNumber))
    assertFalse(rangeOfNumberToDouble.contains(rangeOfInteger))

    val rangeRootedAtDouble = TypeRange(DoubleType(), None)
    assertFalse(rangeOfNumberToDouble.contains(rangeRootedAtDouble))
    assertTrue(rangeRootedAtDouble.contains(rangeOfDouble))

    assertFalse(rangeRootedAtInteger.contains(rangeRootedAtDouble))
  }

  @Test
  def intersectRangeWithOverlappingRangeShouldNotChangeRange() {
    val rangeRootedAtInteger = TypeRange(IntegerType(), None)
    assertEquals(Some(rangeRootedAtInteger), rangeRootedAtInteger & TypeRange(NumberType(), None))

    val rangeOfInteger = TypeRange(IntegerType(), IntegerType())
    assertEquals(Some(rangeOfInteger), rangeOfInteger & TypeRange(NumberType(), None))

    val rangeOfNumber = TypeRange(NumberType(), NumberType())
    assertEquals(Some(rangeOfNumber), rangeOfNumber & TypeRange(NumberType(), None))
  }

  @Test
  def intersectRangeWithIntersectingRangeShouldReturnIntersection() {
    val rangeOfNumber = TypeRange(NumberType(), None)
    assertEquals(Some(TypeRange(NumberType(), NumberType())), rangeOfNumber & TypeRange(AnyType(), NumberType()))

    val rangeToNumber = TypeRange(AnyType(), NumberType())
    assertEquals(Some(TypeRange(NumberType(), NumberType())), rangeToNumber & TypeRange(NumberType(), None))
  }

  @Test
  def intersectRangeToSubRangeShouldReturnSubRange() {
    val rangeOfAll = TypeRange(AnyType(), None)
    assertEquals(Some(TypeRange(AnyType(), NumberType())), rangeOfAll & TypeRange(AnyType(), NumberType()))
    assertEquals(Some(TypeRange(NumberType(), NumberType())), rangeOfAll & TypeRange(NumberType(), NumberType()))
    assertEquals(Some(TypeRange(NumberType(), IntegerType())), rangeOfAll & TypeRange(NumberType(), IntegerType()))

    val rangeOfNumberToInteger = TypeRange(NumberType(), IntegerType())
    assertEquals(Some(TypeRange(NumberType(), NumberType())), rangeOfNumberToInteger & TypeRange(NumberType(), NumberType()))
    assertEquals(Some(TypeRange(IntegerType(), IntegerType())), rangeOfNumberToInteger & TypeRange(IntegerType(), IntegerType()))
  }

  @Test
  def intersectRangeWithinCollection() {
    val rangeFromCollectionAny = TypeRange(CollectionType(AnyType()), None)
    assertEquals(Some(TypeRange(CollectionType(StringType()), None)), rangeFromCollectionAny & TypeRange(CollectionType(StringType()), None))
    assertEquals(Some(TypeRange(CollectionType(StringType()), CollectionType(StringType()))), rangeFromCollectionAny & TypeRange(CollectionType(StringType()), CollectionType(StringType())))
  }

  @Test
  def intersectRangeWithNonOverlappingRangeShouldReturnNone() {
    val rangeFromNumber = TypeRange(NumberType(), None)
    assertEquals(None, rangeFromNumber & TypeRange(StringType(), None))

    val rangeOfNumber = TypeRange(NumberType(), NumberType())
    assertEquals(None, rangeOfNumber & TypeRange(StringType(), None))
    assertEquals(None, rangeOfNumber & TypeRange(BooleanType(), BooleanType()))

    val rangeOfAny = TypeRange(AnyType(), AnyType())
    assertEquals(None, rangeOfAny & rangeFromNumber)
    assertEquals(None, rangeOfAny & rangeOfNumber)
    assertEquals(None, rangeFromNumber & rangeOfAny)
    assertEquals(None, rangeOfNumber & rangeOfAny)
  }

  @Test
  def mergeDownWithSubType() {
    val rangeFromAny = TypeRange(AnyType(), None)
    val rangeOfAny = TypeRange(AnyType(), AnyType())
    assertEquals(Seq(rangeOfAny), rangeFromAny.mergeDown(rangeOfAny))

    val rangeOfInteger = TypeRange(IntegerType(), None)
    assertEquals(Seq(rangeOfAny), rangeOfInteger.mergeDown(rangeOfAny))

    val rangeOfNumber = TypeRange(NumberType(), NumberType())
    assertEquals(Seq(rangeOfNumber), rangeOfInteger.mergeDown(rangeOfNumber))
  }

  @Test
  def mergeDownWithNestedType() {
    val rangeFromCollectionAny = TypeRange(CollectionType(AnyType()), None)
    val rangeOfCollectionAny = TypeRange(CollectionType(AnyType()), CollectionType(AnyType()))
    assertEquals(Seq(rangeOfCollectionAny), rangeFromCollectionAny.mergeDown(rangeOfCollectionAny))

    val rangeFromCollectionString = TypeRange(CollectionType(StringType()), None)
    assertEquals(Seq(TypeRange(CollectionType(AnyType()), CollectionType(StringType())), TypeRange(CollectionType(StringType()), None)),
      rangeFromCollectionAny.mergeDown(rangeFromCollectionString))
  }

  @Test
  def shouldHaveIndefiniteSizeWhenAllowingUnboundAnyAtAnyDepth() {
    assertFalse(TypeRange(AnyType(), None).hasDefiniteSize)
    assertFalse(TypeRange(CollectionType(AnyType()), None).hasDefiniteSize)

    assertTrue(TypeRange(StringType(), None).hasDefiniteSize)
    assertTrue(TypeRange(NumberType(), None).hasDefiniteSize)

    assertTrue(TypeRange(AnyType(), IntegerType()).hasDefiniteSize)

    assertFalse(TypeRange(CollectionType(CollectionType(AnyType())), None).hasDefiniteSize)
    assertTrue(TypeRange(CollectionType(CollectionType(StringType())), None).hasDefiniteSize)
  }

  @Test
  def shouldReparentIntoCollection() {
    assertEquals(TypeRange(CollectionType(StringType()), None), TypeRange(StringType(), None).reparent(CollectionType(_)))
    assertEquals(TypeRange(CollectionType(AnyType()), CollectionType(NumberType())), TypeRange(AnyType(), NumberType()).reparent(CollectionType(_)))
  }
}
