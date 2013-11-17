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
import scala.collection.immutable.SortedSet

class TypeSetTest extends Assertions {

  implicit def orderingOfCypherType[T <: CypherType] : Ordering[T] = Ordering.by(_.toString)

  @Test
  def shouldFormatNoType() {
    assertEquals("", TypeSet().formattedString)
  }

  @Test
  def shouldFormatSingleType() {
    assertEquals("Any", SortedSet(AnyType()).formattedString)
    assertEquals("Node", SortedSet(NodeType()).formattedString)
  }

  @Test
  def shouldFormatTwoTypes() {
    assertEquals("Any or Node", SortedSet(AnyType(), NodeType()).formattedString)
    assertEquals("Node or Relationship", SortedSet(RelationshipType(), NodeType()).formattedString)
  }

  @Test
  def shouldFormatThreeTypes() {
	  assertEquals("Any, Node or Relationship", SortedSet(RelationshipType(), AnyType(), NodeType()).formattedString)
	  assertEquals("Integer, Node or Relationship", SortedSet(RelationshipType(), IntegerType(), NodeType()).formattedString)
  }

  @Test
  def shouldInferTypeSetsUsingMergeDown() {
    assertEquals(Set(NodeType(), NumberType(), AnyType()), Set(NodeType(), NumberType()) mergeDown Set(NodeType(), NumberType()))

    assertEquals(Set(NodeType(), NumberType(), AnyType()), Set(NodeType(), NumberType()) mergeDown Set(NodeType(), NumberType()))
    assertEquals(Set(NumberType(), AnyType()), Set(NodeType(), NumberType()) mergeDown Set(NumberType()))
    assertEquals(Set(NodeType(), NumberType(), MapType(), AnyType()), Set(NodeType(), NumberType()) mergeDown Set(NodeType(), NumberType(), RelationshipType()))
    assertEquals(Set(AnyType()), Set(NodeType(), NumberType()) mergeDown Set(AnyType()))
    assertEquals(Set(AnyType()), Set(AnyType()) mergeDown Set(NodeType(), NumberType()))

    assertEquals(Set(MapType()), Set(RelationshipType()) mergeDown Set(NodeType()))
    assertEquals(Set(MapType(), NumberType(), AnyType()), Set(RelationshipType(), LongType()) mergeDown Set(NodeType(), NumberType()))
  }

  @Test
  def shouldMergeDownCollectionIterable() {
    assertEquals(Set(NumberType(), CollectionType(AnyType()), AnyType()),
      Set(IntegerType(), CollectionType(StringType())) mergeDown Set(NumberType(), CollectionType(IntegerType())))
  }

  @Test
  def shouldMergeUpCollectionIterable() {
    assertEquals(Set(IntegerType()),
      Set(IntegerType(), StringType(), CollectionType(IntegerType())) mergeUp Set(NumberType(), CollectionType(StringType())) )
    assertEquals(Set(IntegerType(), CollectionType(StringType())),
      Set(IntegerType(), StringType(), CollectionType(AnyType())) mergeUp Set(NumberType(), CollectionType(StringType())) )
  }

  @Test
  def shouldInferTypeSetsUsingMergeUp() {
    assertEquals(Set(NodeType(), NumberType()), Set(NodeType(), NumberType()) mergeUp Set(NodeType(), NumberType()))
    assertEquals(Set(NumberType()), Set(NodeType(), NumberType()) mergeUp Set(NumberType()))
    assertEquals(Set(NodeType(), NumberType()), Set(NodeType(), NumberType()) mergeUp Set(NodeType(), NumberType(), RelationshipType()))
    assertEquals(Set(NodeType(), NumberType()), Set(NodeType(), NumberType()) mergeUp Set(AnyType()))
    assertEquals(Set(NodeType(), NumberType()), Set(AnyType()) mergeUp Set(NodeType(), NumberType()))

    assertEquals(Set(), Set(RelationshipType()) mergeUp Set(NodeType()))
    assertEquals(Set(LongType()), Set(RelationshipType(), LongType()) mergeUp Set(NodeType(), NumberType()))
    assertEquals(Set(NodeType(), NumberType()), Set(AnyType()) mergeUp Set(NodeType(), NumberType()))
  }

  @Test
  def shouldConstrainTypeSets() {
    assertEquals(Set(IntegerType(), LongType()), Set(IntegerType(), LongType(), StringType(), MapType()) constrain Set(NodeType(), NumberType()))
    assertEquals(Set(CollectionType(StringType())), Set(IntegerType(), CollectionType(StringType())) constrain Set(CollectionType(AnyType())))
    assertEquals(Set.empty, Set(IntegerType(), CollectionType(MapType())) constrain Set(CollectionType(NodeType())))
    assertEquals(Set(IntegerType(), CollectionType(StringType())), Set(IntegerType(), CollectionType(StringType())) constrain Set(AnyType()))
  }

  @Test
  def allTypesShouldContainAll() {
    assertTrue(TypeSet.all.contains(AnyType()))
    assertTrue(TypeSet.all.contains(StringType()))
    assertTrue(TypeSet.all.contains(NumberType()))
    assertTrue(TypeSet.all.contains(IntegerType()))
    assertTrue(TypeSet.all.contains(DoubleType()))
    assertTrue(TypeSet.all.contains(NodeType()))
    assertTrue(TypeSet.all.contains(CollectionType(AnyType())))
    assertTrue(TypeSet.all.contains(CollectionType(DoubleType())))
    assertTrue(TypeSet.all.contains(CollectionType(CollectionType(DoubleType()))))
  }

  @Test
  def constrainToLeafType() {
    val constrainedToInteger = TypeSet.all.constrain(IntegerType())
    assertTrue(constrainedToInteger.contains(IntegerType()))
    assertFalse(constrainedToInteger.contains(NumberType()))
    assertFalse(constrainedToInteger.contains(DoubleType()))
    assertFalse(constrainedToInteger.contains(AnyType()))
  }

  @Test
  def constrainToBranchType() {
    val constrainedToNumber = TypeSet.all.constrain(NumberType())
    assertTrue(constrainedToNumber.contains(IntegerType()))
    assertTrue(constrainedToNumber.contains(DoubleType()))
    assertTrue(constrainedToNumber.contains(NumberType()))
    assertFalse(constrainedToNumber.contains(StringType()))
    assertFalse(constrainedToNumber.contains(AnyType()))
  }

  @Test
  def constrainWithinMultipleBranchesOfTypeTree() {
    val constrainedToNumberOrString = TypeSet.all.constrain(NumberType(), StringType())
    assertTrue(constrainedToNumberOrString.contains(IntegerType()))
    assertTrue(constrainedToNumberOrString.contains(DoubleType()))
    assertTrue(constrainedToNumberOrString.contains(NumberType()))
    assertTrue(constrainedToNumberOrString.contains(StringType()))
    assertFalse(constrainedToNumberOrString.contains(NodeType()))
    assertFalse(constrainedToNumberOrString.contains(AnyType()))
  }

  @Test
  def constrainToCollectionBranchOfTypeTree() {
    val constrainedToCollection = TypeSet.all.constrain(CollectionType(AnyType()))
    assertTrue(constrainedToCollection.contains(CollectionType(StringType())))
    assertTrue(constrainedToCollection.contains(CollectionType(IntegerType())))
    assertTrue(constrainedToCollection.contains(CollectionType(AnyType())))
    assertTrue(constrainedToCollection.contains(CollectionType(CollectionType(IntegerType()))))
    assertFalse(constrainedToCollection.contains(BooleanType()))
    assertFalse(constrainedToCollection.contains(AnyType()))
  }

  @Test
  def constrainToLeafTypeWithinCollection() {
    val constrainedToCollectionOfNumber = TypeSet.all.constrain(CollectionType(NumberType()))
    assertTrue(constrainedToCollectionOfNumber.contains(CollectionType(IntegerType())))
    assertTrue(constrainedToCollectionOfNumber.contains(CollectionType(DoubleType())))
    assertTrue(constrainedToCollectionOfNumber.contains(CollectionType(NumberType())))
    assertFalse(constrainedToCollectionOfNumber.contains(CollectionType(StringType())))
    assertFalse(constrainedToCollectionOfNumber.contains(AnyType()))
  }

  @Test
  def reconstrainToSupertype() {
    val constrainedToInteger = TypeSet.all.constrain(IntegerType())
    assertTrue(constrainedToInteger.constrain(NumberType()).contains(IntegerType()))
    assertFalse(constrainedToInteger.constrain(NumberType()).contains(NumberType()))
    assertFalse(constrainedToInteger.constrain(NumberType()).contains(DoubleType()))
    assertFalse(constrainedToInteger.constrain(NumberType()).contains(StringType()))
    assertFalse(constrainedToInteger.constrain(NumberType()).contains(AnyType()))
  }

  @Test
  def reconstrainToSupertypeOfOneBranch() {
    val constrainedToIntegerOrString = TypeSet.all.constrain(IntegerType(), StringType())
    assertTrue(constrainedToIntegerOrString.constrain(NumberType()).contains(IntegerType()))
    assertFalse(constrainedToIntegerOrString.constrain(NumberType()).contains(NumberType()))
    assertFalse(constrainedToIntegerOrString.constrain(NumberType()).contains(DoubleType()))
    assertFalse(constrainedToIntegerOrString.constrain(NumberType()).contains(StringType()))
    assertFalse(constrainedToIntegerOrString.constrain(NumberType()).contains(AnyType()))
  }

  @Test
  def reconstrainToSubtypeWithinCollection() {
    val constrainedToCollection = TypeSet.all.constrain(CollectionType(AnyType()))
    assertTrue(constrainedToCollection.constrain(CollectionType(StringType())).contains(CollectionType(StringType())))
    assertFalse(constrainedToCollection.constrain(CollectionType(StringType())).contains(CollectionType(AnyType())))
    assertFalse(constrainedToCollection.constrain(CollectionType(StringType())).contains(CollectionType(IntegerType())))
  }

  @Test
  def reconstrainToAnotherBranch() {
    val constrainedToNumber = TypeSet.all.constrain(NumberType())
    assertFalse(constrainedToNumber.constrain(StringType()).contains(StringType()))
    assertFalse(constrainedToNumber.constrain(StringType()).contains(NumberType()))
    assertFalse(constrainedToNumber.constrain(StringType()).contains(AnyType()))
  }

  @Test
  def constrainToAllTypes() {
    val constrainedToAny = TypeSet.all.constrain(TypeSet.all)
    assertTrue(constrainedToAny.contains(AnyType()))
    assertTrue(constrainedToAny.contains(StringType()))
    assertTrue(constrainedToAny.contains(NumberType()))
    assertTrue(constrainedToAny.contains(IntegerType()))
    assertTrue(constrainedToAny.contains(DoubleType()))
    assertTrue(constrainedToAny.contains(NodeType()))
    assertTrue(constrainedToAny.contains(CollectionType(AnyType())))
    assertTrue(constrainedToAny.contains(CollectionType(DoubleType())))
  }

  @Test
  def constrainToAllTypesInABranch() {
    val constrainedToNumber = TypeSet.all.constrain(NumberType())
    assertTrue(TypeSet.all.constrain(constrainedToNumber).contains(NumberType()))
    assertTrue(TypeSet.all.constrain(constrainedToNumber).contains(IntegerType()))
    assertTrue(TypeSet.all.constrain(constrainedToNumber).contains(DoubleType()))
    assertFalse(TypeSet.all.constrain(constrainedToNumber).contains(AnyType()))
  }

  @Test
  def reconstrainToAllTypes() {
    val constrainedToNumber = TypeSet.all.constrain(NumberType())
    assertTrue(constrainedToNumber.constrain(TypeSet.all).contains(NumberType()))
    assertTrue(constrainedToNumber.constrain(TypeSet.all).contains(IntegerType()))
    assertTrue(constrainedToNumber.constrain(TypeSet.all).contains(DoubleType()))
    assertFalse(constrainedToNumber.constrain(TypeSet.all).contains(AnyType()))
  }

  @Test
  def reconstrainToAllTypesInDifferentBranch() {
    val constrainedToString = TypeSet.all.constrain(StringType())
    val constrainedToNumber = TypeSet.all.constrain(NumberType())
    assertFalse(constrainedToString.constrain(constrainedToNumber).contains(StringType()))
    assertFalse(constrainedToString.constrain(constrainedToNumber).contains(NumberType()))
    assertFalse(constrainedToString.constrain(constrainedToNumber).contains(AnyType()))
  }

  @Test
  def reconstrainFromMultipleBranchesToSubtypeOfASingleBranch() {
    val constrainedToNumberOrCollectionOfAny = TypeSet.all.constrain(NumberType(), CollectionType(AnyType()))
    val constrainedToCollectionOfNumber = TypeSet.all.constrain(CollectionType(NumberType()))
    assertTrue(constrainedToNumberOrCollectionOfAny.constrain(constrainedToCollectionOfNumber).contains(CollectionType(NumberType())))
    assertTrue(constrainedToNumberOrCollectionOfAny.constrain(constrainedToCollectionOfNumber).contains(CollectionType(DoubleType())))
    assertFalse(constrainedToNumberOrCollectionOfAny.constrain(constrainedToCollectionOfNumber).contains(CollectionType(StringType())))
    assertFalse(constrainedToNumberOrCollectionOfAny.constrain(constrainedToCollectionOfNumber).contains(CollectionType(AnyType())))
    assertFalse(constrainedToNumberOrCollectionOfAny.constrain(constrainedToCollectionOfNumber).contains(NumberType()))
    assertFalse(constrainedToNumberOrCollectionOfAny.constrain(constrainedToCollectionOfNumber).contains(DoubleType()))
    assertFalse(constrainedToNumberOrCollectionOfAny.constrain(constrainedToCollectionOfNumber).contains(AnyType()))
  }

  @Test
  def mergeDownToRootType() {
    val mergedWithAny = TypeSet.all.mergeDown(AnyType())
    assertTrue(mergedWithAny.contains(AnyType()))
    assertFalse(mergedWithAny.contains(NumberType()))
    assertFalse(mergedWithAny.contains(StringType()))
    assertFalse(mergedWithAny.contains(IntegerType()))
    assertFalse(mergedWithAny.contains(CollectionType(AnyType())))
  }

  @Test
  def mergeDownToLeafType() {
    val mergedWithInteger = TypeSet.all.mergeDown(IntegerType())
    assertTrue(mergedWithInteger.contains(AnyType()))
    assertTrue(mergedWithInteger.contains(NumberType()))
    assertTrue(mergedWithInteger.contains(IntegerType()))
    assertFalse(mergedWithInteger.contains(DoubleType()))
    assertFalse(mergedWithInteger.contains(StringType()))
    assertFalse(mergedWithInteger.contains(CollectionType(AnyType())))
  }

  @Test
  def mergeDownToCollectionType() {
    val mergedWithCollectionOfAny = TypeSet.all.mergeDown(CollectionType(AnyType()))
    assertTrue(mergedWithCollectionOfAny.contains(CollectionType(AnyType())))
    assertTrue(mergedWithCollectionOfAny.contains(AnyType()))
    assertFalse(mergedWithCollectionOfAny.contains(CollectionType(StringType())))
    assertFalse(mergedWithCollectionOfAny.contains(StringType()))

    val mergedWithCollectionOfString = TypeSet.all.mergeDown(CollectionType(StringType()))
    assertTrue(mergedWithCollectionOfString.contains(CollectionType(StringType())))
    assertTrue(mergedWithCollectionOfString.contains(CollectionType(AnyType())))
    assertTrue(mergedWithCollectionOfString.contains(AnyType()))
    assertFalse(mergedWithCollectionOfString.contains(StringType()))
  }

  @Test
  def mergeDownToMultipleTypes() {
    val mergedWithIntegerOrString = TypeSet.all.mergeDown(IntegerType(), StringType())
    assertTrue(mergedWithIntegerOrString.contains(AnyType()))
    assertTrue(mergedWithIntegerOrString.contains(NumberType()))
    assertTrue(mergedWithIntegerOrString.contains(IntegerType()))
    assertFalse(mergedWithIntegerOrString.contains(DoubleType()))
    assertTrue(mergedWithIntegerOrString.contains(StringType()))
    assertFalse(mergedWithIntegerOrString.contains(CollectionType(AnyType())))

    val mergedWithCollectionOfStringOrInteger = TypeSet.all.mergeDown(CollectionType(StringType()), IntegerType())
    assertTrue(mergedWithCollectionOfStringOrInteger.contains(CollectionType(StringType())))
    assertTrue(mergedWithCollectionOfStringOrInteger.contains(CollectionType(AnyType())))
    assertTrue(mergedWithCollectionOfStringOrInteger.contains(IntegerType()))
    assertTrue(mergedWithCollectionOfStringOrInteger.contains(NumberType()))
    assertTrue(mergedWithCollectionOfStringOrInteger.contains(AnyType()))
    assertFalse(mergedWithCollectionOfStringOrInteger.contains(StringType()))
    assertFalse(mergedWithCollectionOfStringOrInteger.contains(DoubleType()))
  }

  @Test
  def mergeDownFromDifferentBranchesToSingleSuperType() {
    val mergedWithCollectionOfStringOrInteger = TypeSet.all.mergeDown(CollectionType(StringType()), IntegerType())
    assertFalse(mergedWithCollectionOfStringOrInteger.mergeDown(NumberType()).contains(CollectionType(StringType())))
    assertFalse(mergedWithCollectionOfStringOrInteger.mergeDown(NumberType()).contains(CollectionType(AnyType())))
    assertFalse(mergedWithCollectionOfStringOrInteger.mergeDown(NumberType()).contains(IntegerType()))
    assertTrue(mergedWithCollectionOfStringOrInteger.mergeDown(NumberType()).contains(NumberType()))
    assertTrue(mergedWithCollectionOfStringOrInteger.mergeDown(NumberType()).contains(AnyType()))
    assertFalse(mergedWithCollectionOfStringOrInteger.mergeDown(NumberType()).contains(DoubleType()))
    assertFalse(mergedWithCollectionOfStringOrInteger.mergeDown(NumberType()).contains(StringType()))

    assertFalse(mergedWithCollectionOfStringOrInteger.mergeDown(CollectionType(AnyType())).contains(CollectionType(StringType())))
    assertTrue(mergedWithCollectionOfStringOrInteger.mergeDown(CollectionType(AnyType())).contains(CollectionType(AnyType())))
    assertTrue(mergedWithCollectionOfStringOrInteger.mergeDown(CollectionType(AnyType())).contains(AnyType()))
    assertFalse(mergedWithCollectionOfStringOrInteger.mergeDown(CollectionType(AnyType())).contains(IntegerType()))
    assertFalse(mergedWithCollectionOfStringOrInteger.mergeDown(CollectionType(AnyType())).contains(NumberType()))
    assertFalse(mergedWithCollectionOfStringOrInteger.mergeDown(CollectionType(AnyType())).contains(StringType()))
  }

  @Test
  def mergeDownFromDifferentBranchesToSingleSubType() {
    val mergedWithCollectionOfStringOrNumber = TypeSet.all.mergeDown(CollectionType(StringType()), NumberType())
    assertFalse(mergedWithCollectionOfStringOrNumber.mergeDown(IntegerType()).contains(CollectionType(StringType())))
    assertFalse(mergedWithCollectionOfStringOrNumber.mergeDown(IntegerType()).contains(CollectionType(AnyType())))
    assertFalse(mergedWithCollectionOfStringOrNumber.mergeDown(IntegerType()).contains(IntegerType()))
    assertTrue(mergedWithCollectionOfStringOrNumber.mergeDown(IntegerType()).contains(NumberType()))
    assertTrue(mergedWithCollectionOfStringOrNumber.mergeDown(IntegerType()).contains(AnyType()))
    assertFalse(mergedWithCollectionOfStringOrNumber.mergeDown(IntegerType()).contains(DoubleType()))
    assertFalse(mergedWithCollectionOfStringOrNumber.mergeDown(IntegerType()).contains(StringType()))
  }

  @Test
  def mergeDownFromConstrainedBranchToSubType() {
    val constrainedToNumber = TypeSet.all.constrain(NumberType())
    assertTrue(constrainedToNumber.mergeDown(IntegerType()).contains(IntegerType()))
    assertTrue(constrainedToNumber.mergeDown(IntegerType()).contains(NumberType()))
    assertFalse(constrainedToNumber.mergeDown(IntegerType()).contains(DoubleType()))
    assertFalse(constrainedToNumber.mergeDown(IntegerType()).contains(AnyType()))
    assertFalse(constrainedToNumber.mergeDown(IntegerType()).contains(StringType()))
  }

  @Test
  def mergeDownFromConstrainedBranchToConstraintRoot() {
    val constrainedToNumber = TypeSet.all.constrain(NumberType())
    assertFalse(constrainedToNumber.mergeDown(NumberType()).contains(IntegerType()))
    assertTrue(constrainedToNumber.mergeDown(NumberType()).contains(NumberType()))
    assertFalse(constrainedToNumber.mergeDown(NumberType()).contains(DoubleType()))
    assertFalse(constrainedToNumber.mergeDown(NumberType()).contains(StringType()))
    assertFalse(constrainedToNumber.mergeDown(NumberType()).contains(AnyType()))
  }

  @Test
  def mergeDownFromConstrainedBranchToSuperType() {
    val constrainedToInteger = TypeSet.all.constrain(IntegerType())
    assertFalse(constrainedToInteger.mergeDown(NumberType()).contains(IntegerType()))
    assertTrue(constrainedToInteger.mergeDown(NumberType()).contains(NumberType()))
    assertFalse(constrainedToInteger.mergeDown(NumberType()).contains(AnyType()))
    assertFalse(constrainedToInteger.mergeDown(NumberType()).contains(StringType()))
  }

  @Test
  def mergeDownFromConstrainedBranchToTypeInDifferentBranch() {
    val constrainedToNumber = TypeSet.all.constrain(NumberType())
    assertFalse(constrainedToNumber.mergeDown(StringType()).contains(IntegerType()))
    assertFalse(constrainedToNumber.mergeDown(StringType()).contains(NumberType()))
    assertFalse(constrainedToNumber.mergeDown(StringType()).contains(StringType()))
    assertTrue(constrainedToNumber.mergeDown(StringType()).contains(AnyType()))

    val constrainedToInteger = TypeSet.all.constrain(IntegerType())
    assertFalse(constrainedToInteger.mergeDown(StringType()).contains(IntegerType()))
    assertFalse(constrainedToInteger.mergeDown(StringType()).contains(NumberType()))
    assertFalse(constrainedToInteger.mergeDown(StringType()).contains(StringType()))
    assertTrue(constrainedToInteger.mergeDown(StringType()).contains(AnyType()))
  }

  @Test
  def mergeDownFromMultipleConstrainedBranchesToSubtypeOfSingleBranch() {
    val constrainedToNumberOrCollectionOfAny = TypeSet.all.constrain(NumberType(), CollectionType(AnyType()))
    assertTrue(constrainedToNumberOrCollectionOfAny.mergeDown(IntegerType()).contains(IntegerType()))
    assertTrue(constrainedToNumberOrCollectionOfAny.mergeDown(IntegerType()).contains(NumberType()))
    assertTrue(constrainedToNumberOrCollectionOfAny.mergeDown(IntegerType()).contains(AnyType()))
    assertFalse(constrainedToNumberOrCollectionOfAny.mergeDown(IntegerType()).contains(StringType()))
    assertFalse(constrainedToNumberOrCollectionOfAny.mergeDown(IntegerType()).contains(CollectionType(AnyType())))

    assertTrue(constrainedToNumberOrCollectionOfAny.mergeDown(CollectionType(IntegerType())).contains(CollectionType(IntegerType())))
    assertTrue(constrainedToNumberOrCollectionOfAny.mergeDown(CollectionType(IntegerType())).contains(CollectionType(NumberType())))
    assertTrue(constrainedToNumberOrCollectionOfAny.mergeDown(CollectionType(IntegerType())).contains(CollectionType(AnyType())))
    assertFalse(constrainedToNumberOrCollectionOfAny.mergeDown(CollectionType(IntegerType())).contains(NumberType()))
    assertFalse(constrainedToNumberOrCollectionOfAny.mergeDown(CollectionType(IntegerType())).contains(StringType()))
    assertTrue(constrainedToNumberOrCollectionOfAny.mergeDown(CollectionType(IntegerType())).contains(AnyType()))
  }

  @Test
  def mergeDownWithIndefiniteSet() {
    assertEquals(TypeSet(AnyType()), TypeSet(AnyType()).mergeDown(TypeSet.all))
    assertEquals(TypeSet(AnyType()), TypeSet.all.mergeDown(TypeSet(AnyType())))
    assertEquals(TypeSet(AnyType()), TypeSet(AnyType()).mergeDown(TypeSet.all.constrain(CollectionType(AnyType()))))
    assertEquals(TypeSet(AnyType()), TypeSet.all.constrain(CollectionType(AnyType())).mergeDown(TypeSet(AnyType())))
  }

  @Test
  def mergeDownFromBranchToSuperSet() {
    val constrainedToNumber = TypeSet.all.constrain(NumberType())
    val constrainedToInteger = TypeSet.all.constrain(IntegerType())
    assertTrue(constrainedToInteger.mergeDown(constrainedToNumber).contains(IntegerType()))
    assertTrue(constrainedToInteger.mergeDown(constrainedToNumber).contains(NumberType()))
    assertFalse(constrainedToInteger.mergeDown(constrainedToNumber).contains(StringType()))
    assertFalse(constrainedToInteger.mergeDown(constrainedToNumber).contains(AnyType()))

    val constrainedToCollectionOfCollection = TypeSet.all.constrain(CollectionType(CollectionType(AnyType())))
    assertTrue(constrainedToCollectionOfCollection.mergeDown(TypeSet.all).contains(CollectionType(CollectionType(StringType()))))
    assertTrue(constrainedToCollectionOfCollection.mergeDown(TypeSet.all).contains(CollectionType(CollectionType(IntegerType()))))
    assertTrue(constrainedToCollectionOfCollection.mergeDown(TypeSet.all).contains(CollectionType(CollectionType(NumberType()))))
    assertTrue(constrainedToCollectionOfCollection.mergeDown(TypeSet.all).contains(CollectionType(CollectionType(AnyType()))))
    assertFalse(constrainedToCollectionOfCollection.mergeDown(TypeSet.all).contains(CollectionType(StringType())))
    assertFalse(constrainedToCollectionOfCollection.mergeDown(TypeSet.all).contains(CollectionType(NumberType())))
    assertTrue(constrainedToCollectionOfCollection.mergeDown(TypeSet.all).contains(CollectionType(AnyType())))
    assertFalse(constrainedToCollectionOfCollection.mergeDown(TypeSet.all).contains(StringType()))
    assertFalse(constrainedToCollectionOfCollection.mergeDown(TypeSet.all).contains(NumberType()))
    assertTrue(constrainedToCollectionOfCollection.mergeDown(TypeSet.all).contains(AnyType()))
  }

  @Test
  def mergeDownFromBranchToSameSet() {
    val constrainedToNumber = TypeSet.all.constrain(NumberType())
    assertTrue(constrainedToNumber.mergeDown(constrainedToNumber).contains(IntegerType()))
    assertTrue(constrainedToNumber.mergeDown(constrainedToNumber).contains(NumberType()))
    assertFalse(constrainedToNumber.mergeDown(constrainedToNumber).contains(StringType()))
    assertFalse(constrainedToNumber.mergeDown(constrainedToNumber).contains(AnyType()))
  }

  @Test
  def mergeDownFromBranchToSubSet() {
    val constrainedToNumber = TypeSet.all.constrain(NumberType())
    val constrainedToInteger = TypeSet.all.constrain(IntegerType())
    assertTrue(constrainedToNumber.mergeDown(constrainedToInteger).contains(IntegerType()))
    assertTrue(constrainedToNumber.mergeDown(constrainedToInteger).contains(NumberType()))
    assertFalse(constrainedToNumber.mergeDown(constrainedToInteger).contains(StringType()))
    assertFalse(constrainedToNumber.mergeDown(constrainedToInteger).contains(AnyType()))

    val constrainedToCollectionOfCollection = TypeSet.all.constrain(CollectionType(CollectionType(AnyType())))
    assertTrue(TypeSet.all.mergeDown(constrainedToCollectionOfCollection).contains(CollectionType(CollectionType(StringType()))))
    assertTrue(TypeSet.all.mergeDown(constrainedToCollectionOfCollection).contains(CollectionType(CollectionType(IntegerType()))))
    assertTrue(TypeSet.all.mergeDown(constrainedToCollectionOfCollection).contains(CollectionType(CollectionType(NumberType()))))
    assertTrue(TypeSet.all.mergeDown(constrainedToCollectionOfCollection).contains(CollectionType(CollectionType(AnyType()))))
    assertFalse(TypeSet.all.mergeDown(constrainedToCollectionOfCollection).contains(CollectionType(StringType())))
    assertFalse(TypeSet.all.mergeDown(constrainedToCollectionOfCollection).contains(CollectionType(NumberType())))
    assertTrue(TypeSet.all.mergeDown(constrainedToCollectionOfCollection).contains(CollectionType(AnyType())))
    assertFalse(TypeSet.all.mergeDown(constrainedToCollectionOfCollection).contains(StringType()))
    assertFalse(TypeSet.all.mergeDown(constrainedToCollectionOfCollection).contains(NumberType()))
    assertTrue(TypeSet.all.mergeDown(constrainedToCollectionOfCollection).contains(AnyType()))
  }

  @Test
  def constrainFromMergedDownSetToSuperType() {
    val allMergedWithInteger = TypeSet.all.mergeDown(IntegerType())
    assertTrue(allMergedWithInteger.constrain(NumberType()).contains(IntegerType()))
    assertTrue(allMergedWithInteger.constrain(NumberType()).contains(NumberType()))
    assertFalse(allMergedWithInteger.constrain(NumberType()).contains(DoubleType()))
    assertFalse(allMergedWithInteger.constrain(NumberType()).contains(AnyType()))
    assertFalse(allMergedWithInteger.constrain(NumberType()).contains(StringType()))
  }

  @Test
  def constrainFromMergedDownSetToMergeType() {
    val allMergedWithNumber = TypeSet.all.mergeDown(NumberType())
    assertFalse(allMergedWithNumber.constrain(NumberType()).contains(IntegerType()))
    assertTrue(allMergedWithNumber.constrain(NumberType()).contains(NumberType()))
    assertFalse(allMergedWithNumber.constrain(NumberType()).contains(DoubleType()))
    assertFalse(allMergedWithNumber.constrain(NumberType()).contains(AnyType()))
    assertFalse(allMergedWithNumber.constrain(NumberType()).contains(StringType()))
  }

  @Test
  def constrainFromMergedDownSetToSubType() {
    val allMergedWithNumber = TypeSet.all.mergeDown(NumberType())
    assertFalse(allMergedWithNumber.constrain(IntegerType()).contains(IntegerType()))
    assertFalse(allMergedWithNumber.constrain(IntegerType()).contains(NumberType()))
    assertFalse(allMergedWithNumber.constrain(IntegerType()).contains(DoubleType()))
    assertFalse(allMergedWithNumber.constrain(IntegerType()).contains(AnyType()))
    assertFalse(allMergedWithNumber.constrain(IntegerType()).contains(StringType()))
  }

  @Test
  def constrainFromMergedDownSetToDifferentBranch() {
    val allMergedWithNumber = TypeSet.all.mergeDown(NumberType())
    assertFalse(allMergedWithNumber.constrain(StringType()).contains(IntegerType()))
    assertFalse(allMergedWithNumber.constrain(StringType()).contains(NumberType()))
    assertFalse(allMergedWithNumber.constrain(StringType()).contains(AnyType()))
    assertFalse(allMergedWithNumber.constrain(StringType()).contains(StringType()))
  }

  @Test
  def constrainFromMergedDownSetToSuperSet() {
    val allMergedWithInteger = TypeSet.all.mergeDown(IntegerType())
    val allMergedWithNumber = TypeSet.all.mergeDown(NumberType())
    assertFalse(allMergedWithNumber.constrain(allMergedWithInteger).contains(IntegerType()))
    assertTrue(allMergedWithNumber.constrain(allMergedWithInteger).contains(NumberType()))
    assertTrue(allMergedWithNumber.constrain(allMergedWithInteger).contains(AnyType()))
    assertFalse(allMergedWithNumber.constrain(allMergedWithInteger).contains(DoubleType()))
    assertFalse(allMergedWithNumber.constrain(allMergedWithInteger).contains(StringType()))
  }

  @Test
  def constrainFromMergedDownSetToSameSet() {
    val allMergedWithNumber = TypeSet.all.mergeDown(NumberType())
    assertFalse(allMergedWithNumber.constrain(allMergedWithNumber).contains(IntegerType()))
    assertTrue(allMergedWithNumber.constrain(allMergedWithNumber).contains(NumberType()))
    assertTrue(allMergedWithNumber.constrain(allMergedWithNumber).contains(AnyType()))
    assertFalse(allMergedWithNumber.constrain(allMergedWithNumber).contains(DoubleType()))
    assertFalse(allMergedWithNumber.constrain(allMergedWithNumber).contains(StringType()))
  }

  @Test
  def constrainFromMergedDownSetToSubSet() {
    val allMergedWithInteger = TypeSet.all.mergeDown(IntegerType())
    val allMergedWithNumber = TypeSet.all.mergeDown(NumberType())
    assertFalse(allMergedWithInteger.constrain(allMergedWithNumber).contains(IntegerType()))
    assertTrue(allMergedWithInteger.constrain(allMergedWithNumber).contains(NumberType()))
    assertTrue(allMergedWithInteger.constrain(allMergedWithNumber).contains(AnyType()))
    assertFalse(allMergedWithInteger.constrain(allMergedWithNumber).contains(DoubleType()))
    assertFalse(allMergedWithInteger.constrain(allMergedWithNumber).contains(StringType()))
  }

  @Test
  def constrainFromMergedDownSetToIntersectingSet() {
    val allMergedWithInteger = TypeSet.all.mergeDown(IntegerType())
    val allMergedWithString = TypeSet.all.mergeDown(StringType())
    assertFalse(allMergedWithInteger.constrain(allMergedWithString).contains(IntegerType()))
    assertFalse(allMergedWithInteger.constrain(allMergedWithString).contains(NumberType()))
    assertTrue(allMergedWithInteger.constrain(allMergedWithString).contains(AnyType()))
    assertFalse(allMergedWithInteger.constrain(allMergedWithString).contains(DoubleType()))
    assertFalse(allMergedWithInteger.constrain(allMergedWithString).contains(StringType()))

    val allMergedWithStringAndNumber = TypeSet.all.mergeDown(StringType(), NumberType())
    assertFalse(allMergedWithInteger.constrain(allMergedWithStringAndNumber).contains(IntegerType()))
    assertTrue(allMergedWithInteger.constrain(allMergedWithStringAndNumber).contains(NumberType()))
    assertTrue(allMergedWithInteger.constrain(allMergedWithStringAndNumber).contains(AnyType()))
    assertFalse(allMergedWithInteger.constrain(allMergedWithStringAndNumber).contains(DoubleType()))
    assertFalse(allMergedWithInteger.constrain(allMergedWithStringAndNumber).contains(StringType()))
  }

  @Test
  def mergeDownFromMergedDownToSuperSet() {
    val allMergedWithInteger = TypeSet.all.mergeDown(IntegerType())
    val allMergedWithNumber = TypeSet.all.mergeDown(NumberType())
    assertFalse(allMergedWithNumber.mergeDown(allMergedWithInteger).contains(IntegerType()))
    assertTrue(allMergedWithNumber.mergeDown(allMergedWithInteger).contains(NumberType()))
    assertFalse(allMergedWithNumber.mergeDown(allMergedWithInteger).contains(StringType()))
    assertTrue(allMergedWithNumber.mergeDown(allMergedWithInteger).contains(AnyType()))
  }

  @Test
  def shouldAddTypeToSet() {
    assertTrue((TypeSet.all.constrain(NumberType()) + StringType()).contains(IntegerType()))
    assertTrue((TypeSet.all.constrain(NumberType()) + StringType()).contains(NumberType()))
    assertTrue((TypeSet.all.constrain(NumberType()) + StringType()).contains(StringType()))
    assertFalse((TypeSet.all.constrain(NumberType()) + StringType()).contains(BooleanType()))
    assertFalse((TypeSet.all.constrain(NumberType()) + StringType()).contains(AnyType()))

    assertTrue((TypeSet.all.constrain(NumberType()) + CollectionType(StringType())).contains(IntegerType()))
    assertTrue((TypeSet.all.constrain(NumberType()) + CollectionType(StringType())).contains(NumberType()))
    assertFalse((TypeSet.all.constrain(NumberType()) + CollectionType(StringType())).contains(StringType()))
    assertFalse((TypeSet.all.constrain(NumberType()) + CollectionType(StringType())).contains(AnyType()))
    assertFalse((TypeSet.all.constrain(NumberType()) + CollectionType(StringType())).contains(CollectionType(AnyType())))
    assertTrue((TypeSet.all.constrain(NumberType()) + CollectionType(StringType())).contains(CollectionType(StringType())))
    assertFalse((TypeSet.all.constrain(NumberType()) + CollectionType(StringType())).contains(CollectionType(BooleanType())))
  }

  //  @Test
  //  def shouldRemoveTypeFromSet() {
  //    assertTrue((TypeSet.all - BooleanType()).contains(IntegerType()))
  //    assertTrue((TypeSet.all - BooleanType()).contains(NumberType()))
  //    assertFalse((TypeSet.all - BooleanType()).contains(BooleanType()))
  //    assertTrue((TypeSet.all - BooleanType()).contains(StringType()))
  //    assertTrue((TypeSet.all - BooleanType()).contains(AnyType()))
  //    assertTrue((TypeSet.all - BooleanType()).contains(CollectionType(AnyType())))
  //  }

  @Test
  def shouldHaveIndefiniteSizeWhenAllowingUnconstrainedAnyAtAnyDepth() {
    assertFalse(TypeSet.all.hasDefiniteSize)
    assertFalse(TypeSet.all.constrain(CollectionType(AnyType())).hasDefiniteSize)

    assertTrue(TypeSet.all.constrain(StringType(), NumberType()).hasDefiniteSize)
    assertTrue(TypeSet.all.constrain(CollectionType(StringType())).hasDefiniteSize)

    assertFalse(TypeSet.all.constrain(CollectionType(CollectionType(AnyType()))).hasDefiniteSize)
    assertTrue(TypeSet.all.constrain(CollectionType(CollectionType(StringType()))).hasDefiniteSize)

    assertTrue(TypeSet.all.mergeDown(AnyType()).hasDefiniteSize)
    assertTrue(TypeSet.all.constrain(CollectionType(AnyType())).mergeDown(CollectionType(AnyType())).hasDefiniteSize)
  }

  @Test
  def shouldBeEmptyWhenNoPossibilitiesRemain() {
    assertFalse(TypeSet.all.isEmpty)
    assertTrue(TypeSet.all.mergeDown(NumberType()).constrain(IntegerType()).isEmpty)
  }

  @Test
  def shouldIterateOverDefiniteSizedSet() {
    assertEquals(Seq(StringType()), TypeSet.all.constrain(StringType()).iterator.toSeq)
    assertEquals(Seq(DoubleType(), IntegerType(), LongType(), NumberType()), TypeSet.all.constrain(NumberType()).iterator.toSeq)
    assertEquals(Seq(BooleanType(), DoubleType(), IntegerType(), LongType(), NumberType()), TypeSet.all.constrain(BooleanType(), NumberType()).iterator.toSeq)
    assertEquals(Seq(AnyType(), NumberType()), TypeSet.all.mergeDown(NumberType()).iterator.toSeq)
    assertEquals(Seq(BooleanType(), StringType(), CollectionType(BooleanType()), CollectionType(StringType())),
      TypeSet.all.constrain(BooleanType(), StringType(), CollectionType(BooleanType()), CollectionType(StringType())).iterator.toSeq)
    assertEquals(Seq(BooleanType(), StringType(), CollectionType(BooleanType()), CollectionType(CollectionType(StringType()))),
      TypeSet.all.constrain(BooleanType(), StringType(), CollectionType(BooleanType()), CollectionType(CollectionType(StringType()))).iterator.toSeq)
  }

  @Test
  def shouldReparentIntoCollection() {
    assertEquals(Set(CollectionType(StringType()), CollectionType(CollectionType(NumberType()))), TypeSet(StringType(), CollectionType(NumberType())).reparent(CollectionType(_)))
    assertEquals(TypeSet.all.constrain(CollectionType(AnyType())), TypeSet.all.reparent(CollectionType(_)))
  }

  @Test
  def definiteSizedSetsShouldEqual() {
    assertEquals(TypeSet(StringType()), TypeSet(StringType()))
    assertEquals(TypeSet.all.constrain(StringType()), TypeSet.all.constrain(StringType()))
    assertEquals(TypeSet(StringType()), TypeSet.all.constrain(StringType()))
    assertEquals(TypeSet.all.constrain(StringType()), TypeSet(StringType()))

    assertEquals(TypeSet(NumberType(), LongType(), IntegerType(), DoubleType()), TypeSet(NumberType(), LongType(), IntegerType(), DoubleType()))
    assertEquals(TypeSet.all.constrain(NumberType()), TypeSet.all.constrain(NumberType()))
    assertEquals(TypeSet(NumberType(), LongType(), IntegerType(), DoubleType()), TypeSet.all.constrain(NumberType()))
    assertEquals(TypeSet.all.constrain(NumberType()), TypeSet(NumberType(), LongType(), IntegerType(), DoubleType()))
  }

  @Test
  def indefiniteSizedSetsShouldEqual() {
    assertEquals(TypeSet.all, TypeSet.all)
    assertEquals(TypeSet.all.constrain(CollectionType(AnyType())), TypeSet.all.constrain(CollectionType(AnyType())))
    assertEquals(TypeSet.all.constrain(NumberType(), CollectionType(AnyType())), TypeSet.all.constrain(NumberType(), CollectionType(AnyType())))
  }

  @Test
  def definiteSizedSetsShouldNotEqualIndefiniteSizedSets() {
    assertNotEquals(TypeSet.all.constrain(NumberType()), TypeSet.all)
    assertNotEquals(TypeSet.all, TypeSet.all.constrain(NumberType()))

    assertNotEquals(TypeSet.all.constrain(NumberType()), TypeSet.all.constrain(CollectionType(AnyType())))
    assertNotEquals(TypeSet.all.constrain(CollectionType(AnyType())), TypeSet.all.constrain(NumberType()))

    assertNotEquals(TypeSet.all, TypeSet(NumberType()))
    assertNotEquals(TypeSet(NumberType()), TypeSet.all)
  }

  @Test
  def shouldFormatToStringForIndefiniteSizedSet() {
    assertEquals("<T>", TypeSet.all.formattedString)
    assertEquals("Boolean or Collection<<T>>", TypeSet.all.constrain(BooleanType(), CollectionType(AnyType())).formattedString)
    assertEquals("Boolean, Collection<String> or Collection<Collection<<T>>>", TypeSet.all.constrain(BooleanType(), CollectionType(StringType()), CollectionType(CollectionType(AnyType()))).formattedString)
  }

  @Test
  def shouldFormatToStringForDefiniteSizedSet() {
    assertEquals("String", TypeSet.all.constrain(StringType()).formattedString)
    assertEquals("Double, Integer, Long or Number", TypeSet.all.constrain(NumberType()).formattedString)
    assertEquals("Boolean, Double, Integer, Long or Number", TypeSet.all.constrain(BooleanType(), NumberType()).formattedString)
    assertEquals("Any or Number", TypeSet.all.mergeDown(NumberType()).formattedString)
    assertEquals("Boolean, String, Collection<Boolean> or Collection<String>",
      TypeSet.all.constrain(BooleanType(), StringType(), CollectionType(BooleanType()), CollectionType(StringType())).formattedString)
    assertEquals("Boolean, String, Collection<Boolean> or Collection<Collection<String>>",
      TypeSet.all.constrain(BooleanType(), StringType(), CollectionType(BooleanType()), CollectionType(CollectionType(StringType()))).formattedString)
  }
}
