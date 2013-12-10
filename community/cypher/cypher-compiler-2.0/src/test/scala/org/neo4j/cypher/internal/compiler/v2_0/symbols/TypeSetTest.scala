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

class TypeSetTest extends Assertions {

  @Test
  def definiteSizedSetsShouldEqual() {
    assertEquals(TypeSet(StringType()), TypeSet(StringType()))

    assertEquals(TypeSet.all constrain StringType(), TypeSet.all constrain StringType())
    assertEquals(TypeSet(StringType()), TypeSet.all constrain StringType())
    assertEquals(TypeSet.all constrain StringType(), TypeSet(StringType()))

    val constrainedToNumber = TypeSet.all constrain NumberType()
    assertEquals(TypeSet(NumberType(), LongType(), IntegerType(), DoubleType()), TypeSet(NumberType(), LongType(), IntegerType(), DoubleType()))
    assertEquals(constrainedToNumber, TypeSet.all constrain NumberType())
    assertEquals(TypeSet(NumberType(), LongType(), IntegerType(), DoubleType()), TypeSet.all constrain NumberType())
    assertEquals(constrainedToNumber, TypeSet(NumberType(), LongType(), IntegerType(), DoubleType()))

    assertEquals(constrainedToNumber, constrainedToNumber + IntegerType())
    assertNotEquals(constrainedToNumber, constrainedToNumber + StringType())
  }

  @Test
  def indefiniteSizedSetsShouldEqual() {
    assertEquals(TypeSet.all, TypeSet.all)
    assertEquals(TypeSet.all constrain CollectionType(AnyType()), TypeSet.all constrain CollectionType(AnyType()))
    assertEquals(TypeSet.all.constrain(NumberType(), CollectionType(AnyType())), TypeSet.all.constrain(NumberType(), CollectionType(AnyType())))

    assertEquals(TypeSet.all, TypeSet.all + StringType())
    assertEquals(TypeSet.all, TypeSet.all ++ TypeSet.all)
  }

  @Test
  def definiteSizedSetsShouldNotEqualIndefiniteSizedSets() {
    assertNotEquals(TypeSet.all.constrain(NumberType()), TypeSet.all)
    assertNotEquals(TypeSet.all, TypeSet.all constrain NumberType())

    assertNotEquals(TypeSet.all constrain NumberType(), TypeSet.all.constrain(CollectionType(AnyType())))
    assertNotEquals(TypeSet.all constrain CollectionType(AnyType()), TypeSet.all constrain NumberType())

    assertNotEquals(TypeSet.all, TypeSet(NumberType()))
    assertNotEquals(TypeSet(NumberType()), TypeSet.all)
  }

  @Test
  def shouldInferTypeSetsUsingMergeDown() {
    assertEquals(TypeSet(NodeType(), NumberType(), AnyType()), TypeSet(NodeType(), NumberType()) mergeDown TypeSet(NodeType(), NumberType()))

    assertEquals(TypeSet(NodeType(), NumberType(), AnyType()), TypeSet(NodeType(), NumberType()) mergeDown TypeSet(NodeType(), NumberType()))
    assertEquals(TypeSet(NumberType(), AnyType()), TypeSet(NodeType(), NumberType()) mergeDown TypeSet(NumberType()))
    assertEquals(TypeSet(NodeType(), NumberType(), MapType(), AnyType()), TypeSet(NodeType(), NumberType()) mergeDown TypeSet(NodeType(), NumberType(), RelationshipType()))
    assertEquals(TypeSet(AnyType()), TypeSet(NodeType(), NumberType()) mergeDown TypeSet(AnyType()))
    assertEquals(TypeSet(AnyType()), TypeSet(AnyType()) mergeDown TypeSet(NodeType(), NumberType()))

    assertEquals(TypeSet(MapType()), TypeSet(RelationshipType()) mergeDown TypeSet(NodeType()))
    assertEquals(TypeSet(MapType(), NumberType(), AnyType()), TypeSet(RelationshipType(), LongType()) mergeDown TypeSet(NodeType(), NumberType()))
  }

  @Test
  def shouldMergeDownCollectionIterable() {
    assertEquals(TypeSet(NumberType(), CollectionType(AnyType()), AnyType()),
      TypeSet(IntegerType(), CollectionType(StringType())) mergeDown TypeSet(NumberType(), CollectionType(IntegerType())))
  }

  @Test
  def shouldConstrainTypeSets() {
    assertEquals(TypeSet(IntegerType(), LongType()), TypeSet(IntegerType(), LongType(), StringType(), MapType()) constrain TypeSet(NodeType(), NumberType()))
    assertEquals(TypeSet(CollectionType(StringType())), TypeSet(IntegerType(), CollectionType(StringType())) constrain TypeSet(CollectionType(AnyType())))
    assertEquals(TypeSet.empty, TypeSet(IntegerType(), CollectionType(MapType())) constrain TypeSet(CollectionType(NodeType())))
    assertEquals(TypeSet(IntegerType(), CollectionType(StringType())), TypeSet(IntegerType(), CollectionType(StringType())) constrain TypeSet(AnyType()))
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
    val constrainedToInteger = TypeSet.all constrain IntegerType()
    assertEquals(TypeSet(IntegerType()), constrainedToInteger)
  }

  @Test
  def constrainToBranchTypeShouldContainAllMoreSpecific() {
    val constrainedToNumber = TypeSet.all constrain NumberType()
    assertEquals(TypeSet(NumberType(), IntegerType(), DoubleType(), LongType()), constrainedToNumber)
  }

  @Test
  def constrainWithinMultipleBranchesOfTypeTree() {
    val constrainedToNumberOrString = TypeSet.all.constrain(NumberType(), StringType())
    assertEquals(TypeSet(NumberType(), IntegerType(), DoubleType(), LongType(), StringType()), constrainedToNumberOrString)
  }

  @Test
  def constrainToCollectionBranchOfTypeTree() {
    val constrainedToCollection = TypeSet.all constrain CollectionType(AnyType())
    assertTrue(constrainedToCollection.contains(CollectionType(StringType())))
    assertTrue(constrainedToCollection.contains(CollectionType(IntegerType())))
    assertTrue(constrainedToCollection.contains(CollectionType(AnyType())))
    assertTrue(constrainedToCollection.contains(CollectionType(CollectionType(IntegerType()))))
    assertFalse(constrainedToCollection.contains(BooleanType()))
    assertFalse(constrainedToCollection.contains(AnyType()))
  }

  @Test
  def constrainToLeafTypeWithinCollection() {
    val constrainedToCollectionOfNumber = TypeSet.all constrain CollectionType(NumberType())
    assertEquals(
      TypeSet(
        CollectionType(NumberType()),
        CollectionType(IntegerType()),
        CollectionType(DoubleType()),
        CollectionType(LongType())
      ),
      constrainedToCollectionOfNumber)
  }

  @Test
  def constrainToSuperType() {
    val constrainedToInteger = TypeSet.all constrain IntegerType()
    assertEquals(constrainedToInteger, constrainedToInteger constrain NumberType())
  }

  @Test
  def constrainToSuperTypeOfOneBranch() {
    val constrainedToIntegerOrString = TypeSet.all.constrain(IntegerType(), StringType())
    assertEquals(TypeSet(IntegerType()), constrainedToIntegerOrString constrain NumberType())
  }

  @Test
  def constrainToSubType() {
    val constrainedToNumber = TypeSet.all constrain NumberType()
    assertEquals(TypeSet(IntegerType()), constrainedToNumber constrain IntegerType())
  }

  @Test
  def constrainToSubTypeWithinCollection() {
    val constrainedToCollection = TypeSet.all constrain CollectionType(AnyType())
    assertEquals(TypeSet(CollectionType(StringType())),
      constrainedToCollection constrain CollectionType(StringType()))
  }

  @Test
  def constrainToAnotherBranch() {
    val constrainedToNumber = TypeSet.all constrain NumberType()
    assertEquals(TypeSet(), constrainedToNumber constrain StringType())
  }

  @Test
  def constrainToAllTypes() {
    assertEquals(TypeSet.all, TypeSet.all constrain TypeSet.all)

    val constrainedToNumber = TypeSet.all constrain NumberType()
    assertEquals(constrainedToNumber, constrainedToNumber constrain TypeSet.all)
  }

  @Test
  def constrainToDifferentBranch() {
    val constrainedToString = TypeSet.all constrain StringType()
    val constrainedToNumber = TypeSet.all constrain NumberType()
    assertEquals(TypeSet(), constrainedToString constrain constrainedToNumber)
  }

  @Test
  def constrainFromMultipleBranchesToSubTypeOfASingleBranch() {
    val constrainedToNumberOrCollectionOfAny = TypeSet.all.constrain(NumberType(), CollectionType(AnyType()))
    val constrainedToCollectionOfNumber = TypeSet.all constrain CollectionType(NumberType())
    assertEquals(constrainedToCollectionOfNumber, constrainedToNumberOrCollectionOfAny constrain constrainedToCollectionOfNumber)
  }

  @Test
  def mergeDownToRootType() {
    val mergedWithAny = TypeSet.all mergeDown AnyType()
    assertEquals(TypeSet(AnyType()), mergedWithAny)
  }

  @Test
  def mergeDownToLeafType() {
    val mergedWithInteger = TypeSet.all mergeDown IntegerType()
    assertEquals(TypeSet(AnyType(), NumberType(), IntegerType()), mergedWithInteger)
  }

  @Test
  def mergeDownToCollectionType() {
    val mergedWithCollectionOfAny = TypeSet.all mergeDown CollectionType(AnyType())
    assertEquals(TypeSet(AnyType(), CollectionType(AnyType())), mergedWithCollectionOfAny)

    val mergedWithCollectionOfString = TypeSet.all mergeDown CollectionType(StringType())
    assertEquals(TypeSet(AnyType(), CollectionType(AnyType()), CollectionType(StringType())), mergedWithCollectionOfString)
  }

  @Test
  def mergeDownToMultipleTypes() {
    val mergedWithIntegerOrString = TypeSet.all.mergeDown(IntegerType(), StringType())
    assertEquals(TypeSet(AnyType(), NumberType(), IntegerType(), StringType()), mergedWithIntegerOrString)

    val mergedWithCollectionOfStringOrInteger = TypeSet.all.mergeDown(CollectionType(StringType()), IntegerType())
    assertEquals(TypeSet(
      AnyType(),
      NumberType(),
      IntegerType(),
      CollectionType(AnyType()),
      CollectionType(StringType())
    ), mergedWithCollectionOfStringOrInteger)
  }

  @Test
  def mergeDownFromDifferentBranchesToSingleSuperType() {
    val mergedWithCollectionOfStringOrInteger = TypeSet.all.mergeDown(CollectionType(StringType()), IntegerType())
    assertEquals(TypeSet(AnyType(), NumberType()), mergedWithCollectionOfStringOrInteger mergeDown NumberType())
    assertEquals(TypeSet(AnyType(), CollectionType(AnyType())),
      mergedWithCollectionOfStringOrInteger mergeDown CollectionType(AnyType()))
  }

  @Test
  def mergeDownFromDifferentBranchesToSingleSubType() {
    val mergedWithCollectionOfStringOrNumber = TypeSet.all.mergeDown(CollectionType(StringType()), NumberType())
    assertEquals(TypeSet(AnyType(), NumberType()), mergedWithCollectionOfStringOrNumber mergeDown IntegerType())
  }

  @Test
  def mergeDownFromConstrainedBranchToSubType() {
    val constrainedToNumber = TypeSet.all constrain NumberType()
    assertEquals(TypeSet(NumberType(), IntegerType()), constrainedToNumber mergeDown IntegerType())
  }

  @Test
  def mergeDownFromConstrainedBranchToConstraintRoot() {
    val constrainedToNumber = TypeSet.all constrain NumberType()
    assertEquals(TypeSet(NumberType()), constrainedToNumber mergeDown NumberType())
  }

  @Test
  def mergeDownFromConstrainedBranchToSuperType() {
    val constrainedToInteger = TypeSet.all constrain IntegerType()
    assertEquals(TypeSet(NumberType()), constrainedToInteger mergeDown NumberType())
  }

  @Test
  def mergeDownFromConstrainedBranchToTypeInDifferentBranch() {
    val constrainedToNumber = TypeSet.all constrain NumberType()
    assertEquals(TypeSet(AnyType()), constrainedToNumber mergeDown StringType())

    val constrainedToInteger = TypeSet.all.constrain(IntegerType())
    assertEquals(TypeSet(AnyType()), constrainedToInteger mergeDown StringType())
  }

  @Test
  def mergeDownFromMultipleConstrainedBranchesToSubtypeOfSingleBranch() {
    val constrainedToNumberOrCollectionOfAny = TypeSet.all.constrain(NumberType(), CollectionType(AnyType()))
    assertEquals(TypeSet(AnyType(), NumberType(), IntegerType()), constrainedToNumberOrCollectionOfAny mergeDown IntegerType())
    assertEquals(TypeSet(AnyType(), CollectionType(AnyType()), CollectionType(NumberType()), CollectionType(IntegerType())),
      constrainedToNumberOrCollectionOfAny mergeDown CollectionType(IntegerType()))
  }

  @Test
  def mergeDownWithIndefiniteSet() {
    assertEquals(TypeSet(AnyType()), TypeSet(AnyType()) mergeDown TypeSet.all)
    assertEquals(TypeSet(AnyType()), TypeSet.all mergeDown TypeSet(AnyType()))
    assertEquals(TypeSet(AnyType()), TypeSet(AnyType()) mergeDown TypeSet.all.constrain(CollectionType(AnyType())))
    assertEquals(TypeSet(AnyType()), (TypeSet.all constrain CollectionType(AnyType())) mergeDown TypeSet(AnyType()))
  }

  @Test
  def mergeDownFromBranchToSuperSet() {
    val constrainedToNumber = TypeSet.all constrain NumberType()
    val constrainedToInteger = TypeSet.all constrain IntegerType()
    assertEquals(TypeSet(NumberType(), IntegerType()), constrainedToInteger mergeDown constrainedToNumber)

    val constrainedToCollectionOfCollection = TypeSet.all constrain CollectionType(CollectionType(AnyType()))
    assertTrue((constrainedToCollectionOfCollection mergeDown TypeSet.all).contains(CollectionType(CollectionType(StringType()))))
    assertTrue((constrainedToCollectionOfCollection mergeDown TypeSet.all).contains(CollectionType(CollectionType(IntegerType()))))
    assertTrue((constrainedToCollectionOfCollection mergeDown TypeSet.all).contains(CollectionType(CollectionType(NumberType()))))
    assertTrue((constrainedToCollectionOfCollection mergeDown TypeSet.all).contains(CollectionType(CollectionType(AnyType()))))
    assertFalse((constrainedToCollectionOfCollection mergeDown TypeSet.all).contains(CollectionType(StringType())))
    assertFalse((constrainedToCollectionOfCollection mergeDown TypeSet.all).contains(CollectionType(NumberType())))
    assertTrue((constrainedToCollectionOfCollection mergeDown TypeSet.all).contains(CollectionType(AnyType())))
    assertFalse((constrainedToCollectionOfCollection mergeDown TypeSet.all).contains(StringType()))
    assertFalse((constrainedToCollectionOfCollection mergeDown TypeSet.all).contains(NumberType()))
    assertTrue((constrainedToCollectionOfCollection mergeDown TypeSet.all).contains(AnyType()))
  }

  @Test
  def mergeDownFromBranchToSameSet() {
    val constrainedToNumber = TypeSet.all constrain NumberType()
    assertEquals(constrainedToNumber, constrainedToNumber mergeDown constrainedToNumber)
  }

  @Test
  def mergeDownFromBranchToSubSet() {
    val constrainedToNumber = TypeSet.all constrain NumberType()
    val constrainedToInteger = TypeSet.all constrain IntegerType()
    assertEquals(TypeSet(IntegerType(), NumberType()), constrainedToNumber mergeDown constrainedToInteger)

    val constrainedToCollectionOfCollection = TypeSet.all constrain CollectionType(CollectionType(AnyType()))
    assertTrue((TypeSet.all mergeDown constrainedToCollectionOfCollection).contains(CollectionType(CollectionType(StringType()))))
    assertTrue((TypeSet.all mergeDown constrainedToCollectionOfCollection).contains(CollectionType(CollectionType(IntegerType()))))
    assertTrue((TypeSet.all mergeDown constrainedToCollectionOfCollection).contains(CollectionType(CollectionType(NumberType()))))
    assertTrue((TypeSet.all mergeDown constrainedToCollectionOfCollection).contains(CollectionType(CollectionType(AnyType()))))
    assertFalse((TypeSet.all mergeDown constrainedToCollectionOfCollection).contains(CollectionType(StringType())))
    assertFalse((TypeSet.all mergeDown constrainedToCollectionOfCollection).contains(CollectionType(NumberType())))
    assertTrue((TypeSet.all mergeDown constrainedToCollectionOfCollection).contains(CollectionType(AnyType())))
    assertFalse((TypeSet.all mergeDown constrainedToCollectionOfCollection).contains(StringType()))
    assertFalse((TypeSet.all mergeDown constrainedToCollectionOfCollection).contains(NumberType()))
    assertTrue((TypeSet.all mergeDown constrainedToCollectionOfCollection).contains(AnyType()))
  }

  @Test
  def constrainFromMergedDownSetToSuperType() {
    val allMergedWithInteger = TypeSet.all mergeDown IntegerType()
    assertEquals(TypeSet(NumberType(), IntegerType()), allMergedWithInteger constrain NumberType())
  }

  @Test
  def constrainFromMergedDownSetToMergeType() {
    val allMergedWithNumber = TypeSet.all mergeDown NumberType()
    assertEquals(TypeSet(NumberType()), allMergedWithNumber constrain NumberType())
  }

  @Test
  def constrainFromMergedDownSetToSubType() {
    val allMergedWithNumber = TypeSet.all mergeDown NumberType()
    assertEquals(TypeSet(), allMergedWithNumber constrain IntegerType())
  }

  @Test
  def constrainFromMergedDownSetToDifferentBranch() {
    val allMergedWithNumber = TypeSet.all mergeDown NumberType()
    assertEquals(TypeSet(), allMergedWithNumber constrain StringType())
  }

  @Test
  def constrainFromMergedDownSetToSuperSet() {
    val allMergedWithInteger = TypeSet.all mergeDown IntegerType()
    val allMergedWithNumber = TypeSet.all mergeDown NumberType()
    assertEquals(TypeSet(AnyType(), NumberType()), allMergedWithNumber constrain allMergedWithInteger)
  }

  @Test
  def constrainFromMergedDownSetToSameSet() {
    val allMergedWithNumber = TypeSet.all mergeDown NumberType()
    assertEquals(TypeSet(AnyType(), NumberType()), allMergedWithNumber constrain allMergedWithNumber)
  }

  @Test
  def constrainFromMergedDownSetToSubSet() {
    val allMergedWithInteger = TypeSet.all mergeDown IntegerType()
    val allMergedWithNumber = TypeSet.all mergeDown NumberType()
    assertEquals(TypeSet(AnyType(), NumberType(), IntegerType()), allMergedWithInteger constrain allMergedWithNumber)
  }

  @Test
  def constrainFromMergedDownSetToIntersectingSet() {
    val allMergedWithInteger = TypeSet.all mergeDown IntegerType()
    val allMergedWithString = TypeSet.all mergeDown StringType()
    assertEquals(TypeSet(AnyType(), NumberType(), IntegerType()), allMergedWithInteger constrain allMergedWithString)

    val allMergedWithStringAndNumber = TypeSet.all.mergeDown(StringType(), NumberType())
    assertEquals(TypeSet(AnyType(), NumberType(), IntegerType()), allMergedWithInteger constrain allMergedWithStringAndNumber)
  }

  @Test
  def mergeDownFromMergedDownToSuperSet() {
    val allMergedWithInteger = TypeSet.all mergeDown IntegerType()
    val allMergedWithNumber = TypeSet.all mergeDown NumberType()
    assertEquals(TypeSet(AnyType(), NumberType()), allMergedWithNumber mergeDown allMergedWithInteger)
  }

  @Test
  def shouldAddTypeToSet() {
    assertEquals(TypeSet(NumberType(), IntegerType(), DoubleType(), LongType(), StringType()), (TypeSet.all constrain NumberType()) + StringType())
    assertEquals(TypeSet(NumberType(), IntegerType(), DoubleType(), LongType(), CollectionType(StringType())), (TypeSet.all constrain NumberType()) + CollectionType(StringType()))
  }

  @Test
  def shouldReturnIntersection() {
    assertEquals(TypeSet(NumberType()), TypeSet(NumberType(), IntegerType()) & TypeSet(AnyType(), NumberType()))
    assertEquals(TypeSet(NumberType()), (TypeSet.all mergeDown NumberType()) & (TypeSet.all constrain NumberType()))
    assertEquals(TypeSet(NumberType()), TypeSet(NumberType(), IntegerType()) & TypeSet(NumberType(), DoubleType()))

    assertEquals(TypeSet(CollectionType(AnyType()), CollectionType(CollectionType(AnyType()))),
      (TypeSet.all mergeDown CollectionType(CollectionType(AnyType()))) & (TypeSet.all constrain CollectionType(AnyType())))

    assertEquals(TypeSet.all constrain NumberType(), TypeSet.all.constrain(NumberType(), CollectionType(AnyType())) & TypeSet.all.constrain(NumberType(), StringType()))
  }

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
    assertEquals(Seq(StringType()), (TypeSet.all constrain StringType()).iterator.toSeq)
    assertEquals(Seq(DoubleType(), IntegerType(), LongType(), NumberType()), (TypeSet.all constrain NumberType()).iterator.toSeq)
    assertEquals(Seq(BooleanType(), DoubleType(), IntegerType(), LongType(), NumberType()), TypeSet.all.constrain(BooleanType(), NumberType()).iterator.toSeq)
    assertEquals(Seq(AnyType(), NumberType()), (TypeSet.all mergeDown NumberType()).iterator.toSeq)
    assertEquals(Seq(BooleanType(), StringType(), CollectionType(BooleanType()), CollectionType(StringType())),
      TypeSet.all.constrain(BooleanType(), StringType(), CollectionType(BooleanType()), CollectionType(StringType())).iterator.toSeq)
    assertEquals(Seq(BooleanType(), StringType(), CollectionType(BooleanType()), CollectionType(CollectionType(StringType()))),
      TypeSet.all.constrain(BooleanType(), StringType(), CollectionType(BooleanType()), CollectionType(CollectionType(StringType()))).iterator.toSeq)
  }

  @Test
  def shouldReparentIntoCollection() {
    assertEquals(TypeSet(CollectionType(StringType()), CollectionType(CollectionType(NumberType()))), TypeSet(StringType(), CollectionType(NumberType())).reparent(CollectionType(_)))
    assertEquals(TypeSet.all constrain CollectionType(AnyType()), TypeSet.all.reparent(CollectionType(_)))
  }

  @Test
  def shouldFormatNoType() {
    assertEquals("()", TypeSet().mkString("(", ", ", " or ", ")"))
  }

  @Test
  def shouldFormatSingleType() {
    assertEquals("(Any)", TypeSet(AnyType()).mkString("(", ", ", " or ", ")"))
    assertEquals("<Node>", TypeSet(NodeType()).mkString("<", ", ", " and ", ">"))
  }

  @Test
  def shouldFormatTwoTypes() {
    assertEquals("Any or Node", TypeSet(AnyType(), NodeType()).mkString("", ", ", " or ", ""))
    assertEquals("-Node or Relationship-", TypeSet(RelationshipType(), NodeType()).mkString("-", ", ", " or ", "-"))
  }

  @Test
  def shouldFormatThreeTypes() {
    assertEquals("Integer, Node, Relationship", TypeSet(RelationshipType(), IntegerType(), NodeType()).mkString(", "))
    assertEquals("(Integer, Node, Relationship)", TypeSet(RelationshipType(), IntegerType(), NodeType()).mkString("(", ", ", ")"))
    assertEquals("(Any, Node or Relationship)", TypeSet(RelationshipType(), AnyType(), NodeType()).mkString("(", ", ", " or ", ")"))
    assertEquals("[Integer, Node and Relationship]", TypeSet(RelationshipType(), IntegerType(), NodeType()).mkString("[", ", ", " and ", "]"))
  }

  @Test
  def shouldFormatToStringForIndefiniteSizedSet() {
    assertEquals("T", TypeSet.all.mkString(", "))
    assertEquals("Boolean, Collection<T>", TypeSet.all.constrain(BooleanType(), CollectionType(AnyType())).mkString(", "))
    assertEquals("Boolean, Collection<String>, Collection<Collection<T>>", TypeSet.all.constrain(BooleanType(), CollectionType(StringType()), CollectionType(CollectionType(AnyType()))).mkString(", "))
  }

  @Test
  def shouldFormatToStringForDefiniteSizedSet() {
    assertEquals("Any", (TypeSet.all mergeDown AnyType()).mkString(", "))
    assertEquals("String", TypeSet.all.constrain(StringType()).mkString(", "))
    assertEquals("Double, Integer, Long, Number", TypeSet.all.constrain(NumberType()).mkString(", "))
    assertEquals("Boolean, Double, Integer, Long, Number", TypeSet.all.constrain(BooleanType(), NumberType()).mkString(", "))
    assertEquals("Any, Number", TypeSet.all.mergeDown(NumberType()).mkString(", "))
    assertEquals("Boolean, String, Collection<Boolean>, Collection<String>",
      TypeSet.all.constrain(BooleanType(), StringType(), CollectionType(BooleanType()), CollectionType(StringType())).mkString(", "))
    assertEquals("Boolean, String, Collection<Boolean>, Collection<Collection<String>>",
      TypeSet.all.constrain(BooleanType(), StringType(), CollectionType(BooleanType()), CollectionType(CollectionType(StringType()))).mkString(", "))
    assertEquals("Any, Collection<Any>", (TypeSet.all mergeDown CollectionType(AnyType())).mkString(", "))
  }
}
