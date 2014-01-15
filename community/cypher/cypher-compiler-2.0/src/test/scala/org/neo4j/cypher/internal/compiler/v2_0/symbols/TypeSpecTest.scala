/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

class TypeSpecTest extends Assertions {

  @Test
  def allTypesShouldContainAll() {
    assertTrue(TypeSpec.all contains CTAny)
    assertTrue(TypeSpec.all contains CTString)
    assertTrue(TypeSpec.all contains CTNumber)
    assertTrue(TypeSpec.all contains CTInteger)
    assertTrue(TypeSpec.all contains CTDouble)
    assertTrue(TypeSpec.all contains CTNode)
    assertTrue(TypeSpec.all contains CTCollection(CTAny))
    assertTrue(TypeSpec.all contains CTCollection(CTDouble))
    assertTrue(TypeSpec.all contains CTCollection(CTCollection(CTDouble)))
  }

  @Test
  def shouldReturnTrueIfContains() {
    assertTrue(CTNumber.covariant contains CTInteger)
    assertFalse(CTNumber.covariant contains CTString)

    val anyCollection = CTCollection(CTAny).covariant
    assertTrue(anyCollection contains CTCollection(CTString))
    assertTrue(anyCollection contains CTCollection(CTInteger))
    assertTrue(anyCollection contains CTCollection(CTAny))
    assertTrue(anyCollection contains CTCollection(CTCollection(CTInteger)))
    assertFalse(anyCollection contains CTBoolean)
    assertFalse(anyCollection contains CTAny)
  }

  @Test
  def shouldReturnTrueIfContainsAny() {
    assertTrue(TypeSpec.all containsAny TypeSpec.all)
    assertTrue(TypeSpec.all containsAny CTNumber.covariant)
    assertTrue(TypeSpec.all containsAny CTNode.invariant)
    assertTrue(CTNumber.covariant containsAny TypeSpec.all)

    assertTrue(CTNumber.covariant containsAny CTInteger)
    assertTrue(CTInteger.covariant containsAny (CTInteger | CTString))
    assertTrue(CTInteger.covariant containsAny CTNumber.covariant)
    assertTrue(CTInteger.covariant containsAny TypeSpec.all)

    assertFalse(CTInteger.covariant containsAny CTString)
    assertFalse(CTNumber.covariant containsAny CTString)
  }

  @Test
  def shouldUnion() {
    assertEquals(CTNumber | CTDouble | CTInteger | CTLong | CTString,
      T <:< CTNumber | T <:< CTString)
    assertEquals(CTNumber | CTDouble | CTInteger | CTLong | CTBoolean,
      T <:< CTNumber | CTBoolean)

    assertEquals(CTNumber | CTDouble | CTInteger | CTLong | CTCollection(CTString),
      T <:< CTNumber union T <:< CTCollection(CTString))
    assertEquals(CTCollection(CTNumber) | CTCollection(CTString),
      CTCollection(CTNumber) union (T <:< CTCollection(CTString)))
  }

  @Test
  def shouldIntersect() {
    assertEquals(CTInteger.invariant, TypeSpec.all =:= CTInteger)
    assertEquals(CTInteger.invariant, CTNumber.covariant =:= CTInteger)
    assertEquals(TypeSpec.none, CTNumber.covariant =:= CTString)

    assertEquals(CTNumber.invariant, (CTNumber | CTInteger) & (CTAny | CTNumber))
    assertEquals(CTNumber.invariant, CTNumber.contravariant & CTNumber.covariant)
    assertEquals(CTNumber.invariant, (CTNumber | CTInteger) & (CTNumber | CTDouble))

    assertEquals(CTCollection(CTAny) | CTCollection(CTCollection(CTAny)),
      CTCollection(CTCollection(CTAny)).contravariant intersect CTCollection(CTAny).covariant)

    assertEquals(T <:< CTNumber,
      (T <:< CTNumber | T <:< CTCollection(CTAny)) intersect (T <:< CTNumber | T <:< CTString))
  }

  @Test
  def shouldConstrain() {
    assertEquals(CTInteger.invariant, T <:< CTInteger)
    assertEquals(CTNumber | CTInteger | CTDouble | CTLong, T <:< CTNumber)

    assertEquals(CTInteger.invariant, CTInteger constrain CTNumber)
    assertEquals(CTInteger.invariant, CTNumber.covariant constrain CTInteger)
    assertEquals(TypeSpec.none, CTNumber constrain CTInteger)

    assertEquals(CTInteger.invariant,
      (CTInteger | CTString | CTMap) constrain CTNumber)
    assertEquals(CTCollection(CTString).invariant,
      (CTInteger | CTCollection(CTString)) constrain CTCollection(CTAny))
    assertEquals(TypeSpec.none,
      (CTInteger | CTCollection(CTMap)) constrain CTCollection(CTNode))
    assertEquals(CTInteger | CTCollection(CTString),
      (CTInteger | CTCollection(CTString)) constrain CTAny)
  }

  @Test
  def constrainToBranchTypeWithinCollectionContains() {
    assertEquals(
      CTCollection(CTNumber) | CTCollection(CTInteger) | CTCollection(CTDouble) | CTCollection(CTLong),
      TypeSpec.all constrain CTCollection(CTNumber))
  }

  @Test
  def constrainToSubTypeWithinCollection() {
    assertEquals(CTCollection(CTString).invariant,
      CTCollection(CTAny).covariant constrain CTCollection(CTString))
  }

  @Test
  def constrainToAnotherBranch() {
    assertEquals(TypeSpec.none,
      CTNumber.covariant constrain CTString)

    assertEquals(TypeSpec.none, (T <:< CTString) constrain (T <:< CTNumber))
  }

  @Test
  def unionTwoBranches() {
    assertEquals(CTNumber | CTInteger | CTDouble | CTLong | CTString,
      T <:< CTNumber | T <:< CTString)
  }

  @Test
  def constrainToSuperTypeOfSome() {
    assertEquals(CTInteger.invariant,
      (CTInteger | CTString) <:< CTNumber)
    assertEquals(CTNumber | CTInteger,
      CTInteger.contravariant constrain CTNumber)
    assertEquals(CTNumber.invariant,
      CTNumber.contravariant constrain CTNumber)
    assertEquals(CTAny | CTNumber,
      CTNumber.contravariant constrain CTInteger.contravariant)
    assertEquals(CTAny | CTNumber,
      CTNumber.contravariant constrain CTNumber.contravariant)
    assertEquals(CTAny | CTNumber | CTInteger,
      CTInteger.contravariant constrain CTNumber.contravariant)

    assertEquals(CTInteger.contravariant,
      CTInteger.contravariant constrain CTString.contravariant)
    assertEquals(CTInteger.contravariant,
      CTInteger.contravariant constrain (CTString.contravariant | CTNumber.contravariant))
  }

  @Test
  def constrainToAllTypes() {
    assertEquals(T, T constrain T)
    assertEquals(T <:< CTNumber, T <:< CTNumber constrain T)
  }

  @Test
  def constrainToSubTypeOfSome() {
    val constrainedToNumberOrCollectionT = T <:< CTNumber | T <:< CTCollection(CTAny)
    val constrainedToCollectionOfNumber = T <:< CTCollection(CTNumber)
    assertEquals(constrainedToCollectionOfNumber, constrainedToNumberOrCollectionT constrain constrainedToCollectionOfNumber)
  }

  @Test
  def constrainToSuperTypeOfNone() {
    assertEquals(TypeSpec.none,
      CTNumber.contravariant constrain CTInteger)
    assertEquals(TypeSpec.none,
      CTNumber.contravariant <:< CTString)
  }

  @Test
  def shouldMergeUpTypeSpecs() {
    assertEquals(CTNode | CTNumber | CTAny,
      (CTNode | CTNumber) mergeUp (CTNode | CTNumber))

    assertEquals(CTNode | CTNumber | CTAny,
      (CTNode | CTNumber) mergeUp (CTNode | CTNumber))
    assertEquals(CTNumber | CTAny,
      (CTNode | CTNumber) mergeUp CTNumber)
    assertEquals(CTNode | CTNumber | CTMap | CTAny,
      (CTNode | CTNumber) mergeUp (CTNode | CTNumber | CTRelationship))
    assertEquals(CTAny.invariant,
      (CTNode | CTNumber) mergeUp CTAny)
    assertEquals(CTAny.invariant,
      CTAny mergeUp (CTNode | CTNumber))

    assertEquals(CTMap.invariant,
      CTRelationship.invariant mergeUp CTNode.invariant)
    assertEquals(CTMap | CTNumber | CTAny,
      (CTRelationship | CTLong) mergeUp (CTNode | CTNumber))

    assertEquals(CTNumber | CTCollection(CTAny) | CTAny,
      (CTInteger | CTCollection(CTString)) mergeUp (CTNumber | CTCollection(CTInteger)))
  }

  @Test
  def mergeUpToRootType() {
    assertEquals(CTAny.invariant, TypeSpec.all mergeUp CTAny)
    assertEquals(CTAny.invariant, CTAny mergeUp TypeSpec.all)
    assertEquals(CTAny.invariant, CTCollection(CTAny).covariant mergeUp CTAny)
    assertEquals(CTAny.invariant, CTAny mergeUp CTCollection(CTAny).covariant)
  }

  @Test
  def mergeUpWithLeafType() {
    assertEquals(CTAny | CTNumber | CTInteger, TypeSpec.all mergeUp CTInteger)
  }

  @Test
  def mergeUpWithCollection() {
    assertEquals(CTAny | CTCollection(CTAny),
      TypeSpec.all mergeUp CTCollection(CTAny))
    assertEquals(CTAny | CTCollection(CTAny) | CTCollection(CTString),
      TypeSpec.all mergeUp CTCollection(CTString))
  }

  @Test
  def mergeUpWithMultipleTypes() {
    assertEquals(CTAny | CTNumber | CTInteger | CTString,
      TypeSpec.all mergeUp (CTInteger | CTString))
    assertEquals(CTAny | CTNumber | CTInteger | CTCollection(CTAny) | CTCollection(CTString),
      TypeSpec.all mergeUp (CTCollection(CTString) | CTInteger))
  }

  @Test
  def mergeUpWithSuperTypeOfSome() {
    assertEquals(CTAny | CTNumber,
      (CTCollection(CTString) | CTInteger) mergeUp CTNumber)
    assertEquals(CTAny | CTCollection(CTAny),
      (CTCollection(CTString) | CTInteger) mergeUp CTCollection(CTAny))

    assertEquals(CTNumber | CTInteger,
      CTInteger mergeUp CTNumber.covariant)

    val mergedSet = CTCollection(CTCollection(CTAny)).covariant mergeUp TypeSpec.all
    assertTrue(mergedSet.contains(CTCollection(CTCollection(CTString))))
    assertTrue(mergedSet.contains(CTCollection(CTCollection(CTInteger))))
    assertTrue(mergedSet.contains(CTCollection(CTCollection(CTNumber))))
    assertTrue(mergedSet.contains(CTCollection(CTCollection(CTAny))))
    assertFalse(mergedSet.contains(CTCollection(CTString)))
    assertFalse(mergedSet.contains(CTCollection(CTNumber)))
    assertTrue(mergedSet.contains(CTCollection(CTAny)))
    assertFalse(mergedSet.contains(CTString))
    assertFalse(mergedSet.contains(CTNumber))
    assertTrue(mergedSet.contains(CTAny))

    assertEquals(CTAny | CTNumber,
      CTNumber.contravariant mergeUp CTInteger.contravariant)
  }

  @Test
  def mergeUpWithSubTypeOfSome() {
    assertEquals(CTAny | CTNumber,
      (CTCollection(CTString) | CTNumber) mergeUp CTInteger)
    assertEquals(CTInteger | CTNumber,
      CTNumber.covariant mergeUp CTInteger)

    val numberOrCollectionT = CTNumber.covariant | CTCollection(CTAny).covariant
    assertEquals(CTAny | CTNumber | CTInteger,
      numberOrCollectionT mergeUp CTInteger)
    assertEquals(CTAny | CTCollection(CTAny) | CTCollection(CTNumber) | CTCollection(CTInteger),
      numberOrCollectionT mergeUp CTCollection(CTInteger))

    val collectionOfCollectionOfAny = CTCollection(CTCollection(CTAny)).covariant
    assertTrue((TypeSpec.all mergeUp collectionOfCollectionOfAny) contains CTCollection(CTCollection(CTString)))
    assertTrue((TypeSpec.all mergeUp collectionOfCollectionOfAny) contains CTCollection(CTCollection(CTInteger)))
    assertTrue((TypeSpec.all mergeUp collectionOfCollectionOfAny) contains CTCollection(CTCollection(CTNumber)))
    assertTrue((TypeSpec.all mergeUp collectionOfCollectionOfAny) contains CTCollection(CTCollection(CTAny)))
    assertFalse((TypeSpec.all mergeUp collectionOfCollectionOfAny) contains CTCollection(CTString))
    assertFalse((TypeSpec.all mergeUp collectionOfCollectionOfAny) contains CTCollection(CTNumber))
    assertTrue((TypeSpec.all mergeUp collectionOfCollectionOfAny) contains CTCollection(CTAny))
    assertFalse((TypeSpec.all mergeUp collectionOfCollectionOfAny) contains CTString)
    assertFalse((TypeSpec.all mergeUp collectionOfCollectionOfAny) contains CTNumber)
    assertTrue((TypeSpec.all mergeUp collectionOfCollectionOfAny) contains CTAny)
  }

  @Test
  def mergeUpFromConstrainedBranchToSubType() {
    assertEquals(CTNumber | CTInteger, CTNumber.covariant mergeUp CTInteger)
  }

  @Test
  def mergeUpFromConstrainedBranchToConstraintRoot() {
    assertEquals(CTNumber.invariant, CTNumber.covariant mergeUp CTNumber)
  }

  @Test
  def mergeUpFromConstrainedBranchToSuperType() {
    assertEquals(CTNumber.invariant, CTInteger.covariant mergeUp CTNumber)
  }

  @Test
  def mergeUpFromConstrainedBranchToUnrelated() {
    assertEquals(CTAny.invariant, CTNumber.covariant mergeUp CTString)
    assertEquals(CTAny.invariant, CTInteger.covariant mergeUp CTString)
  }

  @Test
  def mergeUpWithEquivalent() {
    assertEquals(CTNumber.covariant, CTNumber.covariant mergeUp (CTNumber | CTInteger | CTLong | CTDouble))
  }

  @Test
  def shouldWrapInCollection() {
    assertEquals(CTCollection(CTString) | CTCollection(CTCollection(CTNumber)),
      (CTString | CTCollection(CTNumber)).wrapInCollection)
    assertEquals(CTCollection(CTAny).covariant,
      TypeSpec.all.wrapInCollection)
  }

  @Test
  def shouldIdentifyCoercions() {
    assertEquals(CTBoolean.invariant, (T <:< CTDouble).coercions)
    assertEquals(CTBoolean | CTDouble, (T <:< CTLong).coercions)
    assertEquals(CTBoolean | CTDouble, (CTDouble | CTLong).coercions)
    assertEquals(CTBoolean.invariant, CTCollection(CTAny).covariant.coercions)
    assertEquals(CTBoolean.invariant, TypeSpec.exact(CTCollection(CTPath)).coercions)
    assertEquals(CTBoolean | CTDouble | CTLong, TypeSpec.all.coercions)
    assertEquals(CTBoolean.invariant, (T <:< CTCollection(CTAny)).coercions)
  }

  @Test
  def shouldIntersectWithCoercions() {
    assertEquals(CTInteger.invariant, T =%= CTInteger)
    assertEquals(CTDouble.invariant, CTInteger =%= CTDouble)
    assertEquals(TypeSpec.none, CTNumber =%= CTDouble)
    assertEquals(CTBoolean.invariant, CTCollection(CTAny).covariant =%= CTBoolean)
    assertEquals(CTBoolean.invariant, (T <:< CTNumber) =%= CTBoolean)
    assertEquals(CTBoolean.invariant, CTCollection(CTAny).covariant intersectWithCoercion (CTBoolean | CTString))
    assertEquals(TypeSpec.none, CTInteger =%= CTString)
  }

  @Test
  def shouldConstrainWithCoercions() {
    assertEquals(CTInteger.invariant, T <%< CTInteger)
    assertEquals(CTDouble.invariant, CTInteger <%< CTDouble)
    assertEquals(TypeSpec.none, CTNumber <%< CTDouble)
    assertEquals(CTBoolean.invariant, CTCollection(CTAny).covariant <%< CTBoolean)
    assertEquals(CTBoolean.invariant, (T <:< CTNumber) <%< CTBoolean)
    assertEquals(CTBoolean.invariant, CTCollection(CTAny).covariant constrainWithCoercion (CTBoolean | CTString))
    assertEquals(TypeSpec.none, CTInteger <%< CTString)
  }

  @Test
  def equalTypeSpecsShouldEqual() {
    assertEquals(CTString.invariant, CTString.invariant)

    assertEquals(T <:< CTString, CTString.invariant)
    assertEquals(CTString.invariant, T <:< CTString)

    assertEquals(CTNumber | CTLong | CTInteger | CTDouble, CTDouble | CTInteger | CTLong | CTNumber)
    assertEquals(CTNumber | CTLong | CTInteger | CTDouble, T <:< CTNumber)
    assertEquals(T <:< CTNumber, CTNumber | CTLong | CTInteger | CTDouble)

    assertEquals(T <:< CTNumber, T <:< CTNumber | T <:< CTInteger)
    assertNotEquals(T <:< CTNumber, T <:< CTNumber | T <:< CTString)

    assertEquals(T, T)
    assertEquals(T <:< CTCollection(CTAny), T <:< CTCollection(CTAny))
    assertEquals(T <:< CTCollection(CTAny), T <:< CTCollection(CTAny))

    assertEquals(T, T | (T <:< CTString))
    assertEquals(T, T | T)
  }

  @Test
  def differentTypeSpecsShouldNotEqual() {
    assertNotEquals(T <:< CTNumber, T)
    assertNotEquals(T, T <:< CTNumber)

    assertNotEquals(T <:< CTNumber, T <:< CTCollection(CTAny))
    assertNotEquals(T <:< CTCollection(CTAny), T <:< CTNumber)

    assertNotEquals(TypeSpec.all, CTNumber.invariant)
    assertNotEquals(CTNumber.invariant, TypeSpec.all)
  }

  @Test
  def shouldHaveIndefiniteSizeWhenAllowingUnconstrainedAnyAtAnyDepth() {
    assertFalse(TypeSpec.all.hasDefiniteSize)
    assertFalse(CTCollection(CTAny).covariant.hasDefiniteSize)

    assertTrue((CTString | CTNumber).hasDefiniteSize)
    assertTrue(CTCollection(CTString).covariant.hasDefiniteSize)

    assertFalse(CTCollection(CTCollection(CTAny)).covariant.hasDefiniteSize)
    assertTrue(CTCollection(CTCollection(CTString)).covariant.hasDefiniteSize)

    assertTrue(CTAny.contravariant.hasDefiniteSize)
    assertTrue((CTCollection(CTAny).covariant mergeUp CTCollection(CTAny)).hasDefiniteSize)
  }

  @Test
  def shouldBeEmptyWhenNoPossibilitiesRemain() {
    assertFalse(TypeSpec.all.isEmpty)
    assertTrue(TypeSpec.none.isEmpty)
    assertTrue((CTNumber.contravariant intersect CTInteger).isEmpty)
  }

  @Test
  def shouldFormatNone() {
    assertEquals("()", TypeSpec.none.mkString("(", ", ", " or ", ")"))
  }

  @Test
  def shouldFormatSingleType() {
    assertEquals("(Any)", CTAny.invariant.mkString("(", ", ", " or ", ")"))
    assertEquals("<Node>", CTNode.invariant.mkString("<", ", ", " and ", ">"))
  }

  @Test
  def shouldFormatTwoTypes() {
    assertEquals("Any or Node", (CTAny | CTNode).mkString("", ", ", " or ", ""))
    assertEquals("-Node or Relationship-", (CTRelationship | CTNode).mkString("-", ", ", " or ", "-"))
  }

  @Test
  def shouldFormatThreeTypes() {
    assertEquals("Integer, Node, Relationship", (CTRelationship | CTInteger | CTNode).mkString(", "))
    assertEquals("(Integer, Node, Relationship)", (CTRelationship | CTInteger | CTNode).mkString("(", ", ", ")"))
    assertEquals("(Any, Node or Relationship)", (CTRelationship | CTAny | CTNode).mkString("(", ", ", " or ", ")"))
    assertEquals("[Integer, Node and Relationship]", (CTRelationship | CTInteger | CTNode).mkString("[", ", ", " and ", "]"))
  }

  @Test
  def shouldFormatToStringForIndefiniteSizedSet() {
    assertEquals("T", TypeSpec.all.mkString(", "))
    assertEquals("Collection<T>", CTCollection(CTAny).covariant.mkString(", "))
    assertEquals("Boolean, Collection<T>", (T <:< CTCollection(CTAny) | CTBoolean).mkString(", "))
    assertEquals("Boolean, Collection<String>, Collection<Collection<T>>",
      ((T <:< CTCollection(CTCollection(CTAny))) | CTBoolean | CTCollection(CTString)).mkString(", "))
  }

  @Test
  def shouldFormatToStringForDefiniteSizedSet() {
    assertEquals("Any", CTAny.invariant.mkString(", "))
    assertEquals("String", CTString.invariant.mkString(", "))
    assertEquals("Double, Integer, Long, Number", CTNumber.covariant.mkString(", "))
    assertEquals("Boolean, Double, Integer, Long, Number",
      (T <:< CTNumber | T <:< CTBoolean).mkString(", "))
    assertEquals("Any, Number", CTNumber.contravariant.mkString(", "))
    assertEquals("Boolean, String, Collection<Boolean>, Collection<String>",
      (T <:< CTBoolean | T <:< CTString | T <:< CTCollection(CTBoolean) | T <:< CTCollection(CTString)).mkString(", "))
    assertEquals("Boolean, String, Collection<Boolean>, Collection<Collection<String>>",
      (T <:< CTBoolean | T <:< CTString | T <:< CTCollection(CTBoolean) | T <:< CTCollection(CTCollection(CTString))).mkString(", "))
    assertEquals("Any, Collection<Any>", (T >:> CTCollection(CTAny)).mkString(", "))
  }

  @Test
  def shouldIterateOverDefiniteSizedSet() {
    assertEquals(Seq(CTString),
      CTString.invariant.iterator.toSeq)
    assertEquals(Seq(CTDouble, CTInteger, CTLong, CTNumber),
      CTNumber.covariant.iterator.toSeq)
    assertEquals(Seq(CTBoolean, CTDouble, CTInteger, CTLong, CTNumber),
      (T <:< CTNumber | T <:< CTBoolean).iterator.toSeq)
    assertEquals(Seq(CTAny, CTNumber),
      CTNumber.contravariant.iterator.toSeq)
    assertEquals(Seq(CTBoolean, CTString, CTCollection(CTBoolean), CTCollection(CTString)),
      (CTBoolean | CTString | CTCollection(CTBoolean) | CTCollection(CTString)).iterator.toSeq)
    assertEquals(Seq(CTBoolean, CTString, CTCollection(CTBoolean), CTCollection(CTCollection(CTString))),
      (CTBoolean | CTString | CTCollection(CTBoolean) | CTCollection(CTCollection(CTString))).iterator.toSeq)
  }
}
