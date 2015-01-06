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
package org.neo4j.cypher.internal.compiler.v2_1.symbols

import org.neo4j.cypher.internal.commons.CypherFunSuite

class TypeSpecTest extends CypherFunSuite {

  test("allTypesShouldContainAll") {
    TypeSpec.all contains CTAny should equal(true)
    TypeSpec.all contains CTString should equal(true)
    TypeSpec.all contains CTNumber should equal(true)
    TypeSpec.all contains CTInteger should equal(true)
    TypeSpec.all contains CTFloat should equal(true)
    TypeSpec.all contains CTNode should equal(true)
    TypeSpec.all contains CTCollection(CTAny) should equal(true)
    TypeSpec.all contains CTCollection(CTFloat) should equal(true)
    TypeSpec.all contains CTCollection(CTCollection(CTFloat)) should equal(true)
  }

  test("shouldReturnTrueIfContains") {
    CTNumber.covariant contains CTInteger should equal(true)
    CTNumber.covariant contains CTString should equal(false)

    val anyCollection = CTCollection(CTAny).covariant
    anyCollection contains CTCollection(CTString) should equal(true)
    anyCollection contains CTCollection(CTInteger) should equal(true)
    anyCollection contains CTCollection(CTAny) should equal(true)
    anyCollection contains CTCollection(CTCollection(CTInteger)) should equal(true)
    anyCollection contains CTBoolean should equal(false)
    anyCollection contains CTAny should equal(false)
  }

  test("shouldReturnTrueIfContainsAny") {
    TypeSpec.all containsAny TypeSpec.all should equal(true)
    TypeSpec.all containsAny CTNumber.covariant should equal(true)
    TypeSpec.all containsAny CTNode.invariant should equal(true)
    CTNumber.covariant containsAny TypeSpec.all should equal(true)

    CTNumber.covariant containsAny CTInteger should equal(true)
    CTInteger.covariant containsAny (CTInteger | CTString) should equal(true)
    CTInteger.covariant containsAny CTNumber.covariant should equal(true)
    CTInteger.covariant containsAny TypeSpec.all should equal(true)

    CTInteger.covariant containsAny CTString should equal(false)
    CTNumber.covariant containsAny CTString should equal(false)
  }

  test("shouldUnion") {
    CTNumber.covariant | CTString.covariant should equal(CTNumber | CTFloat | CTInteger | CTString)
    CTNumber.covariant | CTBoolean should equal(CTNumber | CTFloat | CTInteger | CTBoolean)

    CTNumber.covariant union CTCollection(CTString).covariant should equal(CTNumber | CTFloat | CTInteger | CTCollection(CTString))
    CTCollection(CTNumber) union CTCollection(CTString).covariant should equal(CTCollection(CTNumber) | CTCollection(CTString))
  }

  test("shouldIntersect") {
    TypeSpec.all & CTInteger should equal(CTInteger.invariant)
    CTNumber.covariant & CTInteger should equal(CTInteger.invariant)
    CTNumber.covariant & CTString should equal(TypeSpec.none)

    (CTNumber | CTInteger) & (CTAny | CTNumber) should equal(CTNumber.invariant)
    CTNumber.contravariant & CTNumber.covariant should equal(CTNumber.invariant)
    (CTNumber | CTInteger) & (CTNumber | CTFloat) should equal(CTNumber.invariant)

    CTCollection(CTCollection(CTAny)).contravariant intersect CTCollection(CTAny).covariant should equal(CTCollection(CTAny) | CTCollection(CTCollection(CTAny)))

    (CTNumber.covariant | CTCollection(CTAny).covariant) intersect (CTNumber.covariant | CTString.covariant) should equal(CTNumber.covariant)
  }

  test("shouldConstrain") {
    CTInteger.covariant should equal(CTInteger.invariant)
    CTNumber.covariant should equal(CTNumber | CTFloat | CTInteger)

    CTInteger constrain CTNumber should equal(CTInteger.invariant)
    CTNumber.covariant constrain CTInteger should equal(CTInteger.invariant)
    CTNumber constrain CTInteger should equal(TypeSpec.none)

    (CTInteger | CTString | CTMap) constrain CTNumber should equal(CTInteger.invariant)
    (CTInteger | CTCollection(CTString)) constrain CTCollection(CTAny) should equal(CTCollection(CTString).invariant)
    (CTInteger | CTCollection(CTMap)) constrain CTCollection(CTNode) should equal(TypeSpec.none)
    (CTInteger | CTCollection(CTString)) constrain CTAny should equal(CTInteger | CTCollection(CTString))
  }

  test("constrainToBranchTypeWithinCollectionContains") {
    TypeSpec.all constrain CTCollection(CTNumber) should equal(CTCollection(CTNumber) | CTCollection(CTInteger) | CTCollection(CTFloat))
  }

  test("constrainToSubTypeWithinCollection") {
    CTCollection(CTAny).covariant constrain CTCollection(CTString) should equal(CTCollection(CTString).invariant)
  }

  test("constrainToAnotherBranch") {
    CTNumber.covariant constrain CTString should equal(TypeSpec.none)

    CTString.covariant constrain CTNumber should equal(TypeSpec.none)
  }

  test("unionTwoBranches") {
    CTNumber.covariant | CTString.covariant should equal(CTNumber | CTInteger | CTFloat | CTString)
  }

  test("constrainToSuperTypeOfSome") {
    (CTInteger | CTString) constrain CTNumber should equal(CTInteger.invariant)
    CTInteger.contravariant constrain CTNumber should equal(CTNumber | CTInteger)
    CTNumber.contravariant constrain CTNumber should equal(CTNumber.invariant)
    CTNumber.contravariant constrain CTAny should equal(CTAny | CTNumber)
  }

  test("constrainToAny") {
    TypeSpec.all constrain CTAny should equal(TypeSpec.all)
    CTNumber.covariant constrain CTAny should equal(CTNumber.covariant)
  }

  test("constrainToSubTypeOfSome") {
    val constrainedToNumberOrCollectionT = CTNumber.covariant | CTCollection(CTAny).covariant
    constrainedToNumberOrCollectionT constrain CTCollection(CTNumber) should equal(CTCollection(CTNumber) | CTCollection(CTFloat) | CTCollection(CTInteger))
  }

  test("constrainToSuperTypeOfNone") {
    CTNumber.contravariant constrain CTInteger should equal(TypeSpec.none)
    CTNumber.contravariant constrain CTString should equal(TypeSpec.none)
  }

  test("shouldMergeUpTypeSpecs") {
    (CTNode | CTNumber) mergeUp (CTNode | CTNumber) should equal(CTNode | CTNumber | CTAny)

    (CTNode | CTNumber) mergeUp (CTNode | CTNumber) should equal(CTNode | CTNumber | CTAny)
    (CTNode | CTNumber) mergeUp CTNumber should equal(CTNumber | CTAny)
    (CTNode | CTNumber) mergeUp (CTNode | CTNumber | CTRelationship) should equal(CTNode | CTNumber | CTMap | CTAny)
    (CTNode | CTNumber) mergeUp CTAny should equal(CTAny.invariant)
    CTAny mergeUp (CTNode | CTNumber) should equal(CTAny.invariant)

    CTRelationship.invariant mergeUp CTNode.invariant should equal(CTMap.invariant)
    (CTRelationship | CTInteger) mergeUp (CTNode | CTNumber) should equal(CTMap | CTNumber | CTAny)

    (CTInteger | CTCollection(CTString)) mergeUp (CTNumber | CTCollection(CTInteger)) should equal(CTNumber | CTCollection(CTAny) | CTAny)
  }

  test("mergeUpToRootType") {
    TypeSpec.all mergeUp CTAny should equal(CTAny.invariant)
    CTAny mergeUp TypeSpec.all should equal(CTAny.invariant)
    CTCollection(CTAny).covariant mergeUp CTAny should equal(CTAny.invariant)
    CTAny mergeUp CTCollection(CTAny).covariant should equal(CTAny.invariant)
  }

  test("mergeUpWithLeafType") {
    TypeSpec.all mergeUp CTInteger should equal(CTAny | CTNumber | CTInteger)
  }

  test("mergeUpWithCollection") {
    TypeSpec.all mergeUp CTCollection(CTAny) should equal(CTAny | CTCollection(CTAny))
    TypeSpec.all mergeUp CTCollection(CTString) should equal(CTAny | CTCollection(CTAny) | CTCollection(CTString))
  }

  test("mergeUpWithMultipleTypes") {
    TypeSpec.all mergeUp (CTInteger | CTString) should equal(CTAny | CTNumber | CTInteger | CTString)
    TypeSpec.all mergeUp (CTCollection(CTString) | CTInteger) should equal(CTAny | CTNumber | CTInteger | CTCollection(CTAny) | CTCollection(CTString))
  }

  test("mergeUpWithSuperTypeOfSome") {
    (CTCollection(CTString) | CTInteger) mergeUp CTNumber should equal(CTAny | CTNumber)
    (CTCollection(CTString) | CTInteger) mergeUp CTCollection(CTAny) should equal(CTAny | CTCollection(CTAny))

    CTInteger mergeUp CTNumber.covariant should equal(CTNumber | CTInteger)

    val mergedSet = CTCollection(CTCollection(CTAny)).covariant mergeUp TypeSpec.all
    mergedSet.contains(CTCollection(CTCollection(CTString))) should equal(true)
    mergedSet.contains(CTCollection(CTCollection(CTInteger))) should equal(true)
    mergedSet.contains(CTCollection(CTCollection(CTNumber))) should equal(true)
    mergedSet.contains(CTCollection(CTCollection(CTAny))) should equal(true)
    mergedSet.contains(CTCollection(CTString)) should equal(false)
    mergedSet.contains(CTCollection(CTNumber)) should equal(false)
    mergedSet.contains(CTCollection(CTAny)) should equal(true)
    mergedSet.contains(CTString) should equal(false)
    mergedSet.contains(CTNumber) should equal(false)
    mergedSet.contains(CTAny) should equal(true)

    CTNumber.contravariant mergeUp CTInteger.contravariant should equal(CTAny | CTNumber)
  }

  test("mergeUpWithSubTypeOfSome") {
    (CTCollection(CTString) | CTNumber) mergeUp CTInteger should equal(CTAny | CTNumber)
    CTNumber.covariant mergeUp CTInteger should equal(CTInteger | CTNumber)

    val numberOrCollectionT = CTNumber.covariant | CTCollection(CTAny).covariant
    numberOrCollectionT mergeUp CTInteger should equal(CTAny | CTNumber | CTInteger)
    numberOrCollectionT mergeUp CTCollection(CTInteger) should equal(CTAny | CTCollection(CTAny) | CTCollection(CTNumber) | CTCollection(CTInteger))

    val collectionOfCollectionOfAny = CTCollection(CTCollection(CTAny)).covariant
    (TypeSpec.all mergeUp collectionOfCollectionOfAny) contains CTCollection(CTCollection(CTString)) should equal(true)
    (TypeSpec.all mergeUp collectionOfCollectionOfAny) contains CTCollection(CTCollection(CTInteger)) should equal(true)
    (TypeSpec.all mergeUp collectionOfCollectionOfAny) contains CTCollection(CTCollection(CTNumber)) should equal(true)
    (TypeSpec.all mergeUp collectionOfCollectionOfAny) contains CTCollection(CTCollection(CTAny)) should equal(true)
    (TypeSpec.all mergeUp collectionOfCollectionOfAny) contains CTCollection(CTString) should equal(false)
    (TypeSpec.all mergeUp collectionOfCollectionOfAny) contains CTCollection(CTNumber) should equal(false)
    (TypeSpec.all mergeUp collectionOfCollectionOfAny) contains CTCollection(CTAny) should equal(true)
    (TypeSpec.all mergeUp collectionOfCollectionOfAny) contains CTString should equal(false)
    (TypeSpec.all mergeUp collectionOfCollectionOfAny) contains CTNumber should equal(false)
    (TypeSpec.all mergeUp collectionOfCollectionOfAny) contains CTAny should equal(true)
  }

  test("mergeUpFromConstrainedBranchToSubType") {
    CTNumber.covariant mergeUp CTInteger should equal(CTNumber | CTInteger)
  }

  test("mergeUpFromConstrainedBranchToConstraintRoot") {
    CTNumber.covariant mergeUp CTNumber should equal(CTNumber.invariant)
  }

  test("mergeUpFromConstrainedBranchToSuperType") {
    CTInteger.covariant mergeUp CTNumber should equal(CTNumber.invariant)
  }

  test("mergeUpFromConstrainedBranchToUnrelated") {
    CTNumber.covariant mergeUp CTString should equal(CTAny.invariant)
    CTInteger.covariant mergeUp CTString should equal(CTAny.invariant)
  }

  test("mergeUpWithEquivalent") {
    CTNumber.covariant mergeUp (CTNumber | CTInteger | CTFloat) should equal(CTNumber.covariant)
  }

  test("shouldWrapInCollection") {
    (CTString | CTCollection(CTNumber)).wrapInCollection should equal(CTCollection(CTString) | CTCollection(CTCollection(CTNumber)))
    TypeSpec.all.wrapInCollection should equal(CTCollection(CTAny).covariant)
  }

  test("shouldIdentifyCoercions") {
    CTFloat.covariant.coercions should equal(CTBoolean.invariant)
    CTInteger.covariant.coercions should equal(CTBoolean | CTFloat)
    (CTFloat | CTInteger).coercions should equal(CTBoolean | CTFloat)
    CTCollection(CTAny).covariant.coercions should equal(CTBoolean.invariant)
    TypeSpec.exact(CTCollection(CTPath)).coercions should equal(CTBoolean.invariant)
    TypeSpec.all.coercions should equal(CTBoolean | CTFloat)
    CTCollection(CTAny).covariant.coercions should equal(CTBoolean.invariant)
  }

  test("shouldIntersectWithCoercions") {
    TypeSpec.all intersectOrCoerce CTInteger should equal(CTInteger.invariant)
    CTInteger intersectOrCoerce CTFloat should equal(CTFloat.invariant)
    CTNumber intersectOrCoerce CTFloat should equal(TypeSpec.none)
    CTCollection(CTAny).covariant intersectOrCoerce CTBoolean should equal(CTBoolean.invariant)
    CTNumber.covariant intersectOrCoerce CTBoolean should equal(CTBoolean.invariant)
    CTCollection(CTAny).covariant intersectOrCoerce CTBoolean should equal(CTBoolean.invariant)
    CTInteger intersectOrCoerce CTString should equal(TypeSpec.none)
  }

  test("shouldConstrainWithCoercions") {
    TypeSpec.all constrainOrCoerce CTInteger should equal(CTInteger.invariant)
    CTInteger constrainOrCoerce CTFloat should equal(CTFloat.invariant)
    CTNumber constrainOrCoerce CTFloat should equal(TypeSpec.none)
    CTCollection(CTAny).covariant constrainOrCoerce CTBoolean should equal(CTBoolean.invariant)
    CTNumber.covariant constrainOrCoerce CTBoolean should equal(CTBoolean.invariant)
    CTCollection(CTAny).covariant constrainOrCoerce CTBoolean should equal(CTBoolean.invariant)
    CTInteger constrainOrCoerce CTString should equal(TypeSpec.none)
  }

  test("equalTypeSpecsShouldEqual") {
    CTString.invariant should equal(CTString.invariant)

    CTString.invariant should equal(CTString.covariant)
    CTString.covariant should equal(CTString.invariant)

    CTFloat | CTInteger | CTNumber should equal(CTNumber | CTInteger | CTFloat)
    CTNumber.covariant should equal(CTNumber | CTInteger | CTFloat)
    CTNumber | CTInteger | CTFloat should equal(CTNumber.covariant)

    CTNumber.covariant | CTInteger.covariant should equal(CTNumber.covariant)
    CTNumber.covariant | CTString.covariant should not equal(CTNumber.covariant)

    TypeSpec.all should equal(TypeSpec.all)
    CTAny.covariant should equal(TypeSpec.all)
    CTCollection(CTAny).covariant should equal(CTCollection(CTAny).covariant)
    CTCollection(CTAny).covariant should equal(CTCollection(CTAny).covariant)

    TypeSpec.all | CTString.covariant should equal(TypeSpec.all)
    TypeSpec.all | TypeSpec.all should equal(TypeSpec.all)
  }

  test("differentTypeSpecsShouldNotEqual") {
    TypeSpec.all should not equal(CTNumber.covariant)
    CTNumber.covariant should not equal(TypeSpec.all)

    CTCollection(CTAny).covariant should not equal(CTNumber.covariant)
    CTNumber.covariant should not equal(CTCollection(CTAny).covariant)

    CTNumber.invariant should not equal(TypeSpec.all)
    TypeSpec.all should not equal(CTNumber.invariant)
  }

  test("shouldHaveIndefiniteSizeWhenAllowingUnconstrainedAnyAtAnyDepth") {
    TypeSpec.all.hasDefiniteSize should equal(false)
    CTCollection(CTAny).covariant.hasDefiniteSize should equal(false)

    (CTString | CTNumber).hasDefiniteSize should equal(true)
    CTCollection(CTString).covariant.hasDefiniteSize should equal(true)

    CTCollection(CTCollection(CTAny)).covariant.hasDefiniteSize should equal(false)
    CTCollection(CTCollection(CTString)).covariant.hasDefiniteSize should equal(true)

    CTAny.contravariant.hasDefiniteSize should equal(true)
    (CTCollection(CTAny).covariant mergeUp CTCollection(CTAny)).hasDefiniteSize should equal(true)
  }

  test("shouldBeEmptyWhenNoPossibilitiesRemain") {
    TypeSpec.all.isEmpty should equal(false)
    TypeSpec.none.isEmpty should equal(true)
    (CTNumber.contravariant intersect CTInteger).isEmpty should equal(true)
  }

  test("shouldFormatNone") {
    TypeSpec.none.mkString("(", ", ", " or ", ")") should equal("()")
  }

  test("shouldFormatSingleType") {
    CTAny.invariant.mkString("(", ", ", " or ", ")") should equal("(Any)")
    CTNode.invariant.mkString("<", ", ", " and ", ">") should equal("<Node>")
  }

  test("shouldFormatTwoTypes") {
    (CTAny | CTNode).mkString("", ", ", " or ", "") should equal("Any or Node")
    (CTRelationship | CTNode).mkString("-", ", ", " or ", "-") should equal("-Node or Relationship-")
  }

  test("shouldFormatThreeTypes") {
    (CTRelationship | CTInteger | CTNode).mkString(", ") should equal("Integer, Node, Relationship")
    (CTRelationship | CTInteger | CTNode).mkString("(", ", ", ")") should equal("(Integer, Node, Relationship)")
    (CTRelationship | CTAny | CTNode).mkString("(", ", ", " or ", ")") should equal("(Any, Node or Relationship)")
    (CTRelationship | CTInteger | CTNode).mkString("[", ", ", " and ", "]") should equal("[Integer, Node and Relationship]")
  }

  test("shouldFormatToStringForIndefiniteSizedSet") {
    TypeSpec.all.mkString(", ") should equal("T")
    CTCollection(CTAny).covariant.mkString(", ") should equal("Collection<T>")
    (CTCollection(CTAny).covariant | CTBoolean).mkString(", ") should equal("Boolean, Collection<T>")
    (CTCollection(CTCollection(CTAny)).covariant | CTBoolean | CTCollection(CTString)).mkString(", ") should equal("Boolean, Collection<String>, Collection<Collection<T>>")
  }

  test("shouldFormatToStringForDefiniteSizedSet") {
    CTAny.invariant.mkString(", ") should equal("Any")
    CTString.invariant.mkString(", ") should equal("String")
    CTNumber.covariant.mkString(", ") should equal("Float, Integer, Number")
    (CTNumber.covariant | CTBoolean.covariant).mkString(", ") should equal("Boolean, Float, Integer, Number")
    CTNumber.contravariant.mkString(", ") should equal("Any, Number")
    (CTBoolean.covariant | CTString.covariant | CTCollection(CTBoolean).covariant | CTCollection(CTString).covariant).mkString(", ") should equal("Boolean, String, Collection<Boolean>, Collection<String>")
    (CTBoolean.covariant | CTString.covariant | CTCollection(CTBoolean).covariant | CTCollection(CTCollection(CTString)).covariant).mkString(", ") should equal("Boolean, String, Collection<Boolean>, Collection<Collection<String>>")
    CTCollection(CTAny).contravariant.mkString(", ") should equal("Any, Collection<Any>")
  }

  test("shouldIterateOverDefiniteSizedSet") {
    CTString.invariant.iterator.toSeq should equal(Seq(CTString))
    CTNumber.covariant.iterator.toSeq should equal(Seq(CTFloat, CTInteger, CTNumber))
    (CTNumber.covariant | CTBoolean.covariant).iterator.toSeq should equal(Seq(CTBoolean, CTFloat, CTInteger, CTNumber))
    CTNumber.contravariant.iterator.toSeq should equal(Seq(CTAny, CTNumber))
    (CTBoolean | CTString | CTCollection(CTBoolean) | CTCollection(CTString)).iterator.toSeq should equal(Seq(CTBoolean, CTString, CTCollection(CTBoolean), CTCollection(CTString)))
    (CTBoolean | CTString | CTCollection(CTBoolean) | CTCollection(CTCollection(CTString))).iterator.toSeq should equal(Seq(CTBoolean, CTString, CTCollection(CTBoolean), CTCollection(CTCollection(CTString))))
  }
}
