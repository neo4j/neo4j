/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.frontend.symbols

import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTDate
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTNumber
import org.neo4j.cypher.internal.util.symbols.CTPath
import org.neo4j.cypher.internal.util.symbols.CTPoint
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType
import org.neo4j.cypher.internal.util.symbols.NothingType
import org.neo4j.cypher.internal.util.symbols.NullType
import org.neo4j.cypher.internal.util.symbols.TypeSpec
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class TypeSpecTest extends CypherFunSuite {

  test("all types should contain all") {
    TypeSpec.all contains CTAny should equal(true)
    TypeSpec.all contains CTString should equal(true)
    TypeSpec.all contains CTNumber should equal(true)
    TypeSpec.all contains CTInteger should equal(true)
    TypeSpec.all contains CTFloat should equal(true)
    TypeSpec.all contains CTNode should equal(true)
    TypeSpec.all contains CTList(CTAny) should equal(true)
    TypeSpec.all contains CTList(CTFloat) should equal(true)
    TypeSpec.all contains CTList(CTList(CTFloat)) should equal(true)
    TypeSpec.all contains ClosedDynamicUnionType(Set(CTString, CTInteger))(InputPosition.NONE) should equal(true)
  }

  test("should return true if contains") {
    CTNumber.covariant contains CTInteger should equal(true)
    CTNumber.covariant contains CTString should equal(false)
    ClosedDynamicUnionType(Set(CTString, CTInteger))(InputPosition.NONE).covariant contains CTInteger should equal(true)
    ClosedDynamicUnionType(Set(CTString, CTInteger))(InputPosition.NONE).covariant contains CTString should equal(true)
    ClosedDynamicUnionType(Set(CTString, CTInteger))(InputPosition.NONE).covariant contains CTPath should equal(false)

    val anyCollection = CTList(CTAny).covariant
    anyCollection contains CTList(CTString) should equal(true)
    anyCollection contains CTList(CTInteger) should equal(true)
    anyCollection contains CTList(CTAny) should equal(true)
    anyCollection contains CTList(CTList(CTInteger)) should equal(true)
    anyCollection contains CTBoolean should equal(false)
    anyCollection contains CTAny should equal(false)
    anyCollection contains ClosedDynamicUnionType(Set(CTString, CTInteger))(InputPosition.NONE) should equal(false)
  }

  test("should return true if contains CTAny") {
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

    TypeSpec.all containsAny ClosedDynamicUnionType(Set(CTString, CTInteger))(
      InputPosition.NONE
    ).covariant should equal(true)
    TypeSpec.all containsAny ClosedDynamicUnionType(Set(CTString, CTInteger))(
      InputPosition.NONE
    ).invariant should equal(true)
    ClosedDynamicUnionType(Set(CTBoolean, CTInteger))(
      InputPosition.NONE
    ).covariant containsAny (CTInteger | CTString) should equal(true)
    ClosedDynamicUnionType(Set(CTString, CTInteger))(
      InputPosition.NONE
    ).covariant containsAny TypeSpec.all should equal(true)
  }

  test("containsAll") {
    TypeSpec.all containsAll TypeSpec.all should equal(true)
    CTNumber.covariant containsAll CTNumber.covariant should equal(true)
    TypeSpec.all containsAll CTNumber.covariant should equal(true)
    CTNumber.covariant containsAll TypeSpec.all should equal(false)
    CTBoolean.covariant containsAll TypeSpec.union(CTNumber, CTBoolean) should equal(false)
    CTBoolean.covariant containsAll TypeSpec.union(CTBoolean, CTBoolean) should equal(true)
    CTNumber.covariant containsAll TypeSpec.union(CTInteger, CTFloat) should equal(true)
    ClosedDynamicUnionType(Set(CTFloat, CTInteger))(
      InputPosition.NONE
    ).covariant containsAll (CTFloat | CTInteger).covariant should equal(true)
    ClosedDynamicUnionType(Set(CTString, CTList(CTAny)))(InputPosition.NONE).covariant containsAll (CTString | CTList(
      CTAny
    )) should equal(true)

    CTList(CTAny).covariant containsAll CTList(CTAny).covariant should equal(true)
    CTList(CTAny).covariant containsAll CTList(CTNumber) should equal(true)
    CTList(CTAny).covariant containsAll CTList(CTAny) should equal(true)
    CTList(CTAny).covariant containsAll CTList(CTList(CTString)) should equal(true)
    CTList(CTAny).covariant containsAll CTAny should equal(false)
  }

  test("should union") {
    CTNumber.covariant | CTString.covariant should equal(CTNumber | CTFloat | CTInteger | CTString)
    CTNumber.covariant | CTBoolean should equal(CTNumber | CTFloat | CTInteger | CTBoolean)
    ClosedDynamicUnionType(Set(CTString, CTInteger))(InputPosition.NONE) | CTBoolean should equal(
      CTString | CTInteger | CTBoolean
    )
    ClosedDynamicUnionType(Set(CTString, CTInteger))(InputPosition.NONE) | ClosedDynamicUnionType(Set(
      CTBoolean,
      CTDate
    ))(InputPosition.NONE) should equal(CTString | CTInteger | CTBoolean | CTDate)

    CTNumber.covariant union CTList(CTString).covariant should equal(CTNumber | CTFloat | CTInteger | CTList(CTString))
    CTList(CTNumber) union CTList(CTString).covariant should equal(CTList(CTNumber) | CTList(CTString))
  }

  test("should intersect") {
    TypeSpec.all & CTInteger should equal(CTInteger.invariant)
    CTNumber.covariant & CTInteger should equal(CTInteger.invariant)
    CTNumber.covariant & CTString should equal(TypeSpec.none)
    ClosedDynamicUnionType(Set(CTString, CTInteger))(InputPosition.NONE) & CTInteger should equal(CTInteger.invariant)
    ClosedDynamicUnionType(Set(CTString, CTBoolean))(InputPosition.NONE) & CTInteger should equal(TypeSpec.none)
    ClosedDynamicUnionType(Set(CTString, CTInteger))(InputPosition.NONE) & CTInteger.covariant should equal(
      CTInteger.invariant
    )

    (CTNumber | CTInteger) & (CTAny | CTNumber) should equal(CTNumber.invariant)
    CTNumber.contravariant & CTNumber.covariant should equal(CTNumber.invariant)
    (CTNumber | CTInteger) & (CTNumber | CTFloat) should equal(CTNumber.invariant)

    CTList(CTList(CTAny)).contravariant intersect CTList(CTAny).covariant should equal(
      CTList(CTAny) | CTList(CTList(CTAny))
    )

    (CTNumber.covariant | CTList(CTAny).covariant) intersect (CTNumber.covariant | CTString.covariant) should equal(
      CTNumber.covariant
    )

    ClosedDynamicUnionType(Set(CTString, CTInteger))(
      InputPosition.NONE
    ) intersect (CTNumber.covariant | CTBoolean.covariant) should equal(
      CTInteger.covariant
    )
  }

  test("should constrain") {
    CTInteger.covariant should equal(CTInteger.invariant)
    CTNumber.covariant should equal(CTNumber | CTFloat | CTInteger)

    CTInteger constrain CTNumber should equal(CTInteger.invariant)
    CTNumber.covariant constrain CTInteger should equal(CTInteger.invariant)
    CTNumber constrain CTInteger should equal(TypeSpec.none)
    ClosedDynamicUnionType(Set(CTString, CTInteger))(InputPosition.NONE) constrain CTNumber should equal(
      CTInteger.invariant
    )
    ClosedDynamicUnionType(Set(CTString, CTInteger))(InputPosition.NONE) constrain CTBoolean should equal(TypeSpec.none)
    ClosedDynamicUnionType(Set(CTString, CTInteger))(InputPosition.NONE) constrain ClosedDynamicUnionType(Set(
      CTString,
      CTInteger
    ))(InputPosition.NONE) should equal(CTInteger.invariant | CTString.invariant)

    (CTInteger | CTString | CTMap) constrain CTNumber should equal(CTInteger.invariant)
    (CTInteger | CTList(CTString)) constrain CTList(CTAny) should equal(CTList(CTString).invariant)
    (CTInteger | CTList(CTMap)) constrain CTList(CTNode) should equal(TypeSpec.none)
    (CTInteger | CTList(CTString)) constrain CTAny should equal(CTInteger | CTList(CTString))
  }

  test("constrain to branch type within list contains") {
    TypeSpec.all constrain CTList(CTNumber) should equal(CTList(CTNumber) | CTList(CTInteger) | CTList(CTFloat))
    TypeSpec.all constrain CTList(ClosedDynamicUnionType(Set(CTFloat, CTInteger))(InputPosition.NONE)) should equal(
      CTList(CTInteger) | CTList(CTFloat)
    )
  }

  test("constrain to sub type within list") {
    CTList(CTAny).covariant constrain CTList(CTString) should equal(CTList(CTString).invariant)
  }

  test("constrain to another branch") {
    CTNumber.covariant constrain CTString should equal(TypeSpec.none)

    CTString.covariant constrain CTNumber should equal(TypeSpec.none)
  }

  test("union two branches") {
    CTNumber.covariant | CTString.covariant should equal(CTNumber | CTInteger | CTFloat | CTString)
  }

  test("constrain to super type of some") {
    (CTInteger | CTString) constrain CTNumber should equal(CTInteger.invariant)
    CTInteger.contravariant constrain CTNumber should equal(CTNumber | CTInteger)
    CTNumber.contravariant constrain CTNumber should equal(CTNumber.invariant)
    CTNumber.contravariant constrain CTAny should equal(CTAny | CTNumber)
    ClosedDynamicUnionType(Set(CTString, CTInteger))(InputPosition.NONE) constrain CTNumber should equal(
      CTInteger.invariant
    )
  }

  test("constrain to CTAny") {
    TypeSpec.all constrain CTAny should equal(TypeSpec.all)
    CTNumber.covariant constrain CTAny should equal(CTNumber.covariant)
  }

  test("constrain to sub type of some") {
    val constrainedToNumberOrCollectionT = CTNumber.covariant | CTList(CTAny).covariant
    constrainedToNumberOrCollectionT constrain CTList(CTNumber) should equal(
      CTList(CTNumber) | CTList(CTFloat) | CTList(CTInteger)
    )
  }

  test("constrain to super type of none") {
    CTNumber.contravariant constrain CTInteger should equal(TypeSpec.none)
    CTNumber.contravariant constrain CTString should equal(TypeSpec.none)
  }

  test("should find leastUpperBounds of TypeSpecs") {
    (CTNode | CTNumber) leastUpperBounds (CTNode | CTNumber) should equal(CTNode | CTNumber | CTAny)

    (CTNode | CTNumber) leastUpperBounds (CTNode | CTNumber) should equal(CTNode | CTNumber | CTAny)
    (CTNode | CTNumber) leastUpperBounds CTNumber should equal(CTNumber | CTAny)
    (CTNode | CTNumber) leastUpperBounds (CTNode | CTNumber | CTRelationship) should equal(
      CTNode | CTNumber | CTMap | CTAny
    )
    (CTNode | CTNumber) leastUpperBounds CTAny should equal(CTAny.invariant)
    CTAny leastUpperBounds (CTNode | CTNumber) should equal(CTAny.invariant)

    CTRelationship.invariant leastUpperBounds CTNode.invariant should equal(CTMap.invariant)
    (CTRelationship | CTInteger) leastUpperBounds (CTNode | CTNumber) should equal(CTMap | CTNumber | CTAny)

    (CTInteger | CTList(CTString)) leastUpperBounds (CTNumber | CTList(CTInteger)) should equal(
      CTNumber | CTList(CTAny) | CTAny
    )

    ClosedDynamicUnionType(Set(CTString, CTInteger))(InputPosition.NONE) leastUpperBounds CTString should equal(
      CTAny | CTString
    )
  }

  test("leastUpperBounds to root type") {
    TypeSpec.all leastUpperBounds CTAny should equal(CTAny.invariant)
    CTAny leastUpperBounds TypeSpec.all should equal(CTAny.invariant)
    CTList(CTAny).covariant leastUpperBounds CTAny should equal(CTAny.invariant)
    CTAny leastUpperBounds CTList(CTAny).covariant should equal(CTAny.invariant)
  }

  test("leastUpperBounds with leaf type") {
    TypeSpec.all leastUpperBounds CTInteger should equal(CTAny | CTNumber | CTInteger)
  }

  test("leastUpperBounds with list") {
    TypeSpec.all leastUpperBounds CTList(CTAny) should equal(CTAny | CTList(CTAny))
    TypeSpec.all leastUpperBounds CTList(CTString) should equal(CTAny | CTList(CTAny) | CTList(CTString))
  }

  test("leastUpperBounds with multiple types") {
    TypeSpec.all leastUpperBounds (CTInteger | CTString) should equal(CTAny | CTNumber | CTInteger | CTString)
    TypeSpec.all leastUpperBounds (CTList(CTString) | CTInteger) should equal(
      CTAny | CTNumber | CTInteger | CTList(CTAny) | CTList(CTString)
    )
  }

  test("leastUpperBounds with some super types") {
    (CTList(CTString) | CTInteger) leastUpperBounds CTNumber should equal(CTAny | CTNumber)
    (CTList(CTString) | CTInteger) leastUpperBounds CTList(CTAny) should equal(CTAny | CTList(CTAny))

    CTInteger leastUpperBounds CTNumber.covariant should equal(CTNumber | CTInteger)

    val mergedSet = CTList(CTList(CTAny)).covariant leastUpperBounds TypeSpec.all
    mergedSet.contains(CTList(CTList(CTString))) should equal(true)
    mergedSet.contains(CTList(CTList(CTInteger))) should equal(true)
    mergedSet.contains(CTList(CTList(CTNumber))) should equal(true)
    mergedSet.contains(CTList(CTList(CTAny))) should equal(true)
    mergedSet.contains(CTList(CTString)) should equal(false)
    mergedSet.contains(CTList(CTNumber)) should equal(false)
    mergedSet.contains(CTList(CTAny)) should equal(true)
    mergedSet.contains(CTString) should equal(false)
    mergedSet.contains(CTNumber) should equal(false)
    mergedSet.contains(CTAny) should equal(true)

    CTNumber.contravariant leastUpperBounds CTInteger.contravariant should equal(CTAny | CTNumber)
  }

  test("leastUpperBounds with some sub types") {
    (CTList(CTString) | CTNumber) leastUpperBounds CTInteger should equal(CTAny | CTNumber)
    CTNumber.covariant leastUpperBounds CTInteger should equal(CTInteger | CTNumber)

    val numberOrCollectionT = CTNumber.covariant | CTList(CTAny).covariant
    numberOrCollectionT leastUpperBounds CTInteger should equal(CTAny | CTNumber | CTInteger)
    numberOrCollectionT leastUpperBounds CTList(CTInteger) should equal(
      CTAny | CTList(CTAny) | CTList(CTNumber) | CTList(CTInteger)
    )

    val listOfListOfAny = CTList(CTList(CTAny)).covariant
    (TypeSpec.all leastUpperBounds listOfListOfAny) contains CTList(CTList(CTString)) should equal(true)
    (TypeSpec.all leastUpperBounds listOfListOfAny) contains CTList(CTList(CTInteger)) should equal(true)
    (TypeSpec.all leastUpperBounds listOfListOfAny) contains CTList(CTList(CTNumber)) should equal(true)
    (TypeSpec.all leastUpperBounds listOfListOfAny) contains CTList(CTList(CTAny)) should equal(true)
    (TypeSpec.all leastUpperBounds listOfListOfAny) contains CTList(CTString) should equal(false)
    (TypeSpec.all leastUpperBounds listOfListOfAny) contains CTList(CTNumber) should equal(false)
    (TypeSpec.all leastUpperBounds listOfListOfAny) contains CTList(CTAny) should equal(true)
    (TypeSpec.all leastUpperBounds listOfListOfAny) contains CTString should equal(false)
    (TypeSpec.all leastUpperBounds listOfListOfAny) contains CTNumber should equal(false)
    (TypeSpec.all leastUpperBounds listOfListOfAny) contains CTAny should equal(true)
  }

  test("leastUpperBounds of branch with sub type") {
    CTNumber.covariant leastUpperBounds CTInteger should equal(CTNumber | CTInteger)
  }

  test("leastUpperBounds of branch to branch root") {
    CTNumber.covariant leastUpperBounds CTNumber should equal(CTNumber.invariant)
  }

  test("leastUpperBounds of branch to super type") {
    CTInteger.covariant leastUpperBounds CTNumber should equal(CTNumber.invariant)
  }

  test("leastUpperBounds of branch to unrelated type") {
    CTNumber.covariant leastUpperBounds CTString should equal(CTAny.invariant)
    CTInteger.covariant leastUpperBounds CTString should equal(CTAny.invariant)
  }

  test("leastUpperBounds with equivalent") {
    CTNumber.covariant leastUpperBounds (CTNumber | CTInteger | CTFloat) should equal(CTNumber.covariant)
  }

  test("should wrap in list") {
    (CTString | CTList(CTNumber)).wrapInList should equal(CTList(CTString) | CTList(CTList(CTNumber)))
    ClosedDynamicUnionType(Set(CTString, CTInteger))(InputPosition.NONE).wrapInList should equal(
      CTList(CTString) | CTList(CTInteger)
    )
    TypeSpec.all.wrapInList should equal(CTList(CTAny).covariant)
  }

  test("should identify coercions") {
    CTFloat.covariant.coercions should equal(TypeSpec.none)
    CTInteger.covariant.coercions should equal(CTFloat.invariant)
    (CTFloat | CTInteger).coercions should equal(CTFloat.invariant)
    CTList(CTAny).covariant.coercions should equal(CTBoolean.invariant)
    TypeSpec.exact(CTList(CTPath)).coercions should equal(CTBoolean.invariant)
    CTList(CTAny).covariant.coercions should equal(CTBoolean.invariant)
    TypeSpec.all.coercions should equal(TypeSpec.none)
    CTInteger.contravariant.coercions should equal(TypeSpec.none)
    ClosedDynamicUnionType(Set(CTFloat, CTInteger))(InputPosition.NONE).coercions should equal(CTFloat.invariant)
    ClosedDynamicUnionType(Set(CTString, CTInteger))(InputPosition.NONE).coercions should equal(CTFloat.invariant)
  }

  test("should intersect with coercions") {
    TypeSpec.all intersectOrCoerce CTInteger should equal(CTInteger.invariant)
    CTInteger intersectOrCoerce CTFloat should equal(CTFloat.invariant)
    CTNumber intersectOrCoerce CTFloat should equal(TypeSpec.none)
    CTList(CTAny).covariant intersectOrCoerce CTBoolean should equal(CTBoolean.invariant)
    CTNumber.covariant intersectOrCoerce CTBoolean should equal(TypeSpec.none)
    CTInteger intersectOrCoerce CTString should equal(TypeSpec.none)
    ClosedDynamicUnionType(Set(CTString, CTInteger))(InputPosition.NONE) intersectOrCoerce CTFloat should equal(
      CTFloat.invariant
    )
    ClosedDynamicUnionType(Set(CTString, CTInteger))(InputPosition.NONE) intersectOrCoerce CTBoolean should equal(
      TypeSpec.none
    )
    ClosedDynamicUnionType(Set(CTString, CTInteger))(InputPosition.NONE) intersectOrCoerce CTString should equal(
      CTString.invariant
    )
  }

  test("should constrain with coercions") {
    TypeSpec.all constrainOrCoerce CTInteger should equal(CTInteger.invariant)
    CTInteger constrainOrCoerce CTFloat should equal(CTFloat.invariant)
    CTNumber constrainOrCoerce CTFloat should equal(TypeSpec.none)
    CTList(CTAny).covariant constrainOrCoerce CTBoolean should equal(CTBoolean.invariant)
    CTNumber.covariant constrainOrCoerce CTBoolean should equal(TypeSpec.none)
    CTInteger constrainOrCoerce CTString should equal(TypeSpec.none)
    ClosedDynamicUnionType(Set(CTString, CTInteger))(InputPosition.NONE) constrainOrCoerce CTFloat should equal(
      CTFloat.invariant
    )
    ClosedDynamicUnionType(Set(CTString, CTInteger))(InputPosition.NONE) constrainOrCoerce CTBoolean should equal(
      TypeSpec.none
    )
    ClosedDynamicUnionType(Set(CTString, CTInteger))(InputPosition.NONE) constrainOrCoerce CTString should equal(
      CTString.invariant
    )
  }

  test("should leastUpperBound with coercions") {
    CTInteger coerceOrLeastUpperBound CTFloat should equal(CTFloat.invariant)
    CTNumber coerceOrLeastUpperBound CTFloat should equal(CTNumber.invariant)
    CTList(CTAny).covariant coerceOrLeastUpperBound CTBoolean should equal(CTBoolean.invariant)
    CTNumber.covariant coerceOrLeastUpperBound CTBoolean should equal(CTAny.invariant)
    CTInteger coerceOrLeastUpperBound CTString should equal(CTAny.invariant)
    ClosedDynamicUnionType(Set(CTString, CTFloat))(InputPosition.NONE) coerceOrLeastUpperBound CTInteger should equal(
      CTNumber.contravariant
    )
    ClosedDynamicUnionType(Set(CTString, CTInteger))(InputPosition.NONE) coerceOrLeastUpperBound CTBoolean should equal(
      CTAny.invariant
    )
    ClosedDynamicUnionType(Set(CTString, CTInteger))(InputPosition.NONE) coerceOrLeastUpperBound CTString should equal(
      CTString.contravariant
    )
  }

  test("should coerce or convert") {
    CTInteger coerceOrConvert CTInteger should equal(CTInteger.invariant)
    CTInteger coerceOrConvert CTFloat should equal(CTFloat.invariant)
    CTFloat coerceOrConvert CTInteger should equal(CTFloat.invariant)

    CTInteger coerceOrConvert CTString should equal(CTInteger.invariant)

    CTList(CTInteger) coerceOrConvert CTList(CTBoolean) should equal(CTList(CTInteger).invariant)
    CTList(CTInteger) coerceOrConvert CTList(CTAny) should equal(CTList(CTAny).invariant)
  }

  test("equal TypeSpecs should equal") {
    CTString.invariant should equal(CTString.invariant)

    CTString.invariant should equal(CTString.covariant)
    CTString.covariant should equal(CTString.invariant)

    CTFloat | CTInteger | CTNumber should equal(CTNumber | CTInteger | CTFloat)
    CTNumber.covariant should equal(CTNumber | CTInteger | CTFloat)
    CTNumber | CTInteger | CTFloat should equal(CTNumber.covariant)

    CTNumber.covariant | CTInteger.covariant should equal(CTNumber.covariant)
    CTNumber.covariant | CTString.covariant should not equal CTNumber.covariant

    TypeSpec.all should equal(TypeSpec.all)
    CTAny.covariant should equal(TypeSpec.all)
    CTList(CTAny).covariant should equal(CTList(CTAny).covariant)
    CTList(CTAny).covariant should equal(CTList(CTAny).covariant)

    TypeSpec.all | CTString.covariant should equal(TypeSpec.all)
    TypeSpec.all | TypeSpec.all should equal(TypeSpec.all)

    ClosedDynamicUnionType(Set(CTString, CTFloat))(InputPosition.NONE).invariant should equal(
      ClosedDynamicUnionType(Set(CTString, CTFloat))(InputPosition.NONE).invariant
    )
    ClosedDynamicUnionType(Set(CTString, CTFloat))(InputPosition.NONE).covariant should equal(
      ClosedDynamicUnionType(Set(CTString, CTFloat))(InputPosition.NONE).covariant
    )
    ClosedDynamicUnionType(Set(CTString, CTFloat))(InputPosition.NONE).contravariant should equal(
      ClosedDynamicUnionType(Set(CTString, CTFloat))(InputPosition.NONE).contravariant
    )
  }

  test("different TypeSpecs should not equal") {
    TypeSpec.all should not equal CTNumber.covariant
    CTNumber.covariant should not equal TypeSpec.all

    CTList(CTAny).covariant should not equal CTNumber.covariant
    CTNumber.covariant should not equal CTList(CTAny).covariant

    CTNumber.invariant should not equal TypeSpec.all
    TypeSpec.all should not equal CTNumber.invariant

    CTFloat.invariant should not equal ClosedDynamicUnionType(Set(CTString, CTFloat))(InputPosition.NONE).invariant
    CTString.invariant should not equal ClosedDynamicUnionType(Set(CTString, CTFloat))(InputPosition.NONE).invariant
  }

  test("should have indefinite size when allowing unconstrained any at any depth") {
    TypeSpec.all.hasDefiniteSize should equal(false)
    CTList(CTAny).covariant.hasDefiniteSize should equal(false)

    (CTString | CTNumber).hasDefiniteSize should equal(true)
    CTList(CTString).covariant.hasDefiniteSize should equal(true)

    CTList(CTList(CTAny)).covariant.hasDefiniteSize should equal(false)
    CTList(CTList(CTString)).covariant.hasDefiniteSize should equal(true)

    CTAny.contravariant.hasDefiniteSize should equal(true)
    (CTList(CTAny).covariant leastUpperBounds CTList(CTAny)).hasDefiniteSize should equal(true)

    ClosedDynamicUnionType(Set(CTString, CTFloat))(InputPosition.NONE).hasDefiniteSize should equal(true)
    ClosedDynamicUnionType(Set(CTString, CTList(CTAny)))(InputPosition.NONE).covariant.hasDefiniteSize should equal(
      false
    )
  }

  test("should be empty when no possibilities remain") {
    TypeSpec.all.isEmpty should equal(false)
    TypeSpec.none.isEmpty should equal(true)
    (CTNumber.contravariant intersect CTInteger).isEmpty should equal(true)
    (ClosedDynamicUnionType(Set(CTString, CTFloat))(InputPosition.NONE) intersect CTBoolean).isEmpty should equal(true)
  }

  test("should format none") {
    TypeSpec.none.mkString("(", ", ", " or ", ")") should equal("()")
  }

  test("should format single type") {
    CTAny.invariant.mkString("(", ", ", " or ", ")") should equal("(Any)")
    CTNode.invariant.mkString("<", ", ", " and ", ">") should equal("<Node>")
  }

  test("should format two types") {
    (CTAny | CTNode).mkString("", ", ", " or ", "") should equal("Any or Node")
    (CTRelationship | CTNode).mkString("-", ", ", " or ", "-") should equal("-Node or Relationship-")
    ClosedDynamicUnionType(Set(CTFloat, CTInteger))(
      InputPosition.NONE
    ).invariant.mkString("<", ", ", " and ", ">") should equal("<Float and Integer>")
  }

  test("should format three types") {
    (CTRelationship | CTInteger | CTNode).mkString(", ") should equal("Integer, Node, Relationship")
    (CTRelationship | CTInteger | CTNode).mkString("(", ", ", ")") should equal("(Integer, Node, Relationship)")
    (CTRelationship | CTAny | CTNode).mkString("(", ", ", " or ", ")") should equal("(Any, Node or Relationship)")
    (CTRelationship | CTInteger | CTNode).mkString("[", ", ", " and ", "]") should equal(
      "[Integer, Node and Relationship]"
    )
    ClosedDynamicUnionType(Set(CTFloat, CTInteger, CTString))(
      InputPosition.NONE
    ).invariant.mkString("<", ", ", " and ", ">") should equal("<Float, Integer and String>")
  }

  test("should format to string for indefinite sized set") {
    TypeSpec.all.mkString(", ") should equal("T")
    CTList(CTAny).covariant.mkString(", ") should equal("List<T>")
    (CTList(CTAny).covariant | CTBoolean).mkString(", ") should equal("Boolean, List<T>")
    (CTList(CTList(CTAny)).covariant | CTBoolean | CTList(CTString)).mkString(", ") should equal(
      "Boolean, List<String>, List<List<T>>"
    )
  }

  test("should format to string for definite sized set") {
    CTAny.invariant.mkString(", ") should equal("Any")
    CTString.invariant.mkString(", ") should equal("String")
    CTNumber.covariant.mkString(", ") should equal("Float, Integer, Number")
    (CTNumber.covariant | CTBoolean.covariant).mkString(", ") should equal("Boolean, Float, Integer, Number")
    CTNumber.contravariant.mkString(", ") should equal("Any, Number")
    (CTBoolean.covariant | CTString.covariant | CTList(CTBoolean).covariant | CTList(CTString).covariant).mkString(
      ", "
    ) should equal("Boolean, String, List<Boolean>, List<String>")
    (CTBoolean.covariant | CTString.covariant | CTList(CTBoolean).covariant | CTList(
      CTList(CTString)
    ).covariant).mkString(", ") should equal("Boolean, String, List<Boolean>, List<List<String>>")
    CTList(CTAny).contravariant.mkString(", ") should equal("Any, List<Any>")
  }

  test("should iterate over definite sized set") {
    CTString.invariant.iterator.toSeq should equal(Seq(CTString))
    CTNumber.covariant.iterator.toSeq should equal(Seq(CTFloat, CTInteger, CTNumber))
    (CTNumber.covariant | CTBoolean.covariant).iterator.toSeq should equal(Seq(CTBoolean, CTFloat, CTInteger, CTNumber))
    CTNumber.contravariant.iterator.toSeq should equal(Seq(CTAny, CTNumber))
    (CTBoolean | CTString | CTList(CTBoolean) | CTList(CTString)).iterator.toSeq should equal(Seq(
      CTBoolean,
      CTString,
      CTList(CTBoolean),
      CTList(CTString)
    ))
    (CTBoolean | CTString | CTList(CTBoolean) | CTList(CTList(CTString))).iterator.toSeq should equal(Seq(
      CTBoolean,
      CTString,
      CTList(CTBoolean),
      CTList(CTList(CTString))
    ))
  }

  test("combines empty TypeSpec to any") {
    // Given
    val specs = Seq.empty[TypeSpec]

    // when
    val spec = TypeSpec.combineMultipleTypeSpecs(specs)

    // Then
    spec should equal(CTAny)
  }

  test("combines int to int") {
    // Given
    val specs = Seq(CTInteger.invariant)

    // when
    val spec = TypeSpec.combineMultipleTypeSpecs(specs)

    // Then
    spec should equal(CTInteger)
  }

  test("combines int and int to int") {
    // Given
    val specs = Seq(CTInteger.invariant, CTInteger.invariant)

    // when
    val spec = TypeSpec.combineMultipleTypeSpecs(specs)

    // Then
    spec should equal(CTInteger)
  }

  test("combines int and int inside a dynamic union to int") {
    // Given
    val specs = Seq(ClosedDynamicUnionType(Set(CTInteger, CTInteger))(InputPosition.NONE).invariant)

    // when
    val spec = TypeSpec.combineMultipleTypeSpecs(specs)

    // Then
    spec should equal(CTInteger)
  }

  test("combines float and int to number") {
    // Given
    val specs = Seq(CTFloat.invariant, CTInteger.invariant)

    // when
    val spec = TypeSpec.combineMultipleTypeSpecs(specs)

    // Then
    spec should equal(CTNumber)
  }

  test("combines float and point to any") {
    // Given
    val specs = Seq(CTFloat.invariant, CTPoint.invariant)

    // when
    val spec = TypeSpec.combineMultipleTypeSpecs(specs)

    // Then
    spec should equal(CTAny)
  }

  test("combines covariant types") {
    // Given
    val specs = Seq(CTFloat.invariant, CTNumber.covariant)

    // when
    val spec = TypeSpec.combineMultipleTypeSpecs(specs)

    // Then
    spec should equal(CTNumber)
  }

  test("combines union types") {
    // Given
    val specs = Seq(CTFloat.invariant union CTInteger.invariant, CTNumber.covariant)

    // when
    val spec = TypeSpec.combineMultipleTypeSpecs(specs)

    // Then
    spec should equal(CTNumber)
  }

  test("combines same union types") {
    // Given
    val specs = Seq(CTFloat.invariant union CTInteger.invariant, CTFloat.invariant union CTInteger.invariant)

    // when
    val spec = TypeSpec.combineMultipleTypeSpecs(specs)

    // Then
    spec should equal(CTNumber)
  }

  test("combines unrelated union types") {
    // Given
    val specs = Seq(CTFloat.invariant union CTInteger.invariant, CTString.invariant union CTInteger.invariant)

    // when
    val spec = TypeSpec.combineMultipleTypeSpecs(specs)

    // Then
    spec should equal(CTAny)
  }

  // Testing TypeSpec.cypherTypeForTypeSpec

  test("converts any to CTAny") {
    // Given
    val spec = CTAny.invariant

    // when
    val typ = TypeSpec.cypherTypeForTypeSpec(spec)

    // Then
    typ should equal(CTAny)
  }

  test("converts T to CTAny") {
    // Given
    val spec = CTAny.covariant

    // when
    val typ = TypeSpec.cypherTypeForTypeSpec(spec)

    // Then
    typ should equal(CTAny)
  }

  test("converts contravariant type to CTAny") {
    // Given
    val spec = CTFloat.contravariant

    // when
    val typ = TypeSpec.cypherTypeForTypeSpec(spec)

    // Then
    typ should equal(CTAny)
  }

  test("converts union type to CTAny") {
    // Given
    val spec = CTFloat.invariant union CTString.invariant

    // when
    val typ = TypeSpec.cypherTypeForTypeSpec(spec)

    // Then
    typ should equal(CTAny)
  }

  test("converts intersection type to CTAny") {
    // Given
    val spec = CTFloat.invariant intersect CTString.invariant

    // when
    val typ = TypeSpec.cypherTypeForTypeSpec(spec)

    // Then
    typ should equal(CTAny)
  }

  test("Nothing and Null should error when used in TypeSpec Contexts") {
    assertThrows[UnsupportedOperationException](NothingType()(InputPosition.NONE).invariant)
    assertThrows[UnsupportedOperationException](NullType()(InputPosition.NONE).invariant)
    assertThrows[UnsupportedOperationException](NothingType()(InputPosition.NONE).covariant)
    assertThrows[UnsupportedOperationException](NullType()(InputPosition.NONE).covariant)
    assertThrows[UnsupportedOperationException](NothingType()(InputPosition.NONE).contravariant)
    assertThrows[UnsupportedOperationException](NullType()(InputPosition.NONE).contravariant)

    assertThrows[UnsupportedOperationException](NothingType()(InputPosition.NONE) | CTAny)
    assertThrows[UnsupportedOperationException](NullType()(InputPosition.NONE) & CTAny)
  }
}
