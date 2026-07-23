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
package org.neo4j.cypher.internal.util.types

import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTAnyNotNull
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTNothing
import org.neo4j.cypher.internal.util.symbols.CTNull
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.IsSubtypeOf
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.util.Random

class IsSubtypeOfTest extends CypherTypeTestSuite {

  test("base types representatives should not be subtypes of each other") {
    shouldNotBeSubtypesOfEachOther(baseTypeRepresentatives)
  }

  for (l <- Lattice.allLattices) {
    test(s"lattice: ${l.name}") {
      shouldBeSubtypeLattice(l.edges*)
    }
  }

  /*
   * checks and assertions
   */

  private def shouldNotBeSubtypesOfEachOther(ts: Set[CypherType]): Unit = {
    for (sub <- ts) {
      for (sup <- ts) {
        if (sub != sup) {
          assertIsNotSubtypeOf(sub, sup)
        }
      }
      checkInvariants(sub)
    }
  }

  private def shouldBeSubtypeLattice(latticeEdges: (CypherType, CypherType)*): Unit = {
    val allSubtypeRelationships = transitiveClosure(latticeEdges*)
    for ((sub, sup) <- allSubtypeRelationships) {
      checkIsSubtypeOf(sub, sup)
    }
    val allTypes = latticeEdges.flatMap {
      case (sub, sup) => Seq(sub, sup)
    }
    for (t <- allTypes) {
      checkInvariants(t)
    }
    // for the negative tests we sample 10 from each side to keep the test runtime under control
    for {
      a <- Random.shuffle(allTypes).take(10)
      b <- Random.shuffle(allTypes).take(10) if a != b && !allSubtypeRelationships.contains((a, b))
    } checkIsNotSubtypeOf(a, b)
  }

  private def checkInvariants(t: CypherType, nestingLevel: Int = 0): Unit = {
    checkAgainstAny(t)
    checkAgainstNothingAndNull(t)
    checkAgainstItselfAndNullability(t)
    // check for lists
    if (nestingLevel < 3) checkInvariants(l(t), nestingLevel + 1)
    // check for records
    if (nestingLevel < 3) checkInvariants(rt("x" :: t), nestingLevel + 1)
  }

  private def checkAgainstAny(t: CypherType): Unit = {
    assertIsSubtypeOf(t, CTAny)
    if (!t.isNullable) assertIsSubtypeOf(t, CTAnyNotNull) else assertIsNotSubtypeOf(t, CTAnyNotNull)
    if (t != CTAny) assertIsNotSubtypeOf(CTAny, t)
  }

  private def checkAgainstNothingAndNull(t: CypherType): Unit = {
    assertIsSubtypeOf(CTNothing, t)
    if (t.isNullable) assertIsSubtypeOf(CTNull, t) else assertIsNotSubtypeOf(CTNull, t)
    if (t != CTNothing) assertIsNotSubtypeOf(t, CTNothing)
  }

  private def checkAgainstItselfAndNullability(t: CypherType): Unit = {
    assertIsSubtypeOf(t, t)
    assertIsSubtypeOf(t.notNull, t.nullable)
    assertIsNotSubtypeOf(t.nullable, t.notNull)
  }

  private def checkIsSubtypeOf(sub: CypherType, sup: CypherType, nestingLevel: Int = 0): Unit = {
    assertIsSubtypeOf(sub, sup)
    checkNullabilityInvariantForSubtypes(sub, sup)
    // given SUB <: SUP
    // check for list covariance
    {
      if (nestingLevel < 3) {
        // LIST<SUB> <: LIST<SUP>
        checkIsSubtypeOf(l(sub), l(sup), nestingLevel + 1)
        // LIST<SUB - NULL> <: LIST<SUP + NULL>
        checkIsSubtypeOf(l(sub.notNull), l(sup.nullable), nestingLevel + 1)
        // but not LIST<SUB + NULL> <: LIST<SUP - NULL>
        checkIsNotSubtypeOf(l(sub.nullable), l(sup.notNull), nestingLevel + 1)
      }
      // if SUB is not a LIST or the nothing type, then not SUB <: LIST<SUP>
      if (noListType(sub) && sub != CTNothing && sub != CTNull) checkIsNotSubtypeOf(sub, l(sup), nestingLevel + 1)
      // if SUP is not a RECORD or the nothing type, then not LIST<SUB> <: SUP
      if (noListType(sup) && sup != CTAny) checkIsNotSubtypeOf(l(sub), sup, nestingLevel + 1)
    }
    // check for record covariance
    {
      if (nestingLevel < 3) {
        // { x :: SUB } <: { x :: SUP }
        checkIsSubtypeOf(rt("x" :: sub), rt("x" :: sup), nestingLevel + 1)
        // { x :: SUB, y :: BOOLEAN, z :: SUB } <: { x :: SUP, y :: BOOLEAN, z :: SUP }
        checkIsSubtypeOf(
          rt("x" :: sub, "y" :: CTBoolean, "z" :: sub),
          rt("x" :: sup, "y" :: CTBoolean, "z" :: sup),
          nestingLevel + 1
        )
        // { x :: SUB - NULL } <: { x :: SUP + NULL }
        checkIsSubtypeOf(rt("x" :: sub.notNull), rt("x" :: sup.nullable), nestingLevel + 1)
        // but not { x :: SUB + NULL } <: { x :: SUP - NULL }
        checkIsNotSubtypeOf(rt("x" :: sub.nullable), rt("x" :: sup.notNull), nestingLevel + 1)
      }
      // if SUB is not a RECORD or the nothing type, then not SUB <: { x :: SUP }
      if (noRecordType(sub) && sub != CTNothing && sub != CTNull)
        checkIsNotSubtypeOf(sub, rt("x" :: sup), nestingLevel + 1)
      // if SUP is not a RECORD or the nothing type, then not { x :: SUB } <: SUP
      if (noRecordType(sup) && sup != CTAny) checkIsNotSubtypeOf(rt("x" :: sub), sup, nestingLevel + 1)
    }
  }

  private def checkNullabilityInvariantForSubtypes(sub: CypherType, sup: CypherType): Unit = {
    assertIsSubtypeOf(sub.notNull, sup.nullable)
    assertIsSubtypeOf(sub.notNull, sup.notNull)
    assertIsSubtypeOf(sub.nullable, sup.nullable)
    assertIsNotSubtypeOf(sub.nullable, sup.notNull)
  }

  private def checkIsNotSubtypeOf(sub: CypherType, sup: CypherType, nestingLevel: Int = 0): Unit = {
    assertIsNotSubtypeOf(sub, sup)
    checkNullabilityInvariantForNonSubtypes(sub, sup)
    // check for list covariance
    if (nestingLevel < 3) checkIsNotSubtypeOf(l(sub), l(sup), nestingLevel + 1)
    // check for record covariance
    if (nestingLevel < 3) {
      checkIsNotSubtypeOf(rt("x" :: sub), rt("x" :: sup), nestingLevel + 1)
      checkIsNotSubtypeOf(rt("x" :: sub, "y" :: sup), rt("x" :: sup, "y" :: sub), nestingLevel + 1)
      checkIsNotSubtypeOf(
        rt("x" :: sub, "y" :: CTBoolean, "z" :: sub),
        rt("x" :: sup, "y" :: CTBoolean, "z" :: sup),
        nestingLevel + 1
      )
    }
  }

  private def checkNullabilityInvariantForNonSubtypes(sub: CypherType, sup: CypherType): Unit = {
    if (sub.notNull != CTNothing) {
      if (sup.nullable != CTAny) assertIsNotSubtypeOf(sub.notNull, sup.nullable)
      if (sup.notNull != CTAnyNotNull) assertIsNotSubtypeOf(sub.notNull, sup.notNull)
    }
    if (sub.nullable != CTNull) {
      if (sup.nullable != CTAny) assertIsNotSubtypeOf(sub.nullable, sup.nullable)
      if (sup.notNull != CTAnyNotNull) assertIsNotSubtypeOf(sub.nullable, sup.notNull)
    }
  }

  private def assertIsSubtypeOf(sub: CypherType, sup: CypherType): Unit = {
    if (!IsSubtypeOf(sub, sup)) {
      fail(
        s"""${sub.description} should be subtype of ${sup.description}, but is not
           |sub: ${pprint.apply(sub)}
           |sup: ${pprint.apply(sup)}""".stripMargin
      )
    }
  }

  private def assertIsNotSubtypeOf(sub: CypherType, sup: CypherType): Unit = {
    if (IsSubtypeOf(sub, sup)) {
      fail(s"""${sub.description} should not be subtype of ${sup.description}, but is
              |sub: ${pprint.apply(sub)}
              |sup: ${pprint.apply(sup)}""".stripMargin)
    }
  }
}
