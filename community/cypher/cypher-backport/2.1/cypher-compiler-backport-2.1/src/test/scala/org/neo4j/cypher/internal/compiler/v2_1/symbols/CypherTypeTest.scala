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
package org.neo4j.cypher.internal.compiler.v2_1.symbols

import org.neo4j.cypher.internal.commons.CypherFunSuite

class CypherTypeTest extends CypherFunSuite {

  test("testParents") {
    CTInteger.parents should equal(Seq(CTNumber, CTAny))
    CTNumber.parents should equal(Seq(CTAny))
    CTAny.parents should equal(Seq())
    CTCollection(CTString).parents should equal(Seq(CTCollection(CTAny), CTAny))
  }

  test("testTypesAreAssignable") {
    CTNumber.isAssignableFrom(CTInteger) should equal(true)
    CTAny.isAssignableFrom(CTString) should equal(true)
    CTCollection(CTString).isAssignableFrom(CTCollection(CTString)) should equal(true)
    CTCollection(CTNumber).isAssignableFrom(CTCollection(CTInteger)) should equal(true)
    CTInteger.isAssignableFrom(CTNumber) should equal(false)
    CTCollection(CTInteger).isAssignableFrom(CTCollection(CTString)) should equal(false)
  }

  test("testTypeMergeUp") {
    assertCorrectTypeMergeUp(CTNumber, CTNumber, CTNumber)
    assertCorrectTypeMergeUp(CTNumber, CTAny, CTAny)
    assertCorrectTypeMergeUp(CTNumber, CTString, CTAny)
    assertCorrectTypeMergeUp(CTNumber, CTCollection(CTAny), CTAny)
    assertCorrectTypeMergeUp(CTInteger, CTFloat, CTNumber)
    assertCorrectTypeMergeUp(CTMap, CTFloat, CTAny)
  }

  test("testTypeMergeDown") {
    assertCorrectTypeMergeDown(CTNumber, CTNumber, Some(CTNumber))
    assertCorrectTypeMergeDown(CTNumber, CTAny, Some(CTNumber))
    assertCorrectTypeMergeDown(CTCollection(CTNumber), CTCollection(CTInteger), Some(CTCollection(CTInteger)))
    assertCorrectTypeMergeDown(CTNumber, CTString, None)
    assertCorrectTypeMergeDown(CTNumber, CTCollection(CTAny), None)
    assertCorrectTypeMergeDown(CTInteger, CTFloat, None)
    assertCorrectTypeMergeDown(CTMap, CTFloat, None)
    assertCorrectTypeMergeDown(CTBoolean, CTCollection(CTAny), None)
  }

  private def assertCorrectTypeMergeDown(a: CypherType, b: CypherType, result: Option[CypherType]) {
    val simpleMergedType: Option[CypherType] = a mergeDown b
    simpleMergedType should equal(result)
    val collectionMergedType: Option[CypherType] = CTCollection(a) mergeDown CTCollection(b)
    collectionMergedType should equal(for (t <- result) yield CTCollection(t))
  }

  private def assertCorrectTypeMergeUp(a: CypherType, b: CypherType, result: CypherType) {
    val simpleMergedType: CypherType = a mergeUp b
    simpleMergedType should equal(result)
    val collectionMergedType: CypherType = CTCollection(a) mergeUp CTCollection(b)
    collectionMergedType should equal(CTCollection(result))
  }

}
