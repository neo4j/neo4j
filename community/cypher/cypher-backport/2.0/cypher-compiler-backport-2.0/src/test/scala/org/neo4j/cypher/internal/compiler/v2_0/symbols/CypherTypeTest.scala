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

import org.junit.Test
import org.scalatest.Assertions

class CypherTypeTest extends Assertions {
  @Test
  def testParents() {
    assert(CTInteger.parents === Seq(CTNumber, CTAny))
    assert(CTNumber.parents === Seq(CTAny))
    assert(CTAny.parents === Seq())
    assert(CTCollection(CTString).parents === Seq(CTCollection(CTAny), CTAny))
  }

  @Test
  def testTypesAreAssignable() {
    assert(CTNumber.isAssignableFrom(CTInteger) === true)
    assert(CTAny.isAssignableFrom(CTString) === true)
    assert(CTCollection(CTString).isAssignableFrom(CTCollection(CTString)) === true)
    assert(CTCollection(CTNumber).isAssignableFrom(CTCollection(CTInteger)) === true)
    assert(CTInteger.isAssignableFrom(CTNumber) === false)
    assert(CTCollection(CTInteger).isAssignableFrom(CTCollection(CTString)) === false)
  }

  @Test
  def testTypeMergeUp() {
    assertCorrectTypeMergeUp(CTNumber, CTNumber, CTNumber)
    assertCorrectTypeMergeUp(CTNumber, CTAny, CTAny)
    assertCorrectTypeMergeUp(CTNumber, CTString, CTAny)
    assertCorrectTypeMergeUp(CTNumber, CTCollection(CTAny), CTAny)
    assertCorrectTypeMergeUp(CTInteger, CTFloat, CTNumber)
    assertCorrectTypeMergeUp(CTMap, CTFloat, CTAny)
  }

  @Test
  def testTypeMergeDown() {
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
    assert(simpleMergedType === result)
    val collectionMergedType: Option[CypherType] = CTCollection(a) mergeDown CTCollection(b)
    assert(collectionMergedType === (for (t <- result) yield CTCollection(t)))
  }

  private def assertCorrectTypeMergeUp(a: CypherType, b: CypherType, result: CypherType) {
    val simpleMergedType: CypherType = a mergeUp b
    assert(simpleMergedType === result)
    val collectionMergedType: CypherType = CTCollection(a) mergeUp CTCollection(b)
    assert(collectionMergedType === CTCollection(result))
  }

}
