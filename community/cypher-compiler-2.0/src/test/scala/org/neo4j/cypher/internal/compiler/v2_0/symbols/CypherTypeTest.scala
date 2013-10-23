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

import org.junit.Test
import org.scalatest.Assertions

class CypherTypeTest extends Assertions {
  @Test
  def testTypeMergeDown() {
    assertCorrectTypeMergeDown(NumberType(), NumberType(), NumberType())
    assertCorrectTypeMergeDown(NumberType(), AnyType(), AnyType())
    assertCorrectTypeMergeDown(NumberType(), StringType(), AnyType())
    assertCorrectTypeMergeDown(NumberType(), CollectionType(AnyType()), AnyType())
    assertCorrectTypeMergeDown(LongType(), DoubleType(), NumberType())
    assertCorrectTypeMergeDown(MapType(), DoubleType(), AnyType())
  }

  @Test
  def testTypeMergeUp() {
    assertCorrectTypeMergeUp(NumberType(), NumberType(), Some(NumberType()))
    assertCorrectTypeMergeUp(NumberType(), AnyType(), Some(NumberType()))
    assertCorrectTypeMergeUp(CollectionType(NumberType()), CollectionType(LongType()), Some(CollectionType(LongType())))
    assertCorrectTypeMergeUp(NumberType(), StringType(), None)
    assertCorrectTypeMergeUp(NumberType(), CollectionType(AnyType()), None)
    assertCorrectTypeMergeUp(LongType(), DoubleType(), None)
    assertCorrectTypeMergeUp(MapType(), DoubleType(), None)
    assertCorrectTypeMergeUp(BooleanType(), CollectionType(AnyType()), Some(BooleanType()))
  }

  private def assertCorrectTypeMergeUp(a: CypherType, b: CypherType, result: Option[CypherType]) {
    val simpleMergedType: Option[CypherType] = a mergeUp b
    assert(simpleMergedType === result)
    val collectionMergedType: Option[CypherType] = CollectionType(a) mergeUp CollectionType(b)
    assert(collectionMergedType === (for (t <- result) yield CollectionType(t)))
  }

  private def assertCorrectTypeMergeDown(a: CypherType, b: CypherType, result: CypherType) {
    val simpleMergedType: CypherType = a mergeDown b
    assert(simpleMergedType === result)
    val collectionMergedType: CypherType = CollectionType(a) mergeDown CollectionType(b)
    assert(collectionMergedType === CollectionType(result))
  }

}
