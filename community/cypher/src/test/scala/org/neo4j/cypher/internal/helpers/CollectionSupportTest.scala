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
package org.neo4j.cypher.internal.helpers

import org.junit.Test

class CollectionSupportTest extends CollectionSupport {

  val tester: PartialFunction[Any, Int] = { case x: Int => x }
  val mapper: PartialFunction[Any, Int] = { case x: Int => x + 1 }

  @Test
  def testIsCollectionOfMatching() {
    // Given
    val given = Seq(1, 2, 3, 4)

    // When
    val result = isCollectionOf[Int](tester)(given)

    // Then
    assert(result == Some(given))
  }

  @Test
  def testIsCollectionOf() {
    // Given
    val given = Seq(1, 2, 3, 4)

    // When
    val result = isCollectionOf[Int](tester)(given)

    // Then
    assert(result == Some(given))
  }

  @Test
  def testIsCollectionOfMapping() {
    // Given
    val given: Seq[Int] = Seq(1, 2, 3, 4)

    // When
    val result = isCollectionOf[Int](mapper)(given)

    // Then
    assert(result == Some(Seq(2, 3, 4, 5)))
  }

  @Test
  def testIsCollectionOfFailing() {
    // Given
    val given: Seq[Any] = Seq(1, 2, "foo", 3, 4)

    // When
    val result = isCollectionOf[Int](mapper)(given.toTraversable)

    // Then
    assert(result == None)
  }

  @Test
  def testIsCollectionOfEmpty() {
    // Given
    val given: Seq[String] = Seq.empty

    // When
    val result = isCollectionOf[Int](mapper)(given.toTraversable)

    // Then
    assert(result == Some(given))
  }
}
