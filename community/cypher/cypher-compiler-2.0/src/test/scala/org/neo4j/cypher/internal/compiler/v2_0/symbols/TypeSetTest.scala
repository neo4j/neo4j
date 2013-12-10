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
import scala.collection.immutable.SortedSet

class TypeSetTest extends Assertions {

  implicit def orderingOfCypherType[T <: CypherType] : Ordering[T] = Ordering.by(_.toString)

  @Test
  def shouldFormatNoType() {
    assertEquals("", TypeSet().formattedString)
  }

  @Test
  def shouldFormatSingleType() {
    assertEquals("Any", SortedSet(CTAny).formattedString)
    assertEquals("Node", SortedSet(CTNode).formattedString)
  }

  @Test
  def shouldFormatTwoTypes() {
    assertEquals("Any or Node", SortedSet(CTAny, CTNode).formattedString)
    assertEquals("Node or Relationship", SortedSet(CTRelationship, CTNode).formattedString)
  }

  @Test
  def shouldFormatThreeTypes() {
	  assertEquals("Any, Node or Relationship", SortedSet(CTRelationship, CTAny, CTNode).formattedString)
	  assertEquals("Integer, Node or Relationship", SortedSet(CTRelationship, CTInteger, CTNode).formattedString)
  }

  @Test
  def shouldInferTypeSetsUsingCovariantMergeUp() {
    assertEquals(Set(CTNode, CTNumber), Set(CTNode, CTNumber) mergeUp Set(CTNode, CTNumber))

    assertEquals(Set(CTNode, CTNumber), Set(CTNode, CTNumber) mergeUp Set(CTNode, CTNumber))
    assertEquals(Set(CTNumber), Set(CTNode, CTNumber) mergeUp Set(CTNumber))
    assertEquals(Set(CTNode, CTNumber), Set(CTNode, CTNumber) mergeUp Set(CTNode, CTNumber, CTRelationship))
    assertEquals(Set(CTAny), Set(CTNode, CTNumber) mergeUp Set(CTAny))
    assertEquals(Set(CTAny), Set(CTAny) mergeUp Set(CTNode, CTNumber))

    assertEquals(Set(CTMap), Set(CTRelationship) mergeUp Set(CTNode))
    assertEquals(Set(CTMap, CTNumber), Set(CTRelationship, CTLong) mergeUp Set(CTNode, CTNumber))
  }

  @Test
  def shouldMergeUpCollectionIterable() {
    assertEquals(Set(CTNumber, CTCollectionAny),
      Set(CTInteger, CTCollection(CTString)) mergeUp Set(CTNumber, CTCollection(CTInteger)))
  }

  @Test
  def shouldMergeDownCollectionIterable() {
    assertEquals(Set(CTInteger),
      Set(CTInteger, CTString, CTCollection(CTInteger)) mergeDown Set(CTNumber, CTCollection(CTString)) )
    assertEquals(Set(CTInteger, CTCollection(CTString)),
      Set(CTInteger, CTString, CTCollectionAny) mergeDown Set(CTNumber, CTCollection(CTString)) )
  }

  @Test
  def shouldInferTypeSetsUsingContravariantMergeDown() {
    assertEquals(Set(CTNode, CTNumber), Set(CTNode, CTNumber) mergeDown Set(CTNode, CTNumber))
    assertEquals(Set(CTNumber), Set(CTNode, CTNumber) mergeDown Set(CTNumber))
    assertEquals(Set(CTNode, CTNumber), Set(CTNode, CTNumber) mergeDown Set(CTNode, CTNumber, CTRelationship))
    assertEquals(Set(CTNode, CTNumber), Set(CTNode, CTNumber) mergeDown Set(CTAny))
    assertEquals(Set(CTNode, CTNumber), Set(CTAny) mergeDown Set(CTNode, CTNumber))

    assertEquals(Set(), Set(CTRelationship) mergeDown Set(CTNode))
    assertEquals(Set(CTLong), Set(CTRelationship, CTLong) mergeDown Set(CTNode, CTNumber))
    assertEquals(Set(CTNode, CTNumber), Set(CTAny) mergeDown Set(CTNode, CTNumber))
  }
}
