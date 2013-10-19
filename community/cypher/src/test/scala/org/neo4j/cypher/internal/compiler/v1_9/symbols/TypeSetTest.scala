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
package org.neo4j.cypher.internal.compiler.v1_9.symbols

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
    assertEquals("Any", SortedSet(AnyType()).formattedString)
    assertEquals("Node", SortedSet(NodeType()).formattedString)
  }

  @Test
  def shouldFormatTwoTypes() {
    assertEquals("Any or Node", SortedSet(AnyType(), NodeType()).formattedString)
    assertEquals("Node or Relationship", SortedSet(RelationshipType(), NodeType()).formattedString)
  }

  @Test
  def shouldFormatThreeTypes() {
	  assertEquals("Any, Node or Relationship", SortedSet(RelationshipType(), AnyType(), NodeType()).formattedString)
	  assertEquals("Integer, Node or Relationship", SortedSet(RelationshipType(), IntegerType(), NodeType()).formattedString)
  }

  @Test
  def shouldInferTypeSetsUsingCovariantMergeDown() {
    assertEquals(Set(NodeType(), NumberType()), Set(NodeType(), NumberType()) mergeDown Set(NodeType(), NumberType()))

    assertEquals(Set(NodeType(), NumberType()), Set(NodeType(), NumberType()) mergeDown Set(NodeType(), NumberType()))
    assertEquals(Set(NumberType()), Set(NodeType(), NumberType()) mergeDown Set(NumberType()))
    assertEquals(Set(NodeType(), NumberType()), Set(NodeType(), NumberType()) mergeDown Set(NodeType(), NumberType(), RelationshipType()))
    assertEquals(Set(AnyType()), Set(NodeType(), NumberType()) mergeDown Set(AnyType()))
    assertEquals(Set(AnyType()), Set(AnyType()) mergeDown Set(NodeType(), NumberType()))

    assertEquals(Set(MapType()), Set(RelationshipType()) mergeDown Set(NodeType()))
    assertEquals(Set(MapType(), NumberType()), Set(RelationshipType(), LongType()) mergeDown Set(NodeType(), NumberType()))
  }

  @Test
  def shouldMergeDownCollectionIterable() {
    assertEquals(Set(NumberType(), CollectionType(AnyType())),
      Set(IntegerType(), CollectionType(StringType())) mergeDown Set(NumberType(), CollectionType(IntegerType())))
  }

  @Test
  def shouldMergeUpCollectionIterable() {
    assertEquals(Set(IntegerType()),
      Set(IntegerType(), StringType(), CollectionType(IntegerType())) mergeUp Set(NumberType(), CollectionType(StringType())) )
    assertEquals(Set(IntegerType(), CollectionType(StringType())),
      Set(IntegerType(), StringType(), CollectionType(AnyType())) mergeUp Set(NumberType(), CollectionType(StringType())) )
  }

  @Test
  def shouldInferTypeSetsUsingContravariantMergeUp() {
    assertEquals(Set(NodeType(), NumberType()), Set(NodeType(), NumberType()) mergeUp Set(NodeType(), NumberType()))
    assertEquals(Set(NumberType()), Set(NodeType(), NumberType()) mergeUp Set(NumberType()))
    assertEquals(Set(NodeType(), NumberType()), Set(NodeType(), NumberType()) mergeUp Set(NodeType(), NumberType(), RelationshipType()))
    assertEquals(Set(NodeType(), NumberType()), Set(NodeType(), NumberType()) mergeUp Set(AnyType()))
    assertEquals(Set(NodeType(), NumberType()), Set(AnyType()) mergeUp Set(NodeType(), NumberType()))

    assertEquals(Set(), Set(RelationshipType()) mergeUp Set(NodeType()))
    assertEquals(Set(LongType()), Set(RelationshipType(), LongType()) mergeUp Set(NodeType(), NumberType()))
    assertEquals(Set(NodeType(), NumberType()), Set(AnyType()) mergeUp Set(NodeType(), NumberType()))
  }
}
