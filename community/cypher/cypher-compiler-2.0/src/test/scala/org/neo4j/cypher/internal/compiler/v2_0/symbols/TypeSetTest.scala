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

import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions

class TypeSetTest extends Assertions {

  @Test
  def shouldInferTypeSetsUsingMergeDown() {
    assertEquals(TypeSet(NodeType(), NumberType(), AnyType()), TypeSet(NodeType(), NumberType()) mergeDown TypeSet(NodeType(), NumberType()))

    assertEquals(TypeSet(NodeType(), NumberType(), AnyType()), TypeSet(NodeType(), NumberType()) mergeDown TypeSet(NodeType(), NumberType()))
    assertEquals(TypeSet(NumberType(), AnyType()), TypeSet(NodeType(), NumberType()) mergeDown TypeSet(NumberType()))
    assertEquals(TypeSet(NodeType(), NumberType(), MapType(), AnyType()), TypeSet(NodeType(), NumberType()) mergeDown TypeSet(NodeType(), NumberType(), RelationshipType()))
    assertEquals(TypeSet(AnyType()), TypeSet(NodeType(), NumberType()) mergeDown TypeSet(AnyType()))
    assertEquals(TypeSet(AnyType()), TypeSet(AnyType()) mergeDown TypeSet(NodeType(), NumberType()))

    assertEquals(TypeSet(MapType()), TypeSet(RelationshipType()) mergeDown TypeSet(NodeType()))
    assertEquals(TypeSet(MapType(), NumberType(), AnyType()), TypeSet(RelationshipType(), LongType()) mergeDown TypeSet(NodeType(), NumberType()))
  }

  @Test
  def shouldMergeDownCollectionIterable() {
    assertEquals(TypeSet(NumberType(), CollectionType(AnyType()), AnyType()),
      TypeSet(IntegerType(), CollectionType(StringType())) mergeDown TypeSet(NumberType(), CollectionType(IntegerType())))
  }

  @Test
  def shouldMergeUpCollectionIterable() {
    assertEquals(TypeSet(IntegerType()),
      TypeSet(IntegerType(), StringType(), CollectionType(IntegerType())) mergeUp TypeSet(NumberType(), CollectionType(StringType())) )
    assertEquals(TypeSet(IntegerType(), CollectionType(StringType())),
      TypeSet(IntegerType(), StringType(), CollectionType(AnyType())) mergeUp TypeSet(NumberType(), CollectionType(StringType())) )
  }

  @Test
  def shouldInferTypeSetsUsingMergeUp() {
    assertEquals(TypeSet(NodeType(), NumberType()), TypeSet(NodeType(), NumberType()) mergeUp TypeSet(NodeType(), NumberType()))
    assertEquals(TypeSet(NumberType()), TypeSet(NodeType(), NumberType()) mergeUp TypeSet(NumberType()))
    assertEquals(TypeSet(NodeType(), NumberType()), TypeSet(NodeType(), NumberType()) mergeUp TypeSet(NodeType(), NumberType(), RelationshipType()))
    assertEquals(TypeSet(NodeType(), NumberType()), TypeSet(NodeType(), NumberType()) mergeUp TypeSet(AnyType()))
    assertEquals(TypeSet(NodeType(), NumberType()), TypeSet(AnyType()) mergeUp TypeSet(NodeType(), NumberType()))

    assertEquals(TypeSet(), TypeSet(RelationshipType()) mergeUp TypeSet(NodeType()))
    assertEquals(TypeSet(LongType()), TypeSet(RelationshipType(), LongType()) mergeUp TypeSet(NodeType(), NumberType()))
    assertEquals(TypeSet(NodeType(), NumberType()), TypeSet(AnyType()) mergeUp TypeSet(NodeType(), NumberType()))
  }

  @Test
  def shouldConstrainTypeSets() {
    assertEquals(TypeSet(IntegerType(), LongType()), TypeSet(IntegerType(), LongType(), StringType(), MapType()) constrain TypeSet(NodeType(), NumberType()))
    assertEquals(TypeSet(CollectionType(StringType())), TypeSet(IntegerType(), CollectionType(StringType())) constrain TypeSet(CollectionType(AnyType())))
    assertEquals(TypeSet.empty, TypeSet(IntegerType(), CollectionType(MapType())) constrain TypeSet(CollectionType(NodeType())))
    assertEquals(TypeSet(IntegerType(), CollectionType(StringType())), TypeSet(IntegerType(), CollectionType(StringType())) constrain TypeSet(AnyType()))
  }

  @Test
  def shouldFormatNoType() {
    assertEquals("()", TypeSet().mkString("(", ", ", " or ", ")"))
  }

  @Test
  def shouldFormatSingleType() {
    assertEquals("(Any)", TypeSet(AnyType()).mkString("(", ", ", " or ", ")"))
    assertEquals("<Node>", TypeSet(NodeType()).mkString("<", ", ", " and ", ">"))
  }

  @Test
  def shouldFormatTwoTypes() {
    assertEquals("Any or Node", TypeSet(AnyType(), NodeType()).mkString("", ", ", " or ", ""))
    assertEquals("-Node or Relationship-", TypeSet(RelationshipType(), NodeType()).mkString("-", ", ", " or ", "-"))
  }

  @Test
  def shouldFormatThreeTypes() {
    assertEquals("Integer, Node, Relationship", TypeSet(RelationshipType(), IntegerType(), NodeType()).mkString(", "))
    assertEquals("(Integer, Node, Relationship)", TypeSet(RelationshipType(), IntegerType(), NodeType()).mkString("(", ", ", ")"))
    assertEquals("(Any, Node or Relationship)", TypeSet(RelationshipType(), AnyType(), NodeType()).mkString("(", ", ", " or ", ")"))
    assertEquals("[Integer, Node and Relationship]", TypeSet(RelationshipType(), IntegerType(), NodeType()).mkString("[", ", ", " and ", "]"))
  }
}
