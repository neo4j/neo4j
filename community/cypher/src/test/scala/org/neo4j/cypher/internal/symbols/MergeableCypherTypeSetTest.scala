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
package org.neo4j.cypher.internal.symbols

import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions

class MergeableCypherTypeSetTest extends Assertions {

  @Test
  def shouldInferTypeSetsUsingCovariantMergeDown() {
    assertEquals(Set(NodeType(), NumberType()), Set(NodeType(), NumberType()) mergeDown Set(NodeType(), NumberType()))

    assertEquals(Set(NodeType(), NumberType()), Set(NodeType(), NumberType()) mergeDown Set(NodeType(), NumberType()))
    assertEquals(Set(NumberType()), Set(NodeType(), NumberType()) mergeDown Set(NumberType()))
    assertEquals(Set(NodeType(), NumberType()), Set(NodeType(), NumberType()) mergeDown Set(NodeType(), NumberType(), RelationshipType()))
    assertEquals(Set(ScalarType()), Set(NodeType(), NumberType()) mergeDown Set(ScalarType()))
    assertEquals(Set(ScalarType()), Set(ScalarType()) mergeDown Set(NodeType(), NumberType()))

    assertEquals(Set(MapType()), Set(RelationshipType()) mergeDown Set(NodeType()))
    assertEquals(Set(MapType(), NumberType()), Set(RelationshipType(), LongType()) mergeDown Set(NodeType(), NumberType()))
  }

  @Test
  def shouldMergeDownCollectionIterable() {
    assertEquals(Set(NumberType(), CollectionType(ScalarType())),
        Set(IntegerType(), CollectionType(StringType())) mergeDown Set(NumberType(), CollectionType(IntegerType())))
  }

  @Test
  def shouldMergeUpCollectionIterable() {
	assertEquals(Set(IntegerType()),
        Set(IntegerType(), StringType(), CollectionType(IntegerType())) mergeUp Set(NumberType(), CollectionType(StringType())) )
    assertEquals(Set(IntegerType(), CollectionType(StringType())),
        Set(IntegerType(), StringType(), CollectionType(ScalarType())) mergeUp Set(NumberType(), CollectionType(StringType())) )
  }

  @Test
  def shouldInferTypeSetsUsingContravariantMergeUp() {
    assertEquals(Set(NodeType(), NumberType()), Set(NodeType(), NumberType()) mergeUp Set(NodeType(), NumberType()))
    assertEquals(Set(NumberType()), Set(NodeType(), NumberType()) mergeUp Set(NumberType()))
    assertEquals(Set(NodeType(), NumberType()), Set(NodeType(), NumberType()) mergeUp Set(NodeType(), NumberType(), RelationshipType()))
    assertEquals(Set(NodeType(), NumberType()), Set(NodeType(), NumberType()) mergeUp Set(ScalarType()))
    assertEquals(Set(NodeType(), NumberType()), Set(ScalarType()) mergeUp Set(NodeType(), NumberType()))

    assertEquals(Set(), Set(RelationshipType()) mergeUp Set(NodeType()))
    assertEquals(Set(LongType()), Set(RelationshipType(), LongType()) mergeUp Set(NodeType(), NumberType()))
    assertEquals(Set(NodeType(), NumberType()), Set(AnyType()) mergeUp Set(NodeType(), NumberType()))
  }
}
