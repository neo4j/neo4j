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
import scala.collection.immutable.SortedSet

class FormattableCypherTypeSetTest extends Assertions {

  implicit def orderingOfCypherType[T <: CypherType] : Ordering[T] = Ordering.by(_.toString)

  @Test
  def shouldFormatNoType() {
    assertEquals("", Set[CypherType]().formattedString)
  }

  @Test
  def shouldFormatSingleType() {
    assertEquals("Scalar", SortedSet(ScalarType()).formattedString)
    assertEquals("Node", SortedSet(NodeType()).formattedString)
  }

  @Test
  def shouldFormatTwoTypes() {
    assertEquals("Node or Scalar", SortedSet(ScalarType(), NodeType()).formattedString)
    assertEquals("Node or Relationship", SortedSet(RelationshipType(), NodeType()).formattedString)
  }

  @Test
  def shouldFormatThreeTypes() {
	  assertEquals("Node, Relationship or Scalar", SortedSet(RelationshipType(), ScalarType(), NodeType()).formattedString)
	  assertEquals("Integer, Node or Relationship", SortedSet(RelationshipType(), IntegerType(), NodeType()).formattedString)
  }

}
