/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.codegen

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.codegen.CompiledCursorUtils.{nodeGetProperty, nodeHasLabel, relationshipGetProperty}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException
import org.neo4j.internal.kernel.api.{NodeCursor, PropertyCursor, Read, RelationshipScanCursor}
import org.neo4j.kernel.impl.newapi.Labels
import org.neo4j.values.storable.Values.{NO_VALUE, stringValue}

class CompiledCursorUtilsTest extends CypherFunSuite {

  test("should find a property from a node cursor") {
    val nodeCursor = mock[NodeCursor]
    val propertyCursor = mock[PropertyCursor]
    when(nodeCursor.next()).thenReturn(true)
    when(propertyCursor.next()).thenReturn(true, true, true, false)
    when(propertyCursor.propertyKey()).thenReturn(1336, 1337, 1338)
    when(propertyCursor.propertyValue()).thenReturn(stringValue("hello"))

    // When
    val value = nodeGetProperty(mock[Read], nodeCursor, 42L, propertyCursor, 1337)

    // Then
    value should equal(stringValue("hello"))
  }

  test("should return NO_VALUE if the node doesn't have property") {
    val nodeCursor = mock[NodeCursor]
    val propertyCursor = mock[PropertyCursor]
    when(nodeCursor.next()).thenReturn(true)
    when(propertyCursor.next()).thenReturn(true, true, true, false)
    when(propertyCursor.propertyKey()).thenReturn(1336, 1337, 1338)
    when(propertyCursor.propertyValue()).thenReturn(stringValue("hello"))

    // When
    val value = nodeGetProperty(mock[Read], nodeCursor, 42L, propertyCursor, 1339)

    // Then
    value should equal(NO_VALUE)
  }

  test("should throw if node is missing when querying for property") {
    // Given
    val read = mock[Read]
    val nodeCursor = mock[NodeCursor]
    when(nodeCursor.next()).thenReturn(false)

    // Expect
    an [EntityNotFoundException] shouldBe thrownBy(nodeGetProperty(read, nodeCursor, 42L, mock[PropertyCursor], 1337))
  }

  test("should find a property from a relationship cursor") {
    val relationshipCursor = mock[RelationshipScanCursor]
    val propertyCursor = mock[PropertyCursor]
    when(relationshipCursor.next()).thenReturn(true)
    when(propertyCursor.next()).thenReturn(true, true, true, false)
    when(propertyCursor.propertyKey()).thenReturn(1336, 1337, 1338)
    when(propertyCursor.propertyValue()).thenReturn(stringValue("hello"))

    // When
    val value = relationshipGetProperty(mock[Read], relationshipCursor, 42L, propertyCursor, 1337)

    // Then
    value should equal(stringValue("hello"))
  }

  test("should return NO_VALUE if the relationship doesn't have property") {
    val relationshipCursor = mock[RelationshipScanCursor]
    val propertyCursor = mock[PropertyCursor]
    when(relationshipCursor.next()).thenReturn(true)
    when(propertyCursor.next()).thenReturn(true, true, true, false)
    when(propertyCursor.propertyKey()).thenReturn(1336, 1337, 1338)
    when(propertyCursor.propertyValue()).thenReturn(stringValue("hello"))

    // When
    val value = relationshipGetProperty(mock[Read], relationshipCursor, 42L, propertyCursor, 1339)

    // Then
    value should equal(NO_VALUE)
  }

  test("should throw if relationship is missing when querying for property") {
    // Given
    val read = mock[Read]
    val relationshipCursor = mock[RelationshipScanCursor]
    when(relationshipCursor.next()).thenReturn(false)

    // Expect
    an [EntityNotFoundException] shouldBe thrownBy(relationshipGetProperty(read, relationshipCursor, 42L, mock[PropertyCursor], 1337))
  }

  test("should find if a node has a label") {
    // Given
    val nodeCursor = mock[NodeCursor]
    when(nodeCursor.next()).thenReturn(true)
    when(nodeCursor.labels()).thenReturn(Labels.from(Array(1337L, 42L, 13L)))

    // Then
    nodeHasLabel(mock[Read], nodeCursor, 1L, 1337) shouldBe true
    nodeHasLabel(mock[Read], nodeCursor, 1L, 1980) shouldBe false
  }

  test("should throw when node is missing when checking label") {
    // Given
    val nodeCursor = mock[NodeCursor]
    when(nodeCursor.next()).thenReturn(false)

    // Expect
    an [EntityNotFoundException] shouldBe thrownBy(nodeHasLabel(mock[Read], nodeCursor, 1L, 1337))
  }
}
