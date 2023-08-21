/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.operations

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.operations.CursorUtils.nodeGetProperty
import org.neo4j.cypher.operations.CursorUtils.nodeHasLabel
import org.neo4j.cypher.operations.CursorUtils.relationshipGetProperty
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.Read
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.stringValue

class CursorUtilsTest extends CypherFunSuite {

  test("should find a property from a node cursor") {
    val nodeCursor = mock[NodeCursor]
    val propertyCursor = mock[PropertyCursor]
    when(nodeCursor.next()).thenReturn(true)
    when(propertyCursor.next()).thenReturn(true)
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
    when(propertyCursor.next()).thenReturn(false)

    // When
    val value = nodeGetProperty(mock[Read], nodeCursor, 42L, propertyCursor, 1339)

    // Then
    value should equal(NO_VALUE)
  }

  test("should return NO_VALUE if node is missing when querying for property") {
    // Given
    val read = mock[Read]
    val nodeCursor = mock[NodeCursor]
    when(nodeCursor.next()).thenReturn(false)

    // Expect
    nodeGetProperty(read, nodeCursor, 42L, mock[PropertyCursor], 1337) shouldBe NO_VALUE
  }

  test("should find a property from a loaded node cursor") {
    val nodeCursor = mock[NodeCursor]
    val propertyCursor = mock[PropertyCursor]
    when(propertyCursor.next()).thenReturn(true)
    when(propertyCursor.propertyValue()).thenReturn(stringValue("hello"))

    // When
    val value = nodeGetProperty(nodeCursor, propertyCursor, 1337)

    // Then
    value should equal(stringValue("hello"))
  }

  test("should return NO_VALUE if the loaded node doesn't have property") {
    val nodeCursor = mock[NodeCursor]
    val propertyCursor = mock[PropertyCursor]
    when(propertyCursor.next()).thenReturn(false)

    // When
    val value = nodeGetProperty(nodeCursor, propertyCursor, 1339)

    // Then
    value should equal(NO_VALUE)
  }

  test("should find a property from a relationship cursor") {
    val relationshipCursor = mock[RelationshipScanCursor]
    val propertyCursor = mock[PropertyCursor]
    when(relationshipCursor.next()).thenReturn(true)
    when(propertyCursor.next()).thenReturn(true)
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
    when(propertyCursor.next()).thenReturn(false)

    // When
    val value = relationshipGetProperty(mock[Read], relationshipCursor, 42L, propertyCursor, 1339)

    // Then
    value should equal(NO_VALUE)
  }

  test("should return NO_VALUE if relationship is missing when querying for property") {
    // Given
    val read = mock[Read]
    val relationshipCursor = mock[RelationshipScanCursor]
    when(relationshipCursor.next()).thenReturn(false)

    // Expect
    relationshipGetProperty(read, relationshipCursor, 42L, mock[PropertyCursor], 1337) shouldBe NO_VALUE
  }

  test("should find a property from a loaded relationship cursor") {
    val relationshipCursor = mock[RelationshipScanCursor]
    val propertyCursor = mock[PropertyCursor]
    when(propertyCursor.next()).thenReturn(true)
    when(propertyCursor.propertyValue()).thenReturn(stringValue("hello"))

    // When
    val value = relationshipGetProperty(relationshipCursor, propertyCursor, 1337)

    // Then
    value should equal(stringValue("hello"))
  }

  test("should return NO_VALUE if the loaded relationship doesn't have property") {
    val relationshipCursor = mock[RelationshipScanCursor]
    val propertyCursor = mock[PropertyCursor]
    when(relationshipCursor.next()).thenReturn(true)
    when(propertyCursor.next()).thenReturn(false)

    // When
    val value = relationshipGetProperty(relationshipCursor, propertyCursor, 1339)

    // Then
    value should equal(NO_VALUE)
  }

  test("should find if a node has a label") {
    // Given
    val nodeCursor = mock[NodeCursor]
    when(nodeCursor.next()).thenReturn(true)
    when(nodeCursor.hasLabel(1337)).thenReturn(true)
    when(nodeCursor.hasLabel(1980)).thenReturn(false)

    // Then
    nodeHasLabel(mock[Read], nodeCursor, 1L, 1337) shouldBe true
    nodeHasLabel(mock[Read], nodeCursor, 1L, 1980) shouldBe false
  }

  test("should return false when node is missing when checking label") {
    // Given
    val nodeCursor = mock[NodeCursor]
    when(nodeCursor.next()).thenReturn(false)

    // Expect
    nodeHasLabel(mock[Read], nodeCursor, 1L, 1337) shouldBe false
  }
}
