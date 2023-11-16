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
package org.neo4j.values.storable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.values.virtual.VirtualValues.fromArray;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.exceptions.CypherTypeException;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.VirtualValues;

@ExtendWith(RandomExtension.class)
class ListValueFuzzTest {
    @Inject
    private RandomSupport random;

    private static final int ITERATIONS = 1000;

    @Test
    void shouldBeStorableIfAppendToStorableWithCompatibleTypes() {
        for (int i = 0; i < ITERATIONS; i++) {
            for (ValueType valueType : ValueType.arrayTypes()) {
                // Given
                ArrayValue arrayValue = (ArrayValue) random.nextValue(valueType);
                ListValue inner = fromArray(arrayValue);

                // When
                ListValue appended = inner.append(nextCompatible(arrayValue));

                // Then
                assertEquals(appended, fromArray(appended.toStorableArray()));
            }
        }
    }

    @Test
    void shouldNotBeStorableIfAppendToStorableWithIncompatibleTypes() {
        for (int i = 0; i < ITERATIONS; i++) {
            for (ValueType valueType : ValueType.arrayTypes()) {
                // Given
                ArrayValue arrayValue = (ArrayValue) random.nextValue(valueType);
                ListValue inner = fromArray(arrayValue);

                // When
                ListValue appended = inner.append(nextIncompatible(arrayValue));

                // Then
                assertThrows(CypherTypeException.class, appended::toStorableArray);
            }
        }
    }

    @Test
    void shouldBeStorableIfPrependToStorableWithCompatibleTypes() {
        for (int i = 0; i < ITERATIONS; i++) {
            for (ValueType valueType : ValueType.arrayTypes()) {
                // Given
                ArrayValue arrayValue = (ArrayValue) random.nextValue(valueType);
                ListValue inner = fromArray(arrayValue);

                // When
                ListValue prepended = inner.prepend(nextCompatible(arrayValue));

                // Then
                assertEquals(prepended, fromArray(prepended.toStorableArray()));
            }
        }
    }

    @Test
    void shouldNotBeStorableIfPrependToStorableWithIncompatibleTypes() {
        for (int i = 0; i < ITERATIONS; i++) {
            for (ValueType valueType : ValueType.arrayTypes()) {
                // Given
                ArrayValue arrayValue = (ArrayValue) random.nextValue(valueType);
                ListValue inner = fromArray(arrayValue);

                // When
                ListValue prepended = inner.prepend(nextIncompatible(arrayValue));

                // Then
                assertThrows(CypherTypeException.class, prepended::toStorableArray);
            }
        }
    }

    @Test
    void shouldCreateStorableLists() {
        for (int i = 0; i < ITERATIONS; i++) {
            boolean seenStorable = false;
            boolean seenNonStorable = false;
            for (ValueType valueType : ValueType.values()) {
                AnyValue value = random.nextValue(valueType);
                if (value.valueRepresentation().canCreateArrayOfValueGroup()) {
                    ListValue list = VirtualValues.list(value, value, value);
                    assertEquals(list, fromArray(list.toStorableArray()));
                    seenStorable = true;
                } else {
                    ListValue list = VirtualValues.list(value, value, value);
                    assertThrows(CypherTypeException.class, list::toStorableArray);
                    seenNonStorable = true;
                }
            }

            assertTrue(seenStorable);
            assertTrue(seenNonStorable);
        }
    }

    private Value nextCompatible(ArrayValue value) {
        ValueType[] types = ValueType.values();
        while (true) {
            Value nextValue = random.nextValue(types[random.nextInt(types.length)]);
            if (value.hasCompatibleType(nextValue)) {
                return nextValue;
            }
        }
    }

    private Value nextIncompatible(ArrayValue value) {
        ValueType[] types = ValueType.values();
        while (true) {
            Value nextValue = random.nextValue(types[random.nextInt(types.length)]);
            if (value.isEmpty()) {
                return nextValue;
            }
            if (!value.value(0)
                    .valueRepresentation()
                    .coerce(nextValue.valueRepresentation())
                    .canCreateArrayOfValueGroup()) {
                return nextValue;
            }
        }
    }
}
