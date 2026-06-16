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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.neo4j.values.virtual.VirtualValues.fromArray;

import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.CypherTypeException;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.VirtualValues;

@RandomSupportExtension
class ListValueFuzzTest {
    @Inject
    private RandomSupport random;

    private static final int ITERATIONS = 1000;

    @Test
    void shouldBeStorableIfAppendToStorableWithCompatibleTypes() {
        for (int i = 0; i < ITERATIONS; i++) {
            for (ValueType valueType : ValueType.ARRAY_TYPES) {
                // Given
                ArrayValue arrayValue = (ArrayValue) random.nextValue(valueType);
                ListValue inner = fromArray(arrayValue);

                // When
                ListValue appended = inner.append(nextCompatible(arrayValue));

                // Then
                assertThat(fromArray(appended.toStorableArray())).isEqualTo(appended);
            }
        }
    }

    @Test
    void shouldNotBeStorableIfAppendToStorableWithIncompatibleTypes() {
        for (int i = 0; i < ITERATIONS; i++) {
            for (ValueType valueType : ValueType.ARRAY_TYPES) {
                // Given
                ArrayValue arrayValue = (ArrayValue) random.nextValue(valueType);
                ListValue inner = fromArray(arrayValue);

                // When
                ListValue appended = inner.append(nextIncompatible(arrayValue));

                // Then
                assertThatExceptionOfType(CypherTypeException.class).isThrownBy(appended::toStorableArray);
            }
        }
    }

    @Test
    void shouldBeStorableIfPrependToStorableWithCompatibleTypes() {
        for (int i = 0; i < ITERATIONS; i++) {
            for (ValueType valueType : ValueType.ARRAY_TYPES) {
                // Given
                ArrayValue arrayValue = (ArrayValue) random.nextValue(valueType);
                ListValue inner = fromArray(arrayValue);

                // When
                ListValue prepended = inner.prepend(nextCompatible(arrayValue));

                // Then
                assertThat(fromArray(prepended.toStorableArray())).isEqualTo(prepended);
            }
        }
    }

    @Test
    void shouldNotBeStorableIfPrependToStorableWithIncompatibleTypes() {
        for (int i = 0; i < ITERATIONS; i++) {
            for (ValueType valueType : ValueType.ARRAY_TYPES) {
                // Given
                ArrayValue arrayValue = (ArrayValue) random.nextValue(valueType);
                ListValue inner = fromArray(arrayValue);

                // When
                ListValue prepended = inner.prepend(nextIncompatible(arrayValue));

                // Then
                assertThatExceptionOfType(CypherTypeException.class).isThrownBy(prepended::toStorableArray);
            }
        }
    }

    @Test
    void shouldCreateStorableLists() {
        for (int i = 0; i < ITERATIONS; i++) {
            boolean seenStorable = false;
            boolean seenNonStorable = false;
            for (ValueType valueType : ValueType.ALL_TYPES) {
                AnyValue value = random.nextValue(valueType);
                if (value.valueRepresentation().canCreateArrayOfValueGroup()) {
                    assertThat(valueType.valueGroup.category())
                            .as("remove when VectorArray is storable, IND-468")
                            .isNotEqualTo(ValueCategory.VECTOR);
                    ListValue list = VirtualValues.list(value, value, value);
                    assertThat(fromArray(list.toStorableArray())).isEqualTo(list);
                    seenStorable = true;
                } else {
                    assumeThat(valueType.valueGroup.category())
                            .as("remove when VectorArray is storable, IND-468")
                            .isNotEqualTo(ValueCategory.VECTOR);
                    ListValue list = VirtualValues.list(value, value, value);
                    assertThatExceptionOfType(CypherTypeException.class).isThrownBy(list::toStorableArray);
                    seenNonStorable = true;
                }
            }

            assertThat(seenStorable).isTrue();
            assertThat(seenNonStorable).isTrue();
        }
    }

    private Value nextCompatible(ArrayValue value) {
        ValueType[] types = ValueType.ALL_TYPES;
        while (true) {
            Value nextValue = random.nextValue(types[random.nextInt(types.length)]);
            if (value.hasCompatibleType(nextValue)) {
                return nextValue;
            }
        }
    }

    private Value nextIncompatible(ArrayValue value) {
        ValueType[] types = ValueType.ALL_TYPES;
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
