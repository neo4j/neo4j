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
package org.neo4j.cypher.internal.collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.ValueType;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.ListValueAsJava;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

class ListValueAsJavaTest {
    private final DefaultValueMapper javaMapper = new DefaultValueMapper(null);

    @ParameterizedTest
    @MethodSource("arrayValues")
    void shouldBeEquvalentToJavaMapper(ArrayValue arrayToTest) {
        final var optimisedJavaList = asOptimisedList(arrayToTest);
        assertThat(optimisedJavaList).isEqualTo(asBasicListValue(arrayToTest).map(javaMapper));
        assertThat(optimisedJavaList).isEqualTo(asManuallyMappedJavaList(arrayToTest));
        assertImmutable(optimisedJavaList);
    }

    @Test
    void shouldBeEquvalentToJavaMapperRandomValues() {
        final var rand = RandomValues.create();
        for (final var arrayType : ValueType.arrayTypes()) {
            for (int i = 0; i < 10; ++i) {
                final var randomArray = (ArrayValue) rand.nextValueOfType(arrayType);

                final var optimisedJavaList = asOptimisedList(randomArray);
                if (optimisedJavaList != null) {
                    assertThat(optimisedJavaList)
                            .isEqualTo(asBasicListValue(randomArray).map(javaMapper));
                    assertThat(optimisedJavaList).isEqualTo(asManuallyMappedJavaList(randomArray));
                    assertImmutable(optimisedJavaList);
                }
            }
        }
    }

    private void assertImmutable(List<?> list) {
        assertThatThrownBy(() -> list.remove(0)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> list.add(null)).isInstanceOf(UnsupportedOperationException.class);
        final var iterator = list.iterator();
        if (iterator.hasNext()) {
            iterator.next();
            assertThatThrownBy(iterator::remove).isInstanceOf(UnsupportedOperationException.class);
        }
    }

    private ListValue asBasicListValue(ArrayValue arrayValue) {
        final var builder = ListValueBuilder.newListBuilder(arrayValue.length());
        arrayValue.forEach(builder::add);
        return builder.build();
    }

    private List<?> asManuallyMappedJavaList(ArrayValue arrayValue) {
        final var list = new ArrayList<>();
        arrayValue.forEach(value -> list.add(value.map(javaMapper)));
        return list;
    }

    private List<?> asOptimisedList(ArrayValue arrayValue) {
        return ListValueAsJava.asObject(VirtualValues.fromArray(arrayValue));
    }

    static Stream<ArrayValue> arrayValues() {
        return Stream.of(
                Values.doubleArray(new double[] {Double.MIN_VALUE, Double.MAX_VALUE, 0.0, 1.33333333333333333333333}),
                Values.intArray(new int[] {Integer.MAX_VALUE, Integer.MIN_VALUE, 0, -1}),
                Values.floatArray(new float[] {Float.MIN_VALUE, Float.MAX_VALUE, 0, -1.0f}),
                Values.byteArray(new byte[] {Byte.MIN_VALUE, Byte.MAX_VALUE, 0, -1}),
                Values.booleanArray(new boolean[] {true, false}),
                Values.shortArray(new short[] {Short.MIN_VALUE, Short.MAX_VALUE, -1, 0}),
                Values.longArray(new long[] {Long.MAX_VALUE, Long.MIN_VALUE, -1}),
                Values.stringArray("", "🎂"));
    }
}
