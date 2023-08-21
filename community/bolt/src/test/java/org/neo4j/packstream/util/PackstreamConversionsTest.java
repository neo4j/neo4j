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
package org.neo4j.packstream.util;

import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

class PackstreamConversionsTest {

    @Test
    void asNullableListValueShouldAcceptNoValue() throws IllegalStructArgumentException {
        var result = PackstreamConversions.asNullableListValue("someField", Values.NO_VALUE);

        Assertions.assertThat(result).isNull();
    }

    @Test
    void asNullableListValueShouldAcceptListValue() throws IllegalStructArgumentException {
        var builder = ListValueBuilder.newListBuilder();
        builder.add(Values.stringValue("foo"));
        builder.add(Values.stringValue("bar"));
        builder.add(Values.booleanValue(false));
        var listValue = builder.build();

        var result = PackstreamConversions.asNullableListValue("someField", listValue);

        Assertions.assertThat(result).isNotNull().isSameAs(listValue);
    }

    @TestFactory
    Stream<DynamicTest> asNullableListValueShouldRejectOtherTypes() {
        return Stream.of(
                        Values.booleanValue(true),
                        Values.stringValue("foo"),
                        Values.longValue(42),
                        VirtualValues.map(new String[] {"foo"}, new AnyValue[] {Values.stringValue("bar")}))
                .map(value -> DynamicTest.dynamicTest(value.toString(), () -> Assertions.assertThatExceptionOfType(
                                IllegalStructArgumentException.class)
                        .isThrownBy(() -> PackstreamConversions.asNullableListValue("someField", value))
                        .withMessage("Illegal value for field \"someField\": Expected list")
                        .withNoCause()));
    }

    @Test
    void asLongShouldRejectNullValues() {
        Assertions.assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> PackstreamConversions.asLong("someField", null))
                .withMessage("Illegal value for field \"someField\": Expected value to be non-null")
                .withNoCause();
    }

    @TestFactory
    Stream<DynamicTest> asLongShouldAcceptBoxedLongValues() {
        return LongStream.rangeClosed(0, 16)
                .map(value -> value * 128 + value % 2)
                .mapToObj(value -> value)
                .map(value -> DynamicTest.dynamicTest(value.toString(), () -> {
                    var result = PackstreamConversions.asLong("someField", value);

                    Assertions.assertThat(result).isEqualTo(value);
                }));
    }

    @TestFactory
    Stream<DynamicTest> asLongShouldRejectArbitraryValues() {
        return Stream.of(14, false, "foo", Map.of("foo", "bar"), List.of("foo", "bar", 42))
                .map(value -> DynamicTest.dynamicTest(value.toString(), () -> Assertions.assertThatExceptionOfType(
                                IllegalStructArgumentException.class)
                        .isThrownBy(() -> PackstreamConversions.asLong("someField", value))
                        .withMessage("Illegal value for field \"someField\": Expected long")
                        .withNoCause()));
    }

    @Test
    void asNullableLongShouldAcceptNullValues() throws IllegalStructArgumentException {
        var result = PackstreamConversions.asNullableLong("someField", null);

        Assertions.assertThat(result).isNull();
    }

    @TestFactory
    Stream<DynamicTest> asNullableLongShouldAcceptBoxedLongValues() {
        return LongStream.rangeClosed(0, 16)
                .map(value -> value * 128 + value % 2)
                .mapToObj(value -> value)
                .map(value -> DynamicTest.dynamicTest(value.toString(), () -> {
                    var result = PackstreamConversions.asNullableLong("someField", value);

                    Assertions.assertThat(result).isEqualTo(value);
                }));
    }

    @TestFactory
    Stream<DynamicTest> asNullableLongShouldRejectArbitraryValues() {
        return Stream.of(14, false, "foo", Map.of("foo", "bar"), List.of("foo", "bar", 42))
                .map(value -> DynamicTest.dynamicTest(value.toString(), () -> Assertions.assertThatExceptionOfType(
                                IllegalStructArgumentException.class)
                        .isThrownBy(() -> PackstreamConversions.asNullableLong("someField", value))
                        .withMessage("Illegal value for field \"someField\": Expected long")
                        .withNoCause()));
    }

    @Test
    void asLongValueShouldRejectNoneValues() {
        Assertions.assertThatExceptionOfType(IllegalStructArgumentException.class)
                .isThrownBy(() -> PackstreamConversions.asLongValue("someField", Values.NO_VALUE))
                .withMessage("Illegal value for field \"someField\": Expected value to be non-null")
                .withNoCause();
    }

    @TestFactory
    Stream<DynamicTest> asLongValueShouldAcceptLongValues() {
        return LongStream.rangeClosed(0, 16)
                .map(value -> value * 128 + value % 2)
                .mapToObj(Values::longValue)
                .map(value -> DynamicTest.dynamicTest(value.toString(), () -> {
                    var result = PackstreamConversions.asLongValue("someField", value);

                    Assertions.assertThat(result).isEqualTo(value.longValue());
                }));
    }

    @TestFactory
    Stream<DynamicTest> asLongValueShouldRejectArbitraryValues() {
        return Stream.of(
                        Values.intValue(42),
                        Values.booleanValue(false),
                        Values.stringValue("foo"),
                        VirtualValues.map(new String[] {"foo"}, new AnyValue[] {Values.stringValue("bar")}),
                        VirtualValues.list(Values.stringValue("foo"), Values.stringValue("bar"), Values.longValue(42)))
                .map(value -> DynamicTest.dynamicTest(value.toString(), () -> Assertions.assertThatExceptionOfType(
                                IllegalStructArgumentException.class)
                        .isThrownBy(() -> PackstreamConversions.asLong("someField", value))
                        .withMessage("Illegal value for field \"someField\": Expected long")
                        .withNoCause()));
    }

    @Test
    void asNullableLongValueShouldAcceptNoneValues() throws IllegalStructArgumentException {
        var result = PackstreamConversions.asNullableLongValue("someField", Values.NO_VALUE);

        Assertions.assertThat(result).isEmpty();
    }

    @TestFactory
    Stream<DynamicTest> asNullableLongValueShouldAcceptLongValues() {
        return LongStream.rangeClosed(0, 16)
                .map(value -> value * 128 + value % 2)
                .mapToObj(Values::longValue)
                .map(value -> DynamicTest.dynamicTest(value.toString(), () -> {
                    var result = PackstreamConversions.asNullableLongValue("someField", value);

                    Assertions.assertThat(result).isNotEmpty().hasValue(value.longValue());
                }));
    }

    @TestFactory
    Stream<DynamicTest> asNullableLongValueShouldRejectArbitraryValues() {
        return Stream.of(
                        Values.intValue(42),
                        Values.booleanValue(false),
                        Values.stringValue("foo"),
                        VirtualValues.map(new String[] {"foo"}, new AnyValue[] {Values.stringValue("bar")}),
                        VirtualValues.list(Values.stringValue("foo"), Values.stringValue("bar"), Values.longValue(42)))
                .map(value -> DynamicTest.dynamicTest(value.toString(), () -> Assertions.assertThatExceptionOfType(
                                IllegalStructArgumentException.class)
                        .isThrownBy(() -> PackstreamConversions.asNullableLongValue("someField", value))
                        .withMessage("Illegal value for field \"someField\": Expected long")
                        .withNoCause()));
    }

    @Test
    void asNullableMapValueShouldAcceptNoValue() throws IllegalStructArgumentException {
        var result = PackstreamConversions.asNullableMapValue("someField", Values.NO_VALUE);

        Assertions.assertThat(result).isNull();
    }

    @Test
    void asNullableMapValueShouldAcceptMapValue() throws IllegalStructArgumentException {
        var builder = new MapValueBuilder();
        builder.add("foo", Values.stringValue("bar"));
        builder.add("the_answer", Values.longValue(42));
        var mapValue = builder.build();

        var result = PackstreamConversions.asNullableMapValue("someField", mapValue);

        Assertions.assertThat(result).isNotNull().isSameAs(mapValue);
    }

    @TestFactory
    Stream<DynamicTest> asNullableMapValueShouldRejectOtherTypes() {
        return Stream.of(
                        Values.booleanValue(true),
                        Values.stringValue("foo"),
                        Values.longValue(42),
                        VirtualValues.list(Values.stringValue("foo"), Values.stringValue("bar"), Values.longValue(42)))
                .map(value -> DynamicTest.dynamicTest(value.toString(), () -> Assertions.assertThatExceptionOfType(
                                IllegalStructArgumentException.class)
                        .isThrownBy(() -> PackstreamConversions.asNullableMapValue("someField", value))
                        .withMessage("Illegal value for field \"someField\": Expected dictionary")
                        .withNoCause()));
    }

    @Test
    void asNullableStringShouldAcceptNullValues() throws IllegalStructArgumentException {
        var result = PackstreamConversions.asNullableString("someField", null);

        Assertions.assertThat(result).isNull();
    }

    @TestFactory
    Stream<DynamicTest> asNullableStringShouldAcceptStringValues() {
        return Stream.of("foo", "bar", "baz")
                .map(value -> DynamicTest.dynamicTest(value, () -> {
                    var result = PackstreamConversions.asNullableString("someField", value);

                    Assertions.assertThat(result).isEqualTo(value);
                }));
    }

    @TestFactory
    Stream<DynamicTest> asNullableStringShouldRejectArbitraryValues() {
        return Stream.of(14, false, 42L, Map.of("foo", "bar"), List.of("foo", "bar", 42))
                .map(value -> DynamicTest.dynamicTest(value.toString(), () -> Assertions.assertThatExceptionOfType(
                                IllegalStructArgumentException.class)
                        .isThrownBy(() -> PackstreamConversions.asNullableString("someField", value))
                        .withMessage("Illegal value for field \"someField\": Expected string")
                        .withNoCause()));
    }

    @Test
    void asNullableStringValueShouldAcceptNoneValues() throws IllegalStructArgumentException {
        var result = PackstreamConversions.asNullableStringValue("someField", Values.NO_VALUE);

        Assertions.assertThat(result).isNull();
    }

    @TestFactory
    Stream<DynamicTest> asNullableStringValueShouldAcceptStringValues() {
        return Stream.of("foo", "bar", "baz")
                .map(Values::stringValue)
                .map(value -> DynamicTest.dynamicTest(value.toString(), () -> {
                    var result = PackstreamConversions.asNullableStringValue("someField", value);

                    Assertions.assertThat(result).isNotNull().isEqualTo(value.stringValue());
                }));
    }

    @TestFactory
    Stream<DynamicTest> asNullableStringValueShouldRejectArbitraryValues() {
        return Stream.of(
                        Values.intValue(42),
                        Values.booleanValue(false),
                        Values.longValue(42),
                        VirtualValues.map(new String[] {"foo"}, new AnyValue[] {Values.stringValue("bar")}),
                        VirtualValues.list(Values.stringValue("foo"), Values.stringValue("bar"), Values.longValue(42)))
                .map(value -> DynamicTest.dynamicTest(value.toString(), () -> Assertions.assertThatExceptionOfType(
                                IllegalStructArgumentException.class)
                        .isThrownBy(() -> PackstreamConversions.asNullableStringValue("someField", value))
                        .withMessage("Illegal value for field \"someField\": Expected string")
                        .withNoCause()));
    }
}
