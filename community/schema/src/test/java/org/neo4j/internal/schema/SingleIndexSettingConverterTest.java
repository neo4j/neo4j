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
package org.neo4j.internal.schema;

import java.util.Locale;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.internal.schema.IndexSettingRecord.IncorrectType;
import org.neo4j.internal.schema.IndexSettingRecord.Pending;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithSetting;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithStorable;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithValue;
import org.neo4j.internal.schema.IndexSettingRecord.Valid;
import org.neo4j.internal.schema.IndexSettingTestUtils.TestIndexSetting;
import org.neo4j.internal.schema.SingleIndexSettingConverter.IntegerToOptionalIntConverter;
import org.neo4j.internal.schema.SingleIndexSettingConverter.StringToUpperCaseConverter;
import org.neo4j.internal.schema.SingleIndexSettingConverter.TypeToOptionalConverter;
import org.neo4j.internal.schema.SingleIndexSettingProcessorTest.SingleProcessorTestBase;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class SingleIndexSettingConverterTest {
    public abstract static class SingleConverterTestBase extends SingleProcessorTestBase {
        protected final SingleIndexSettingConverter<?> converter;

        protected SingleConverterTestBase(SingleIndexSettingConverter<?> converter) {
            super(converter);
            this.converter = converter;
        }
    }

    @Nested
    class IntegerToOptionalIntConverterTest extends SingleConverterTestBase {
        IntegerToOptionalIntConverterTest() {
            super(IntegerToOptionalIntConverter.of(TestIndexSetting.INTEGER));
        }

        @Test
        void incorrectType() {
            RecordWithSetting record = new Pending(setting, "42", Values.utf8Value("42"));

            processForVerificationAndAssertRecord(record, IncorrectType.class)
                    .extracting(IncorrectType::targetType)
                    .isEqualTo(Integer.class);
        }

        @ParameterizedTest
        @NullSource
        @ValueSource(
                ints = {
                    Integer.MIN_VALUE,
                    Short.MIN_VALUE,
                    Byte.MIN_VALUE,
                    0,
                    Byte.MAX_VALUE,
                    Short.MAX_VALUE,
                    Integer.MAX_VALUE
                })
        void pending(Integer value) {
            Value storable = Values.unsafeOf(value, true);
            RecordWithSetting record = new Pending(setting, value, storable);
            OptionalInt processedValue = value != null ? OptionalInt.of(value) : OptionalInt.empty();

            processForVerificationAndAssertRecord(record, Pending.class)
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(processedValue, storable);
        }

        @ParameterizedTest
        @NullSource
        @ValueSource(
                ints = {
                    Integer.MIN_VALUE,
                    Short.MIN_VALUE,
                    Byte.MIN_VALUE,
                    0,
                    Byte.MAX_VALUE,
                    Short.MAX_VALUE,
                    Integer.MAX_VALUE
                })
        void valid(Integer value) {
            Value storable = Values.unsafeOf(value, true);
            RecordWithSetting record = new Valid(setting, value, storable);
            OptionalInt processedValue = value != null ? OptionalInt.of(value) : OptionalInt.empty();

            processForVerificationAndAssertRecord(record, Valid.class)
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(processedValue, storable);

            processForAuthoritativeReadAndAssertRecord(record)
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(processedValue, storable);
        }
    }

    @Nested
    class TypeToOptionalConverterTest extends SingleConverterTestBase {
        TypeToOptionalConverterTest() {
            super(TypeToOptionalConverter.of(TestIndexSetting.STRING, String.class));
        }

        @ParameterizedTest
        @MethodSource
        void incorrectType(Object value) {
            RecordWithSetting record = new Pending(setting, value, Values.of(value));

            processForVerificationAndAssertRecord(record, IncorrectType.class)
                    .extracting(IncorrectType::targetType)
                    .isEqualTo(String.class);
        }

        static Stream<Object> incorrectType() {
            return Stream.of(false, 42, new float[] {3.0f, 4.0f, 5.0f});
        }

        @ParameterizedTest
        @NullSource
        @ValueSource(strings = {"foo", "bar", "baz"})
        void pending(String value) {
            Value storable = Values.unsafeOf(value, true);
            RecordWithSetting record = new Pending(setting, value, storable);
            Optional<?> processedValue = Optional.ofNullable(value);

            processForVerificationAndAssertRecord(record, Pending.class)
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(processedValue, storable);
        }

        @ParameterizedTest
        @NullSource
        @ValueSource(strings = {"foo", "bar", "baz"})
        void valid(String value) {
            Value storable = Values.unsafeOf(value, true);
            RecordWithSetting record = new Valid(setting, value, storable);
            Optional<?> processedValue = Optional.ofNullable(value);

            processForVerificationAndAssertRecord(record, Valid.class)
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(processedValue, storable);

            processForAuthoritativeReadAndAssertRecord(record)
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(processedValue, storable);
        }
    }

    @Nested
    class StringToUpperCaseConverterTest extends SingleConverterTestBase {
        StringToUpperCaseConverterTest() {
            super(StringToUpperCaseConverter.of(TestIndexSetting.STRING));
        }

        @ParameterizedTest
        @MethodSource
        void incorrectType(Object value) {
            RecordWithSetting record = new Pending(setting, value, Values.of(value));

            processForVerificationAndAssertRecord(record, IncorrectType.class)
                    .extracting(IncorrectType::targetType)
                    .isEqualTo(String.class);
        }

        static Stream<Object> incorrectType() {
            return Stream.of(false, 42, new float[] {3.0f, 4.0f, 5.0f});
        }

        @ParameterizedTest
        @ValueSource(strings = {"foo", "bar", "baz"})
        void pending(String value) {
            Value storable = Values.utf8Value(value);
            RecordWithSetting record = new Pending(setting, value, storable);
            String processedValue = value.toUpperCase(Locale.ROOT);

            processForVerificationAndAssertRecord(record, Pending.class)
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(processedValue, storable);
        }

        @ParameterizedTest
        @ValueSource(strings = {"foo", "bar", "baz"})
        void valid(String value) {
            Value storable = Values.utf8Value(value);
            RecordWithSetting record = new Valid(setting, value, storable);
            String processedValue = value.toUpperCase(Locale.ROOT);

            processForVerificationAndAssertRecord(record, Valid.class)
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(processedValue, storable);

            processForAuthoritativeReadAndAssertRecord(record)
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(processedValue, storable);
        }
    }
}
