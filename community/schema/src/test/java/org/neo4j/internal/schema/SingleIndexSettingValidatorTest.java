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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.neo4j.internal.schema.IndexSettingTestUtils.FAKE_VALUE;

import java.util.OptionalInt;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.neo4j.internal.helpers.InclusiveRange;
import org.neo4j.internal.schema.IndexSettingRecord.IncorrectType;
import org.neo4j.internal.schema.IndexSettingRecord.InvalidValue;
import org.neo4j.internal.schema.IndexSettingRecord.Pending;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithSetting;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithStorable;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithValue;
import org.neo4j.internal.schema.IndexSettingRecord.Valid;
import org.neo4j.internal.schema.IndexSettingTestUtils.TestIndexSetting;
import org.neo4j.internal.schema.SingleIndexSettingProcessorTest.SingleProcessorTestBase;
import org.neo4j.internal.schema.SingleIndexSettingValidator.DoubleRangeValidator;
import org.neo4j.internal.schema.SingleIndexSettingValidator.IntegerRangeValidator;
import org.neo4j.internal.schema.SingleIndexSettingValidator.OptionalIntRangeValidator;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class SingleIndexSettingValidatorTest {
    public abstract static class SingleValidatorTestBase extends SingleProcessorTestBase {
        protected final SingleIndexSettingValidator<?> validator;

        protected SingleValidatorTestBase(SingleIndexSettingValidator<?> validator) {
            super(validator);
            this.validator = validator;
        }

        @Test
        void incorrectType() {
            RecordWithSetting record = new Pending(setting, FAKE_VALUE, Values.NO_VALUE);

            processForVerificationAndAssertRecord(record, IncorrectType.class)
                    .extracting(IncorrectType::targetType)
                    .isEqualTo(validator.type);
        }
    }

    @Nested
    class IntegerRangeValidatorTest extends SingleValidatorTestBase {
        private static final int MIN = -5;
        private static final int MAX = 15;

        protected IntegerRangeValidatorTest() {
            super(IntegerRangeValidator.of(TestIndexSetting.INTEGER, MIN, MAX));
        }

        @ParameterizedTest
        @NullSource
        @MethodSource
        void invalidValues(Integer value) {
            Value storable = Values.unsafeOf(value, true);
            RecordWithSetting record = new Pending(setting, value, storable);

            ObjectAssert<InvalidValue> invalidValueAssert =
                    processForVerificationAndAssertRecord(record, InvalidValue.class);
            invalidValueAssert.extracting(RecordWithValue::value).isEqualTo(value);

            invalidValueAssert
                    .extracting(InvalidValue::requirement)
                    .extracting(Supplier::get, type(InclusiveRange.class))
                    .extracting(InclusiveRange::min, InclusiveRange::max)
                    .containsExactly(MIN, MAX);
        }

        static IntStream invalidValues() {
            int half = Math.ceilDiv(MAX - MIN, 2);
            return IntStream.concat(IntStream.range(MIN - half, MIN), IntStream.rangeClosed(MAX + 1, MAX + half));
        }

        @ParameterizedTest
        @MethodSource("validValues")
        void validValuesForVerification(int value) {
            Value storable = Values.intValue(value);
            RecordWithSetting record = new Pending(setting, value, storable);

            processForVerificationAndAssertRecord(record, Valid.class)
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(value, storable);
        }

        @ParameterizedTest
        @MethodSource("validValues")
        void validValuesForAuthoritativeReadShouldPassthrough(int value) {
            Value storable = Values.intValue(value);
            RecordWithSetting record = new Valid(setting, value, storable);

            assertThat(processor.processForAuthoritativeRead(record)).isSameAs(record);
        }

        static IntStream validValues() {
            return IntStream.rangeClosed(MIN, MAX);
        }
    }

    @Nested
    class OptionalIntRangeValidatorTest extends SingleValidatorTestBase {
        private static final int MIN = -5;
        private static final int MAX = 15;

        protected OptionalIntRangeValidatorTest() {
            super(OptionalIntRangeValidator.of(TestIndexSetting.INTEGER, MIN, MAX));
        }

        @ParameterizedTest
        @NullSource
        @MethodSource
        void invalidValues(OptionalInt value) {
            Value storable = value != null && value.isPresent() ? Values.intValue(value.getAsInt()) : Values.NO_VALUE;
            RecordWithSetting record = new Pending(setting, value, storable);

            ObjectAssert<InvalidValue> invalidValueAssert =
                    processForVerificationAndAssertRecord(record, InvalidValue.class);
            invalidValueAssert.extracting(RecordWithValue::value).isEqualTo(value);

            invalidValueAssert
                    .extracting(InvalidValue::requirement)
                    .extracting(Supplier::get, type(InclusiveRange.class))
                    .extracting(InclusiveRange::min, InclusiveRange::max)
                    .containsExactly(MIN, MAX);
        }

        static Stream<OptionalInt> invalidValues() {
            int half = Math.ceilDiv(MAX - MIN, 2);
            return IntStream.concat(IntStream.range(MIN - half, MIN), IntStream.rangeClosed(MAX + 1, MAX + half))
                    .mapToObj(OptionalInt::of);
        }

        @ParameterizedTest
        @MethodSource("validValues")
        void validValuesForVerification(OptionalInt value) {
            Value storable = value.isPresent() ? Values.intValue(value.getAsInt()) : Values.NO_VALUE;
            RecordWithSetting record = new Pending(setting, value, storable);

            processForVerificationAndAssertRecord(record, Valid.class)
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(value, storable);
        }

        @ParameterizedTest
        @MethodSource("validValues")
        void validValuesForAuthoritativeReadShouldPassthrough(OptionalInt value) {
            Value storable = value.isPresent() ? Values.intValue(value.getAsInt()) : Values.NO_VALUE;
            RecordWithSetting record = new Valid(setting, value, storable);

            assertThat(processor.processForAuthoritativeRead(record)).isSameAs(record);
        }

        static Stream<OptionalInt> validValues() {
            return Stream.concat(
                    Stream.of(OptionalInt.empty()),
                    IntStream.rangeClosed(MIN, MAX).mapToObj(OptionalInt::of));
        }
    }

    @Nested
    class DoubleRangeValidatorTest extends SingleValidatorTestBase {
        private static final double MIN = -23.0;
        private static final double MAX = 42.0;

        protected DoubleRangeValidatorTest() {
            super(DoubleRangeValidator.of(TestIndexSetting.DOUBLE, MIN, MAX));
        }

        @ParameterizedTest
        @NullSource
        @MethodSource
        void invalidValues(Double value) {
            Value storable = Values.unsafeOf(value, true);
            RecordWithSetting record = new Pending(setting, value, storable);

            ObjectAssert<InvalidValue> invalidValueAssert =
                    processForVerificationAndAssertRecord(record, InvalidValue.class);
            invalidValueAssert.extracting(RecordWithValue::value).isEqualTo(value);

            invalidValueAssert
                    .extracting(InvalidValue::requirement)
                    .extracting(Supplier::get, type(InclusiveRange.class))
                    .extracting(InclusiveRange::min, InclusiveRange::max)
                    .containsExactly(MIN, MAX);
        }

        static DoubleStream invalidValues() {
            int count = 10;
            double delta = (MIN + MAX) / 2 / count;
            DoubleStream.Builder builder = DoubleStream.builder();
            for (int i = count; i > 0; i--) {
                builder.add(MIN - i * delta);
            }
            for (int i = 1; i <= count; i++) {
                builder.add(MAX + i * delta);
            }
            return builder.build();
        }

        @ParameterizedTest
        @MethodSource("validValues")
        void validValuesForVerification(double value) {
            Value storable = Values.doubleValue(value);
            RecordWithSetting record = new Pending(setting, value, storable);

            processForVerificationAndAssertRecord(record, Valid.class)
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(value, storable);
        }

        @ParameterizedTest
        @MethodSource("validValues")
        void validValuesForAuthoritativeReadShouldPassthrough(double value) {
            Value storable = Values.doubleValue(value);
            RecordWithSetting record = new Valid(setting, value, storable);

            assertThat(processor.processForAuthoritativeRead(record)).isSameAs(record);
        }

        static DoubleStream validValues() {
            int count = 18;
            double min = Math.nextUp(MIN);
            double delta = (min + Math.nextDown(MAX)) / count;
            DoubleStream.Builder builder = DoubleStream.builder();
            builder.add(MIN);
            for (int i = 0; i < count; i++) {
                builder.add(min + i * delta);
            }
            builder.add(MAX);
            return builder.build();
        }
    }
}
