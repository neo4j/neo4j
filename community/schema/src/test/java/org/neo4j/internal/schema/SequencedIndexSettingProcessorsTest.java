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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.neo4j.internal.schema.SequencedIndexSettingProcessors.mergeProcessors;
import static org.neo4j.internal.schema.SequencedIndexSettingProcessors.mergeToValidatingProcessor;

import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.schema.IndexSettingRecord.InvalidValue;
import org.neo4j.internal.schema.IndexSettingRecord.MissingSetting;
import org.neo4j.internal.schema.IndexSettingRecord.Pending;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithSetting;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithStorable;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithValue;
import org.neo4j.internal.schema.IndexSettingRecord.Valid;
import org.neo4j.internal.schema.IndexSettingTestUtils.TestIndexSetting;
import org.neo4j.internal.schema.IndexSettingsProcessor.ValidatingIndexSettingsProcessor;
import org.neo4j.internal.schema.SingleIndexSettingConverter.IntegerToOptionalIntConverter;
import org.neo4j.internal.schema.SingleIndexSettingProcessor.FinalizePending;
import org.neo4j.internal.schema.SingleIndexSettingProcessor.MissingSettingMaterializer;
import org.neo4j.internal.schema.SingleIndexSettingValidator.IntegerRangeValidator;
import org.neo4j.internal.schema.SingleIndexSettingValidator.OptionalIntRangeValidator;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class SequencedIndexSettingProcessorsTest {

    @Test
    void collatesSettings() {
        IndexSettingsProcessor processor = mergeProcessors(
                MissingSettingMaterializer.forVerification(TestIndexSetting.BOOLEAN, false),
                MissingSettingMaterializer.forVerification(TestIndexSetting.INTEGER, 42),
                MissingSettingMaterializer.forVerification(TestIndexSetting.STRING, "foo"),
                IntegerToOptionalIntConverter.of(TestIndexSetting.INTEGER));

        assertThat(processor.settings())
                .containsExactlyInAnyOrder(TestIndexSetting.BOOLEAN, TestIndexSetting.INTEGER, TestIndexSetting.STRING);
    }

    @Test
    void eachSettingNeedsValidationProcessorToBeAValidatingProcessor() {
        assertThatThrownBy(() -> mergeToValidatingProcessor(
                        MissingSettingMaterializer.forVerification(TestIndexSetting.BOOLEAN, false),
                        FinalizePending.of(TestIndexSetting.BOOLEAN),
                        MissingSettingMaterializer.forVerification(
                                TestIndexSetting.DOUBLE, OptionalDouble.empty(), Values.NO_VALUE),
                        MissingSettingMaterializer.forVerification(TestIndexSetting.INTEGER, 42),
                        IntegerRangeValidator.of(TestIndexSetting.INTEGER, 23, 69),
                        MissingSettingMaterializer.forVerification(TestIndexSetting.STRING, "foo")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll(
                        "Provided",
                        IndexSettingsProcessor.class.getSimpleName(),
                        "could not become a",
                        ValidatingIndexSettingsProcessor.class.getSimpleName(),
                        "because the following settings do not have an associated",
                        ValidatingIndexSettingsProcessor.class.getSimpleName(),
                        TestIndexSetting.DOUBLE.getSettingName(),
                        TestIndexSetting.STRING.getSettingName());
    }

    @Test
    void eachSettingHasAValidationProcessor() {
        assertThatCode(ValidatingProcessTest::validatingProcessor).doesNotThrowAnyException();
    }

    @Nested
    class ValidatingProcessTest {
        private KnownIndexSettingRecords records;

        @BeforeEach
        void setup() {
            records = new KnownIndexSettingRecords();
        }

        @Test
        void missingSetting() {
            ValidatingIndexSettingsProcessor processor = validatingProcessor();
            process(
                    processor,
                    new Pending(TestIndexSetting.BOOLEAN, false, Values.booleanValue(false)),
                    new MissingSetting(TestIndexSetting.INTEGER),
                    new Pending(TestIndexSetting.STRING, "foo", Values.utf8Value("foo")));

            assertThat(records.get(TestIndexSetting.BOOLEAN)).isInstanceOf(Valid.class);
            assertThat(records.get(TestIndexSetting.INTEGER)).isInstanceOf(MissingSetting.class);
            assertThat(records.get(TestIndexSetting.STRING)).isInstanceOf(Valid.class);
        }

        @Test
        void valid() {
            ValidatingIndexSettingsProcessor processor = validatingProcessor();
            process(
                    processor,
                    new Pending(TestIndexSetting.BOOLEAN, false, Values.booleanValue(false)),
                    new Pending(TestIndexSetting.INTEGER, 42, Values.intValue(42)),
                    new Pending(TestIndexSetting.STRING, "foo", Values.utf8Value("foo")));

            assertThat(records.get(TestIndexSetting.BOOLEAN)).isInstanceOf(Valid.class);
            assertThat(records.get(TestIndexSetting.INTEGER)).isInstanceOf(Valid.class);
            assertThat(records.get(TestIndexSetting.STRING)).isInstanceOf(Valid.class);
        }

        static ValidatingIndexSettingsProcessor validatingProcessor() {
            return mergeToValidatingProcessor(
                    FinalizePending.of(TestIndexSetting.BOOLEAN),
                    FinalizePending.of(TestIndexSetting.INTEGER),
                    FinalizePending.of(TestIndexSetting.STRING));
        }

        void process(IndexSettingsProcessor processor, RecordWithSetting... records) {
            for (RecordWithSetting record : records) {
                this.records.upsert(record);
            }
            processor.updateForVerification(this.records);
        }
    }

    @Nested
    class ProcessorIsSequencedTest {

        // in: Integer, out: OptionalInt
        // default(null) | validate(Integer) | convert(Integer, OptionalInt)
        private static final IndexSettingsProcessor INTEGER_PROCESSOR = mergeProcessors(
                MissingSettingMaterializer.forVerification(TestIndexSetting.INTEGER, null),
                IntegerRangeValidator.of(TestIndexSetting.INTEGER, 23, 69),
                IntegerToOptionalIntConverter.of(TestIndexSetting.INTEGER));

        // in: Integer, out: OptionalInt
        //  default(null) | convert(Integer, OptionalInt) | validate(OptionalInt)
        private static final IndexSettingsProcessor OPTIONAL_INT_PROCESSOR = mergeProcessors(
                MissingSettingMaterializer.forVerification(TestIndexSetting.INTEGER, null),
                IntegerToOptionalIntConverter.of(TestIndexSetting.INTEGER),
                OptionalIntRangeValidator.of(TestIndexSetting.INTEGER, 23, 69));

        private KnownIndexSettingRecords intRecords;
        private KnownIndexSettingRecords optionaIntRecords;

        @BeforeEach
        void setup() {
            intRecords = new KnownIndexSettingRecords();
            optionaIntRecords = new KnownIndexSettingRecords();
        }

        @Test
        void missingSetting() {
            MissingSetting record = new MissingSetting(TestIndexSetting.INTEGER);
            processForValidation(record);

            RecordWithSetting intRecord = intRecords.get(TestIndexSetting.INTEGER);
            RecordWithSetting optionalIntRecord = optionaIntRecords.get(TestIndexSetting.INTEGER);
            assertThat(intRecord).isNotEqualTo(optionalIntRecord);

            assertThat(intRecord)
                    .asInstanceOf(type(InvalidValue.class))
                    .extracting(RecordWithValue::value)
                    .isNull();

            assertThat(optionalIntRecord)
                    .asInstanceOf(type(Valid.class))
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(OptionalInt.empty(), Values.NO_VALUE);
        }

        @Test
        void outsideRange() {
            int value = 10;
            Value storable = Values.intValue(value);
            processForValidation(new Pending(TestIndexSetting.INTEGER, value, storable));

            RecordWithSetting intRecord = intRecords.get(TestIndexSetting.INTEGER);
            RecordWithSetting optionalIntRecord = optionaIntRecords.get(TestIndexSetting.INTEGER);
            assertThat(intRecord).isNotEqualTo(optionalIntRecord);

            assertThat(intRecord)
                    .asInstanceOf(type(InvalidValue.class))
                    .extracting(RecordWithValue::value)
                    .isEqualTo(value);

            assertThat(optionalIntRecord)
                    .asInstanceOf(type(InvalidValue.class))
                    .extracting(RecordWithValue::value)
                    .isEqualTo(OptionalInt.of(value));
        }

        @Test
        void withinRangeForValidation() {
            int value = 42;
            Value storable = Values.intValue(value);
            processForValidation(new Pending(TestIndexSetting.INTEGER, value, storable));

            RecordWithSetting intRecord = intRecords.get(TestIndexSetting.INTEGER);
            RecordWithSetting optionalIntRecord = optionaIntRecords.get(TestIndexSetting.INTEGER);
            assertThat(intRecord)
                    .isEqualTo(optionalIntRecord)
                    .asInstanceOf(type(Valid.class))
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(OptionalInt.of(value), storable);
        }

        @Test
        void withinRangeForAuthoritativeRead() {
            int value = 42;
            Value storable = Values.intValue(value);
            processForAuthoritativeRead(new Valid(TestIndexSetting.INTEGER, value, storable));

            RecordWithSetting intRecord = intRecords.get(TestIndexSetting.INTEGER);
            RecordWithSetting optionalIntRecord = optionaIntRecords.get(TestIndexSetting.INTEGER);
            assertThat(intRecord)
                    .isEqualTo(optionalIntRecord)
                    .asInstanceOf(type(Valid.class))
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(OptionalInt.of(value), storable);
        }

        void processForValidation(RecordWithSetting... records) {
            process(IndexSettingsProcessor::updateForVerification, records);
        }

        void processForAuthoritativeRead(RecordWithSetting... records) {
            process(IndexSettingsProcessor::updateForAuthoritativeRead, records);
        }

        void process(
                BiConsumer<IndexSettingsProcessor, KnownIndexSettingRecords> update, RecordWithSetting... records) {
            for (RecordWithSetting record : records) {
                intRecords.upsert(record);
                optionaIntRecords.upsert(record);
            }
            update.accept(INTEGER_PROCESSOR, intRecords);
            update.accept(OPTIONAL_INT_PROCESSOR, optionaIntRecords);
        }
    }
}
