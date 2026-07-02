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

import java.util.EnumSet;
import java.util.Objects;
import java.util.stream.Stream;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.schema.IndexSettingRecord.IncorrectType;
import org.neo4j.internal.schema.IndexSettingRecord.InvalidValue;
import org.neo4j.internal.schema.IndexSettingRecord.MissingSetting;
import org.neo4j.internal.schema.IndexSettingRecord.Pending;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithSetting;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithStorable;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithValue;
import org.neo4j.internal.schema.IndexSettingRecord.Valid;
import org.neo4j.internal.schema.IndexSettingTestUtils.Lookup;
import org.neo4j.internal.schema.IndexSettingTestUtils.TestIndexSetting;
import org.neo4j.internal.schema.IndexSettingsRequirements.ClassRequirement;
import org.neo4j.internal.schema.IndexSettingsRequirements.IterableRequirement;
import org.neo4j.internal.schema.SingleIndexSettingProcessor.FinalizePending;
import org.neo4j.internal.schema.SingleIndexSettingProcessor.MissingSettingMaterializer;
import org.neo4j.internal.schema.SingleIndexSettingProcessor.RemoveSetting;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class SingleIndexSettingProcessorTest {
    public abstract static class SingleProcessorTestBase {
        protected final SingleIndexSettingProcessor processor;
        protected final IndexSetting setting;

        protected SingleProcessorTestBase(SingleIndexSettingProcessor processor) {
            this.processor = processor;
            this.setting = processor.setting();
        }

        @Test
        void passthroughProcessed() {
            RecordWithSetting record = new InvalidValue(setting, FAKE_VALUE, null);
            assertThat(processor.processForVerification(record)).isSameAs(record);
            assertThat(processor.processForAuthoritativeRead(record)).isSameAs(record);
        }

        @Nested
        class UpdateSemanticallySameAsManualUpsertTest {

            @ParameterizedTest
            @MethodSource("records")
            void verification(KnownIndexSettingRecords records) {
                KnownIndexSettingRecords copy = new KnownIndexSettingRecords();
                records.forEach(copy::upsert);
                assertThat(records).containsExactlyInAnyOrderElementsOf(copy);

                processor.updateForVerification(records);
                copy.upsertWith(setting, processor::processForVerification);
                assertThat(records).containsExactlyInAnyOrderElementsOf(copy);
            }

            @ParameterizedTest
            @MethodSource("records")
            void authoritativeRead(KnownIndexSettingRecords records) {
                KnownIndexSettingRecords copy = new KnownIndexSettingRecords();
                records.forEach(copy::upsert);
                assertThat(records).containsExactlyInAnyOrderElementsOf(copy);

                processor.updateForAuthoritativeRead(records);
                copy.upsertWith(setting, processor::processForAuthoritativeRead);
                assertThat(records).containsExactlyInAnyOrderElementsOf(copy);
            }

            private static Stream<KnownIndexSettingRecords> records() {
                KnownIndexSettingRecords empty = new KnownIndexSettingRecords();
                KnownIndexSettingRecords missingSetting = new KnownIndexSettingRecords();
                KnownIndexSettingRecords incorrectType = new KnownIndexSettingRecords();
                KnownIndexSettingRecords invalidValue = new KnownIndexSettingRecords();
                KnownIndexSettingRecords pending = new KnownIndexSettingRecords();
                KnownIndexSettingRecords valid = new KnownIndexSettingRecords();
                for (TestIndexSetting setting : TestIndexSetting.values()) {
                    Object value = value(setting);
                    Value storable = Objects.requireNonNullElse(Values.unsafeOf(value, true), Values.NO_VALUE);
                    missingSetting.upsert(new MissingSetting(setting));
                    incorrectType.upsert(new IncorrectType(setting, FAKE_VALUE, setting.getType()));
                    invalidValue.upsert(new InvalidValue(setting, value, new ClassRequirement(setting.getType())));
                    pending.upsert(new Pending(setting, value, storable));
                    valid.upsert(new Valid(setting, value, storable));
                }
                return Stream.of(empty, missingSetting, incorrectType, invalidValue, pending, valid);
            }

            private static Object value(TestIndexSetting setting) {
                return switch (setting) {
                    case OBJECT -> FAKE_VALUE;
                    case BOOLEAN -> false;
                    case DOUBLE -> 0.0;
                    case INTEGER -> 0;
                    case STRING -> "value";
                };
            }
        }

        protected <RECORD extends RecordWithSetting> ObjectAssert<RECORD> processForVerificationAndAssertRecord(
                RecordWithSetting record, Class<RECORD> type) {
            return assertThat(processor.processForVerification(record)).asInstanceOf(type(type));
        }

        protected ObjectAssert<Valid> processForAuthoritativeReadAndAssertRecord(RecordWithSetting record) {
            return processForAuthoritativeReadAndAssertRecord(record, Valid.class);
        }

        protected <RECORD extends RecordWithSetting> ObjectAssert<RECORD> processForAuthoritativeReadAndAssertRecord(
                RecordWithSetting record, Class<RECORD> type) {
            return assertThat(processor.processForAuthoritativeRead(record)).asInstanceOf(type(type));
        }
    }

    abstract static class MissingSettingMaterializerTestBase extends SingleProcessorTestBase {
        protected static final String VALUE_FOR_AUTHORITATIVE_READ = "default authoritative value";
        protected static final String VALUE_FOR_VERIFICATION = "default verification value";
        protected static final Value STORABLE_FOR_VERIFICATION = Values.utf8Value(VALUE_FOR_VERIFICATION);

        protected final Value storableForVerification;

        protected MissingSettingMaterializerTestBase(Value storableForVerification) {
            super(MissingSettingMaterializer.of(
                    TestIndexSetting.STRING,
                    VALUE_FOR_AUTHORITATIVE_READ,
                    VALUE_FOR_VERIFICATION,
                    storableForVerification));
            this.storableForVerification = storableForVerification;
        }

        @Test
        void existingForVerification() {
            String existingValue = "existing Value";
            Value existingStorable = Values.utf8Value(existingValue);
            RecordWithSetting record = new Pending(setting, existingValue, existingStorable);

            processForVerificationAndAssertRecord(record, Pending.class)
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(existingValue, existingStorable);
        }

        @Test
        void existingForAuthoritativeRead() {
            String existingValue = "existing Value";
            Value existingStorable = Values.utf8Value(existingValue);
            RecordWithSetting record = new Valid(setting, existingValue, existingStorable);

            processForAuthoritativeReadAndAssertRecord(record)
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(existingValue, existingStorable);
        }

        @Test
        void useDefault() {
            RecordWithSetting record = new MissingSetting(setting);

            processForVerificationAndAssertRecord(record, Pending.class)
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(VALUE_FOR_VERIFICATION, storableForVerification);

            processForAuthoritativeReadAndAssertRecord(record)
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(VALUE_FOR_AUTHORITATIVE_READ, Values.NO_VALUE);
        }
    }

    @Nested
    class MissingSettingMaterializerWithoutStorableTest extends MissingSettingMaterializerTestBase {
        protected MissingSettingMaterializerWithoutStorableTest() {
            super(Values.NO_VALUE);
        }
    }

    @Nested
    class MissingSettingMaterializerWithStorableTest extends MissingSettingMaterializerTestBase {
        protected MissingSettingMaterializerWithStorableTest() {
            super(STORABLE_FOR_VERIFICATION);
        }
    }

    @Nested
    class RemoveSettingTest extends SingleProcessorTestBase {
        private static final IndexSetting SETTING = TestIndexSetting.OBJECT;
        private static final Valid REMOVED = new Valid(SETTING, null, Values.NO_VALUE);

        protected RemoveSettingTest() {
            super(RemoveSetting.of(SETTING));
        }

        @ParameterizedTest
        @MethodSource
        void processForVerificationInvalidShouldPassthrough(RecordWithSetting invalid) {
            assertThat(processor.processForVerification(invalid)).isSameAs(invalid);
        }

        static Stream<RecordWithSetting> processForVerificationInvalidShouldPassthrough() {
            return Stream.of(
                    new MissingSetting(SETTING),
                    new IncorrectType(SETTING, "pi", double.class),
                    new InvalidValue(
                            SETTING, Lookup.FOO, new IterableRequirement(EnumSet.complementOf(EnumSet.of(Lookup.FOO)))),
                    new Pending(SETTING, 123, Values.NO_VALUE));
        }

        @ParameterizedTest
        @MethodSource
        void processForVerificationShouldRemoveValid(Valid valid) {
            assertThat(processor.processForVerification(valid)).isEqualTo(REMOVED);
        }

        static Stream<Valid> processForVerificationShouldRemoveValid() {
            return Stream.of(
                    new Valid(SETTING, "foo", Values.utf8Value("foo")),
                    new Valid(SETTING, true, Values.booleanValue(true)),
                    new Valid(SETTING, 123, Values.NO_VALUE));
        }

        @Test
        void processForAuthoritativeReadShouldPassthrough() {
            assertThat(processor.processForAuthoritativeRead(REMOVED)).isEqualTo(REMOVED);
        }
    }

    @Nested
    class FinalizePendingTest extends SingleProcessorTestBase {
        private static final IndexSetting SETTING = TestIndexSetting.OBJECT;

        FinalizePendingTest() {
            super(FinalizePending.of(TestIndexSetting.OBJECT));
        }

        @ParameterizedTest
        @MethodSource
        void processForVerification(Pending pending) {
            processForVerificationAndAssertRecord(pending, Valid.class)
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(pending.value(), pending.storable());
        }

        static Stream<Pending> processForVerification() {
            return Stream.of(
                    new Pending(SETTING, "foo", Values.utf8Value("foo")),
                    new Pending(SETTING, true, Values.booleanValue(true)),
                    new Pending(SETTING, 123, Values.NO_VALUE));
        }

        @ParameterizedTest
        @MethodSource
        void processForAuthoritativeReadShouldPassthrough(Valid valid) {
            assertThat(processor.processForAuthoritativeRead(valid)).isSameAs(valid);
        }

        static Stream<Valid> processForAuthoritativeReadShouldPassthrough() {
            return Stream.of(
                    new Valid(SETTING, "foo", Values.utf8Value("foo")),
                    new Valid(SETTING, true, Values.booleanValue(true)),
                    new Valid(SETTING, 123, Values.NO_VALUE));
        }
    }
}
