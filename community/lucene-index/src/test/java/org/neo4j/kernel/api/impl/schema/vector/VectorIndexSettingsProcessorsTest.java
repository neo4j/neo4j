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
package org.neo4j.kernel.api.impl.schema.vector;

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.STRING;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.DEFAULT_SEARCH_EXPANSION_FACTOR;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.QUANTIZATION_ENABLED;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.QUANTIZATION_TYPE;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.internal.schema.IndexConfigUtils.HasSetting;
import org.neo4j.internal.schema.IndexConfigUtils.IndexSettingsRequirement;
import org.neo4j.internal.schema.IndexSettingRecord.InvalidValue;
import org.neo4j.internal.schema.IndexSettingRecord.MissingSetting;
import org.neo4j.internal.schema.IndexSettingRecord.Pending;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithSetting;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithStorable;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithValue;
import org.neo4j.internal.schema.IndexSettingRecord.Valid;
import org.neo4j.internal.schema.IndexSettingsProcessor;
import org.neo4j.internal.schema.IndexSettingsRequirements.ClassRequirement;
import org.neo4j.internal.schema.IndexSettingsRequirements.IterableRequirement;
import org.neo4j.internal.schema.KnownIndexSettingRecords;
import org.neo4j.internal.schema.SingleIndexSettingProcessor;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.MissingDefaultSearchExpansionFactorMaterializer;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.MissingQuantizationTypeMaterializer;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.QuantizationTypeLookup;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.SimpleQuantizationEnabledToTypeMigrator;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class VectorIndexSettingsProcessorsTest {
    abstract static class TestBase {
        protected KnownIndexSettingRecords records;

        @BeforeEach
        void setup() {
            records = new KnownIndexSettingRecords();
        }

        private static Object underlyingRequirement(InvalidValue invalidValue) {
            return invalidValue.requirement().get();
        }
    }

    @Nested
    class MissingDefaultSearchExpansionFactorMaterializerTest extends TestBase {
        private static final Map<VectorQuantizationType, Double> EXPANSION_FACTORS_FOR_VERIFICATION = Map.ofEntries(
                entry(VectorQuantizationType.NONE, 1.0),
                entry(VectorQuantizationType.SCALAR, 2.0),
                entry(VectorQuantizationType.BINARY, 8.0));
        private static final IndexSettingsProcessor DEFAULT =
                MissingDefaultSearchExpansionFactorMaterializer.of(EXPANSION_FACTORS_FOR_VERIFICATION);

        @ParameterizedTest
        @EnumSource
        void existingForVerification(VectorQuantizationType type) {
            double expansionFactor = 10.0 * EXPANSION_FACTORS_FOR_VERIFICATION.get(type);
            DoubleValue storable = Values.doubleValue(expansionFactor);
            RecordWithSetting record =
                    records.upsert(new Pending(DEFAULT_SEARCH_EXPANSION_FACTOR, expansionFactor, storable));

            DEFAULT.updateForVerification(records);
            assertThat(records.get(DEFAULT_SEARCH_EXPANSION_FACTOR)).isSameAs(record);
        }

        @ParameterizedTest
        @EnumSource
        void existingForAuthoritativeRead(VectorQuantizationType type) {
            double expansionFactor = 10.0 * EXPANSION_FACTORS_FOR_VERIFICATION.get(type);
            DoubleValue storable = Values.doubleValue(expansionFactor);
            RecordWithSetting record =
                    records.upsert(new Valid(DEFAULT_SEARCH_EXPANSION_FACTOR, expansionFactor, storable));

            DEFAULT.updateForAuthoritativeRead(records);
            assertThat(records.get(DEFAULT_SEARCH_EXPANSION_FACTOR)).isSameAs(record);
        }

        @Test
        void invalidValue() {
            records.upsert(new InvalidValue(
                    QUANTIZATION_TYPE,
                    "CLEARLYNOTAQUANTIZATIONTYPE",
                    new IterableRequirement(Arrays.asList(VectorQuantizationType.values()))));

            DEFAULT.updateForVerification(records);
            assertThat(records.get(DEFAULT_SEARCH_EXPANSION_FACTOR))
                    .asInstanceOf(type(InvalidValue.class))
                    .extracting(HasSetting::setting, RecordWithValue::value, TestBase::underlyingRequirement)
                    .containsExactly(DEFAULT_SEARCH_EXPANSION_FACTOR, null, double.class);
        }

        @ParameterizedTest
        @EnumSource
        void useDefaultForVerification(VectorQuantizationType type) {
            double expansionFactor = EXPANSION_FACTORS_FOR_VERIFICATION.get(type);
            DoubleValue storable = Values.doubleValue(expansionFactor);
            records.upsert(new Valid(QUANTIZATION_TYPE, type, Values.utf8Value(type.name())));

            DEFAULT.updateForVerification(records);
            assertThat(records.get(DEFAULT_SEARCH_EXPANSION_FACTOR))
                    .asInstanceOf(type(Pending.class))
                    .extracting(HasSetting::setting, RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(DEFAULT_SEARCH_EXPANSION_FACTOR, expansionFactor, storable);
        }
    }

    @Nested
    class SimpleQuantizationEnabledToTypeMigratorTest extends TestBase {
        private static final VectorQuantizationType CORRESPONDING_ENABLED_TYPE = VectorQuantizationType.SCALAR;
        private static final SingleIndexSettingProcessor MIGRATOR =
                SimpleQuantizationEnabledToTypeMigrator.of(CORRESPONDING_ENABLED_TYPE);

        @ParameterizedTest
        @ValueSource(booleans = {false, true})
        void invalidValue(boolean enabled) {
            RecordWithSetting record =
                    records.upsert(new Pending(QUANTIZATION_ENABLED, enabled, Values.booleanValue(enabled)));

            RecordWithSetting processedRecord = MIGRATOR.processForVerification(record);
            assertThat(processedRecord)
                    .asInstanceOf(type(InvalidValue.class))
                    .extracting(HasSetting::setting, RecordWithValue::value, TestBase::underlyingRequirement)
                    .containsExactly(QUANTIZATION_TYPE, null, VectorQuantizationType.class);

            MIGRATOR.updateForVerification(records);
            assertThat(records.get(QUANTIZATION_ENABLED)).isSameAs(record);
            assertThat(records.get(QUANTIZATION_TYPE)).isEqualTo(processedRecord);
        }

        @ParameterizedTest
        @ValueSource(strings = {"false", "true"})
        void incorrectType(String value) {
            RecordWithSetting record = records.upsert(new Valid(QUANTIZATION_ENABLED, value, Values.utf8Value(value)));

            RecordWithSetting processedRecord = MIGRATOR.processForVerification(record);
            assertThat(processedRecord)
                    .asInstanceOf(type(InvalidValue.class))
                    .extracting(HasSetting::setting, RecordWithValue::value, TestBase::underlyingRequirement)
                    .containsExactly(QUANTIZATION_TYPE, null, VectorQuantizationType.class);

            MIGRATOR.updateForVerification(records);
            assertThat(records.get(QUANTIZATION_ENABLED)).isSameAs(record);
            assertThat(records.get(QUANTIZATION_TYPE)).isEqualTo(processedRecord);
        }

        @ParameterizedTest
        @ValueSource(booleans = {false, true})
        void validForVerification(boolean enabled) {
            RecordWithSetting record =
                    records.upsert(new Valid(QUANTIZATION_ENABLED, enabled, Values.booleanValue(enabled)));
            VectorQuantizationType processedValue = enabled ? CORRESPONDING_ENABLED_TYPE : VectorQuantizationType.NONE;
            Value storable = Values.utf8Value(processedValue.name());

            RecordWithSetting processedRecord = MIGRATOR.processForVerification(record);
            assertThat(processedRecord)
                    .asInstanceOf(type(Valid.class))
                    .extracting(HasSetting::setting, RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(QUANTIZATION_TYPE, processedValue, storable);

            MIGRATOR.updateForVerification(records);
            assertThat(records.get(QUANTIZATION_ENABLED)).isSameAs(record);
            assertThat(records.get(QUANTIZATION_TYPE)).isEqualTo(processedRecord);
        }

        @ParameterizedTest
        @ValueSource(booleans = {false, true})
        void validForAuthoritativeRead(boolean enabled) {
            RecordWithSetting record =
                    records.upsert(new Valid(QUANTIZATION_ENABLED, enabled, Values.booleanValue(enabled)));
            VectorQuantizationType processedValue = enabled ? CORRESPONDING_ENABLED_TYPE : VectorQuantizationType.NONE;
            Value storable = Values.utf8Value(processedValue.name());

            RecordWithSetting processedRecord = MIGRATOR.processForAuthoritativeRead(record);
            assertThat(processedRecord)
                    .asInstanceOf(type(Valid.class))
                    .extracting(HasSetting::setting, RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(QUANTIZATION_TYPE, processedValue, storable);

            MIGRATOR.updateForAuthoritativeRead(records);
            assertThat(records.get(QUANTIZATION_ENABLED)).isSameAs(record);
            assertThat(records.get(QUANTIZATION_TYPE)).isEqualTo(processedRecord);
        }
    }

    @Nested
    class MissingQuantizationTypeMaterializerTest extends TestBase {
        private static final VectorQuantizationType ENABLED_TYPE = VectorQuantizationType.SCALAR;
        private static final Map<Optional<Boolean>, String> QUANTIZATION_TYPES_FOR_VERIFICATION = Map.ofEntries(
                entry(Optional.empty(), ENABLED_TYPE.name()),
                entry(Optional.of(false), VectorQuantizationType.NONE.name()),
                entry(Optional.of(true), ENABLED_TYPE.name()));

        private static final IndexSettingsProcessor DEFAULT = MissingQuantizationTypeMaterializer.of(ENABLED_TYPE);

        @ParameterizedTest
        @EnumSource
        void existingForVerification(VectorQuantizationType type) {
            String name = type.name();
            TextValue storable = Values.utf8Value(name);
            RecordWithSetting record = records.upsert(new Pending(QUANTIZATION_TYPE, name, storable));

            DEFAULT.updateForVerification(records);
            assertThat(records.get(QUANTIZATION_TYPE)).isSameAs(record);
        }

        @ParameterizedTest
        @EnumSource
        void existingForAuthoritativeRead(VectorQuantizationType type) {
            String name = type.name();
            TextValue storable = Values.utf8Value(name);
            RecordWithSetting record = records.upsert(new Valid(QUANTIZATION_TYPE, name, storable));

            DEFAULT.updateForAuthoritativeRead(records);
            assertThat(records.get(QUANTIZATION_TYPE)).isSameAs(record);
        }

        @Test
        void invalidValue() {
            records.upsert(new InvalidValue(
                    QUANTIZATION_ENABLED, "CLEARLYNOTAQUANTIZATIONTYPE", new ClassRequirement(boolean.class)));

            DEFAULT.updateForVerification(records);
            assertThat(records.get(QUANTIZATION_TYPE))
                    .asInstanceOf(type(InvalidValue.class))
                    .extracting(HasSetting::setting, RecordWithValue::value, TestBase::underlyingRequirement)
                    .containsExactly(QUANTIZATION_TYPE, null, String.class);
        }

        @ParameterizedTest
        @NullSource
        @ValueSource(booleans = {false, true})
        void useDefaultForVerification(Boolean rawEnabled) {
            Optional<Boolean> enabled = Optional.ofNullable(rawEnabled);
            String name = QUANTIZATION_TYPES_FOR_VERIFICATION.get(enabled);
            TextValue storable = Values.utf8Value(name);
            RecordWithSetting record = records.upsert(new Pending(QUANTIZATION_TYPE, name, storable));

            DEFAULT.updateForVerification(records);
            assertThat(records.get(QUANTIZATION_TYPE)).isSameAs(record);
        }
    }

    @Nested
    class QuantizationTypeLookupTest extends TestBase {
        private static final IndexSettingsProcessor LOOKUP =
                QuantizationTypeLookup.of(EnumSet.allOf(VectorQuantizationType.class));

        // ===================================
        //  Invalid tuples of (enabled, type)
        // ===================================

        @ParameterizedTest
        @EnumSource
        void invalidValueViaEnabled(VectorQuantizationType type) {
            Value storable = Values.utf8Value(type.name());
            RecordWithSetting enabledRecord = records.upsert(new MissingSetting(QUANTIZATION_ENABLED));
            records.upsert(new Pending(QUANTIZATION_TYPE, type.name(), storable));
            LOOKUP.updateForVerification(records);

            assertThat(records.get(QUANTIZATION_ENABLED)).isSameAs(enabledRecord);

            assertThat(records.get(QUANTIZATION_TYPE))
                    .asInstanceOf(type(Valid.class))
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(type, storable);
        }

        @ParameterizedTest
        @NullSource
        @ValueSource(booleans = {false, true})
        void invalidValueViaType(Boolean enabled) {
            Optional<Boolean> optionalEnabled = Optional.ofNullable(enabled);
            Value storable = optionalEnabled.map(Values::of).orElse(Values.NO_VALUE);
            records.upsert(new Pending(QUANTIZATION_ENABLED, optionalEnabled, storable));
            RecordWithSetting typeRecord = records.upsert(new MissingSetting(QUANTIZATION_TYPE));
            LOOKUP.updateForVerification(records);

            assertThat(records.get(QUANTIZATION_ENABLED))
                    .asInstanceOf(type(Valid.class))
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(optionalEnabled, storable);

            assertThat(records.get(QUANTIZATION_TYPE)).isSameAs(typeRecord);
        }

        @Test
        void invalidValueViaBoth() {
            RecordWithSetting enabledRecord = records.upsert(new MissingSetting(QUANTIZATION_ENABLED));
            RecordWithSetting typeRecord = records.upsert(new MissingSetting(QUANTIZATION_TYPE));
            LOOKUP.updateForVerification(records);

            assertThat(records.get(QUANTIZATION_ENABLED)).isSameAs(enabledRecord);
            assertThat(records.get(QUANTIZATION_TYPE)).isSameAs(typeRecord);
        }

        @Test
        void conflict() {
            boolean enabled = true;
            Optional<Boolean> optionalEnabled = Optional.of(enabled);
            VectorQuantizationType type = VectorQuantizationType.NONE;
            Value storable = Values.utf8Value(type.name());
            records.upsert(new Pending(QUANTIZATION_ENABLED, optionalEnabled, Values.booleanValue(enabled)));
            records.upsert(new Pending(QUANTIZATION_TYPE, type.name(), storable));
            LOOKUP.updateForVerification(records);

            ObjectAssert<InvalidValue> invalidEnabledValueAssert =
                    assertThat(records.get(QUANTIZATION_ENABLED)).asInstanceOf(type(InvalidValue.class));
            invalidEnabledValueAssert.extracting(RecordWithValue::value).isEqualTo(optionalEnabled);
            invalidEnabledValueAssert
                    .extracting(InvalidValue::requirement)
                    .extracting(IndexSettingsRequirement::supported, STRING)
                    .containsSubsequence(
                            QUANTIZATION_ENABLED.getSettingName(),
                            QUANTIZATION_TYPE.getSettingName(),
                            String.valueOf(Optional.of(false)),
                            VectorQuantizationType.NONE.name(),
                            String.valueOf(Optional.of(true)),
                            VectorQuantizationType.BINARY.name(),
                            String.valueOf(Optional.of(true)),
                            VectorQuantizationType.SCALAR.name(),
                            String.valueOf(Optional.empty()),
                            VectorQuantizationType.NONE.name(),
                            String.valueOf(Optional.empty()),
                            VectorQuantizationType.BINARY.name(),
                            String.valueOf(Optional.empty()),
                            VectorQuantizationType.SCALAR.name());

            ObjectAssert<InvalidValue> invalidTypeValueAssert =
                    assertThat(records.get(QUANTIZATION_TYPE)).asInstanceOf(type(InvalidValue.class));
            invalidTypeValueAssert.extracting(RecordWithValue::value).isEqualTo(type);
            invalidTypeValueAssert
                    .extracting(InvalidValue::requirement)
                    .extracting(IndexSettingsRequirement::supported, STRING)
                    .containsSubsequence(
                            QUANTIZATION_ENABLED.getSettingName(),
                            QUANTIZATION_TYPE.getSettingName(),
                            String.valueOf(Optional.of(false)),
                            VectorQuantizationType.NONE.name(),
                            String.valueOf(Optional.of(true)),
                            VectorQuantizationType.BINARY.name(),
                            String.valueOf(Optional.of(true)),
                            VectorQuantizationType.SCALAR.name(),
                            String.valueOf(Optional.empty()),
                            VectorQuantizationType.NONE.name(),
                            String.valueOf(Optional.empty()),
                            VectorQuantizationType.BINARY.name(),
                            String.valueOf(Optional.empty()),
                            VectorQuantizationType.SCALAR.name());
        }

        @ParameterizedTest
        @EnumSource(mode = Mode.EXCLUDE, names = "NONE")
        void conflict(VectorQuantizationType type) {
            boolean enabled = false;
            Optional<Boolean> optionalEnabled = Optional.of(enabled);
            Value storable = Values.utf8Value(type.name());
            records.upsert(new Pending(QUANTIZATION_ENABLED, optionalEnabled, Values.booleanValue(enabled)));
            records.upsert(new Pending(QUANTIZATION_TYPE, type.name(), storable));
            LOOKUP.updateForVerification(records);

            ObjectAssert<InvalidValue> invalidEnabledValueAssert =
                    assertThat(records.get(QUANTIZATION_ENABLED)).asInstanceOf(type(InvalidValue.class));
            invalidEnabledValueAssert.extracting(RecordWithValue::value).isEqualTo(optionalEnabled);
            invalidEnabledValueAssert
                    .extracting(InvalidValue::requirement)
                    .extracting(IndexSettingsRequirement::supported, STRING)
                    .containsSubsequence(
                            QUANTIZATION_ENABLED.getSettingName(),
                            QUANTIZATION_TYPE.getSettingName(),
                            String.valueOf(Optional.of(false)),
                            VectorQuantizationType.NONE.name(),
                            String.valueOf(Optional.of(true)),
                            VectorQuantizationType.BINARY.name(),
                            String.valueOf(Optional.of(true)),
                            VectorQuantizationType.SCALAR.name(),
                            String.valueOf(Optional.empty()),
                            VectorQuantizationType.NONE.name(),
                            String.valueOf(Optional.empty()),
                            VectorQuantizationType.BINARY.name(),
                            String.valueOf(Optional.empty()),
                            VectorQuantizationType.SCALAR.name());

            ObjectAssert<InvalidValue> invalidTypeValueAssert =
                    assertThat(records.get(QUANTIZATION_TYPE)).asInstanceOf(type(InvalidValue.class));
            invalidTypeValueAssert.extracting(RecordWithValue::value).isEqualTo(type);
            invalidTypeValueAssert
                    .extracting(InvalidValue::requirement)
                    .extracting(IndexSettingsRequirement::supported, STRING)
                    .containsSubsequence(
                            QUANTIZATION_ENABLED.getSettingName(),
                            QUANTIZATION_TYPE.getSettingName(),
                            String.valueOf(Optional.of(false)),
                            VectorQuantizationType.NONE.name(),
                            String.valueOf(Optional.of(true)),
                            VectorQuantizationType.BINARY.name(),
                            String.valueOf(Optional.of(true)),
                            VectorQuantizationType.SCALAR.name(),
                            String.valueOf(Optional.empty()),
                            VectorQuantizationType.NONE.name(),
                            String.valueOf(Optional.empty()),
                            VectorQuantizationType.BINARY.name(),
                            String.valueOf(Optional.empty()),
                            VectorQuantizationType.SCALAR.name());
        }

        // =================================
        //  Valid tuples of (enabled, type)
        // =================================

        @ParameterizedTest
        @EnumSource
        void missingEnabledForVerification(VectorQuantizationType type) {
            Value storable = Values.utf8Value(type.name());
            records.upsert(new Pending(QUANTIZATION_ENABLED, Optional.empty(), Values.NO_VALUE));
            records.upsert(new Pending(QUANTIZATION_TYPE, type.name(), storable));
            LOOKUP.updateForVerification(records);

            assertThat(records.get(QUANTIZATION_ENABLED))
                    .asInstanceOf(type(Valid.class))
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(Optional.empty(), Values.NO_VALUE);

            assertThat(records.get(QUANTIZATION_TYPE))
                    .asInstanceOf(type(Valid.class))
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(type, storable);
        }

        @ParameterizedTest
        @EnumSource
        void missingEnabledForAuthoritativeRead(VectorQuantizationType type) {
            Value storable = Values.utf8Value(type.name());
            Valid enabledRecord = records.upsert(new Valid(QUANTIZATION_ENABLED, Optional.empty(), Values.NO_VALUE));
            records.upsert(new Valid(QUANTIZATION_TYPE, type.name(), storable));
            LOOKUP.updateForAuthoritativeRead(records);

            assertThat(records.get(QUANTIZATION_ENABLED)).isEqualTo(enabledRecord);

            assertThat(records.get(QUANTIZATION_TYPE))
                    .asInstanceOf(type(Valid.class))
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(type, storable);
        }

        @Test
        void disabledForVerification() {
            boolean enabled = false;
            Optional<Boolean> optionalEnabled = Optional.of(enabled);
            VectorQuantizationType type = VectorQuantizationType.NONE;
            Value storableEnabled = Values.booleanValue(enabled);
            Value storableType = Values.utf8Value(type.name());
            records.upsert(new Pending(QUANTIZATION_ENABLED, optionalEnabled, storableEnabled));
            records.upsert(new Pending(QUANTIZATION_TYPE, type.name(), storableType));
            LOOKUP.updateForVerification(records);

            assertThat(records.get(QUANTIZATION_ENABLED))
                    .asInstanceOf(type(Valid.class))
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(optionalEnabled, storableEnabled);

            assertThat(records.get(QUANTIZATION_TYPE))
                    .asInstanceOf(type(Valid.class))
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(type, storableType);
        }

        @Test
        void disabledForAuthoritativeRead() {
            boolean enabled = false;
            Optional<Boolean> optionalEnabled = Optional.of(enabled);
            VectorQuantizationType type = VectorQuantizationType.NONE;
            Value storableEnabled = Values.booleanValue(enabled);
            Value storableType = Values.utf8Value(type.name());
            records.upsert(new Valid(QUANTIZATION_ENABLED, optionalEnabled, storableEnabled));
            records.upsert(new Valid(QUANTIZATION_TYPE, type.name(), storableType));
            LOOKUP.updateForAuthoritativeRead(records);

            assertThat(records.get(QUANTIZATION_ENABLED))
                    .asInstanceOf(type(Valid.class))
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(optionalEnabled, storableEnabled);

            assertThat(records.get(QUANTIZATION_TYPE))
                    .asInstanceOf(type(Valid.class))
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(type, storableType);
        }

        @ParameterizedTest
        @EnumSource(mode = Mode.EXCLUDE, names = "NONE")
        void enabledForVerification(VectorQuantizationType type) {
            boolean enabled = true;
            Optional<Boolean> optionalEnabled = Optional.of(enabled);
            Value storableEnabled = Values.booleanValue(enabled);
            Value storableType = Values.utf8Value(type.name());
            records.upsert(new Pending(QUANTIZATION_ENABLED, optionalEnabled, Values.booleanValue(enabled)));
            records.upsert(new Pending(QUANTIZATION_TYPE, type.name(), storableType));
            LOOKUP.updateForVerification(records);

            assertThat(records.get(QUANTIZATION_ENABLED))
                    .asInstanceOf(type(Valid.class))
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(optionalEnabled, storableEnabled);

            assertThat(records.get(QUANTIZATION_TYPE))
                    .asInstanceOf(type(Valid.class))
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(type, storableType);
        }

        @ParameterizedTest
        @EnumSource(mode = Mode.EXCLUDE, names = "NONE")
        void enabledForAuthoritativeRead(VectorQuantizationType type) {
            boolean enabled = true;
            Optional<Boolean> optionalEnabled = Optional.of(enabled);
            Value storableEnabled = Values.booleanValue(enabled);
            Value storableType = Values.utf8Value(type.name());
            records.upsert(new Valid(QUANTIZATION_ENABLED, optionalEnabled, storableEnabled));
            records.upsert(new Valid(QUANTIZATION_TYPE, type.name(), storableType));
            LOOKUP.updateForAuthoritativeRead(records);

            assertThat(records.get(QUANTIZATION_ENABLED))
                    .asInstanceOf(type(Valid.class))
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(optionalEnabled, storableEnabled);

            assertThat(records.get(QUANTIZATION_TYPE))
                    .asInstanceOf(type(Valid.class))
                    .extracting(RecordWithValue::value, RecordWithStorable::storable)
                    .containsExactly(type, storableType);
        }
    }
}
