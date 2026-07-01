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

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.neo4j.internal.schema.SequencedIndexSettingProcessors.mergeToValidatingProcessor;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.schema.IndexSettingExtractors.BooleanExtractor;
import org.neo4j.internal.schema.IndexSettingExtractors.IntegerExtractor;
import org.neo4j.internal.schema.IndexSettingExtractors.StringExtractor;
import org.neo4j.internal.schema.IndexSettingRecord.Invalid;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithSetting;
import org.neo4j.internal.schema.IndexSettingRecord.State;
import org.neo4j.internal.schema.IndexSettingRecord.Valid;
import org.neo4j.internal.schema.IndexSettingTestUtils.Lookup;
import org.neo4j.internal.schema.IndexSettingTestUtils.TestIndexSetting;
import org.neo4j.internal.schema.IndexSettingsProcessor.ValidatingIndexSettingsProcessor;
import org.neo4j.internal.schema.IndexSettingsRequirements.DefaultRequirement;
import org.neo4j.internal.schema.SettingsAccessor.IndexSettingObjectMapAccessor;
import org.neo4j.internal.schema.SingleIndexSettingConverter.IntegerToOptionalIntConverter;
import org.neo4j.internal.schema.SingleIndexSettingLookup.NameToEnumLookup;
import org.neo4j.internal.schema.SingleIndexSettingProcessor.FinalizePending;
import org.neo4j.internal.schema.SingleIndexSettingProcessor.MissingSettingMaterializer;
import org.neo4j.internal.schema.SingleIndexSettingValidator.OptionalIntRangeValidator;
import org.neo4j.values.storable.Values;

class DefaultIndexSettingsValidatorTest {
    @Test
    void processorShouldCoverAllExtractedSettings() {
        IndexSettingExtractors extractors = new IndexSettingExtractors(
                IntegerExtractor.of(TestIndexSetting.INTEGER), BooleanExtractor.of(TestIndexSetting.BOOLEAN));
        ValidatingIndexSettingsProcessor processor = FinalizePending.of(TestIndexSetting.INTEGER);

        assertThatThrownBy(() -> new DefaultIndexSettingsValidator(extractors, processor))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll(
                        "Provided",
                        ValidatingIndexSettingsProcessor.class.getSimpleName(),
                        "does not cover all of the",
                        IndexSetting.class.getSimpleName(),
                        "of the provided",
                        IndexSettingExtractors.class.getSimpleName(),
                        "Missing",
                        TestIndexSetting.BOOLEAN.getSettingName());
    }

    @Test
    void implicitSettingsShouldNotHaveDuplicateSettings() {
        IndexSettingExtractors extractors = new IndexSettingExtractors(IntegerExtractor.of(TestIndexSetting.INTEGER));
        ValidatingIndexSettingsProcessor processor = FinalizePending.of(TestIndexSetting.INTEGER);
        IndexSettingEntry[] implicitSettings = new IndexSettingEntry[] {
            new IndexSettingEntry(TestIndexSetting.BOOLEAN, true),
            new IndexSettingEntry(TestIndexSetting.STRING, "foo"),
            new IndexSettingEntry(TestIndexSetting.DOUBLE, Math.PI),
            new IndexSettingEntry(TestIndexSetting.STRING, "bar"),
            new IndexSettingEntry(TestIndexSetting.BOOLEAN, false),
        };

        assertThatThrownBy(() -> new DefaultIndexSettingsValidator(extractors, processor, implicitSettings))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll(
                        "Expected a single",
                        IndexSetting.class.getSimpleName(),
                        "to be provided for each setting injected",
                        "Provided duplicates for",
                        TestIndexSetting.BOOLEAN.getSettingName(),
                        TestIndexSetting.STRING.getSettingName());
    }

    @Test
    void implicitSettingsShouldNotIncludeWhatIsProcessed() {
        IndexSettingExtractors extractors = new IndexSettingExtractors(
                IntegerExtractor.of(TestIndexSetting.INTEGER), BooleanExtractor.of(TestIndexSetting.BOOLEAN));
        ValidatingIndexSettingsProcessor processor = mergeToValidatingProcessor(
                FinalizePending.of(TestIndexSetting.INTEGER), FinalizePending.of(TestIndexSetting.BOOLEAN));
        IndexSettingEntry[] implicitSettings = new IndexSettingEntry[] {
            new IndexSettingEntry(TestIndexSetting.BOOLEAN, true),
            new IndexSettingEntry(TestIndexSetting.STRING, "foo"),
            new IndexSettingEntry(TestIndexSetting.INTEGER, 42),
            new IndexSettingEntry(TestIndexSetting.DOUBLE, Math.PI)
        };

        assertThatThrownBy(() -> new DefaultIndexSettingsValidator(extractors, processor, implicitSettings))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll(
                        "Provided implicit settings for injection contains",
                        IndexSetting.class.getSimpleName(),
                        "handled by the",
                        ValidatingIndexSettingsProcessor.class.getSimpleName(),
                        "Settings",
                        TestIndexSetting.BOOLEAN.getSettingName(),
                        TestIndexSetting.INTEGER.getSettingName());
    }

    @Test
    void correctAcceptedSettings() {
        IndexSettingExtractors extractors = new IndexSettingExtractors(
                IntegerExtractor.of(TestIndexSetting.INTEGER), BooleanExtractor.of(TestIndexSetting.BOOLEAN));
        ValidatingIndexSettingsProcessor processor = mergeToValidatingProcessor(
                FinalizePending.of(TestIndexSetting.INTEGER), FinalizePending.of(TestIndexSetting.BOOLEAN));
        IndexSettingEntry[] implicitSettings = new IndexSettingEntry[] {
            new IndexSettingEntry(TestIndexSetting.STRING, "foo"),
            new IndexSettingEntry(TestIndexSetting.DOUBLE, Math.PI)
        };

        IndexSettingsValidator validator = new DefaultIndexSettingsValidator(extractors, processor, implicitSettings);
        assertThat(validator.acceptedSettings())
                .containsExactlyInAnyOrder(TestIndexSetting.BOOLEAN, TestIndexSetting.INTEGER);
    }

    @Test
    void incompeleteProcessorShouldFailValidation() {
        IndexSettingExtractors extractors = new IndexSettingExtractors(
                IntegerExtractor.of(TestIndexSetting.INTEGER), BooleanExtractor.of(TestIndexSetting.BOOLEAN));
        ValidatingIndexSettingsProcessor processor = new ValidatingIndexSettingsProcessor() {
            @Override
            public void updateForVerification(KnownIndexSettingRecords records) {
                /* NOOP */
            }

            @Override
            public void updateForAuthoritativeRead(KnownIndexSettingRecords records) {
                /* NOOP */
            }

            @Override
            public Set<IndexSetting> settings() {
                return Set.of(TestIndexSetting.INTEGER, TestIndexSetting.BOOLEAN);
            }
        };

        IndexSettingsValidator validator = new DefaultIndexSettingsValidator(extractors, processor);
        SettingsAccessor accessor = new IndexSettingObjectMapAccessor(
                Map.ofEntries(entry(TestIndexSetting.BOOLEAN, true), entry(TestIndexSetting.INTEGER, 42)));

        IndexSettingRecordsByState records = validator.validate(accessor);
        assertThat(records.invalid()).isTrue();

        for (Invalid record : records.invalidRecords()) {
            assertThat(record.state()).isEqualTo(State.PENDING);
        }
    }

    @Test
    void providingUnregonizedSettingsShouldSupercedeImplict() {
        IndexSettingExtractors extractors = new IndexSettingExtractors(
                IntegerExtractor.of(TestIndexSetting.INTEGER), BooleanExtractor.of(TestIndexSetting.BOOLEAN));
        ValidatingIndexSettingsProcessor processor = mergeToValidatingProcessor(
                FinalizePending.of(TestIndexSetting.INTEGER), FinalizePending.of(TestIndexSetting.BOOLEAN));
        IndexSettingEntry[] implicitSettings = new IndexSettingEntry[] {
            new IndexSettingEntry(TestIndexSetting.STRING, "foo"),
            new IndexSettingEntry(TestIndexSetting.DOUBLE, Math.PI)
        };

        IndexSettingsValidator validator = new DefaultIndexSettingsValidator(extractors, processor, implicitSettings);

        SettingsAccessor accessor = new IndexSettingObjectMapAccessor(Map.ofEntries(
                entry(TestIndexSetting.BOOLEAN, true),
                entry(TestIndexSetting.STRING, "error"),
                entry(TestIndexSetting.INTEGER, 42)));

        IndexSettingRecordsByState records = validator.validate(accessor);
        assertThat(records.invalid()).isTrue();

        for (Invalid record : records.invalidRecords()) {
            assertThat(record.state()).isEqualTo(State.UNRECOGNIZED_SETTING);
        }
    }

    @Test
    void providingUnregonizedSettingsShouldSupercedeMigrator() {
        IndexSettingExtractors extractors = new IndexSettingExtractors(
                IntegerExtractor.of(TestIndexSetting.INTEGER), BooleanExtractor.of(TestIndexSetting.BOOLEAN));
        ValidatingIndexSettingsProcessor processor = mergeToValidatingProcessor(
                FinalizePending.of(TestIndexSetting.INTEGER),
                FinalizePending.of(TestIndexSetting.BOOLEAN),
                new SingleIndexSettingMigrator<>(
                        TestIndexSetting.BOOLEAN, Boolean.class, TestIndexSetting.OBJECT, Lookup.class) {
                    @Override
                    protected Lookup migrate(Boolean value) {
                        return value ? Lookup.FOO : Lookup.BAR;
                    }
                },
                FinalizePending.of(TestIndexSetting.OBJECT));

        IndexSettingsValidator validator = new DefaultIndexSettingsValidator(extractors, processor);

        SettingsAccessor accessor = new IndexSettingObjectMapAccessor(Map.ofEntries(
                entry(TestIndexSetting.BOOLEAN, true),
                entry(TestIndexSetting.OBJECT, Lookup.FOO.name()),
                entry(TestIndexSetting.INTEGER, 42)));

        IndexSettingRecordsByState records = validator.validate(accessor);
        assertThat(records.invalid()).isTrue();

        for (Invalid record : records.invalidRecords()) {
            assertThat(record.state()).isEqualTo(State.UNRECOGNIZED_SETTING);
        }
    }

    @Nested
    class ProcessingTest {
        private static final IndexSettingsValidator VALIDATOR = new DefaultIndexSettingsValidator(
                new IndexSettingExtractors(
                        BooleanExtractor.of(TestIndexSetting.BOOLEAN),
                        IntegerExtractor.of(TestIndexSetting.INTEGER),
                        StringExtractor.of(TestIndexSetting.STRING),
                        StringExtractor.of(TestIndexSetting.OBJECT)),
                mergeToValidatingProcessor(
                        MissingSettingMaterializer.forVerification(
                                TestIndexSetting.BOOLEAN, false, Values.booleanValue(false)),
                        FinalizePending.of(TestIndexSetting.BOOLEAN),
                        IntegerToOptionalIntConverter.of(TestIndexSetting.INTEGER),
                        MissingSettingMaterializer.of(
                                TestIndexSetting.INTEGER, OptionalInt.empty(), OptionalInt.empty(), Values.NO_VALUE),
                        OptionalIntRangeValidator.of(TestIndexSetting.INTEGER, 1, 64),
                        new SingleIndexSettingValidator<>(
                                TestIndexSetting.STRING, String.class, new DefaultRequirement<>("lowercase")) {
                            @Override
                            protected boolean isValid(String value) {
                                return Objects.equals(value, value.toLowerCase(Locale.ROOT));
                            }
                        },
                        MissingSettingMaterializer.of(
                                TestIndexSetting.OBJECT,
                                Lookup.BAR.name(),
                                Lookup.FOO.name(),
                                Values.utf8Value(Lookup.FOO.name())),
                        NameToEnumLookup.allOf(TestIndexSetting.OBJECT, Lookup.class)),
                new IndexSettingEntry(TestIndexSetting.DOUBLE, Math.PI));

        @ParameterizedTest
        @MethodSource
        void validateInvalidSettings(SettingsAccessor accessor, Map<IndexSetting, State> expected) {
            IndexSettingRecordsByState records = VALIDATOR.validate(accessor);
            assertThat(records.invalid()).isTrue();

            for (Invalid record : records.invalidRecords()) {
                RecordWithSetting hasSetting = assertThat(record)
                        .asInstanceOf(type(RecordWithSetting.class))
                        .actual();
                assertThat(hasSetting.state()).isEqualTo(expected.get(hasSetting.setting()));
            }
        }

        private static Stream<Arguments> validateInvalidSettings() {
            return Stream.of(
                    Arguments.of(
                            new IndexSettingObjectMapAccessor(Map.ofEntries(
                                    entry(TestIndexSetting.BOOLEAN, true),
                                    entry(TestIndexSetting.INTEGER, 42),
                                    entry(TestIndexSetting.OBJECT, Lookup.BAZ.name()))),
                            Map.of(TestIndexSetting.STRING, State.MISSING_SETTING)),
                    Arguments.of(
                            new IndexSettingObjectMapAccessor(Map.ofEntries(
                                    entry(TestIndexSetting.BOOLEAN, true),
                                    entry(TestIndexSetting.INTEGER, 42),
                                    entry(TestIndexSetting.STRING, "FOO"),
                                    entry(TestIndexSetting.OBJECT, Lookup.BAZ.name()))),
                            Map.of(TestIndexSetting.STRING, State.INVALID_VALUE)),
                    Arguments.of(
                            new IndexSettingObjectMapAccessor(Map.ofEntries(
                                    entry(TestIndexSetting.BOOLEAN, true),
                                    entry(TestIndexSetting.INTEGER, "42"),
                                    entry(TestIndexSetting.STRING, "foo"),
                                    entry(TestIndexSetting.OBJECT, Lookup.BAZ.name()))),
                            Map.of(TestIndexSetting.INTEGER, State.INCORRECT_TYPE)),
                    Arguments.of(
                            new IndexSettingObjectMapAccessor(Map.ofEntries(
                                    entry(TestIndexSetting.BOOLEAN, true),
                                    entry(TestIndexSetting.INTEGER, Math.E),
                                    entry(TestIndexSetting.STRING, "Foo"),
                                    entry(TestIndexSetting.OBJECT, "nope"))),
                            Map.ofEntries(
                                    entry(TestIndexSetting.INTEGER, State.INCORRECT_TYPE),
                                    entry(TestIndexSetting.STRING, State.INVALID_VALUE),
                                    entry(TestIndexSetting.OBJECT, State.INVALID_VALUE))));
        }

        @ParameterizedTest
        @MethodSource
        void validateValidSettings(SettingsAccessor accessor, Map<IndexSetting, Object> expected) {
            IndexSettingRecordsByState records = VALIDATOR.validate(accessor);
            assertThat(records.valid()).isTrue();

            for (Valid record : records.validRecords()) {
                assertThat(record.value()).isEqualTo(expected.get(record.setting()));
            }
        }

        private static Stream<Arguments> validateValidSettings() {
            return Stream.of(
                    Arguments.of(
                            new IndexSettingObjectMapAccessor(Map.ofEntries(
                                    entry(TestIndexSetting.BOOLEAN, true),
                                    entry(TestIndexSetting.INTEGER, 42),
                                    entry(TestIndexSetting.STRING, "foo"),
                                    entry(TestIndexSetting.OBJECT, Lookup.BAZ.name()))),
                            Map.ofEntries(
                                    entry(TestIndexSetting.BOOLEAN, true),
                                    entry(TestIndexSetting.INTEGER, OptionalInt.of(42)),
                                    entry(TestIndexSetting.STRING, "foo"),
                                    entry(TestIndexSetting.OBJECT, Lookup.BAZ),
                                    entry(TestIndexSetting.DOUBLE, Math.PI))),
                    Arguments.of(
                            new IndexSettingObjectMapAccessor(Map.of(TestIndexSetting.STRING, "foo")),
                            Map.ofEntries(
                                    entry(TestIndexSetting.BOOLEAN, false),
                                    entry(TestIndexSetting.INTEGER, OptionalInt.empty()),
                                    entry(TestIndexSetting.STRING, "foo"),
                                    entry(TestIndexSetting.OBJECT, Lookup.FOO),
                                    entry(TestIndexSetting.DOUBLE, Math.PI))));
        }

        @ParameterizedTest
        @MethodSource
        void trustIsValidSettings(SettingsAccessor accessor, Map<IndexSetting, Object> expected) {
            for (Valid record : VALIDATOR.interpretAuthoritative(accessor)) {
                assertThat(record.value()).isEqualTo(expected.get(record.setting()));
            }
        }

        private static Stream<Arguments> trustIsValidSettings() {
            return Stream.of(
                    Arguments.of(
                            new IndexSettingObjectMapAccessor(Map.ofEntries(
                                    entry(TestIndexSetting.BOOLEAN, true),
                                    entry(TestIndexSetting.INTEGER, 42),
                                    entry(TestIndexSetting.STRING, "foo"),
                                    entry(TestIndexSetting.OBJECT, Lookup.BAZ.name()))),
                            Map.ofEntries(
                                    entry(TestIndexSetting.BOOLEAN, true),
                                    entry(TestIndexSetting.INTEGER, OptionalInt.of(42)),
                                    entry(TestIndexSetting.STRING, "foo"),
                                    entry(TestIndexSetting.OBJECT, Lookup.BAZ),
                                    entry(TestIndexSetting.DOUBLE, Math.PI))),
                    Arguments.of(
                            new IndexSettingObjectMapAccessor(Map.ofEntries(
                                    entry(TestIndexSetting.BOOLEAN, false), entry(TestIndexSetting.STRING, "foo"))),
                            Map.ofEntries(
                                    entry(TestIndexSetting.BOOLEAN, false),
                                    entry(TestIndexSetting.INTEGER, OptionalInt.empty()),
                                    entry(TestIndexSetting.STRING, "foo"),
                                    entry(TestIndexSetting.OBJECT, Lookup.BAR),
                                    entry(TestIndexSetting.DOUBLE, Math.PI))));
        }
    }
}
