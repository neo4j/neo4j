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
import static org.neo4j.gqlstatus.ErrorGqlStatusObjectAssertions.assertThatThrownBy;
import static org.neo4j.internal.schema.IndexSettingTestUtils.FAKE_VALUE;

import java.util.EnumSet;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.schema.IndexSettingRecord.Valid;
import org.neo4j.internal.schema.IndexSettingTestUtils.Lookup;
import org.neo4j.internal.schema.IndexSettingTestUtils.TestIndexSetting;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class TypedIndexConfigTest {
    private static final IndexProviderDescriptor DESCRIPTOR = new IndexProviderDescriptor("test", "1.0");

    private static final EnumSet<TestIndexSetting> ACCEPTED_SETTINGS =
            EnumSet.of(TestIndexSetting.BOOLEAN, TestIndexSetting.INTEGER, TestIndexSetting.STRING);

    private static final Iterable<Valid> RECORDS = Iterables.asIterable(
            new Valid(TestIndexSetting.BOOLEAN, true, Values.booleanValue(true)),
            new Valid(TestIndexSetting.INTEGER, OptionalInt.empty(), Values.NO_VALUE),
            new Valid(TestIndexSetting.STRING, Lookup.FOO, Values.utf8Value(Lookup.FOO.name())),
            new Valid(TestIndexSetting.OBJECT, FAKE_VALUE, null));

    private static final TypedIndexConfig CONFIG = new TestTypedIndexConfig(DESCRIPTOR, ACCEPTED_SETTINGS, RECORDS);

    private static final EnumSet<TestIndexSetting> EXPOSED_SETTINGS;

    static {
        EXPOSED_SETTINGS = EnumSet.noneOf(TestIndexSetting.class);
        RECORDS.forEach(record -> EXPOSED_SETTINGS.add((TestIndexSetting) record.setting()));
    }

    @Test
    void correctDescriptor() {
        assertThat(CONFIG.descriptor()).isEqualTo(DESCRIPTOR);
    }

    @ParameterizedTest
    @MethodSource
    void shouldGetValueForAcceptedSetting(IndexSetting setting) {
        assertThatCode(() -> CONFIG.getValue(setting)).doesNotThrowAnyException();
    }

    static Stream<TestIndexSetting> shouldGetValueForAcceptedSetting() {
        return ACCEPTED_SETTINGS.stream();
    }

    @ParameterizedTest
    @MethodSource
    void shouldThrowGetValueForNonAcceptedSetting(IndexSetting setting) {
        assertThatThrownBy(() -> CONFIG.getValue(setting))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessageContainingAll(
                        "Invalid index config key",
                        setting.getSettingName(),
                        "it was not recognized as an index setting")
                .gqlStatusObject()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22G03)
                .gqlCause()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N27);
    }

    static Stream<TestIndexSetting> shouldThrowGetValueForNonAcceptedSetting() {
        return EnumSet.complementOf(ACCEPTED_SETTINGS).stream();
    }

    @ParameterizedTest
    @MethodSource
    void shouldGetForExposedSettings(IndexSetting setting) {
        assertThatCode(() -> CONFIG.get(setting)).doesNotThrowAnyException();
    }

    static Stream<TestIndexSetting> shouldGetForExposedSettings() {
        return EXPOSED_SETTINGS.stream();
    }

    @ParameterizedTest
    @MethodSource
    void shouldThrowValueForNonExposedSetting(IndexSetting setting) {
        assertThatThrownBy(() -> CONFIG.get(setting))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessageContainingAll(
                        "Invalid index config key",
                        setting.getSettingName(),
                        "it was not recognized as an index setting")
                .gqlStatusObject()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22G03)
                .gqlCause()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N27);
    }

    static Stream<TestIndexSetting> shouldThrowValueForNonExposedSetting() {
        return EnumSet.complementOf(EXPOSED_SETTINGS).stream();
    }

    @ParameterizedTest
    @MethodSource
    void expectedValues(Valid record) {
        IndexSetting setting = record.setting();

        Object value = CONFIG.get(setting);
        assertThat(value).isEqualTo(record.value());

        if (ACCEPTED_SETTINGS.contains(setting)) {
            Value storable = CONFIG.getValue(setting);
            assertThat(storable).isEqualTo(Objects.requireNonNullElse(record.storable(), Values.NO_VALUE));
        }
    }

    static Stream<Valid> expectedValues() {
        return Iterables.stream(RECORDS);
    }

    private static class TestTypedIndexConfig extends TypedIndexConfig {
        TestTypedIndexConfig(
                IndexProviderDescriptor descriptor,
                Set<? extends IndexSetting> acceptedSettings,
                Iterable<Valid> records) {
            super(descriptor, Set.copyOf(acceptedSettings), records);
        }
    }
}
