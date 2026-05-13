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

import java.util.Set;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.schema.IndexSettingRecord.IncorrectType;
import org.neo4j.internal.schema.IndexSettingRecord.Invalid;
import org.neo4j.internal.schema.IndexSettingRecord.InvalidValue;
import org.neo4j.internal.schema.IndexSettingRecord.MissingSetting;
import org.neo4j.internal.schema.IndexSettingRecord.Pending;
import org.neo4j.internal.schema.IndexSettingRecord.State;
import org.neo4j.internal.schema.IndexSettingRecord.Unprocessed;
import org.neo4j.internal.schema.IndexSettingRecord.UnrecognizedSetting;
import org.neo4j.internal.schema.IndexSettingRecord.Valid;
import org.neo4j.internal.schema.IndexSettingTestUtils.TestIndexSetting;
import org.neo4j.internal.schema.IndexSettingsRequirements.IterableRequirement;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class IndexSettingRecordsByStateTest {
    @Test
    void get() {
        Iterable<IndexSettingRecord> validRecords = Iterables.asIterable(
                recordFor(State.VALID, TestIndexSetting.INTEGER), recordFor(State.VALID, TestIndexSetting.BOOLEAN));
        Iterable<IndexSettingRecord> missingSettingRecords = Iterables.asIterable(
                recordFor(State.MISSING_SETTING, TestIndexSetting.STRING),
                recordFor(State.MISSING_SETTING, TestIndexSetting.OBJECT));
        Iterable<IndexSettingRecord> incorrectTypeRecords =
                Iterables.asIterable(recordFor(State.INCORRECT_TYPE, TestIndexSetting.DOUBLE));

        IndexSettingRecords records = new IndexSettingRecords();
        records.upsertAll(validRecords);
        records.upsertAll(missingSettingRecords);
        records.upsertAll(incorrectTypeRecords);
        IndexSettingRecordsByState recordsByState = records.groupByState();

        assertThat(recordsByState.get(State.VALID)).containsExactlyInAnyOrderElementsOf(validRecords);
        assertThat(recordsByState.get(State.MISSING_SETTING))
                .containsExactlyInAnyOrderElementsOf(missingSettingRecords);
        assertThat(recordsByState.get(State.INCORRECT_TYPE)).containsExactlyInAnyOrderElementsOf(incorrectTypeRecords);
    }

    @Test
    void invalid() {

        Iterable<IndexSettingRecord> validRecords = Iterables.asIterable(
                recordFor(State.VALID, TestIndexSetting.INTEGER), recordFor(State.VALID, TestIndexSetting.BOOLEAN));
        Iterable<IndexSettingRecord> missingSettingRecords = Iterables.asIterable(
                recordFor(State.MISSING_SETTING, TestIndexSetting.STRING),
                recordFor(State.MISSING_SETTING, TestIndexSetting.OBJECT));
        Iterable<IndexSettingRecord> incorrectTypeRecords =
                Iterables.asIterable(recordFor(State.INCORRECT_TYPE, TestIndexSetting.DOUBLE));

        IndexSettingRecords records = new IndexSettingRecords();
        records.upsertAll(validRecords);
        records.upsertAll(missingSettingRecords);
        records.upsertAll(incorrectTypeRecords);
        IndexSettingRecordsByState recordsByState = records.groupByState();

        assertThat(recordsByState.invalid()).isTrue();
        assertThat(recordsByState.valid()).isFalse();

        assertThat(recordsByState.validRecords())
                .map(IndexSettingRecord.class::cast)
                .containsExactlyInAnyOrderElementsOf(validRecords);

        Iterable<Invalid> invalidRecords = recordsByState.invalidRecords();
        assertThat(invalidRecords)
                .map(IndexSettingRecord.class::cast)
                .containsAll(missingSettingRecords)
                .containsAll(incorrectTypeRecords)
                .doesNotContainAnyElementsOf(validRecords);

        assertThat(recordsByState.getFirstInvalidRecordOrNull())
                .isEqualTo(invalidRecords.iterator().next());
    }

    @Test
    void valid() {
        Iterable<IndexSettingRecord> validRecords = Iterables.asIterable(
                recordFor(State.VALID, TestIndexSetting.INTEGER),
                recordFor(State.VALID, TestIndexSetting.BOOLEAN),
                recordFor(State.VALID, TestIndexSetting.STRING));

        IndexSettingRecords records = new IndexSettingRecords();
        records.upsertAll(validRecords);
        IndexSettingRecordsByState recordsByState = records.groupByState();

        assertThat(recordsByState.invalid()).isFalse();
        assertThat(recordsByState.valid()).isTrue();

        assertThat(recordsByState.validRecords())
                .map(IndexSettingRecord.class::cast)
                .containsExactlyInAnyOrderElementsOf(validRecords);

        assertThat(recordsByState.invalidRecords()).isEmpty();
        assertThat(recordsByState.getFirstInvalidRecordOrNull()).isNull();
    }

    private static IndexSettingRecord recordFor(State state, IndexSetting setting) {
        int value = 42;
        Value storable = Values.intValue(42);
        return switch (state) {
            case VALID -> new Valid(setting, value, storable);
            case UNPROCESSED -> new Unprocessed(setting, storable);
            case PENDING -> new Pending(setting, value, storable);
            case UNRECOGNIZED_SETTING -> new UnrecognizedSetting("unknown-" + setting.getSettingName());
            case MISSING_SETTING -> new MissingSetting(setting);
            case INCORRECT_TYPE -> new IncorrectType(setting, (float) value, Integer.class);
            case INVALID_VALUE -> new InvalidValue(setting, -23, new IterableRequirement(Set.of(23, 42, 64)));
        };
    }
}
