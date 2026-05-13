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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.schema.IndexSettingRecord.InvalidValue;
import org.neo4j.internal.schema.IndexSettingRecord.MissingSetting;
import org.neo4j.internal.schema.IndexSettingRecord.Pending;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithSetting;
import org.neo4j.internal.schema.IndexSettingRecord.State;
import org.neo4j.internal.schema.IndexSettingRecord.UnrecognizedSetting;
import org.neo4j.internal.schema.IndexSettingRecord.Valid;
import org.neo4j.internal.schema.IndexSettingTestUtils.TestIndexSetting;
import org.neo4j.internal.schema.IndexSettingsRequirements.IterableRequirement;
import org.neo4j.values.storable.Values;

class IndexSettingRecordsTest {
    private IndexSettingRecords records;

    @BeforeEach
    void setup() {
        records = new IndexSettingRecords();
    }

    @Test
    void emptyOnCreation() {
        assertThat(records).isEmpty();
    }

    @Test
    void upsertRecordWithSetting() {
        RecordWithSetting record = new MissingSetting(TestIndexSetting.STRING);
        assertThat(record).isSameAs(records.upsert(record));
        assertThat(records).containsExactly(record);
    }

    @Test
    void upsertUnrecognizedSetting() {
        UnrecognizedSetting record = new UnrecognizedSetting("unknown");
        assertThat(record).isSameAs(records.upsert(record));
        assertThat(records).containsExactly(record);
    }

    @Test
    void upsertNullRecord() {
        assertThatThrownBy(() -> records.upsert(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("record must not be null");
    }

    @Test
    void upsertRecords() {
        Iterable<IndexSettingRecord> provided = Iterables.asIterable(
                new UnrecognizedSetting("unknown"),
                new InvalidValue(TestIndexSetting.STRING, "foo", new IterableRequirement(Set.of("bar", "baz"))),
                new Pending(TestIndexSetting.INTEGER, 42, Values.intValue(42)),
                new Valid(TestIndexSetting.DOUBLE, Math.PI, Values.doubleValue(Math.PI)));

        records.upsertAll(provided);
        assertThat(records).containsExactlyInAnyOrderElementsOf(provided);
    }

    @Test
    void groupByState() {
        Iterable<UnrecognizedSetting> unrecognizedSettings = Iterables.asIterable(
                records.upsert(new UnrecognizedSetting("unknown")),
                records.upsert(new UnrecognizedSetting(IndexSetting.fulltext_Analyzer())));

        Iterable<MissingSetting> missingSetting =
                Iterables.asIterable(records.upsert(new MissingSetting(TestIndexSetting.OBJECT)));

        Iterable<InvalidValue> invalidValue = Iterables.asIterable(records.upsert(
                new InvalidValue(TestIndexSetting.STRING, "foo", new IterableRequirement(Set.of("bar", "baz")))));

        Iterable<Pending> pending = Iterables.asIterable(
                records.upsert(new Pending(TestIndexSetting.INTEGER, 42, Values.intValue(42))),
                records.upsert(new Pending(TestIndexSetting.BOOLEAN, false, Values.NO_VALUE)));

        Iterable<Valid> valid = Iterables.asIterable(
                records.upsert(new Valid(TestIndexSetting.DOUBLE, Math.PI, Values.doubleValue(Math.PI))));

        IndexSettingRecordsByState recordsByState = records.groupByState();
        assertThat(recordsByState).containsExactlyInAnyOrderElementsOf(records);
        assertThat(recordsByState.get(State.UNRECOGNIZED_SETTING))
                .containsExactlyInAnyOrderElementsOf(unrecognizedSettings);
        assertThat(recordsByState.get(State.MISSING_SETTING)).containsExactlyInAnyOrderElementsOf(missingSetting);
        assertThat(recordsByState.get(State.INVALID_VALUE)).containsExactlyInAnyOrderElementsOf(invalidValue);
        assertThat(recordsByState.get(State.PENDING)).containsExactlyInAnyOrderElementsOf(pending);
        assertThat(recordsByState.get(State.VALID)).containsExactlyInAnyOrderElementsOf(valid);
    }
}
