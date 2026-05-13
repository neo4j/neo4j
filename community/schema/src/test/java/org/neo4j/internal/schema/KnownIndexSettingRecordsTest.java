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
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.neo4j.internal.schema.IndexSettingTestUtils.FAKE_VALUE;

import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.schema.IndexSettingRecord.InvalidValue;
import org.neo4j.internal.schema.IndexSettingRecord.MissingSetting;
import org.neo4j.internal.schema.IndexSettingRecord.Pending;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithSetting;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithValue;
import org.neo4j.internal.schema.IndexSettingRecord.Valid;
import org.neo4j.internal.schema.IndexSettingTestUtils.TestIndexSetting;
import org.neo4j.internal.schema.IndexSettingsRequirements.IterableRequirement;
import org.neo4j.internal.schema.KnownIndexSettingRecords.RecordProcessor;
import org.neo4j.values.storable.Values;

class KnownIndexSettingRecordsTest {
    private KnownIndexSettingRecords records;

    @BeforeEach
    void setup() {
        records = new KnownIndexSettingRecords();
    }

    @Test
    void emptyOnCreation() {
        assertThat(records).isEmpty();
    }

    @Test
    void upsertRecord() {
        RecordWithSetting record = new MissingSetting(TestIndexSetting.STRING);
        assertThat(record).isSameAs(records.upsert(record)).isSameAs(records.get(TestIndexSetting.STRING));
        assertThat(records).containsExactly(record);
    }

    @Test
    void upsertNullRecord() {
        assertThatThrownBy(() -> records.upsert(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("record must not be null");
    }

    @Test
    void upsertWithNullProcessor() {
        RecordProcessor processor = record -> null;

        RecordWithSetting intRecord = records.upsert(new Pending(TestIndexSetting.INTEGER, 42, Values.intValue(42)));
        records.upsert(new Pending(TestIndexSetting.STRING, "foo", Values.utf8Value("foo")));
        records.upsertWith(TestIndexSetting.STRING, processor);

        assertThat(records).hasSize(2);
        assertThat(records.get(TestIndexSetting.INTEGER)).isSameAs(intRecord);
        assertThat(records.get(TestIndexSetting.STRING)).isInstanceOf(MissingSetting.class);
    }

    @Test
    void upsertWithProcessor() {
        RecordProcessor processor = record -> new InvalidValue(record, FAKE_VALUE, null);

        RecordWithSetting intRecord = records.upsert(new Pending(TestIndexSetting.INTEGER, 42, Values.intValue(42)));
        records.upsert(new Pending(TestIndexSetting.STRING, "foo", Values.utf8Value("foo")));
        records.upsertWith(TestIndexSetting.STRING, processor);

        assertThat(records).hasSize(2);
        assertThat(records.get(TestIndexSetting.INTEGER)).isSameAs(intRecord);
        assertThat(records.get(TestIndexSetting.STRING))
                .asInstanceOf(type(InvalidValue.class))
                .extracting(RecordWithValue::value)
                .isEqualTo(FAKE_VALUE);
    }

    @Test
    void upsertWithProcessorNoSettingRecord() {
        RecordProcessor processor = record -> switch (record) {
            case MissingSetting missingSetting -> new Pending(missingSetting, FAKE_VALUE, null);
            default -> new InvalidValue(record, FAKE_VALUE, null);
        };

        RecordWithSetting intRecord = records.upsert(new Pending(TestIndexSetting.INTEGER, 42, Values.intValue(42)));
        RecordWithSetting stringRecord =
                records.upsert(new Pending(TestIndexSetting.STRING, "foo", Values.utf8Value("foo")));
        records.upsertWith(TestIndexSetting.OBJECT, processor);

        assertThat(records).hasSize(3);
        assertThat(records.get(TestIndexSetting.INTEGER)).isSameAs(intRecord);
        assertThat(records.get(TestIndexSetting.STRING)).isSameAs(stringRecord);
        assertThat(records.get(TestIndexSetting.OBJECT))
                .asInstanceOf(type(Pending.class))
                .extracting(RecordWithValue::value)
                .isEqualTo(FAKE_VALUE);
    }

    @Test
    void toIndexSettings() {
        Iterable<RecordWithSetting> provided = Iterables.asIterable(
                records.upsert(new MissingSetting(TestIndexSetting.OBJECT)),
                records.upsert(new InvalidValue(
                        TestIndexSetting.STRING, "foo", new IterableRequirement(Set.of("bar", "baz")))),
                records.upsert(new Pending(TestIndexSetting.INTEGER, 42, Values.intValue(42))),
                records.upsert(new Pending(TestIndexSetting.BOOLEAN, false, Values.NO_VALUE)),
                records.upsert(new Valid(TestIndexSetting.DOUBLE, Math.PI, Values.doubleValue(Math.PI))));

        IndexSettingRecords indexSettingRecords = records.toIndexSettingRecords();
        assertThat(indexSettingRecords).containsExactlyInAnyOrderElementsOf(provided);
    }
}
