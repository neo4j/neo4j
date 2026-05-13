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

import static org.neo4j.internal.schema.IndexConfigUtils.INDEX_SETTING_COMPARATOR;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.schema.IndexSettingRecord.MissingSetting;
import org.neo4j.internal.schema.IndexSettingRecord.RecordWithSetting;
import org.neo4j.util.Preconditions;

/// A mutable collection like [IndexSettingRecords], but typed to work with known settings,
/// mapping [IndexSetting]s to [RecordWithSetting]s.
/// @see IndexSettingRecords
public class KnownIndexSettingRecords implements Iterable<RecordWithSetting> {
    private final Map<IndexSetting, RecordWithSetting> records = new TreeMap<>(INDEX_SETTING_COMPARATOR);

    public <RECORD extends RecordWithSetting> RECORD upsert(RECORD record) {
        Preconditions.requireNonNull(record, "record must not be null");
        IndexSetting setting = Preconditions.requireNonNull(record.setting(), "setting must not be null");
        records.put(setting, record);
        return record;
    }

    public RecordWithSetting upsertWith(IndexSetting setting, RecordProcessor processor) {
        Preconditions.requireNonNull(setting, "setting must not be null");
        Preconditions.requireNonNull(processor, "processor must not be null");
        RecordWithSetting missing = new MissingSetting(setting);
        RecordWithSetting record = Objects.requireNonNullElse(records.get(setting), missing);
        RecordWithSetting processedRecord = Objects.requireNonNullElse(processor.process(record), missing);
        return record.equals(processedRecord) ? record : upsert(processedRecord);
    }

    public RecordWithSetting get(IndexSetting setting) {
        return records.computeIfAbsent(
                Preconditions.requireNonNull(setting, "setting must not be null"), MissingSetting::new);
    }

    public IndexSettingRecords toIndexSettingRecords() {
        IndexSettingRecords records = new IndexSettingRecords();
        records.upsertAll(this);
        return records;
    }

    @Override
    public Iterator<RecordWithSetting> iterator() {
        return records.values().iterator();
    }

    @Override
    public String toString() {
        return Iterables.toString(this, ", ", getClass().getSimpleName() + "[", "]");
    }

    public interface RecordProcessor {
        RecordWithSetting process(RecordWithSetting record);
    }
}
