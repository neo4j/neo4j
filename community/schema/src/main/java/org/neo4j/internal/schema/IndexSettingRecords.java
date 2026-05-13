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

import static java.lang.String.CASE_INSENSITIVE_ORDER;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.util.Preconditions;

/// A mutable collection of [IndexSettingRecord]s for building up state with the intent to create a
/// [IndexSettingRecordsByState]
public class IndexSettingRecords implements Iterable<IndexSettingRecord> {
    private final Map<String, IndexSettingRecord> records = new TreeMap<>(CASE_INSENSITIVE_ORDER);

    public <RECORD extends IndexSettingRecord> RECORD upsert(RECORD record) {
        Preconditions.requireNonNull(record, "record must not be null");
        String settingName = Preconditions.requireNonNull(record.settingName(), "setting must not be null");
        records.put(settingName, record);
        return record;
    }

    public void upsertAll(Iterable<? extends IndexSettingRecord> records) {
        for (IndexSettingRecord record : records) {
            upsert(record);
        }
    }

    public IndexSettingRecordsByState groupByState() {
        return new IndexSettingRecordsByState(this);
    }

    @Override
    public Iterator<IndexSettingRecord> iterator() {
        return records.values().iterator();
    }

    @Override
    public String toString() {
        return Iterables.toString(this, ", ", getClass().getSimpleName() + "[", "]");
    }
}
