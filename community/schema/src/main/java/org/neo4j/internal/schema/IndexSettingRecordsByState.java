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

import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.schema.IndexSettingRecord.Invalid;
import org.neo4j.internal.schema.IndexSettingRecord.State;
import org.neo4j.internal.schema.IndexSettingRecord.Valid;

public class IndexSettingRecordsByState implements Iterable<IndexSettingRecord> {
    private static final Function<State, SortedSet<IndexSettingRecord>> NEW_RECORD_SORTED_SET =
            ignored -> new TreeSet<>();

    protected final SortedMap<State, SortedSet<IndexSettingRecord>> records;

    IndexSettingRecordsByState(Iterable<IndexSettingRecord> records) {
        SortedMap<State, SortedSet<IndexSettingRecord>> sortedRecords = new TreeMap<>();
        for (IndexSettingRecord record : records) {
            sortedRecords.computeIfAbsent(record.state(), NEW_RECORD_SORTED_SET).add(record);
        }
        this.records = Collections.unmodifiableSortedMap(sortedRecords);
    }

    public Iterable<IndexSettingRecord> get(State state) {
        SortedSet<IndexSettingRecord> recordsForState = records.get(state);
        return recordsForState != null ? Collections.unmodifiableSortedSet(recordsForState) : Iterables.empty();
    }

    public boolean valid() {
        return !invalid();
    }

    public boolean invalid() {
        for (State state : records.keySet()) {
            if (state != State.VALID) {
                return true;
            }
        }
        return false;
    }

    public Invalid getFirstInvalidRecordOrNull() {
        for (Entry<State, SortedSet<IndexSettingRecord>> entry : records.entrySet()) {
            if (entry.getKey() == State.VALID) {
                continue;
            }

            IndexSettingRecord record = entry.getValue().getFirst();
            if (!(record instanceof Invalid invalid)) {
                throw new IllegalStateException("%s has %s state but was not an instance of %s"
                        .formatted(record, record.state(), Invalid.class.getSimpleName()));
            }
            return invalid;
        }
        return null;
    }

    public Iterable<Invalid> invalidRecords() {
        SortedSet<Invalid> invalidRecords = new TreeSet<>();
        for (Entry<State, SortedSet<IndexSettingRecord>> entry : records.entrySet()) {
            if (entry.getKey() == State.VALID) {
                continue;
            }

            for (IndexSettingRecord record : entry.getValue()) {
                if (!(record instanceof Invalid invalid)) {
                    throw new IllegalStateException("%s has %s state but was not an instance of %s"
                            .formatted(record, record.state(), Invalid.class.getSimpleName()));
                }
                invalidRecords.add(invalid);
            }
        }
        return invalidRecords.isEmpty() ? Iterables.empty() : Collections.unmodifiableSortedSet(invalidRecords);
    }

    public Iterable<Valid> validRecords() {
        SortedSet<IndexSettingRecord> shouldBeValidRecords = records.get(State.VALID);
        if (shouldBeValidRecords == null) {
            return Iterables.empty();
        }

        SortedSet<Valid> validRecords = new TreeSet<>();
        for (IndexSettingRecord record : shouldBeValidRecords) {
            if (!(record instanceof Valid valid)) {
                throw new IllegalStateException("%s has %s state but was not an instance of %s"
                        .formatted(record, State.VALID, Valid.class.getSimpleName()));
            }
            validRecords.add(valid);
        }
        return Collections.unmodifiableSortedSet(validRecords);
    }

    @Override
    public Iterator<IndexSettingRecord> iterator() {
        return Iterables.concat(records.values()).iterator();
    }

    @Override
    public String toString() {
        return Iterables.toString(this, ", ", getClass().getSimpleName() + "[", "]");
    }
}
