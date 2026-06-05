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
package org.neo4j.kernel.impl.api.state;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.LongPredicate;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.neo4j.collection.trackable.HeapTrackingCollections;
import org.neo4j.collection.trackable.HeapTrackingIntObjectHashMap;
import org.neo4j.collection.trackable.HeapTrackingLongObjectHashMap;
import org.neo4j.collection.trackable.HeapTrackingUnifiedSet;
import org.neo4j.internal.schema.IndexRemovalSnapshot;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.txstate.TransactionStateBehaviour;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

class IndexUpdate {
    private static final ValueTuple NO_VALUE_TUPLE = ValueTuple.of(Values.NO_VALUE);
    private final MemoryTracker memoryTracker;
    protected final HeapTrackingUnifiedSet<ValueTuple> changedValues;
    protected final HeapTrackingLongObjectHashMap<ValueTupleWithVersion> removedValueEntries;
    protected final HeapTrackingLongObjectHashMap<ValueTuple> addedEntriesIds;
    private Map<ValueTuple, MutableLongSet> addedEntriesValues;
    private boolean isSorted = false;
    protected int removeVersion = 0;

    private IndexUpdate(MemoryTracker stateMemoryTracker) {
        this.memoryTracker = stateMemoryTracker;
        changedValues = HeapTrackingCollections.newSet(stateMemoryTracker);
        removedValueEntries = HeapTrackingCollections.newLongObjectMap(stateMemoryTracker);
        addedEntriesIds = HeapTrackingCollections.newLongObjectMap(stateMemoryTracker);
        addedEntriesValues = HeapTrackingCollections.newMap(stateMemoryTracker);
    }

    static IndexUpdate createIndexUpdate(TransactionStateBehaviour behaviour, MemoryTracker stateMemoryTracker) {
        if (behaviour.useIndexCommands()) {
            return new IndexUpdateWithIndexCommands(stateMemoryTracker);
        } else {
            return new IndexUpdate(stateMemoryTracker);
        }
    }

    void addEntry(ValueTuple values, long entityId) {
        boolean added = changedValues.add(values);
        if (!added) {
            values = changedValues.get(values);
        }
        removePrevAddedEntry(entityId);
        addedEntriesIds.put(entityId, values);
        addedEntriesValues
                .computeIfAbsent(values, v -> HeapTrackingCollections.newLongSet(memoryTracker))
                .add(entityId);
    }

    void removeEntry(ValueTuple values, long entityId) {
        removePrevAddedEntry(entityId);
        removedValueEntries.getIfAbsentPut(entityId, () -> new EmptyValueWithVersion(removeVersion));
    }

    protected void removePrevAddedEntry(long entityId) {
        ValueTuple prevValue = addedEntriesIds.remove(entityId);
        if (prevValue != null) {
            addedEntriesValues.get(prevValue).remove(entityId);
        }
    }

    Map<ValueTuple, MutableLongSet> getAddedValueEntries() {
        return addedEntriesValues;
    }

    TreeMap<ValueTuple, MutableLongSet> getSortedAddedValueEntries() {
        if (!isSorted) {
            Map<ValueTuple, MutableLongSet> sortedMap = new TreeMap<>(ValueTuple.COMPARATOR);
            sortedMap.putAll(addedEntriesValues);
            addedEntriesValues = sortedMap;
            isSorted = true;
        }
        return (TreeMap<ValueTuple, MutableLongSet>) addedEntriesValues;
    }

    HeapTrackingLongObjectHashMap<ValueTupleWithVersion> getRemovedValueEntries() {
        throw new IllegalArgumentException("Must use index commands to get removed entries with values");
    }

    @VisibleForTesting
    MutableLongSet getRemovedEntityIds() {
        return removedValueEntries.keySet();
    }

    IndexRemovalSnapshot removedSnapshot() {
        int version = removeVersion++;
        LongPredicate isRemoved = entityId -> {
            ValueTupleWithVersion removedEntry = removedValueEntries.get(entityId);
            return removedEntry != null && removedEntry.version() <= version;
        };
        return new IndexRemovalSnapshot(version, isRemoved, removedValueEntries.isEmpty());
    }

    private static class IndexUpdateWithIndexCommands extends IndexUpdate {
        private final HeapTrackingIntObjectHashMap<MutableLongSet> removedInPreviousVersions;

        IndexUpdateWithIndexCommands(MemoryTracker stateMemoryTracker) {
            super(stateMemoryTracker);
            this.removedInPreviousVersions = HeapTrackingCollections.newIntObjectHashMap(stateMemoryTracker);
        }

        @Override
        void addEntry(ValueTuple values, long entityId) {
            ValueTupleWithVersion prevRemoved = removedValueEntries.get(entityId);
            if (prevRemoved != null && Objects.equals(prevRemoved.valueTuple(), values)) {
                for (int i = prevRemoved.version(); i < removeVersion; i++) {
                    removedInPreviousVersions
                            .getIfAbsentPut(i, LongSets.mutable.empty())
                            .add(entityId);
                }
                removedValueEntries.remove(entityId);
                return;
            }
            super.addEntry(values, entityId);
        }

        @Override
        void removeEntry(ValueTuple values, long entityId) {
            assert values != null;
            ValueTuple prevAdded = addedEntriesIds.get(entityId);
            if (Objects.equals(prevAdded, values)) {
                removePrevAddedEntry(entityId);
                return;
            }
            boolean added = changedValues.add(values);
            if (!added) {
                values = changedValues.get(values);
            }
            removedValueEntries.put(entityId, new ValueTupleWithVersionImpl(values, removeVersion));
        }

        @Override
        HeapTrackingLongObjectHashMap<ValueTupleWithVersion> getRemovedValueEntries() {
            return removedValueEntries;
        }

        @Override
        IndexRemovalSnapshot removedSnapshot() {
            int version = removeVersion++;
            LongPredicate isRemoved = entityId -> {
                ValueTupleWithVersion removedEntry = removedValueEntries.get(entityId);
                boolean removedNow = removedEntry != null && removedEntry.version() <= version;
                MutableLongSet removedPreviously = removedInPreviousVersions.get(version);
                return removedNow || (removedPreviously != null && removedPreviously.contains(entityId));
            };
            return new IndexRemovalSnapshot(version, isRemoved, removedValueEntries.isEmpty());
        }
    }

    interface ValueTupleWithVersion {
        ValueTuple valueTuple();

        int version();
    }

    record ValueTupleWithVersionImpl(ValueTuple valueTuple, int version) implements ValueTupleWithVersion {}

    record EmptyValueWithVersion(int version) implements ValueTupleWithVersion {

        @Override
        public ValueTuple valueTuple() {
            return NO_VALUE_TUPLE;
        }
    }
}
