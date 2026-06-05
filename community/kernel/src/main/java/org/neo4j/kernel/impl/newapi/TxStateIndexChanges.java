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
package org.neo4j.kernel.impl.newapi;

import static org.neo4j.util.Preconditions.requireNonEmpty;
import static org.neo4j.values.storable.Values.NO_VALUE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.block.procedure.primitive.LongProcedure;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.UnmodifiableMap;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.internal.schema.IndexRemovalSnapshot;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

/**
 * This class provides static utility methods that calculate relevant index updates from a transaction state for several index operations.
 */
public class TxStateIndexChanges {

    private static final AddedWithValuesAndRemoved EMPTY_ADDED_AND_REMOVED_WITH_VALUES =
            new AddedWithValuesAndRemoved(Collections.emptyList(), IndexRemovalSnapshot.EMPTY);
    private static final AddedAndRemoved EMPTY_ADDED_AND_REMOVED =
            new AddedAndRemoved(LongLists.immutable.empty(), IndexRemovalSnapshot.EMPTY);

    private interface QueryDispatch<T> {
        T empty();

        T forSeek(ReadableTransactionState txState, IndexDescriptor descriptor, ValueTuple values);

        T forScan(ReadableTransactionState txState, IndexDescriptor descriptor, IndexOrder indexOrder);

        T forRangeSeek(
                ReadableTransactionState txState,
                IndexDescriptor descriptor,
                Value[] equalityPrefix,
                PropertyIndexQuery.RangePredicate<?> predicate,
                IndexOrder indexOrder);

        T forBoundingBoxSeek(
                ReadableTransactionState txState,
                IndexDescriptor descriptor,
                Value[] equalityPrefix,
                PropertyIndexQuery.BoundingBoxPredicate predicate);

        T forPrefixSeek(
                ReadableTransactionState txState,
                IndexDescriptor descriptor,
                Value[] equalityPrefix,
                TextValue prefix,
                IndexOrder indexOrder);

        T forSuffixOrContains(
                ReadableTransactionState txState,
                IndexDescriptor descriptor,
                PropertyIndexQuery query,
                IndexOrder indexOrder);
    }

    private static final QueryDispatch<AddedWithValuesAndRemoved> WITH_VALUES = new QueryDispatch<>() {
        @Override
        public AddedWithValuesAndRemoved empty() {
            return EMPTY_ADDED_AND_REMOVED_WITH_VALUES;
        }

        @Override
        public AddedWithValuesAndRemoved forSeek(
                ReadableTransactionState txState, IndexDescriptor descriptor, ValueTuple values) {
            return indexUpdatesWithValuesForSeek(txState, descriptor, values);
        }

        @Override
        public AddedWithValuesAndRemoved forScan(
                ReadableTransactionState txState, IndexDescriptor descriptor, IndexOrder indexOrder) {
            return indexUpdatesWithValuesForScan(txState, descriptor, indexOrder);
        }

        @Override
        public AddedWithValuesAndRemoved forRangeSeek(
                ReadableTransactionState txState,
                IndexDescriptor descriptor,
                Value[] equalityPrefix,
                PropertyIndexQuery.RangePredicate<?> predicate,
                IndexOrder indexOrder) {
            return indexUpdatesWithValuesForRangeSeek(txState, descriptor, equalityPrefix, predicate, indexOrder);
        }

        @Override
        public AddedWithValuesAndRemoved forBoundingBoxSeek(
                ReadableTransactionState txState,
                IndexDescriptor descriptor,
                Value[] equalityPrefix,
                PropertyIndexQuery.BoundingBoxPredicate predicate) {
            return indexUpdatesWithValuesForBoundingBoxSeek(txState, descriptor, equalityPrefix, predicate);
        }

        @Override
        public AddedWithValuesAndRemoved forPrefixSeek(
                ReadableTransactionState txState,
                IndexDescriptor descriptor,
                Value[] equalityPrefix,
                TextValue prefix,
                IndexOrder indexOrder) {
            return indexUpdatesWithValuesForRangeSeekByPrefix(txState, descriptor, equalityPrefix, prefix, indexOrder);
        }

        @Override
        public AddedWithValuesAndRemoved forSuffixOrContains(
                ReadableTransactionState txState,
                IndexDescriptor descriptor,
                PropertyIndexQuery query,
                IndexOrder indexOrder) {
            return indexUpdatesWithValuesForSuffixOrContains(txState, descriptor, query, indexOrder);
        }
    };

    private static final QueryDispatch<AddedAndRemoved> WITHOUT_VALUES = new QueryDispatch<>() {
        @Override
        public AddedAndRemoved empty() {
            return EMPTY_ADDED_AND_REMOVED;
        }

        @Override
        public AddedAndRemoved forSeek(
                ReadableTransactionState txState, IndexDescriptor descriptor, ValueTuple values) {
            return indexUpdatesForSeek(txState, descriptor, values);
        }

        @Override
        public AddedAndRemoved forScan(
                ReadableTransactionState txState, IndexDescriptor descriptor, IndexOrder indexOrder) {
            return indexUpdatesForScan(txState, descriptor, indexOrder);
        }

        @Override
        public AddedAndRemoved forRangeSeek(
                ReadableTransactionState txState,
                IndexDescriptor descriptor,
                Value[] equalityPrefix,
                PropertyIndexQuery.RangePredicate<?> predicate,
                IndexOrder indexOrder) {
            return indexUpdatesForRangeSeek(txState, descriptor, equalityPrefix, predicate, indexOrder);
        }

        @Override
        public AddedAndRemoved forBoundingBoxSeek(
                ReadableTransactionState txState,
                IndexDescriptor descriptor,
                Value[] equalityPrefix,
                PropertyIndexQuery.BoundingBoxPredicate predicate) {
            return indexUpdatesForBoundingBoxSeek(txState, descriptor, equalityPrefix, predicate);
        }

        @Override
        public AddedAndRemoved forPrefixSeek(
                ReadableTransactionState txState,
                IndexDescriptor descriptor,
                Value[] equalityPrefix,
                TextValue prefix,
                IndexOrder indexOrder) {
            return indexUpdatesForRangeSeekByPrefix(txState, descriptor, equalityPrefix, prefix, indexOrder);
        }

        @Override
        public AddedAndRemoved forSuffixOrContains(
                ReadableTransactionState txState,
                IndexDescriptor descriptor,
                PropertyIndexQuery query,
                IndexOrder indexOrder) {
            return indexUpdatesForSuffixOrContains(txState, descriptor, query, indexOrder);
        }
    };

    private static <T> T computeForQuery(
            QueryDispatch<T> dispatch,
            ReadableTransactionState txState,
            IndexDescriptor descriptor,
            IndexOrder indexOrder,
            PropertyIndexQuery... queries) {
        requireNonEmpty(queries);
        if (!txState.hasIndexUpdates(descriptor)) {
            return dispatch.empty();
        }

        // Extract equality prefix — EXACT queries are always contiguous at the start by Neo4j index contract
        List<Value> exactQueryValues = new ArrayList<>(queries.length);
        int i = 0;
        while (i < queries.length && queries[i].type() == IndexQueryType.EXACT) {
            exactQueryValues.add(((PropertyIndexQuery.ExactPredicate) queries[i]).value());
            i++;
        }
        Value[] exactValues = exactQueryValues.toArray(new Value[0]);

        if (i == queries.length) {
            // Seek ignores indexOrder — all values in the tuple are identical
            return dispatch.forSeek(txState, descriptor, ValueTuple.of(exactValues));
        }

        PropertyIndexQuery nextQuery = queries[i];
        return switch (nextQuery.type()) {
            case ALL_ENTRIES, EXISTS -> {
                // If no exact prefix, this is a full scan.
                // Otherwise, use range seek with null predicate — this is intentional,
                // it queries the range subtree under the equality prefix.
                if (exactQueryValues.isEmpty()) {
                    yield dispatch.forScan(txState, descriptor, indexOrder);
                } else {
                    yield dispatch.forRangeSeek(txState, descriptor, exactValues, null, indexOrder);
                }
            }
            case RANGE ->
                dispatch.forRangeSeek(
                        txState, descriptor, exactValues, (PropertyIndexQuery.RangePredicate<?>) nextQuery, indexOrder);
            case BOUNDING_BOX ->
                dispatch.forBoundingBoxSeek(
                        txState, descriptor, exactValues, (PropertyIndexQuery.BoundingBoxPredicate) nextQuery);
            case STRING_PREFIX ->
                dispatch.forPrefixSeek(
                        txState,
                        descriptor,
                        exactValues,
                        ((PropertyIndexQuery.StringPrefixPredicate) nextQuery).prefix(),
                        indexOrder);
            case STRING_SUFFIX, STRING_CONTAINS ->
                dispatch.forSuffixOrContains(txState, descriptor, nextQuery, indexOrder);
            case NEAREST_NEIGHBORS -> dispatch.empty();
            default ->
                throw new UnsupportedOperationException(
                        "Query type not supported: " + nextQuery.type() + " in " + Arrays.toString(queries));
        };
    }

    /**
     * Computes added entities (with values) and removed entity IDs from tx state
     * for the given query predicates.
     */
    public static AddedWithValuesAndRemoved computeForQueryWithValues(
            ReadableTransactionState txState,
            IndexDescriptor descriptor,
            IndexOrder indexOrder,
            PropertyIndexQuery... queries) {
        return computeForQuery(WITH_VALUES, txState, descriptor, indexOrder, queries);
    }

    /**
     * Computes added entity IDs (without values) and removed entity IDs from tx state
     * for the given query predicates.
     */
    public static AddedAndRemoved computeForQueryWithoutValues(
            ReadableTransactionState txState,
            IndexDescriptor descriptor,
            IndexOrder indexOrder,
            PropertyIndexQuery... queries) {
        return computeForQuery(WITHOUT_VALUES, txState, descriptor, indexOrder, queries);
    }

    // SCAN

    static AddedAndRemoved indexUpdatesForScan(
            ReadableTransactionState txState, IndexDescriptor descriptor, IndexOrder indexOrder) {
        return indexUpdatesForScanAndFilter(txState, descriptor, null, indexOrder);
    }

    static AddedWithValuesAndRemoved indexUpdatesWithValuesForScan(
            ReadableTransactionState txState, IndexDescriptor descriptor, IndexOrder indexOrder) {
        return indexUpdatesWithValuesScanAndFilter(txState, descriptor, null, indexOrder);
    }

    // SUFFIX or CONTAINS

    static AddedAndRemoved indexUpdatesForSuffixOrContains(
            ReadableTransactionState txState,
            IndexDescriptor descriptor,
            PropertyIndexQuery query,
            IndexOrder indexOrder) {
        if (descriptor.schema().getPropertyIds().length != 1) {
            throw new IllegalStateException(
                    "Suffix and contains queries on multiple property queries should have been rewritten as existence and filter before now");
        }
        return indexUpdatesForScanAndFilter(txState, descriptor, query, indexOrder);
    }

    static AddedWithValuesAndRemoved indexUpdatesWithValuesForSuffixOrContains(
            ReadableTransactionState txState,
            IndexDescriptor descriptor,
            PropertyIndexQuery query,
            IndexOrder indexOrder) {
        if (descriptor.schema().getPropertyIds().length != 1) {
            throw new IllegalStateException(
                    "Suffix and contains queries on multiple property queries should have been rewritten as existence and filter before now");
        }
        return indexUpdatesWithValuesScanAndFilter(txState, descriptor, query, indexOrder);
    }

    // SEEK

    static AddedAndRemoved indexUpdatesForSeek(
            ReadableTransactionState txState, IndexDescriptor descriptor, ValueTuple values) {
        if (!txState.hasIndexUpdates(descriptor)) {
            return EMPTY_ADDED_AND_REMOVED;
        }
        UnmodifiableMap<ValueTuple, MutableLongSet> additions = txState.getAddedIndexUpdates(descriptor);
        IndexRemovalSnapshot removed = txState.getRemovedFromIndex(descriptor);

        MutableLongSet added = additions.get(values);
        if (added == null) {
            added = LongSets.mutable.empty();
        }
        return new AddedAndRemoved(LongLists.mutable.ofAll(added), removed);
    }

    static AddedWithValuesAndRemoved indexUpdatesWithValuesForSeek(
            ReadableTransactionState txState, IndexDescriptor descriptor, ValueTuple values) {
        if (!txState.hasIndexUpdates(descriptor)) {
            return EMPTY_ADDED_AND_REMOVED_WITH_VALUES;
        }
        UnmodifiableMap<ValueTuple, MutableLongSet> additions = txState.getAddedIndexUpdates(descriptor);
        IndexRemovalSnapshot removed = txState.getRemovedFromIndex(descriptor);

        MutableLongSet added = additions.get(values);
        if (added == null) {
            added = LongSets.mutable.empty();
        }
        Value[] valueArray = values.getValues();
        MutableList<EntityWithPropertyValues> addedList = Lists.mutable.empty();
        added.forEach((LongProcedure) l -> addedList.add(new EntityWithPropertyValues(l, valueArray)));

        return new AddedWithValuesAndRemoved(addedList, removed);
    }

    // RANGE SEEK

    static AddedAndRemoved indexUpdatesForRangeSeek(
            ReadableTransactionState txState,
            IndexDescriptor descriptor,
            Value[] equalityPrefix,
            PropertyIndexQuery.RangePredicate<?> predicate,
            IndexOrder indexOrder) {
        if (!txState.hasIndexUpdates(descriptor)) {
            return EMPTY_ADDED_AND_REMOVED;
        }
        NavigableMap<ValueTuple, MutableLongSet> sortedAdditions = txState.getSortedAddedIndexUpdates(descriptor);
        IndexRemovalSnapshot removed = txState.getRemovedFromIndex(descriptor);

        int size = descriptor.schema().getPropertyIds().length;
        RangeFilterValues rangeFilter = predicate == null
                ? RangeFilterValues.fromExists(size, equalityPrefix)
                : RangeFilterValues.fromRange(size, equalityPrefix, predicate);

        MutableLongList added = LongLists.mutable.empty();

        Map<ValueTuple, MutableLongSet> inRange =
                sortedAdditions.subMap(rangeFilter.lower, true, rangeFilter.upper, true);
        for (Map.Entry<ValueTuple, ? extends MutableLongSet> entry : inRange.entrySet()) {
            Value rangeKey = entry.getKey().valueAt(equalityPrefix.length);

            // Needs to manually filter for if lower or upper should be included
            // since we only wants to compare the first value of the key and not all of them for composite indexes
            boolean allowed = rangeFilter.allowedEntry(rangeKey, equalityPrefix.length);

            if (allowed && (predicate == null || predicate.acceptsValue(rangeKey))) {
                added.addAll(entry.getValue());
            }
        }
        return new AddedAndRemoved(indexOrder == IndexOrder.DESCENDING ? added.asReversed() : added, removed);
    }

    static AddedWithValuesAndRemoved indexUpdatesWithValuesForRangeSeek(
            ReadableTransactionState txState,
            IndexDescriptor descriptor,
            Value[] equalityPrefix,
            PropertyIndexQuery.RangePredicate<?> predicate,
            IndexOrder indexOrder) {
        if (!txState.hasIndexUpdates(descriptor)) {
            return EMPTY_ADDED_AND_REMOVED_WITH_VALUES;
        }
        NavigableMap<ValueTuple, MutableLongSet> sortedAdditions = txState.getSortedAddedIndexUpdates(descriptor);
        IndexRemovalSnapshot removed = txState.getRemovedFromIndex(descriptor);

        int size = descriptor.schema().getPropertyIds().length;
        RangeFilterValues rangeFilter = predicate == null
                ? RangeFilterValues.fromExists(size, equalityPrefix)
                : RangeFilterValues.fromRange(size, equalityPrefix, predicate);

        MutableList<EntityWithPropertyValues> added = Lists.mutable.empty();

        Map<ValueTuple, MutableLongSet> inRange =
                sortedAdditions.subMap(rangeFilter.lower, true, rangeFilter.upper, true);
        for (Map.Entry<ValueTuple, ? extends MutableLongSet> entry : inRange.entrySet()) {
            Value rangeKey = entry.getKey().valueAt(equalityPrefix.length);

            // Needs to manually filter for if lower or upper should be included
            // since we only wants to compare the first value of the key and not all of them for composite indexes
            boolean allowed = rangeFilter.allowedEntry(rangeKey, equalityPrefix.length);

            if (allowed && (predicate == null || predicate.acceptsValue(rangeKey))) {
                entry.getValue()
                        .each(id -> added.add(
                                new EntityWithPropertyValues(id, entry.getKey().getValues())));
            }
        }
        return new AddedWithValuesAndRemoved(indexOrder == IndexOrder.DESCENDING ? added.asReversed() : added, removed);
    }

    // BOUNDING BOX SEEK

    static AddedAndRemoved indexUpdatesForBoundingBoxSeek(
            ReadableTransactionState txState,
            IndexDescriptor descriptor,
            Value[] equalityPrefix,
            PropertyIndexQuery.BoundingBoxPredicate predicate) {
        if (!txState.hasIndexUpdates(descriptor)) {
            return EMPTY_ADDED_AND_REMOVED;
        }
        NavigableMap<ValueTuple, MutableLongSet> sortedAdditions = txState.getSortedAddedIndexUpdates(descriptor);
        IndexRemovalSnapshot removed = txState.getRemovedFromIndex(descriptor);

        int size = descriptor.schema().getPropertyIds().length;
        RangeFilterValues rangeFilter = RangeFilterValues.fromBoundingBox(size, equalityPrefix, predicate);

        MutableLongList added = LongLists.mutable.empty();

        Map<ValueTuple, MutableLongSet> inRange =
                sortedAdditions.subMap(rangeFilter.lower, true, rangeFilter.upper, true);
        for (Map.Entry<ValueTuple, ? extends MutableLongSet> entry : inRange.entrySet()) {
            Value rangeKey = entry.getKey().valueAt(equalityPrefix.length);

            // The TreeMap cannot perfectly order multi-dimensional types (spatial) and need additional filtering out
            // false positives
            if (predicate.acceptsValue(rangeKey)) {
                added.addAll(entry.getValue());
            }
        }
        return new AddedAndRemoved(added, removed);
    }

    static AddedWithValuesAndRemoved indexUpdatesWithValuesForBoundingBoxSeek(
            ReadableTransactionState txState,
            IndexDescriptor descriptor,
            Value[] equalityPrefix,
            PropertyIndexQuery.BoundingBoxPredicate predicate) {
        if (!txState.hasIndexUpdates(descriptor)) {
            return EMPTY_ADDED_AND_REMOVED_WITH_VALUES;
        }
        NavigableMap<ValueTuple, MutableLongSet> sortedAdditions = txState.getSortedAddedIndexUpdates(descriptor);
        IndexRemovalSnapshot removed = txState.getRemovedFromIndex(descriptor);

        int size = descriptor.schema().getPropertyIds().length;
        RangeFilterValues rangeFilter = RangeFilterValues.fromBoundingBox(size, equalityPrefix, predicate);

        MutableList<EntityWithPropertyValues> added = Lists.mutable.empty();

        Map<ValueTuple, MutableLongSet> inRange =
                sortedAdditions.subMap(rangeFilter.lower, true, rangeFilter.upper, true);
        for (Map.Entry<ValueTuple, ? extends MutableLongSet> entry : inRange.entrySet()) {
            Value rangeKey = entry.getKey().valueAt(equalityPrefix.length);

            // The TreeMap cannot perfectly order multi-dimensional types (spatial) and need additional filtering out
            // false positives
            if (predicate.acceptsValue(rangeKey)) {
                entry.getValue()
                        .each(id -> added.add(
                                new EntityWithPropertyValues(id, entry.getKey().getValues())));
            }
        }
        return new AddedWithValuesAndRemoved(added, removed);
    }

    // PREFIX

    static AddedAndRemoved indexUpdatesForRangeSeekByPrefix(
            ReadableTransactionState txState,
            IndexDescriptor descriptor,
            Value[] equalityPrefix,
            TextValue prefix,
            IndexOrder indexOrder) {
        if (!txState.hasIndexUpdates(descriptor)) {
            return EMPTY_ADDED_AND_REMOVED;
        }
        NavigableMap<ValueTuple, MutableLongSet> sortedAdditions = txState.getSortedAddedIndexUpdates(descriptor);
        IndexRemovalSnapshot removed = txState.getRemovedFromIndex(descriptor);

        int size = descriptor.schema().getPropertyIds().length;
        ValueTuple floor = getCompositeValueTuple(size, equalityPrefix, prefix, true);
        ValueTuple maxString = getCompositeValueTuple(size, equalityPrefix, Values.MAX_STRING, false);

        MutableLongList added = LongLists.mutable.empty();

        for (Map.Entry<ValueTuple, ? extends MutableLongSet> entry :
                sortedAdditions.subMap(floor, maxString).entrySet()) {
            Value key = entry.getKey().valueAt(equalityPrefix.length);
            // Needs to check type since the subMap might include non-TextValue for composite index
            if (key.valueGroup() == ValueGroup.TEXT && ((TextValue) key).startsWith(prefix)) {
                added.addAll(entry.getValue());
            } else {
                break;
            }
        }
        return new AddedAndRemoved(indexOrder == IndexOrder.DESCENDING ? added.asReversed() : added, removed);
    }

    static AddedWithValuesAndRemoved indexUpdatesWithValuesForRangeSeekByPrefix(
            ReadableTransactionState txState,
            IndexDescriptor descriptor,
            Value[] equalityPrefix,
            TextValue prefix,
            IndexOrder indexOrder) {
        if (!txState.hasIndexUpdates(descriptor)) {
            return EMPTY_ADDED_AND_REMOVED_WITH_VALUES;
        }
        NavigableMap<ValueTuple, MutableLongSet> sortedAdditions = txState.getSortedAddedIndexUpdates(descriptor);
        IndexRemovalSnapshot removed = txState.getRemovedFromIndex(descriptor);

        int keySize = descriptor.schema().getPropertyIds().length;
        ValueTuple floor = getCompositeValueTuple(keySize, equalityPrefix, prefix, true);
        ValueTuple maxString = getCompositeValueTuple(keySize, equalityPrefix, Values.MAX_STRING, false);

        MutableList<EntityWithPropertyValues> added = Lists.mutable.empty();

        for (Map.Entry<ValueTuple, ? extends MutableLongSet> entry :
                sortedAdditions.subMap(floor, maxString).entrySet()) {
            Value prefixKey = entry.getKey().valueAt(equalityPrefix.length);
            // Needs to check type since the subMap might include non-TextValue for composite index
            if (prefixKey.valueGroup() == ValueGroup.TEXT && ((TextValue) prefixKey).startsWith(prefix)) {
                entry.getValue()
                        .each(id -> added.add(
                                new EntityWithPropertyValues(id, entry.getKey().getValues())));
            } else {
                break;
            }
        }
        return new AddedWithValuesAndRemoved(indexOrder == IndexOrder.DESCENDING ? added.asReversed() : added, removed);
    }

    // HELPERS

    private static AddedAndRemoved indexUpdatesForScanAndFilter(
            ReadableTransactionState txState,
            IndexDescriptor descriptor,
            PropertyIndexQuery filter,
            IndexOrder indexOrder) {
        if (!txState.hasIndexUpdates(descriptor)) {
            return EMPTY_ADDED_AND_REMOVED;
        }

        Map<ValueTuple, ? extends MutableLongSet> additions = getAdded(txState, descriptor, indexOrder);
        IndexRemovalSnapshot removed = txState.getRemovedFromIndex(descriptor);

        MutableLongList added = LongLists.mutable.empty();

        for (Map.Entry<ValueTuple, ? extends MutableLongSet> entry : additions.entrySet()) {
            Value[] values = entry.getKey().getValues();
            if (descriptor.getCapability().areValuesAccepted(values)
                    && (filter == null || filter.acceptsValue(values[0]))) {
                added.addAll(entry.getValue());
            }
        }
        return new AddedAndRemoved(indexOrder == IndexOrder.DESCENDING ? added.asReversed() : added, removed);
    }

    private static AddedWithValuesAndRemoved indexUpdatesWithValuesScanAndFilter(
            ReadableTransactionState txState,
            IndexDescriptor descriptor,
            PropertyIndexQuery filter,
            IndexOrder indexOrder) {
        if (!txState.hasIndexUpdates(descriptor)) {
            return EMPTY_ADDED_AND_REMOVED_WITH_VALUES;
        }

        Map<ValueTuple, ? extends MutableLongSet> additions = getAdded(txState, descriptor, indexOrder);
        IndexRemovalSnapshot removed = txState.getRemovedFromIndex(descriptor);

        MutableList<EntityWithPropertyValues> added = Lists.mutable.empty();

        for (Map.Entry<ValueTuple, ? extends MutableLongSet> entry : additions.entrySet()) {
            Value[] values = entry.getKey().getValues();
            if (descriptor.getCapability().areValuesAccepted(values)
                    && (filter == null || filter.acceptsValue(values[0]))) {
                entry.getValue()
                        .each(id -> added.add(
                                new EntityWithPropertyValues(id, entry.getKey().getValues())));
            }
        }
        return new AddedWithValuesAndRemoved(indexOrder == IndexOrder.DESCENDING ? added.asReversed() : added, removed);
    }

    private static Map<ValueTuple, ? extends MutableLongSet> getAdded(
            ReadableTransactionState txState, IndexDescriptor descriptor, IndexOrder indexOrder) {
        return indexOrder == IndexOrder.NONE
                ? txState.getAddedIndexUpdates(descriptor)
                : txState.getSortedAddedIndexUpdates(descriptor);
    }

    private static ValueTuple getCompositeValueTuple(
            int size, Value[] equalityValues, Value nextValue, boolean minValue) {
        Value[] values = new Value[size];
        Value restOfValues = minValue ? Values.MIN_GLOBAL : Values.MAX_GLOBAL;

        System.arraycopy(equalityValues, 0, values, 0, equalityValues.length);
        values[equalityValues.length] = nextValue == null ? restOfValues : nextValue;
        for (int i = equalityValues.length + 1; i < size; i++) {
            values[i] = restOfValues;
        }
        return ValueTuple.of(values);
    }

    public record AddedAndRemoved(LongIterable added, IndexRemovalSnapshot removed) {
        public boolean isEmpty() {
            return added.isEmpty() && removed.isEmpty();
        }
    }

    public record AddedWithValuesAndRemoved(Iterable<EntityWithPropertyValues> added, IndexRemovalSnapshot removed) {
        public boolean isEmpty() {
            return !added.iterator().hasNext() && removed.isEmpty();
        }
    }

    private static class RangeFilterValues {
        ValueTuple lower;
        ValueTuple upper;
        boolean includeLower;
        boolean includeUpper;

        private RangeFilterValues(ValueTuple lower, boolean includeLower, ValueTuple upper, boolean includeUpper) {
            this.lower = lower;
            this.upper = upper;
            this.includeLower = includeLower;
            this.includeUpper = includeUpper;
        }

        private static RangeFilterValues fromRange(
                int size, Value[] equalityValues, PropertyIndexQuery.RangePredicate<?> predicate) {
            final var from = predicate.fromValue();
            final var to = predicate.toValue();
            final var valueGroup = predicate.valueGroup();

            if (from == null || to == null) {
                throw new IllegalStateException("Use Values.NO_VALUE to encode the lack of a bound");
            }

            ValueTuple selectedLower;
            boolean selectedIncludeLower;

            ValueTuple selectedUpper;
            boolean selectedIncludeUpper;

            if (from == NO_VALUE) {
                final var min = Values.minValue(valueGroup, to);
                selectedLower = getCompositeValueTuple(size, equalityValues, min, true);
                selectedIncludeLower = true;
            } else {
                selectedLower = getCompositeValueTuple(size, equalityValues, from, true);
                selectedIncludeLower = predicate.fromInclusive();
            }

            if (to == NO_VALUE) {
                final var max = Values.maxValue(valueGroup, from);
                selectedUpper = getCompositeValueTuple(size, equalityValues, max, false);
                selectedIncludeUpper = false;
            } else {
                selectedUpper = getCompositeValueTuple(size, equalityValues, to, false);
                selectedIncludeUpper = predicate.toInclusive();
            }

            return new RangeFilterValues(selectedLower, selectedIncludeLower, selectedUpper, selectedIncludeUpper);
        }

        private static RangeFilterValues fromBoundingBox(
                int size, Value[] equalityValues, PropertyIndexQuery.BoundingBoxPredicate predicate) {
            ValueTuple selectedLower = getCompositeValueTuple(size, equalityValues, predicate.from(), true);
            ValueTuple selectedUpper = getCompositeValueTuple(size, equalityValues, predicate.to(), false);
            return new RangeFilterValues(selectedLower, true, selectedUpper, true);
        }

        private static RangeFilterValues fromExists(int size, Value[] equalityValues) {
            ValueTuple min = getCompositeValueTuple(size, equalityValues, null, true);
            ValueTuple max = getCompositeValueTuple(size, equalityValues, null, false);

            return new RangeFilterValues(min, true, max, true);
        }

        private boolean allowedEntry(Value entry, int length) {
            // Entry is already filtered on being between (or equal to) lower and upper
            // Here we just check if we are allowed to be equal to lower and/or upper
            if (includeLower && includeUpper) {
                return true;
            }

            int compareLower = Values.COMPARATOR.compare(lower.valueAt(length), entry); // gets value 0 or -1
            int compareUpper = Values.COMPARATOR.compare(upper.valueAt(length), entry); // gets value 0 or 1

            if (compareLower == compareUpper) // only equal on 0
            {
                return false;
            } else {
                return (compareLower != 0 && compareUpper != 0)
                        || (compareLower == 0 && includeLower)
                        || (compareUpper == 0 && includeUpper);
            }
        }
    }
}
