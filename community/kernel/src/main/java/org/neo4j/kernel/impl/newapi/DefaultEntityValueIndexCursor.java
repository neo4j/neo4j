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

import static java.util.Arrays.stream;
import static org.neo4j.internal.kernel.api.Read.NO_ID;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForBoundingBoxSeek;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForRangeSeek;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForRangeSeekByPrefix;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForScan;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForSeek;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForSuffixOrContains;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesWithValuesForBoundingBoxSeek;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesWithValuesForRangeSeek;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesWithValuesForRangeSeekByPrefix;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesWithValuesForScan;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesWithValuesForSeek;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesWithValuesForSuffixOrContains;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.IndexResultScore;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.ValueIndexCursor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.newapi.TxStateIndexChanges.AddedAndRemoved;
import org.neo4j.kernel.impl.newapi.TxStateIndexChanges.AddedWithValuesAndRemoved;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;

abstract class DefaultEntityValueIndexCursor<CURSOR> extends IndexCursor<IndexProgressor, CURSOR>
        implements ValueIndexCursor, IndexResultScore, EntityIndexSeekClient, SortedMergeJoin.Sink {
    protected KernelRead read;
    protected long entity;
    private float score;
    private PropertyIndexQuery[] query;
    private Value[] values;

    private LongIterator added = ImmutableEmptyLongIterator.INSTANCE;
    private Iterator<EntityWithPropertyValues> addedWithValues = Collections.emptyIterator();
    private LongSet removed = LongSets.immutable.empty();
    private boolean needsValues;
    private IndexOrder indexOrder;
    private final SortedMergeJoin sortedMergeJoin = new SortedMergeJoin();
    private boolean shortcutSecurity;
    private boolean needStoreFilter;
    private PropertySelection propertySelection;
    private final boolean applyAccessModeToTxState;

    DefaultEntityValueIndexCursor(CursorPool<CURSOR> pool, boolean applyAccessModeToTxState) {
        super(pool);
        entity = NO_ID;
        score = Float.NaN;
        indexOrder = IndexOrder.NONE;
        this.applyAccessModeToTxState = applyAccessModeToTxState;
    }

    @Override
    public final void initialize(
            IndexDescriptor descriptor,
            IndexProgressor progressor,
            boolean indexIncludesTransactionState,
            boolean needStoreFilter,
            IndexQueryConstraints constraints,
            PropertyIndexQuery... query) {
        assert query != null;
        super.initialize(progressor);
        this.indexOrder = constraints.order();
        this.needsValues = constraints.needsValues();
        this.needStoreFilter = needStoreFilter;
        this.propertySelection = PropertySelection.selection(indexQueryKeys(query));
        sortedMergeJoin.initialize(indexOrder);

        this.query = query;

        if (tracer != null) {
            tracer.onIndexSeek();
        }

        shortcutSecurity = setupSecurity(descriptor);

        if (!indexIncludesTransactionState && read.txStateHolder.hasTxStateWithChanges() && query.length > 0) {
            // Extract out the equality queries
            List<Value> exactQueryValues = new ArrayList<>(query.length);
            int i = 0;
            while (i < query.length && query[i].type() == IndexQueryType.EXACT) {
                exactQueryValues.add(((PropertyIndexQuery.ExactPredicate) query[i]).value());
                i++;
            }
            Value[] exactValues = exactQueryValues.toArray(new Value[0]);

            if (i == query.length) {
                // Only exact queries
                // No need to order, all values are the same
                this.indexOrder = IndexOrder.NONE;
                seekQuery(descriptor, exactValues);
            } else {
                PropertyIndexQuery nextQuery = query[i];
                switch (nextQuery.type()) {
                    case ALL_ENTRIES, EXISTS -> {
                        // This also covers the rewritten suffix/contains for composite index
                        // If composite index all following will be exists as well so no need to consider those
                        setNeedsValuesIfRequiresOrder();
                        if (exactQueryValues.isEmpty()) {
                            // First query is allEntries or exists, use scan
                            scanQuery(descriptor);
                        } else {
                            rangeQuery(descriptor, exactValues, null);
                        }
                    }

                    case RANGE -> {
                        // This case covers first query to be range or exact followed by range
                        // If composite index all following will be exists as well so no need to consider those
                        setNeedsValuesIfRequiresOrder();
                        rangeQuery(descriptor, exactValues, (PropertyIndexQuery.RangePredicate<?>) nextQuery);
                    }

                    case BOUNDING_BOX -> {
                        // This case covers first query to be bounding box or exact followed by bounding box
                        // If composite index all following will be exists as well so no need to consider those
                        setNeedsValuesIfRequiresOrder();
                        boundingBoxQuery(descriptor, exactValues, (PropertyIndexQuery.BoundingBoxPredicate) nextQuery);
                    }

                    case STRING_PREFIX -> {
                        // This case covers first query to be prefix or exact followed by prefix
                        // If composite index all following will be exists as well so no need to consider those
                        setNeedsValuesIfRequiresOrder();
                        prefixQuery(descriptor, exactValues, (PropertyIndexQuery.StringPrefixPredicate) nextQuery);
                    }

                    case STRING_SUFFIX, STRING_CONTAINS -> {
                        // This case covers suffix/contains for singular indexes
                        // for composite index, the suffix/contains should already
                        // have been rewritten as exists + filter, so no need to consider it here
                        assert query.length == 1;
                        suffixOrContainsQuery(descriptor, nextQuery);
                    }

                    case NEAREST_NEIGHBORS -> {
                        // Vector indexes currently ignore transaction state.
                        // TODO VECTOR: handle transaction state!
                    }

                    default -> throw new UnsupportedOperationException(
                            "Query not supported: " + Arrays.toString(query));
                }
            }
        }
    }

    /**
     * If we require order, we can only do the merge sort if we also get values.
     * This implicitly relies on the fact that if we can get order, we can also get values.
     */
    private void setNeedsValuesIfRequiresOrder() {
        if (indexOrder != IndexOrder.NONE) {
            this.needsValues = true;
        }
    }

    private boolean isRemoved(long reference) {
        return removed.contains(reference);
    }

    @Override
    public final boolean acceptEntity(long reference, float score, Value... values) {
        if (isRemoved(reference) || !allowed(reference) || !storeValuePassesQueryFilter(reference)) {
            return false;
        } else {
            this.entity = reference;
            this.score = score;
            this.values = values;
            return true;
        }
    }

    private boolean storeValuePassesQueryFilter(long reference) {
        if (!needStoreFilter) {
            return true;
        }
        return doStoreValuePassesQueryFilter(reference, propertySelection, query);
    }

    protected abstract boolean doStoreValuePassesQueryFilter(
            long reference, PropertySelection propertySelection, PropertyIndexQuery[] query);

    protected boolean allowsAll() {
        return false;
    }

    @Override
    public final boolean needsValues() {
        return needsValues;
    }

    @Override
    public boolean next() {
        if (indexOrder == IndexOrder.NONE) {
            return nextWithoutOrder();
        } else {
            return nextWithOrdering();
        }
    }

    private boolean nextWithoutOrder() {
        if (!needsValues && added.hasNext()) {
            while (added.hasNext()) {
                long next = added.next();
                if (!applyAccessModeToTxState || allowed(next)) {
                    this.entity = next;
                    this.values = null;
                    if (tracer != null) {
                        traceOnEntity(tracer, entity);
                    }
                    return true;
                }
            }
        }

        if (needsValues && addedWithValues.hasNext()) {
            while (addedWithValues.hasNext()) {
                EntityWithPropertyValues entityWithPropertyValues = addedWithValues.next();
                long entityId = entityWithPropertyValues.getEntityId();
                if (!applyAccessModeToTxState || allowed(entityId)) {
                    this.entity = entityId;
                    this.values = entityWithPropertyValues.getValues();
                    if (tracer != null) {
                        traceOnEntity(tracer, entity);
                    }
                    return true;
                }
            }
        }

        if (added.hasNext() || addedWithValues.hasNext()) {
            throw new IllegalStateException(
                    "Index cursor cannot have transaction state with values and without values simultaneously");
        }

        boolean next = indexNext();
        if (tracer != null && next) {
            traceOnEntity(tracer, entity);
        }
        return next;
    }

    private boolean nextWithOrdering() {
        if (sortedMergeJoin.needsA() && addedWithValues.hasNext()) {
            while (addedWithValues.hasNext()) {
                EntityWithPropertyValues entityWithPropertyValues = addedWithValues.next();
                if (!applyAccessModeToTxState || canAccessEntityAndProperties(entityWithPropertyValues.getEntityId())) {
                    sortedMergeJoin.setA(entityWithPropertyValues.getEntityId(), entityWithPropertyValues.getValues());
                    break;
                }
            }
        }

        if (sortedMergeJoin.needsB()) {
            while (indexNext()) {
                if (!applyAccessModeToTxState || canAccessEntityAndProperties(entity)) {
                    sortedMergeJoin.setB(entity, values);
                    break;
                }
            }
        }

        boolean next = sortedMergeJoin.next(this);
        if (tracer != null && next) {
            traceOnEntity(tracer, entity);
        }
        return next;
    }

    @Override
    public final void acceptSortedMergeJoin(long entityId, Value[] values) {
        this.entity = entityId;
        this.values = values;
    }

    @Override
    public final void setRead(KernelRead read) {
        this.read = read;
    }

    @Override
    public final int numberOfProperties() {
        return query == null ? 0 : query.length;
    }

    @Override
    public final boolean hasValue() {
        return values != null;
    }

    @Override
    public final float score() {
        return score;
    }

    @Override
    public final Value propertyValue(int offset) {
        return values[offset];
    }

    @Override
    public final void closeInternal() {
        if (!isClosed()) {
            closeProgressor();
            this.entity = NO_ID;
            this.score = Float.NaN;
            this.query = null;
            this.values = null;
            this.read = null;
            this.added = ImmutableEmptyLongIterator.INSTANCE;
            this.addedWithValues = Collections.emptyIterator();
            this.removed = LongSets.immutable.empty();
        }
        super.closeInternal();
    }

    @Override
    public final boolean isClosed() {
        return isProgressorClosed();
    }

    @Override
    public String toString() {
        if (isClosed()) {
            return implementationName() + "[closed state]";
        } else {
            String keys = query == null
                    ? "unknown"
                    : Arrays.toString(
                            stream(query).map(PropertyIndexQuery::propertyKeyId).toArray(Integer[]::new));
            return implementationName() + "[entity=" + entity + ", open state with: keys=" + keys + ", values="
                    + Arrays.toString(values) + "]";
        }
    }

    private void prefixQuery(
            IndexDescriptor descriptor, Value[] equalityPrefix, PropertyIndexQuery.StringPrefixPredicate predicate) {
        TransactionState txState = read.txStateHolder.txState();

        if (needsValues) {
            AddedWithValuesAndRemoved changes = indexUpdatesWithValuesForRangeSeekByPrefix(
                    txState, descriptor, equalityPrefix, predicate.prefix(), indexOrder);
            addedWithValues = changes.added().iterator();
            removed = removed(txState, changes.removed());
        } else {
            AddedAndRemoved changes = indexUpdatesForRangeSeekByPrefix(
                    txState, descriptor, equalityPrefix, predicate.prefix(), indexOrder);
            added = changes.added().longIterator();
            removed = removed(txState, changes.removed());
        }
    }

    private void rangeQuery(
            IndexDescriptor descriptor, Value[] equalityPrefix, PropertyIndexQuery.RangePredicate<?> predicate) {
        TransactionState txState = read.txStateHolder.txState();

        if (needsValues) {
            AddedWithValuesAndRemoved changes =
                    indexUpdatesWithValuesForRangeSeek(txState, descriptor, equalityPrefix, predicate, indexOrder);
            addedWithValues = changes.added().iterator();
            removed = removed(txState, changes.removed());
        } else {
            AddedAndRemoved changes =
                    indexUpdatesForRangeSeek(txState, descriptor, equalityPrefix, predicate, indexOrder);
            added = changes.added().longIterator();
            removed = removed(txState, changes.removed());
        }
    }

    private void boundingBoxQuery(
            IndexDescriptor descriptor, Value[] equalityPrefix, PropertyIndexQuery.BoundingBoxPredicate predicate) {
        TransactionState txState = read.txStateHolder.txState();

        if (needsValues) {
            AddedWithValuesAndRemoved changes =
                    indexUpdatesWithValuesForBoundingBoxSeek(txState, descriptor, equalityPrefix, predicate);
            addedWithValues = changes.added().iterator();
            removed = removed(txState, changes.removed());
        } else {
            AddedAndRemoved changes = indexUpdatesForBoundingBoxSeek(txState, descriptor, equalityPrefix, predicate);
            added = changes.added().longIterator();
            removed = removed(txState, changes.removed());
        }
    }

    private void scanQuery(IndexDescriptor descriptor) {
        TransactionState txState = read.txStateHolder.txState();

        if (needsValues) {
            AddedWithValuesAndRemoved changes = indexUpdatesWithValuesForScan(txState, descriptor, indexOrder);
            addedWithValues = changes.added().iterator();
            removed = removed(txState, changes.removed());
        } else {
            AddedAndRemoved changes = indexUpdatesForScan(txState, descriptor, indexOrder);
            added = changes.added().longIterator();
            removed = removed(txState, changes.removed());
        }
    }

    private void suffixOrContainsQuery(IndexDescriptor descriptor, PropertyIndexQuery query) {
        TransactionState txState = read.txStateHolder.txState();

        if (needsValues) {
            AddedWithValuesAndRemoved changes =
                    indexUpdatesWithValuesForSuffixOrContains(txState, descriptor, query, indexOrder);
            addedWithValues = changes.added().iterator();
            removed = removed(txState, changes.removed());
        } else {
            AddedAndRemoved changes = indexUpdatesForSuffixOrContains(txState, descriptor, query, indexOrder);
            added = changes.added().longIterator();
            removed = removed(txState, changes.removed());
        }
    }

    private void seekQuery(IndexDescriptor descriptor, Value[] values) {
        TransactionState txState = read.txStateHolder.txState();

        if (needsValues) {
            AddedWithValuesAndRemoved changes =
                    indexUpdatesWithValuesForSeek(txState, descriptor, ValueTuple.of(values));
            addedWithValues = changes.added().iterator();
            removed = removed(txState, changes.removed());
        } else {
            AddedAndRemoved changes = indexUpdatesForSeek(txState, descriptor, ValueTuple.of(values));
            added = changes.added().longIterator();
            removed = removed(txState, changes.removed());
        }
    }

    final long entityReference() {
        return entity;
    }

    final void readEntity(EntityReader entityReader) {
        entityReader.read(read);
    }

    private boolean setupSecurity(IndexDescriptor descriptor) {
        return allowsAll() || canAccessAllDescribedEntities(descriptor);
    }

    private static int[] indexQueryKeys(PropertyIndexQuery[] query) {
        int[] keys = new int[query.length];
        for (int i = 0; i < query.length; i++) {
            keys[i] = query[i].propertyKeyId();
        }
        return keys;
    }

    final boolean allowed(long reference) {
        return shortcutSecurity || canAccessEntityAndProperties(reference);
    }

    /**
     * Check that the user is allowed to access all entities and properties given by the index descriptor.
     * <p>
     * If {@code true} is returned, it means that security check does not need to be performed for each item in the cursor.
     */
    abstract boolean canAccessAllDescribedEntities(IndexDescriptor descriptor);

    /**
     * Gets entities removed in the current transaction that are relevant for the index.
     */
    abstract LongSet removed(TransactionState txState, LongSet removedFromIndex);

    /**
     * Checks if the user is allowed to see the entity and properties the cursor is currently pointing at.
     */
    protected abstract boolean canAccessEntityAndProperties(long reference);

    /**
     * An abstraction over {@link KernelReadTracer#onNode(long)} and {@link KernelReadTracer#onRelationship(long)}.
     */
    abstract void traceOnEntity(KernelReadTracer tracer, long entity);

    /**
     * Name of the concrete implementation used in {@link #toString()}.
     */
    abstract String implementationName();

    @FunctionalInterface
    interface EntityReader {
        void read(KernelRead read);
    }
}
