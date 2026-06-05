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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;
import org.neo4j.internal.kernel.api.EntityIndexCursor;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.ValueIndexCursor;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.internal.schema.IndexRemovalSnapshot;
import org.neo4j.kernel.api.AccessModeProvider;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.newapi.TxStateIndexChanges.AddedAndRemoved;
import org.neo4j.kernel.impl.newapi.TxStateIndexChanges.AddedWithValuesAndRemoved;
import org.neo4j.storageengine.api.LongReference;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.values.storable.Value;

public abstract class DefaultEntityValueIndexCursor<CURSOR> extends IndexCursor<IndexProgressor, CURSOR>
        implements ValueIndexCursor, EntityIndexSeekClient, SortedMergeJoin.Sink, EntityIndexCursor {
    protected Read read;
    protected TxStateHolder txStateHolder;
    protected AccessModeProvider accessModeProvider;
    protected AccessMode accessMode;
    protected long entity;
    private float score;
    private PropertyIndexQuery[] query;
    private Value[] values;

    private LongIterator added = ImmutableEmptyLongIterator.INSTANCE;
    private Iterator<EntityWithPropertyValues> addedWithValues = Collections.emptyIterator();
    private LongSetContains removed = (value) -> false;
    private boolean needsValues;
    private IndexOrder indexOrder;
    private final SortedMergeJoin sortedMergeJoin = new SortedMergeJoin();
    private boolean shortcutSecurity;
    private boolean needStoreFilter;
    private PropertySelection propertySelection;
    protected final boolean applyAccessModeToTxState;
    private int numberOfProperties;

    DefaultEntityValueIndexCursor(CursorPool<CURSOR> pool, boolean applyAccessModeToTxState) {
        super(pool);
        entity = LongReference.NULL;
        score = Float.NaN;
        indexOrder = IndexOrder.NONE;
        this.applyAccessModeToTxState = applyAccessModeToTxState;
    }

    @Override
    public final void initializeQuery(
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
        // The query[] length doesn't always match the number of properties in the index since we allow a single
        // "*" query even for composite indexes.
        numberOfProperties = descriptor.schema().getPropertyIds().length;

        this.query = query;

        if (tracer != null) {
            tracer.onIndexSeek(descriptor);
        }

        shortcutSecurity = canAccessAllDescribedEntities(descriptor);

        if (!indexIncludesTransactionState && txStateHolder.hasTxStateWithChanges() && query.length > 0) {
            int i = 0;
            while (i < query.length && query[i].type() == IndexQueryType.EXACT) {
                i++;
            }

            if (i == query.length) {
                // Only exact queries — no need to order, all values are the same.
                this.indexOrder = IndexOrder.NONE;
                consultTxState(descriptor);
            } else {
                PropertyIndexQuery nextQuery = query[i];
                switch (nextQuery.type()) {
                    case ALL_ENTRIES, EXISTS -> {
                        // This also covers the rewritten suffix/contains for composite index
                        // If composite index all following will be exists as well so no need to consider those
                        setNeedsValuesIfRequiresOrder();
                        consultTxState(descriptor);
                    }

                    case RANGE -> {
                        // This case covers first query to be range or exact followed by range
                        // If composite index all following will be exists as well so no need to consider those
                        setNeedsValuesIfRequiresOrder();
                        consultTxState(descriptor);
                    }

                    case BOUNDING_BOX -> {
                        // This case covers first query to be bounding box or exact followed by bounding box
                        // If composite index all following will be exists as well so no need to consider those
                        setNeedsValuesIfRequiresOrder();
                        consultTxState(descriptor);
                    }

                    case STRING_PREFIX -> {
                        // This case covers first query to be prefix or exact followed by prefix
                        // If composite index all following will be exists as well so no need to consider those
                        setNeedsValuesIfRequiresOrder();
                        consultTxState(descriptor);
                    }

                    case STRING_SUFFIX, STRING_CONTAINS -> {
                        // This case covers suffix/contains for singular indexes
                        // for composite index, the suffix/contains should already
                        // have been rewritten as exists + filter, so no need to consider it here
                        assert query.length == 1;
                        consultTxState(descriptor);
                    }

                    case NEAREST_NEIGHBORS -> {
                        // Vector indexes currently ignore transaction state.
                        // TODO VECTOR: handle transaction state!
                    }

                    default ->
                        throw new UnsupportedOperationException("Query not supported: " + Arrays.toString(query));
                }
            }
        }
    }

    private void consultTxState(IndexDescriptor descriptor) {
        TransactionState txState = txStateHolder.txState();
        if (needsValues) {
            AddedWithValuesAndRemoved changes =
                    TxStateIndexChanges.computeForQueryWithValues(txState, descriptor, indexOrder, query);
            addedWithValues = changes.added().iterator();
            removed = removed(txState, changes.removed());
        } else {
            AddedAndRemoved changes =
                    TxStateIndexChanges.computeForQueryWithoutValues(txState, descriptor, indexOrder, query);
            added = changes.added().longIterator();
            removed = removed(txState, changes.removed());
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
                if (!applyAccessModeToTxState || allowed(entityWithPropertyValues.getEntityId())) {
                    sortedMergeJoin.setA(entityWithPropertyValues.getEntityId(), entityWithPropertyValues.getValues());
                    break;
                }
            }
        }

        if (sortedMergeJoin.needsB()) {
            while (indexNext()) {
                if (!applyAccessModeToTxState || allowed(entity)) {
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
    public final void initState(
            Read read,
            TxStateHolder txStateHolder,
            AccessModeProvider accessModeProvider,
            boolean includeChangesFromThisTransaction) {
        this.read = read;
        this.txStateHolder = includeChangesFromThisTransaction ? txStateHolder : TxStateHolder.EMPTY_TX_STATE;
        this.accessModeProvider = accessModeProvider;
        this.accessMode = accessModeProvider.getAccessMode();
    }

    @Override
    public final int numberOfProperties() {
        return numberOfProperties;
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
            this.entity = LongReference.NULL;
            this.score = Float.NaN;
            this.query = null;
            this.values = null;
            this.read = null;
            this.txStateHolder = null;
            this.accessModeProvider = null;
            this.accessMode = null;
            this.added = ImmutableEmptyLongIterator.INSTANCE;
            this.addedWithValues = Collections.emptyIterator();
            this.removed = (value) -> false;
            this.numberOfProperties = 0;
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

    final long entityReference() {
        return entity;
    }

    private static int[] indexQueryKeys(PropertyIndexQuery[] query) {
        int[] keys = new int[query.length];
        for (int i = 0; i < query.length; i++) {
            keys[i] = query[i].propertyKeyId();
        }
        return keys;
    }

    private boolean allowed(long reference) {
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
    abstract LongSetContains removed(TransactionState txState, IndexRemovalSnapshot removedFromIndex);

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
    protected interface LongSetContains {
        boolean contains(long value);
    }
}
