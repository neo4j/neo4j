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

import static org.neo4j.collection.PrimitiveLongCollections.iterator;
import static org.neo4j.collection.PrimitiveLongCollections.reverseIterator;
import static org.neo4j.internal.schema.IndexOrder.DESCENDING;

import java.util.NoSuchElementException;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.index.schema.TokenScanValueIndexProgressor;
import org.neo4j.storageengine.api.LongReference;

/**
 * Base for index cursors that can handle scans with IndexOrder.
 */
abstract class DefaultEntityTokenIndexCursor<SELF extends DefaultEntityTokenIndexCursor<SELF>>
        extends IndexCursor<IndexProgressor, SELF> implements InternalTokenIndexCursor {
    protected KernelRead read;

    protected long entity;
    protected long entityFromIndex;

    protected int tokenId;
    // Defaulting order to ASCENDING, argument being that if it is important that you do not assume a order you can
    // specify none
    // And if you are indifferent to index order BUT you can treat an index with order then it might be beneficial
    // to assume ascending for functionality like skipUntil
    protected IndexOrder order = IndexOrder.ASCENDING;
    private PeekableLongIterator added;
    private LongSet removed;
    private boolean useMergeSort;
    private final PrimitiveSortedMergeJoin sortedMergeJoin = new PrimitiveSortedMergeJoin();
    private boolean shortcutSecurity;
    private final boolean applyAccessModeToTxState;

    DefaultEntityTokenIndexCursor(CursorPool<SELF> pool, boolean applyAccessModeToTxState) {
        super(pool);
        this.entity = LongReference.NULL;
        this.applyAccessModeToTxState = applyAccessModeToTxState;
    }

    @Override
    public abstract void release();

    protected abstract boolean innerNext();

    protected abstract LongIterator createAddedInTxState(TransactionState txState, int token, IndexOrder order);

    /**
     * The returned LongSet must be immutable or a private copy.
     */
    protected abstract LongSet createDeletedInTxState(TransactionState txState, int token);

    protected abstract void traceScan(KernelReadTracer tracer, int token);

    protected abstract void traceNext(KernelReadTracer tracer, long entity);

    protected abstract boolean allowedToSeeAllEntitiesWithToken(int token);

    protected abstract boolean allowedToSeeEntity(long entityReference);

    private PeekableLongIterator peekable(LongIterator actual) {
        return actual != null ? new PeekableLongIterator(actual) : null;
    }

    @Override
    public void initialize(IndexProgressor progressor, int token, IndexOrder order) {
        initialize(progressor);
        if (read.txStateHolder.hasTxStateWithChanges()) {
            added = peekable(createAddedInTxState(read.txStateHolder.txState(), token, order));
            removed = createDeletedInTxState(read.txStateHolder.txState(), token);
            useMergeSort = order != IndexOrder.NONE;
            if (useMergeSort) {
                sortedMergeJoin.initialize(order);
            }
        } else {
            useMergeSort = false;
        }
        tokenId = token;
        initSecurity(token);

        if (tracer != null) {
            traceScan(tracer, token);
        }
        this.order = order;
    }

    @Override
    public void initialize(IndexProgressor progressor, int token, LongIterator added, LongSet removed) {
        initialize(progressor);
        useMergeSort = false;
        this.added = peekable(added);
        this.removed = removed;
        this.tokenId = token;
        initSecurity(token);

        if (tracer != null) {
            traceScan(tracer, token);
        }
    }

    @Override
    public boolean acceptEntity(long reference, int tokenId) {
        if (isRemoved(reference) || !allowed(reference)) {
            return false;
        }
        this.entityFromIndex = reference;
        this.tokenId = tokenId;

        return true;
    }

    @Override
    public boolean next() {
        entity = LongReference.NULL;
        entityFromIndex = LongReference.NULL;
        final var hasNext = useMergeSort ? nextWithOrdering() : nextWithoutOrder();
        if (hasNext && tracer != null) {
            traceNext(tracer, entity);
        }
        return hasNext;
    }

    @Override
    public void closeInternal() {
        if (!isClosed()) {
            closeProgressor();
            entity = LongReference.NULL;
            entityFromIndex = LongReference.NULL;
            tokenId = (int) LongReference.NULL;
            read = null;
            added = null;
            removed = null;
        }
        super.closeInternal();
    }

    @Override
    public boolean isClosed() {
        return isProgressorClosed();
    }

    @Override
    public void setRead(KernelRead read) {
        this.read = read;
    }

    public long entityReference() {
        return entity;
    }

    protected boolean allowed(long reference) {
        return shortcutSecurity || allowedToSeeEntity(reference);
    }

    protected long nextEntity() {
        return entityFromIndex;
    }

    private void initSecurity(int token) {
        shortcutSecurity = allowedToSeeAllEntitiesWithToken(token);
    }

    private boolean nextWithoutOrder() {
        if (added != null && added.hasNext()) {
            while (added.hasNext()) {
                long next = added.next();
                if (!applyAccessModeToTxState || allowed(next)) {
                    entity = next;
                    break;
                }
            }
        }

        if (entity == LongReference.NULL) {
            while (innerNext()) {
                long next = nextEntity();
                if (!applyAccessModeToTxState || allowed(next)) {
                    entity = next;
                    break;
                }
            }
        }

        return entity != LongReference.NULL;
    }

    private boolean nextWithOrdering() {
        // items from Tx state
        if (sortedMergeJoin.needsA() && added.hasNext()) {
            while (added.hasNext()) {
                long next = added.next();
                if (!applyAccessModeToTxState || allowed(next)) {
                    sortedMergeJoin.setA(next);
                    break;
                }
            }
        }

        // items from index/store
        if (sortedMergeJoin.needsB()) {
            while (innerNext()) {
                if (!applyAccessModeToTxState || allowed(entityFromIndex)) {
                    sortedMergeJoin.setB(entityFromIndex);
                    break;
                }
            }
        }

        final var nextId = sortedMergeJoin.next();
        if (nextId == LongReference.NULL) {
            return false;
        } else {
            entity = nextId;
            return true;
        }
    }

    private boolean isRemoved(long reference) {
        return removed != null && removed.contains(reference);
    }

    protected static LongIterator sortTxState(LongSet frozenAdded, IndexOrder order) {
        return switch (order) {
            case NONE -> frozenAdded.longIterator();
            case ASCENDING, DESCENDING -> sorted(frozenAdded.toSortedArray(), order);
        };
    }

    private static LongIterator sorted(long[] items, IndexOrder order) {
        return DESCENDING == order ? reverseIterator(items) : iterator(items);
    }

    public void skipUntil(long id) {
        TokenScanValueIndexProgressor indexProgressor = (TokenScanValueIndexProgressor) progressor;

        if (order == IndexOrder.NONE) {
            throw new IllegalStateException("IndexOrder " + order + " not supported for skipUntil");
        }

        if (added != null) {
            if (order != DESCENDING) {
                while (added.hasNext() && added.peek() < id) {
                    added.next();
                }
            } else {
                while (added.hasNext() && added.peek() > id) {
                    added.next();
                }
            }
        }

        // Move progressor to correct spot
        indexProgressor.skipUntil(id);
    }

    private static class PeekableLongIterator extends PrimitiveLongCollections.AbstractPrimitiveLongBaseIterator {
        private final LongIterator iterator;

        PeekableLongIterator(LongIterator iterator) {
            this.iterator = iterator;
        }

        @Override
        protected boolean fetchNext() {
            return iterator.hasNext() && next(iterator.next());
        }

        public long peek() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return next;
        }
    }
}
