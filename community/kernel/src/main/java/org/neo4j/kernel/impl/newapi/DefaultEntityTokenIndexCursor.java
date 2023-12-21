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
import static org.neo4j.kernel.impl.newapi.Read.NO_ID;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.txstate.TransactionState;

/**
 * Base for index cursors that can handle scans with IndexOrder.
 */
abstract class DefaultEntityTokenIndexCursor<SELF extends DefaultEntityTokenIndexCursor<SELF>>
        extends IndexCursor<IndexProgressor, SELF> implements InternalTokenIndexCursor {
    protected Read read;

    protected long entity;
    protected long entityFromIndex;

    protected int tokenId;

    private LongIterator added;
    private LongSet removed;
    private boolean useMergeSort;
    private final PrimitiveSortedMergeJoin sortedMergeJoin = new PrimitiveSortedMergeJoin();
    private boolean shortcutSecurity;

    DefaultEntityTokenIndexCursor(CursorPool<SELF> pool) {
        super(pool);
        this.entity = NO_ID;
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

    @Override
    public void initialize(IndexProgressor progressor, int token, IndexOrder order) {
        initialize(progressor);
        if (read.hasTxStateWithChanges()) {
            added = createAddedInTxState(read.txState(), token, order);
            removed = createDeletedInTxState(read.txState(), token);
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
    }

    @Override
    public void initialize(IndexProgressor progressor, int token, LongIterator added, LongSet removed) {
        initialize(progressor);
        useMergeSort = false;
        this.added = added;
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
        entity = NO_ID;
        entityFromIndex = NO_ID;
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
            entity = NO_ID;
            entityFromIndex = NO_ID;
            tokenId = (int) NO_ID;
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
    public void setRead(Read read) {
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
            entity = added.next();
        } else if (innerNext()) {
            entity = nextEntity();
        }

        return entity != NO_ID;
    }

    private boolean nextWithOrdering() {
        // items from Tx state
        if (sortedMergeJoin.needsA() && added.hasNext()) {
            sortedMergeJoin.setA(added.next());
        }

        // items from index/store
        if (sortedMergeJoin.needsB() && innerNext()) {
            sortedMergeJoin.setB(entityFromIndex);
        }

        final var nextId = sortedMergeJoin.next();
        if (nextId == NO_ID) {
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
}
