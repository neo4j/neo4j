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

import static org.neo4j.collection.PrimitiveLongCollections.mergeToSet;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.txstate.TransactionState;

class DefaultNodeLabelIndexCursor extends DefaultEntityTokenIndexCursor<DefaultNodeLabelIndexCursor>
        implements NodeLabelIndexCursor {
    private final InternalCursorFactory internalCursors;
    private final boolean applyAccessModeToTxState;
    private DefaultNodeCursor securityNodeCursor;

    DefaultNodeLabelIndexCursor(
            CursorPool<DefaultNodeLabelIndexCursor> pool,
            InternalCursorFactory internalCursors,
            boolean applyAccessModeToTxState) {
        super(pool);
        this.internalCursors = internalCursors;
        this.applyAccessModeToTxState = applyAccessModeToTxState;
    }

    @Override
    protected boolean innerNext() {
        return indexNext();
    }

    @Override
    protected LongIterator createAddedInTxState(TransactionState txState, int token, IndexOrder order) {
        return sortTxState(txState.nodesWithLabelChanged(token).getAdded().freeze(), order);
    }

    @Override
    protected LongSet createDeletedInTxState(TransactionState txState, int token) {
        return mergeToSet(
                        txState.addedAndRemovedNodes().getRemoved(),
                        txState.nodesWithLabelChanged(token).getRemoved())
                .asUnmodifiable();
    }

    @Override
    protected void traceScan(KernelReadTracer tracer, int token) {
        tracer.onLabelScan(token);
    }

    @Override
    protected void traceNext(KernelReadTracer tracer, long entity) {
        tracer.onNode(entity);
    }

    @Override
    protected boolean allowedToSeeAllEntitiesWithToken(int token) {
        return read.getAccessMode().allowsTraverseAllNodesWithLabel(token);
    }

    @Override
    protected boolean allowedToSeeEntity(long entityReference) {
        if (read.getAccessMode().allowsTraverseAllLabels()) {
            return true;
        }
        if (securityNodeCursor == null) {
            securityNodeCursor = internalCursors.allocateNodeCursor();
        }
        read.singleNode(entityReference, securityNodeCursor);
        return securityNodeCursor.next();
    }

    @Override
    public void node(NodeCursor cursor) {
        read.singleNode(entityReference(), cursor);
    }

    @Override
    public long nodeReference() {
        return entityReference();
    }

    @Override
    public float score() {
        return Float.NaN;
    }

    @Override
    public String toString() {
        if (isClosed()) {
            return "NodeLabelIndexCursor[closed state]";
        } else {
            return "NodeLabelIndexCursor[node=" + entityReference() + ", label= " + tokenId + "]";
        }
    }

    @Override
    public void release() {
        if (securityNodeCursor != null) {
            securityNodeCursor.close();
            securityNodeCursor.release();
            securityNodeCursor = null;
        }
    }
}
