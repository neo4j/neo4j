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

import static java.lang.String.format;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;
import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.storageengine.api.LongReference;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;

class DefaultRelationshipTraversalCursor extends DefaultRelationshipCursor<DefaultRelationshipTraversalCursor>
        implements RelationshipTraversalCursor {
    private final StorageRelationshipTraversalCursor storeCursor;
    private final InternalCursorFactory internalCursors;
    private final boolean applyAccessModeToTxState;
    private DefaultNodeCursor securityNodeCursor;
    private LongIterator addedRelationships;
    private long originNodeReference;
    private RelationshipSelection selection;

    private long neighbourNodeReference;

    DefaultRelationshipTraversalCursor(
            CursorPool<DefaultRelationshipTraversalCursor> pool,
            StorageRelationshipTraversalCursor storeCursor,
            InternalCursorFactory internalCursors,
            boolean applyAccessModeToTxState) {
        super(storeCursor, pool);
        this.storeCursor = storeCursor;
        this.internalCursors = internalCursors;
        this.applyAccessModeToTxState = applyAccessModeToTxState;
    }

    /**
     * Initializes this cursor to traverse over relationships, with a reference that was earlier retrieved from {@link NodeCursor#relationshipsReference()}.
     *
     * @param nodeReference reference to the origin node.
     * @param reference reference to the place to start traversing these relationships.
     * @param selection the relationship selector
     * @param read reference to {@link KernelRead}.
     */
    void init(long nodeReference, long reference, RelationshipSelection selection, KernelRead read) {
        this.originNodeReference = nodeReference;
        this.selection = selection;
        this.neighbourNodeReference = -1;
        this.storeCursor.init(nodeReference, reference, selection);
        init(read);
        this.addedRelationships = ImmutableEmptyLongIterator.INSTANCE;
    }

    /**
     * Initializes this cursor to traverse over relationships, directly from the {@link NodeCursor}.
     *
     * @param nodeCursor {@link NodeCursor} at the origin node.
     * @param selection the relationship selector
     * @param read reference to {@link KernelRead}.
     */
    void init(DefaultNodeCursor nodeCursor, RelationshipSelection selection, KernelRead read) {
        this.originNodeReference = nodeCursor.nodeReference();
        this.selection = selection;
        this.neighbourNodeReference = LongReference.NULL;
        if (!nodeCursor.currentNodeIsAddedInTx()) {
            nodeCursor.storeCursor.relationships(storeCursor, selection);
        } else {
            storeCursor.reset();
        }
        init(read);
        this.addedRelationships = ImmutableEmptyLongIterator.INSTANCE;
    }

    /**
     * Initializes this cursor to access a details of a relationship from the transaction state using the ID provided.
     *
     * @param addedRelationship the relationship to access
     * @param read reference to {@link KernelRead}.
     */
    void init(long addedRelationship, KernelRead read) {
        assert addedRelationship != LongReference.NULL;
        this.originNodeReference = LongReference.NULL;
        this.neighbourNodeReference = LongReference.NULL;
        this.selection = null;
        storeCursor.reset();
        init(read);
        this.checkHasChanges = false;
        this.hasChanges = true;
        this.addedRelationships = PrimitiveLongCollections.single(addedRelationship);
    }

    void init(
            DefaultNodeCursor nodeCursor,
            RelationshipSelection selection,
            long neighbourNodeReference,
            KernelRead read) {
        this.originNodeReference = nodeCursor.nodeReference();
        this.selection = selection;
        this.neighbourNodeReference = neighbourNodeReference;
        if (!nodeCursor.currentNodeIsAddedInTx()) {
            nodeCursor.storeCursor.relationshipsTo(storeCursor, selection, neighbourNodeReference);
        }
        init(read);
        this.addedRelationships = ImmutableEmptyLongIterator.INSTANCE;
    }

    @Override
    public void otherNode(NodeCursor cursor) {
        read.singleNode(otherNodeReference(), cursor);
    }

    @Override
    public long otherNodeReference() {
        if (currentAddedInTx != LongReference.NULL) {
            // Here we compare the source/target nodes from tx-state to the origin node and decide the neighbour node
            // from it
            long originNodeReference = originNodeReference();
            if (txStateSourceNodeReference == originNodeReference) {
                return txStateTargetNodeReference;
            } else if (txStateTargetNodeReference == originNodeReference) {
                return txStateSourceNodeReference;
            } else {
                throw new IllegalStateException(format(
                        "Relationship[%d] which was added in tx has an origin node [%d] which is neither source [%d] nor target [%d]",
                        currentAddedInTx, originNodeReference, txStateSourceNodeReference, txStateTargetNodeReference));
            }
        }
        return storeCursor.neighbourNodeReference();
    }

    @Override
    public long originNodeReference() {
        return originNodeReference;
    }

    @Override
    public boolean next() {
        boolean hasChanges = hasChanges();

        // tx-state relationships
        if (hasChanges) {
            while (addedRelationships.hasNext()) {
                long next = addedRelationships.next();
                read.txStateHolder.txState().relationshipVisit(next, relationshipTxStateDataVisitor);

                if (!applyAccessModeToTxState || allowed()) {
                    if (neighbourNodeReference != LongReference.NULL
                            && otherNodeReference() != neighbourNodeReference) {
                        continue;
                    }

                    if (tracer != null) {
                        tracer.onRelationship(relationshipReference());
                    }
                    return true;
                }
            }
            currentAddedInTx = LongReference.NULL;
        }

        while (storeCursor.next()) {
            boolean skip = hasChanges
                    && read.txStateHolder.txState().relationshipIsDeletedInThisBatch(storeCursor.entityReference());
            if (!skip && allowed()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void setTracer(KernelReadTracer tracer) {
        super.setTracer(tracer);
        storeCursor.setTracer(tracer);
    }

    @Override
    public void removeTracer() {
        storeCursor.removeTracer();
        super.removeTracer();
    }

    protected boolean allowed() {
        AccessMode accessMode = read.getAccessMode();
        if (accessMode.allowsTraverseRelType(type())) {
            if (accessMode.allowsTraverseAllLabels()) {
                return true;
            }
            if (securityNodeCursor == null) {
                securityNodeCursor = internalCursors.allocateNodeCursor();
            }
            if (applyAccessModeToTxState
                    && this.currentAddedInTx != LongReference.NULL
                    && neighbourNodeReference != LongReference.NULL) {
                read.singleNode(neighbourNodeReference, securityNodeCursor);
            } else {
                read.singleNode(storeCursor.neighbourNodeReference(), securityNodeCursor);
            }
            return securityNodeCursor.next();
        }
        return false;
    }

    @Override
    public void closeInternal() {
        if (!isClosed()) {
            read = null;
            selection = null;
            storeCursor.close();

            if (securityNodeCursor != null) {
                securityNodeCursor.close();
                securityNodeCursor.release();
                securityNodeCursor = null;
            }
        }
        super.closeInternal();
    }

    @Override
    protected void collectAddedTxStateSnapshot() {
        if (selection != null) {
            addedRelationships =
                    selection.addedRelationships(read.txStateHolder.txState().getNodeState(originNodeReference));
        }
    }

    @Override
    public boolean isClosed() {
        return read == null;
    }

    @Override
    public void release() {
        if (storeCursor != null) {
            storeCursor.close();
        }
        if (securityNodeCursor != null) {
            securityNodeCursor.close();
            securityNodeCursor.release();
            securityNodeCursor = null;
        }
    }

    @Override
    public String toString() {
        if (isClosed()) {
            return "RelationshipTraversalCursor[closed state]";
        } else {
            return "RelationshipTraversalCursor[id=" + storeCursor.entityReference() + ", " + storeCursor + "]";
        }
    }
}
