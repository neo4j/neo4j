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

import static org.neo4j.kernel.impl.newapi.Read.NO_ID;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.storageengine.api.AllRelationshipsScan;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;

class DefaultRelationshipScanCursor extends DefaultRelationshipCursor implements RelationshipScanCursor {
    private final StorageRelationshipScanCursor storeCursor;
    private final InternalCursorFactory internalCursors;
    private final boolean applyAccessModeToTxState;
    private long single;
    private boolean isSingle;
    private LongIterator addedRelationships;
    private DefaultNodeCursor securityNodeCursor;

    DefaultRelationshipScanCursor(
            CursorPool<DefaultRelationshipScanCursor> pool,
            StorageRelationshipScanCursor storeCursor,
            InternalCursorFactory internalCursors,
            boolean applyAccessModeToTxState) {
        super(storeCursor, pool);
        this.storeCursor = storeCursor;
        this.internalCursors = internalCursors;
        this.applyAccessModeToTxState = applyAccessModeToTxState;
    }

    void scan(Read read) {
        storeCursor.scan();
        this.single = NO_ID;
        this.isSingle = false;
        init(read);
        this.addedRelationships = ImmutableEmptyLongIterator.INSTANCE;
    }

    boolean scanBatch(
            Read read, AllRelationshipsScan scan, long sizeHint, LongIterator addedRelationships, boolean hasChanges) {
        this.read = read;
        this.single = NO_ID;
        this.isSingle = false;
        this.currentAddedInTx = NO_ID;
        this.addedRelationships = addedRelationships;
        this.hasChanges = hasChanges;
        this.checkHasChanges = false;
        boolean scanBatch = storeCursor.scanBatch(scan, sizeHint);
        return addedRelationships.hasNext() || scanBatch;
    }

    void single(long reference, Read read) {
        storeCursor.single(reference);
        this.single = reference;
        this.isSingle = true;
        init(read);
        this.addedRelationships = ImmutableEmptyLongIterator.INSTANCE;
    }

    void single(long reference, long sourceNodeReference, int type, long targetNodeReference, Read read) {
        storeCursor.single(reference, sourceNodeReference, type, targetNodeReference);
        this.single = reference;
        this.isSingle = true;
        init(read);
        this.addedRelationships = ImmutableEmptyLongIterator.INSTANCE;
    }

    @Override
    public boolean next() {
        // Check tx state
        boolean hasChanges = hasChanges();

        if (hasChanges) {
            if (addedRelationships.hasNext()) {
                read.txState().relationshipVisit(addedRelationships.next(), relationshipTxStateDataVisitor);
                if (tracer != null) {
                    tracer.onRelationship(relationshipReference());
                }
                return true;
            } else {
                currentAddedInTx = NO_ID;
            }
        }

        while (storeCursor.next()) {
            boolean skip = hasChanges && read.txState().relationshipIsDeletedInThisBatch(storeCursor.entityReference());
            if (!skip && allowed()) {
                if (tracer != null) {
                    tracer.onRelationship(relationshipReference());
                }
                return true;
            }
        }
        return false;
    }

    protected boolean allowed() {
        AccessMode accessMode = read.getAccessMode();
        return accessMode.allowsTraverseRelType(storeCursor.type()) && allowedToSeeEndNode(accessMode);
    }

    private boolean allowedToSeeEndNode(AccessMode mode) {
        if (mode.allowsTraverseAllLabels()) {
            return true;
        }
        if (securityNodeCursor == null) {
            securityNodeCursor = internalCursors.allocateNodeCursor();
        }
        read.singleNode(storeCursor.sourceNodeReference(), securityNodeCursor);
        if (securityNodeCursor.next()) {
            read.singleNode(storeCursor.targetNodeReference(), securityNodeCursor);
            return securityNodeCursor.next();
        }
        return false;
    }

    @Override
    public void closeInternal() {
        if (!isClosed()) {
            read = null;
            storeCursor.close();
        }
        super.closeInternal();
    }

    @Override
    public boolean isClosed() {
        return read == null;
    }

    @Override
    public String toString() {
        if (isClosed()) {
            return "RelationshipScanCursor[closed state]";
        } else {
            return "RelationshipScanCursor[id=" + storeCursor.entityReference() + ", open state with: single="
                    + single + ", "
                    + storeCursor + "]";
        }
    }

    @Override
    protected void collectAddedTxStateSnapshot() {
        if (isSingle) {
            addedRelationships = read.txState().relationshipIsAddedInThisBatch(single)
                    ? LongHashSet.newSetWith(single).longIterator()
                    : ImmutableEmptyLongIterator.INSTANCE;
        } else {
            addedRelationships =
                    read.txState().addedAndRemovedRelationships().getAdded().longIterator();
        }
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
}
