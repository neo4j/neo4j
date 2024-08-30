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
package org.neo4j.internal.recordstorage;

import static org.neo4j.kernel.impl.store.record.RecordLoad.ALWAYS;
import static org.neo4j.storageengine.api.RelationshipDirection.INCOMING;
import static org.neo4j.storageengine.api.RelationshipDirection.LOOP;
import static org.neo4j.storageengine.api.RelationshipDirection.OUTGOING;

import org.neo4j.internal.counts.RelationshipGroupDegreesStore;
import org.neo4j.internal.helpers.Numbers;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.RecordLoadOverride;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.Degrees;
import org.neo4j.storageengine.api.LongReference;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.string.Mask;

class RecordRelationshipGroupCursor extends RelationshipGroupRecord implements AutoCloseable {
    private final RelationshipStore relationshipStore;
    private final RelationshipGroupStore groupStore;
    private final RelationshipGroupDegreesStore groupDegreesStore;
    private final CursorContext cursorContext;
    private final RelationshipRecord edge = new RelationshipRecord(LongReference.NULL);

    private PageCursor page;
    private PageCursor edgePage;
    private boolean open;
    RecordLoadOverride loadMode;
    private final StoreCursors storeCursors;
    private final MemoryTracker memoryTracker;

    RecordRelationshipGroupCursor(
            RelationshipStore relationshipStore,
            RelationshipGroupStore groupStore,
            RelationshipGroupDegreesStore groupDegreesStore,
            RecordLoadOverride loadMode,
            CursorContext cursorContext,
            StoreCursors storeCursors,
            MemoryTracker memoryTracker) {
        super(LongReference.NULL);
        this.relationshipStore = relationshipStore;
        this.groupStore = groupStore;
        this.groupDegreesStore = groupDegreesStore;
        this.cursorContext = cursorContext;
        this.loadMode = loadMode;
        this.storeCursors = storeCursors;
        this.memoryTracker = memoryTracker;
    }

    void init(long nodeReference, long reference, boolean nodeIsDense) {
        // the relationships for this node are not grouped in the store
        if (reference != LongReference.NULL && !nodeIsDense) {
            throw new UnsupportedOperationException("Not a dense node");
        } else // this is a normal group reference.
        {
            direct(nodeReference, reference);
        }
        open = true;
    }

    /**
     * Dense node, real groups iterated with every call to next.
     */
    void direct(long nodeReference, long reference) {
        clear();
        setOwningNode(nodeReference);
        setNext(reference);
        ensureCursor();
    }

    boolean next() {
        do {
            if (getNext() == LongReference.NULL) {
                // We have now run out of groups from the store, however there may still
                // be new types that was added in the transaction that we haven't visited yet.
                return false;
            }
            group(this, getNext(), page);
        } while (!inUse());

        return true;
    }

    boolean degree(Degrees.Mutator mutator, RelationshipSelection selection) {
        if (selection.test(getType())) {
            return count(outgoingRawId(), hasExternalDegreesOut(), OUTGOING, mutator, selection)
                    && count(incomingRawId(), hasExternalDegreesIn(), INCOMING, mutator, selection)
                    && count(loopsRawId(), hasExternalDegreesLoop(), LOOP, mutator, selection);
        }
        return true;
    }

    private boolean count(
            long reference,
            boolean hasExternal,
            RelationshipDirection direction,
            Degrees.Mutator mutator,
            RelationshipSelection selection) {
        if (reference == LongReference.NULL || !selection.test(direction)) {
            return true;
        }

        // Since we have a pointer we have at least 1. Sometimes that's all we're looking for
        if (!add(direction, mutator, 1)) {
            return false;
        }

        int count;
        if (hasExternal) {
            count = Numbers.safeCastLongToInt(groupDegreesStore.degree(getId(), direction, cursorContext));
        } else {
            if (edgePage == null) {
                edgePage = storeCursors.readCursor(RecordCursorTypes.RELATIONSHIP_CURSOR);
            }
            relationshipStore.getRecordByCursor(reference, edge, loadMode.orElse(ALWAYS), edgePage, memoryTracker);
            count = (int) edge.getPrevRel(getOwningNode());
        }
        return add(direction, mutator, Math.max(count - 1, 0));
    }

    private boolean add(RelationshipDirection direction, Degrees.Mutator mutator, int count) {
        return switch (direction) {
            case OUTGOING -> mutator.add(getType(), count, 0, 0);
            case INCOMING -> mutator.add(getType(), 0, count, 0);
            case LOOP -> mutator.add(getType(), 0, 0, count);
        };
    }

    @Override
    public String toString(Mask mask) {
        if (!open) {
            return "RelationshipGroupCursor[closed state]";
        } else {
            return "RelationshipGroupCursor[id=" + getId() + ", open state with: underlying record="
                    + super.toString(mask) + "]";
        }
    }

    /**
     * Implementation detail which provides the raw non-encoded outgoing relationship id
     */
    long outgoingRawId() {
        return getFirstOut();
    }

    /**
     * Implementation detail which provides the raw non-encoded incoming relationship id
     */
    long incomingRawId() {
        return getFirstIn();
    }

    /**
     * Implementation detail which provides the raw non-encoded loops relationship id
     */
    long loopsRawId() {
        return getFirstLoop();
    }

    @Override
    public void close() {
        // Cursors owned by StoreCursors cache so not closed here
        page = null;
        edgePage = null;
    }

    private void ensureCursor() {
        if (page == null) {
            page = storeCursors.readCursor(RecordCursorTypes.GROUP_CURSOR);
        }
    }

    private void group(RelationshipGroupRecord record, long reference, PageCursor page) {
        // We need to load forcefully here since otherwise we cannot traverse over groups
        // records which have been concurrently deleted (flagged as inUse = false).
        // @see #org.neo4j.kernel.impl.store.RelationshipChainPointerChasingTest
        groupStore.getRecordByCursor(reference, record, loadMode.orElse(ALWAYS), page, memoryTracker);
    }
}
