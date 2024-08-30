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

import static java.lang.Math.min;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.AllRelationshipsScan;
import org.neo4j.storageengine.api.LongReference;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.string.Mask;

public class RecordRelationshipScanCursor extends RecordRelationshipCursor implements StorageRelationshipScanCursor {
    private final StoreCursors storeCursors;
    private final CursorContext cursorContext;
    private long next;
    private long highMark;
    private long nextStoreReference;
    private PageCursor currentCursor;
    private PageCursor singleCursor;
    private PageCursor scanCursor;
    private boolean open;
    private boolean batched;

    RecordRelationshipScanCursor(
            RelationshipStore relationshipStore,
            CursorContext cursorContext,
            StoreCursors storeCursors,
            MemoryTracker memoryTracker) {
        super(relationshipStore, cursorContext, memoryTracker);
        this.cursorContext = cursorContext;
        this.storeCursors = storeCursors;
    }

    @Override
    public void scan() {
        if (getId() != LongReference.NULL) {
            resetState();
        }
        selectScanCursor();
        this.next = 0;
        this.highMark = relationshipHighMark();
        this.nextStoreReference = LongReference.NULL;
        this.open = true;
    }

    @Override
    public void single(long reference) {
        if (getId() != LongReference.NULL) {
            resetState();
        }
        selectSingleCursor();
        this.next = reference >= 0 ? reference : LongReference.NULL;
        this.highMark = LongReference.NULL;
        this.nextStoreReference = LongReference.NULL;
        this.open = true;
    }

    @Override
    public void single(long reference, long sourceNodeReference, int type, long targetNodeReference) {
        single(reference);
    }

    @Override
    public boolean scanBatch(AllRelationshipsScan scan, long sizeHint) {
        if (getId() != LongReference.NULL) {
            reset();
        }
        this.batched = true;
        this.open = true;
        this.nextStoreReference = LongReference.NULL;

        return ((RecordRelationshipScan) scan).scanBatch(sizeHint, this);
    }

    boolean scanRange(long start, long stop) {
        long max = relationshipHighMark();
        if (start > max) {
            reset();
            return false;
        }
        if (start > stop) {
            reset();
            return true;
        }
        selectScanCursor();
        next = start;
        highMark = min(stop, max);
        return true;
    }

    @Override
    public boolean next() {
        if (next == LongReference.NULL) {
            resetState();
            return false;
        }

        do {
            if (nextStoreReference == next) {
                relationshipAdvance(this, currentCursor);
                next++;
                nextStoreReference++;
            } else {
                relationship(this, next++, currentCursor);
                nextStoreReference = next;
            }

            if (next > highMark) {
                if (isSingle() || batched) {
                    // we are a "single cursor" or a "batched scan"
                    // we don't want to set a new highMark
                    next = LongReference.NULL;
                    return inUse();
                } else {
                    // we are a "scan cursor"
                    // Check if there is a new high mark
                    highMark = relationshipHighMark();
                    if (next > highMark) {
                        next = LongReference.NULL;
                        return inUse();
                    }
                }
            }
        } while (!inUse());
        return true;
    }

    @Override
    public void reset() {
        if (open) {
            open = false;
            resetState();
        }
    }

    @Override
    protected void resetState() {
        super.resetState();
        setId(next = LongReference.NULL);
    }

    @Override
    public String toString(Mask mask) {
        if (!open) {
            return "RelationshipScanCursor[closed state]";
        } else {
            return "RelationshipScanCursor[id=" + getId() + ", open state with: highMark=" + highMark + ", next=" + next
                    + ", underlying record=" + super.toString(mask) + "]";
        }
    }

    private boolean isSingle() {
        return highMark == LongReference.NULL;
    }

    @Override
    public void close() {
        if (scanCursor != null) {
            scanCursor.close();
            scanCursor = null;
        }
        currentCursor = null;
        singleCursor = null; // Cursor owned by StoreCursors cache so not closed here
    }

    private void selectScanCursor() {
        // For node scans we used a local cursor to skip the overhead of positioning it on every node
        if (scanCursor == null) {
            scanCursor = relationshipStore.openPageCursorForReading(0, cursorContext);
        }
        currentCursor = scanCursor;
    }

    private void selectSingleCursor() {
        if (singleCursor == null) {
            singleCursor = storeCursors.readCursor(RecordCursorTypes.RELATIONSHIP_CURSOR);
        }
        currentCursor = singleCursor;
    }

    private void relationshipAdvance(RelationshipRecord record, PageCursor pageCursor) {
        // When scanning, we inspect RelationshipRecord.inUse(), so using RecordLoad.CHECK is fine
        relationshipStore.nextRecordByCursor(record, loadMode.orElse(CHECK).lenient(), pageCursor, memoryTracker);
    }
}
