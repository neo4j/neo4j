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
import static org.neo4j.internal.kernel.api.TokenRead.ANY_RELATIONSHIP_TYPE;
import static org.neo4j.internal.recordstorage.RelationshipReferenceEncoding.encodeDense;
import static org.neo4j.storageengine.api.LongReference.longReference;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;

import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.neo4j.internal.counts.RelationshipGroupDegreesStore;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RecordLoadOverride;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.AllNodeScan;
import org.neo4j.storageengine.api.Degrees;
import org.neo4j.storageengine.api.LongReference;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.string.Mask;

public class RecordNodeCursor extends NodeRecord implements StorageNodeCursor {
    private final NodeStore read;
    private final RelationshipGroupDegreesStore groupDegreesStore;
    private final CursorContext cursorContext;
    private final StoreCursors storeCursors;
    private final RelationshipStore relationshipStore;
    private final RelationshipGroupStore groupStore;
    private PageCursor singleCursor;
    private PageCursor scanCursor;
    private PageCursor currentCursor;
    private long next;
    private long highMark;
    private long nextStoreReference;
    private boolean open;
    private boolean batched;
    private RecordRelationshipGroupCursor groupCursor;
    private RecordRelationshipTraversalCursor relationshipCursor;
    private RecordRelationshipScanCursor relationshipScanCursor;
    private RecordLoadOverride loadMode;
    private final MemoryTracker memoryTracker;

    RecordNodeCursor(
            NodeStore read,
            RelationshipStore relationshipStore,
            RelationshipGroupStore groupStore,
            RelationshipGroupDegreesStore groupDegreesStore,
            CursorContext cursorContext,
            StoreCursors storeCursors,
            MemoryTracker memoryTracker) {
        super(LongReference.NULL);
        this.read = read;
        this.groupDegreesStore = groupDegreesStore;
        this.cursorContext = cursorContext;
        this.storeCursors = storeCursors;
        this.relationshipStore = relationshipStore;
        this.groupStore = groupStore;
        this.memoryTracker = memoryTracker;
        this.loadMode = RecordLoadOverride.none();
    }

    @Override
    public void scan() {
        if (getId() != LongReference.NULL) {
            resetState();
        }
        selectScanCursor();
        this.next = 0;
        this.highMark = nodeHighMark();
        this.nextStoreReference = LongReference.NULL;
        this.open = true;
        this.batched = false;
    }

    @Override
    public void single(long reference) {
        if (getId() != LongReference.NULL) {
            resetState();
        }
        selectSingleCursor();
        this.next = reference >= 0 ? reference : LongReference.NULL;
        // This marks the cursor as a "single cursor"
        this.highMark = LongReference.NULL;
        this.nextStoreReference = LongReference.NULL;
        this.open = true;
        this.batched = false;
    }

    @Override
    public boolean scanBatch(AllNodeScan scan, long sizeHint) {
        if (getId() != LongReference.NULL) {
            reset();
        }
        this.batched = true;
        this.open = true;
        this.nextStoreReference = LongReference.NULL;

        return ((RecordNodeScan) scan).scanBatch(sizeHint, this);
    }

    boolean scanRange(long start, long stop) {
        long max = nodeHighMark();
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
    public long entityReference() {
        return getId();
    }

    @Override
    public int[] labels() {
        return NodeLabelsField.get(this, read, storeCursors, memoryTracker);
    }

    @Override
    public boolean hasLabel(int label) {
        return NodeLabelsField.hasLabel(this, read, storeCursors, label, memoryTracker);
    }

    @Override
    public boolean hasLabel() {
        return getLabelField() != Record.NO_LABELS_FIELD.intValue();
    }

    @Override
    public boolean hasProperties() {
        return nextProp != LongReference.NULL;
    }

    @Override
    public long relationshipsReference() {
        return relationshipsReferenceWithDenseMarker(getNextRel(), isDense());
    }

    /**
     * Marks a relationships reference with a special flag if the node is dense, because if that case the reference actually points
     * to a relationship group record.
     */
    static long relationshipsReferenceWithDenseMarker(long nextRel, boolean isDense) {
        return isDense ? encodeDense(nextRel) : nextRel;
    }

    @Override
    public void relationships(StorageRelationshipTraversalCursor traversalCursor, RelationshipSelection selection) {
        ((RecordRelationshipTraversalCursor) traversalCursor).init(this, selection);
    }

    @Override
    public boolean supportsFastRelationshipsTo() {
        return false;
    }

    @Override
    public void relationshipsTo(
            StorageRelationshipTraversalCursor traversalCursor,
            RelationshipSelection selection,
            long neighbourNodeReference) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int[] relationshipTypes() {
        MutableIntSet types = IntSets.mutable.empty();
        if (!isDense()) {
            ensureRelationshipTraversalCursorInitialized();
            relationshipCursor.init(this, ALL_RELATIONSHIPS);
            while (relationshipCursor.next()) {
                types.add(relationshipCursor.type());
            }
        } else {
            if (groupCursor == null) {
                groupCursor = new RecordRelationshipGroupCursor(
                        relationshipStore,
                        groupStore,
                        groupDegreesStore,
                        loadMode,
                        cursorContext,
                        storeCursors,
                        memoryTracker);
            }
            groupCursor.init(entityReference(), getNextRel(), true);
            while (groupCursor.next()) {
                types.add(groupCursor.getType());
            }
        }
        return types.toArray();
    }

    private void ensureRelationshipTraversalCursorInitialized() {
        if (relationshipCursor == null) {
            relationshipCursor = new RecordRelationshipTraversalCursor(
                    relationshipStore, groupStore, groupDegreesStore, cursorContext, storeCursors, memoryTracker);
        }
    }

    private void ensureRelationshipScanCursorInitialized() {
        if (relationshipScanCursor == null) {
            relationshipScanCursor =
                    new RecordRelationshipScanCursor(relationshipStore, cursorContext, storeCursors, memoryTracker);
        }
    }

    @Override
    public void degrees(RelationshipSelection selection, Degrees.Mutator mutator) {
        if (!mutator.isSplit() && !isDense() && !selection.isLimited()) {
            // There's an optimization for getting only the total degree directly
            ensureRelationshipScanCursorInitialized();
            relationshipScanCursor.single(getNextRel());
            if (relationshipScanCursor.next()) {
                int degree = relationshipScanCursor.sourceNodeReference() == getId()
                        ? (int) relationshipScanCursor.getFirstPrevRel()
                        : (int) relationshipScanCursor.getSecondPrevRel();
                mutator.add(ANY_RELATIONSHIP_TYPE, degree, 0, 0);
            }
            return;
        }

        if (!isDense()) {
            ensureRelationshipTraversalCursorInitialized();
            relationshipCursor.init(this, ALL_RELATIONSHIPS);
            while (relationshipCursor.next()) {
                if (selection.test(relationshipCursor.type())) {
                    int outgoing = 0;
                    int incoming = 0;
                    int loop = 0;
                    if (relationshipCursor.sourceNodeReference() == entityReference()) {
                        if (relationshipCursor.targetNodeReference() == entityReference()) {
                            loop++;
                        } else if (selection.test(RelationshipDirection.OUTGOING)) {
                            outgoing++;
                        }
                    } else if (selection.test(RelationshipDirection.INCOMING)) {
                        incoming++;
                    }
                    if (!mutator.add(relationshipCursor.type(), outgoing, incoming, loop)) {
                        return;
                    }
                }
            }
        } else {
            if (groupCursor == null) {
                groupCursor = new RecordRelationshipGroupCursor(
                        relationshipStore,
                        groupStore,
                        groupDegreesStore,
                        loadMode,
                        cursorContext,
                        storeCursors,
                        memoryTracker);
            }
            groupCursor.init(entityReference(), getNextRel(), isDense());
            int criteriaMet = 0;
            boolean typeLimited = selection.isTypeLimited();
            int numCriteria = selection.numberOfCriteria();
            while (groupCursor.next()) {
                int type = groupCursor.getType();
                if (selection.test(type)) {
                    if (!groupCursor.degree(mutator, selection)) {
                        return;
                    }
                    if (typeLimited && ++criteriaMet >= numCriteria) {
                        break;
                    }
                }
            }
        }
    }

    @Override
    public boolean supportsFastDegreeLookup() {
        return isDense();
    }

    @Override
    public void setForceLoad() {
        this.loadMode = RecordLoadOverride.FORCE;
        if (groupCursor != null) {
            groupCursor.loadMode = RecordLoadOverride.FORCE;
        }
    }

    @Override
    public Reference propertiesReference() {
        return longReference(getNextProp());
    }

    @Override
    public void properties(StoragePropertyCursor propertyCursor, PropertySelection selection) {
        propertyCursor.initNodeProperties(longReference(getNextProp()), selection);
    }

    @Override
    public boolean next() {
        if (next == LongReference.NULL) {
            resetState();
            return false;
        }

        do {
            if (nextStoreReference == next) {
                nodeAdvance(this, currentCursor);
                next++;
                nextStoreReference++;
            } else {
                node(this, next++, currentCursor);
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
                    highMark = nodeHighMark();
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

    private void resetState() {
        next = LongReference.NULL;
        setId(LongReference.NULL);
        clear();
        this.loadMode = RecordLoadOverride.none();
        if (groupCursor != null) {
            groupCursor.loadMode = RecordLoadOverride.none();
        }
    }

    private boolean isSingle() {
        return highMark == LongReference.NULL;
    }

    @Override
    public String toString(Mask mask) {
        if (!open) {
            return "RecordNodeCursor[closed state]";
        } else {
            return "RecordNodeCursor[id=" + getId() + ", open state with: highMark="
                    + highMark + ", next="
                    + next + ", underlying record="
                    + super.toString(mask) + "]";
        }
    }

    @Override
    public void close() {
        if (scanCursor != null) {
            scanCursor.close();
            scanCursor = null;
        }
        singleCursor = null; // Cursor owned by StoreCursors cache so not closed here
        currentCursor = null;
        if (groupCursor != null) {
            groupCursor.close();
            groupCursor = null;
        }
        if (relationshipCursor != null) {
            relationshipCursor.close();
            relationshipCursor = null;
        }
        if (relationshipScanCursor != null) {
            relationshipScanCursor.close();
            relationshipScanCursor = null;
        }
    }

    private void selectScanCursor() {
        // For node scans we used a local cursor to skip the overhead of positioning it on every node
        if (scanCursor == null) {
            scanCursor = read.openPageCursorForReading(0, cursorContext);
        }
        currentCursor = scanCursor;
    }

    private void selectSingleCursor() {
        if (singleCursor == null) {
            singleCursor = storeCursors.readCursor(RecordCursorTypes.NODE_CURSOR);
        }
        currentCursor = singleCursor;
    }

    private long nodeHighMark() {
        return read.getHighestPossibleIdInUse(cursorContext);
    }

    private void node(NodeRecord record, long reference, PageCursor pageCursor) {
        read.getRecordByCursor(
                reference, record, loadMode.orElse(RecordLoad.CHECK).lenient(), pageCursor, memoryTracker);
    }

    private void nodeAdvance(NodeRecord record, PageCursor pageCursor) {
        read.nextRecordByCursor(record, loadMode.orElse(RecordLoad.CHECK).lenient(), pageCursor, memoryTracker);
    }
}
