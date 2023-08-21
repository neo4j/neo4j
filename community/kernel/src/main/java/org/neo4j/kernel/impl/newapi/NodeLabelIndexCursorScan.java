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
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.index.schema.TokenScan;

class NodeLabelIndexCursorScan extends BaseCursorScan<NodeLabelIndexCursor, TokenScan> {
    private final LongSet removed;
    private final int label;

    NodeLabelIndexCursorScan(Read read, int label, TokenScan tokenScan) {
        super(
                tokenScan,
                read,
                () -> read.txState().nodesWithLabelChanged(label).getAdded().toArray());
        this.label = label;
        if (hasChanges) {
            TransactionState txState = read.txState();
            this.removed = mergeToSet(
                    txState.addedAndRemovedNodes().getRemoved(),
                    txState.nodesWithLabelChanged(label).getRemoved());
        } else {
            this.removed = LongSets.immutable.empty();
        }
    }

    @Override
    protected boolean scanStore(
            NodeLabelIndexCursor cursor,
            int sizeHint,
            LongIterator addedItems,
            CursorContext cursorContext,
            AccessMode accessMode) {
        DefaultNodeLabelIndexCursor indexCursor = (DefaultNodeLabelIndexCursor) cursor;
        indexCursor.setRead(read);
        IndexProgressor indexProgressor = storageScan.initializeBatch(indexCursor, sizeHint, cursorContext);

        if (indexProgressor == IndexProgressor.EMPTY && !addedItems.hasNext()) {
            return false;
        } else {
            indexCursor.initialize(indexProgressor, label, addedItems, removed, accessMode);
            return true;
        }
    }
}
