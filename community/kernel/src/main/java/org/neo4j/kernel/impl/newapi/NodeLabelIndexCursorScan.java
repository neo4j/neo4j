/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.newapi;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;

import org.neo4j.internal.index.label.LabelScan;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.txstate.TransactionState;

import static org.neo4j.collection.PrimitiveLongCollections.mergeToSet;

class NodeLabelIndexCursorScan extends BaseCursorScan<NodeLabelIndexCursor,LabelScan>
{
    private final LongSet removed;
    private final int label;

    NodeLabelIndexCursorScan( Read read, int label, LabelScan labelScan )
    {
        super( labelScan, read, () -> read.txState().nodesWithLabelChanged( label ).getAdded().toArray() );
        if ( hasChanges )
        {
            TransactionState txState = read.txState();
            this.removed = mergeToSet( txState.addedAndRemovedNodes().getRemoved(),
                    txState.nodesWithLabelChanged( label ).getRemoved() );
        }
        else
        {
            this.removed = LongSets.immutable.empty();
        }
        this.label = label;
    }

    @Override
    protected boolean scanStore( NodeLabelIndexCursor cursor, int sizeHint, LongIterator addedItems )
    {
        DefaultNodeLabelIndexCursor indexCursor = (DefaultNodeLabelIndexCursor) cursor;
        indexCursor.setRead( read );
        IndexProgressor indexProgressor;

        AccessMode mode = read.ktx.securityContext().mode();
        if ( mode.allowsTraverseLabel( label ) )
        {
            // all nodes will be allowed
            indexProgressor = storageScan.initializeBatch( indexCursor.nodeLabelClient(), sizeHint );
        }
        else if ( mode.disallowsTraverseLabel( label ) )
        {
            // no nodes with this label will be allowed
            indexProgressor = IndexProgressor.EMPTY;
        }
        else
        {
            // some nodes of this label might be blocked. we need to filter
            // TODO: Find which code path hits this and write tests for DENY (seems to be part of parallel runtime)
            indexProgressor = storageScan.initializeBatch( read.filteringNodeLabelClient( indexCursor.nodeLabelClient(), mode ), sizeHint );
        }

        if ( indexProgressor == IndexProgressor.EMPTY && !addedItems.hasNext() )
        {
            return false;
        }
        else
        {
            indexCursor.scan( indexProgressor, addedItems, removed );
            return true;
        }
    }
}
