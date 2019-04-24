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

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.storageengine.api.StorageNodeCursor;

import static org.neo4j.internal.kernel.api.Read.NO_ID;

public class FullAccessNodeCursor extends DefaultNodeCursor
{
    FullAccessNodeCursor( CursorPool<DefaultNodeCursor> pool, StorageNodeCursor storeCursor )
    {
        super( pool, storeCursor );
    }

    @Override
    public LabelSet labels()
    {
        if ( currentAddedInTx != NO_ID )
        {
            //Node added in tx-state, no reason to go down to store and check
            TransactionState txState = read.txState();
            return Labels.from( txState.nodeStateLabelDiffSets( currentAddedInTx ).getAdded() );
        }
        else if ( hasChanges() )
        {
            //Get labels from store and put in intSet, unfortunately we get longs back
            TransactionState txState = read.txState();
            long[] longs = storeCursor.labels();
            final MutableLongSet labels = new LongHashSet();
            for ( long labelToken : longs )
            {
                labels.add( labelToken );
            }

            //Augment what was found in store with what we have in tx state
            return Labels.from( txState.augmentLabels( labels, txState.getNodeState( storeCursor.entityReference() ) ) );
        }
        else
        {
            //Nothing in tx state, just read the data.
            return Labels.from( storeCursor.labels() );
        }
    }

    @Override
    boolean allowedLabels( long... labels )
    {
        return true;
    }

    @Override
    boolean allowsTraverse()
    {
        return true;
    }
}
