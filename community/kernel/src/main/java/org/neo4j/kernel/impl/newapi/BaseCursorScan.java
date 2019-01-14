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
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;

import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.collection.RangeLongIterator;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.Scan;

import static java.lang.Math.min;
import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.util.Preconditions.requirePositive;

abstract class BaseCursorScan<C extends Cursor, S> implements Scan<C>
{
    final S storageScan;
    final Read read;
    final boolean hasChanges;
    private volatile boolean addedItemsConsumed;
    private final long[] addedItemsArray;
    private final AtomicInteger addedChunk = new AtomicInteger( 0 );

    BaseCursorScan( S storageScan, Read read )
    {
        this.storageScan = storageScan;
        this.read = read;
        this.hasChanges = read.hasTxStateWithChanges();
        this.addedItemsArray = hasChanges ? addedInTransaction() : EMPTY_LONG_ARRAY;
        this.addedItemsConsumed = addedItemsArray.length == 0;
    }

    @Override
    public boolean reserveBatch( C cursor, int sizeHint )
    {
        requirePositive( sizeHint );

        LongIterator addedItems = ImmutableEmptyLongIterator.INSTANCE;
        if ( hasChanges && !addedItemsConsumed )
        {
            //the idea here is to give each batch an exclusive range of the underlying
            //memory so that each thread can read in parallel without contention.
            int addedStart = addedChunk.getAndAdd( sizeHint );
            if ( addedStart < addedItemsArray.length )
            {
                int batchSize = min( sizeHint, addedItemsArray.length - addedStart  );
                sizeHint -= batchSize;
                addedItems = new RangeLongIterator( addedItemsArray, addedStart, batchSize );
            }
            else
            {
                addedItemsConsumed = true;
            }
        }
        return scanStore( cursor, sizeHint, addedItems );
    }

    abstract long[] addedInTransaction();

    abstract boolean scanStore( C cursor, int sizeHint, LongIterator addedItems );
}
