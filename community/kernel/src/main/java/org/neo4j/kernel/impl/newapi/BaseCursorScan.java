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

import static java.lang.Math.min;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;
import static org.neo4j.util.Preconditions.requirePositive;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;
import org.neo4j.collection.RangeLongIterator;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.io.pagecache.context.CursorContext;

abstract class BaseCursorScan<C extends Cursor, S> implements Scan<C> {
    final S storageScan;
    final Read read;
    final boolean hasChanges;
    private volatile boolean addedItemsConsumed;
    private final long[] addedItemsArray;
    private final AtomicInteger addedChunk = new AtomicInteger(0);

    BaseCursorScan(S storageScan, Read read, Supplier<long[]> addedInTransaction) {
        this.storageScan = storageScan;
        this.read = read;
        this.hasChanges = read.hasTxStateWithChanges();
        this.addedItemsArray = hasChanges ? addedInTransaction.get() : EMPTY_LONG_ARRAY;
        this.addedItemsConsumed = addedItemsArray.length == 0;
    }

    @Override
    public boolean reserveBatch(C cursor, int sizeHint, CursorContext cursorContext, AccessMode accessMode) {
        requirePositive(sizeHint);

        LongIterator addedItems = ImmutableEmptyLongIterator.INSTANCE;
        if (hasChanges && !addedItemsConsumed) {
            // the idea here is to give each batch an exclusive range of the underlying
            // memory so that each thread can read in parallel without contention.
            int addedStart = addedChunk.getAndAdd(sizeHint);
            if (addedStart < addedItemsArray.length) {
                int batchSize = min(sizeHint, addedItemsArray.length - addedStart);
                sizeHint -= batchSize;
                addedItems = new RangeLongIterator(addedItemsArray, addedStart, batchSize);
            } else {
                addedItemsConsumed = true;
            }
        }
        return scanStore(cursor, sizeHint, addedItems, cursorContext, accessMode);
    }

    abstract boolean scanStore(
            C cursor, int sizeHint, LongIterator addedItems, CursorContext cursorContext, AccessMode accessMode);
}
