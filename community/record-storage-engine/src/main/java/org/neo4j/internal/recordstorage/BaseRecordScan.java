/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.recordstorage;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.kernel.impl.store.record.PrimitiveRecord;

/**
 * Maintains state when performing batched scans, potentially from multiple threads.
 * <p>
 * Will break up the scan in ranges depending on the provided size hint.
 */
abstract class BaseRecordScan<C extends PrimitiveRecord>
{
    private final AtomicLong nextStart = new AtomicLong( 0 );

    boolean scanBatch( int sizeHint, C cursor )
    {
        long start = nextStart.getAndAdd( sizeHint );
        long stopInclusive = start + sizeHint - 1;
        return scanRange( cursor, start, stopInclusive );
    }

    abstract boolean scanRange( C cursor, long start, long stopInclusive );
}
