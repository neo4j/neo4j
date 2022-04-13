/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.io.pagecache.impl.muninn.versioned;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.mem.MemoryAllocator;

public class InMemoryVersionStorage implements VersionStorage
{
    private static final long CURSOR_OFFSET = UnsafeUtil.getFieldOffset( InMemoryVersionStorage.class, "cursor" );
    static final long MAX_VERSIONED_STORAGE = ByteUnit.mebiBytes( 100 );
    private final long baseAddress;
    private final int pageSize;
    @SuppressWarnings( "FieldMayBeFinal" )
    private volatile long cursor;

    public InMemoryVersionStorage( MemoryAllocator memoryAllocator, int pageSize )
    {
        this.baseAddress = memoryAllocator.allocateAligned( MAX_VERSIONED_STORAGE, Long.BYTES );
        this.pageSize = pageSize;
        this.cursor = baseAddress;
    }

    @Override
    public long copyPage( long sourcePage )
    {
        long destination = cursor;
        long newCursor = destination + pageSize;
        while ( !UnsafeUtil.compareAndSwapLong( this, CURSOR_OFFSET, destination, newCursor ) )
        {
            destination = cursor;
            newCursor = destination + pageSize;
        }
        long totalRequestedMemory = newCursor - baseAddress;
        if ( totalRequestedMemory > MAX_VERSIONED_STORAGE )
        {
            throw new OutOfMemoryError(
                    "Out of version storage memory. Max available memory: " + MAX_VERSIONED_STORAGE + ". Totally requested memory: " + totalRequestedMemory );
        }
        UnsafeUtil.copyMemory( sourcePage, destination, pageSize );
        return destination;
    }

    long getAllocatedBytes()
    {
        return cursor - baseAddress;
    }
}
