/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.store.standard;

import java.io.IOException;

import org.neo4j.io.fs.StoreChannel;

/** Store metadata and tools used by a store and its associated components. */
public class StoreToolkit
{
    private final int recordSize;
    private final int pageSize;
    private final long firstRecordId;
    private final StoreChannel channel;
    private final StoreIdGenerator idGenerator;

    public StoreToolkit( int recordSize, int pageSize, long firstRecordId, StoreChannel channel, StoreIdGenerator idGenerator )
    {
        this.recordSize = recordSize;
        this.pageSize = pageSize;
        this.firstRecordId = firstRecordId;
        this.channel = channel;
        this.idGenerator = idGenerator;
    }

    public long pageId( long recordId )
    {
        return recordId * recordSize / pageSize;
    }

    /** Offset inside a page that a given record can be found at. */
    public int recordOffset( long recordId )
    {
        return (int) (recordId * recordSize % pageSize);
    }

    /** The store page size. NOTE that this may be different from the system-wide page size reported by the PageCache */
    public int pageSize()
    {
        return pageSize - pageSize % recordSize;
    }

    public int recordSize()
    {
        return recordSize;
    }

    /** An id that has only one guarantee: No in-use records will have ids higher than this. */
    public long highestKnownId() { return idGenerator.highestIdInUse(); }

    /** Get the id of the first record in the store. This exists because initial ids may be reserved for headers. */
    public long firstRecordId()
    {
        return firstRecordId;
    }

    /**
     * Get the current file size on disk. This may not represent the "real" size of the file, as much of it may be
     * unflushed pages in the page cache.
     */
    public long fileSize() throws IOException
    {
        return channel.size();
    }
}
