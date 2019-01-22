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
package org.neo4j.kernel.impl.store.allocator;

import java.util.Collection;
import java.util.Iterator;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.impl.store.DynamicRecordAllocator;
import org.neo4j.kernel.impl.store.record.DynamicRecord;

/**
 * Dynamic allocator that allow to use already available records in dynamic records allocation.
 * As part of record allocation provided records will be marked as created if they where not used before
 * and marked as used.
 */
public class ReusableRecordsAllocator implements DynamicRecordAllocator
{
    private final int recordSize;
    private final Iterator<DynamicRecord> recordIterator;

    public ReusableRecordsAllocator( int recordSize, DynamicRecord... records )
    {
        this.recordSize = recordSize;
        this.recordIterator = Iterators.iterator( records );
    }

    public ReusableRecordsAllocator( int recordSize, Collection<DynamicRecord> records )
    {
        this.recordSize = recordSize;
        this.recordIterator = records.iterator();
    }

    public ReusableRecordsAllocator( int recordSize, Iterator<DynamicRecord> recordsIterator )
    {
        this.recordSize = recordSize;
        this.recordIterator = recordsIterator;
    }

    @Override
    public int getRecordDataSize()
    {
        return recordSize;
    }

    @Override
    public DynamicRecord nextRecord()
    {
        DynamicRecord record = recordIterator.next();
        if ( !record.inUse() )
        {
            record.setCreated();
        }
        record.setInUse( true );
        return record;
    }

    /**
     * Check if we have more available pre allocated records
     * @return true if record is available
     */
    public boolean hasNext()
    {
        return recordIterator.hasNext();
    }
}
