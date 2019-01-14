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

import org.neo4j.kernel.impl.store.DynamicRecordAllocator;
import org.neo4j.kernel.impl.store.record.DynamicRecord;

/**
 * Composite allocator that allows to use available records first and then switch to provided record allocator to
 * allocate more records if required.
 */
public class ReusableRecordsCompositeAllocator implements DynamicRecordAllocator
{
    private final ReusableRecordsAllocator reusableRecordsAllocator;
    private final DynamicRecordAllocator recordAllocator;

    public ReusableRecordsCompositeAllocator( Collection<DynamicRecord> records,
            DynamicRecordAllocator recordAllocator )
    {
        this( records.iterator(), recordAllocator );
    }

    public ReusableRecordsCompositeAllocator( Iterator<DynamicRecord> recordsIterator,
            DynamicRecordAllocator recordAllocator )
    {
        this.reusableRecordsAllocator = new ReusableRecordsAllocator( recordAllocator.getRecordDataSize(), recordsIterator );
        this.recordAllocator = recordAllocator;
    }

    @Override
    public int getRecordDataSize()
    {
        return recordAllocator.getRecordDataSize();
    }

    @Override
    public DynamicRecord nextRecord()
    {
        return reusableRecordsAllocator.hasNext() ? reusableRecordsAllocator.nextRecord() : recordAllocator.nextRecord();
    }
}
