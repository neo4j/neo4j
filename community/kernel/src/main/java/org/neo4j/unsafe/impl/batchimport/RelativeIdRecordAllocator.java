/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport;

import java.util.Iterator;

import org.neo4j.kernel.impl.store.DynamicRecordAllocator;
import org.neo4j.kernel.impl.store.record.DynamicRecord;

/**
 * {@link DynamicRecordAllocator} that allocates new {@link DynamicRecord dynamic records} which has,
 * relative ids, {@link #initialize() starting} at 0, ignoring any global id generator. This is because when using
 * this allocator it is assumed that the ids are re-assigned later anyway.
 */
public class RelativeIdRecordAllocator implements DynamicRecordAllocator
{
    private final int dataSize;
    private long id;

    public RelativeIdRecordAllocator( int dataSize )
    {
        this.dataSize = dataSize;
    }

    public RelativeIdRecordAllocator initialize()
    {
        this.id = 0;
        return this;
    }

    @Override
    public int dataSize()
    {
        return dataSize;
    }

    @Override
    public DynamicRecord nextUsedRecordOrNew( Iterator<DynamicRecord> recordsToUseFirst )
    {
        DynamicRecord record = new DynamicRecord( id++ );
        record.setInUse( true );
        record.setCreated();
        return record;
    }
}
