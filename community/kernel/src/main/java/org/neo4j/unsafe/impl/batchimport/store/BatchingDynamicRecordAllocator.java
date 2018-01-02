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
package org.neo4j.unsafe.impl.batchimport.store;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.kernel.impl.store.DynamicRecordAllocator;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.record.DynamicRecord;

import static java.lang.Integer.parseInt;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.string_block_size;
import static org.neo4j.kernel.impl.store.AbstractDynamicStore.BLOCK_HEADER_SIZE;

/**
 * {@link DynamicRecordAllocator} that allocates records using a {@link BatchingIdSequence}
 * and makes available all allocated records.
 */
public class BatchingDynamicRecordAllocator implements DynamicRecordAllocator
{
    private final int dataSize = parseInt( string_block_size.getDefaultValue() ) - BLOCK_HEADER_SIZE;
    private final IdSequence idSequence;
    private final List<DynamicRecord> records = new ArrayList<>();

    public BatchingDynamicRecordAllocator()
    {
        this( new BatchingIdSequence() );
    }

    public BatchingDynamicRecordAllocator( IdSequence idSequence )
    {
        this.idSequence = idSequence;
    }

    @Override
    public int dataSize()
    {
        return dataSize;
    }

    @Override
    public DynamicRecord nextUsedRecordOrNew( Iterator<DynamicRecord> recordsToUseFirst )
    {
        DynamicRecord record = new DynamicRecord( idSequence.nextId() );
        record.setInUse( true );
        records.add( record );
        return record;
    }

    public List<DynamicRecord> records()
    {
        return records;
    }
}
