/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.store;

import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.record.DynamicRecord;

public class StandardDynamicRecordAllocator implements DynamicRecordAllocator
{
    protected final IdSequence idGenerator;
    private final int dataSize;

    public StandardDynamicRecordAllocator( IdSequence idGenerator, int dataSize )
    {
        this.idGenerator = idGenerator;
        this.dataSize = dataSize;
    }

    @Override
    public int getRecordDataSize()
    {
        return dataSize;
    }

    @Override
    public DynamicRecord nextRecord()
    {
        return allocateRecord( idGenerator.nextId() );
    }

    public static DynamicRecord allocateRecord( long id )
    {
        DynamicRecord record = new DynamicRecord( id );
        record.setCreated();
        record.setInUse( true );
        return record;
    }
}
