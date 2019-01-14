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
package org.neo4j.kernel.impl.store.format.standard;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.StoreHeader;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;

public class NoRecordFormat<RECORD extends AbstractBaseRecord> implements RecordFormat<RECORD>
{
    @Override
    public RECORD newRecord()
    {
        return null;
    }

    @Override
    public int getRecordSize( StoreHeader storeHeader )
    {
        return NO_RECORD_SIZE;
    }

    @Override
    public int getRecordHeaderSize()
    {
        return 0;
    }

    @Override
    public boolean isInUse( PageCursor cursor )
    {
        return false;
    }

    @Override
    public void read( RECORD record, PageCursor cursor, RecordLoad mode, int recordSize )
    {
        record.clear();
    }

    @Override
    public void prepare( RECORD record, int recordSize, IdSequence idSequence )
    {
    }

    @Override
    public void write( RECORD record, PageCursor cursor, int recordSize )
    {
    }

    @Override
    public long getNextRecordReference( RECORD record )
    {
        return Record.NULL_REFERENCE.intValue();
    }

    @Override
    public long getMaxId()
    {
        return 0;
    }
}
