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
import org.neo4j.kernel.impl.store.format.BaseOneByteHeaderRecordFormat;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.SchemaRecord;

public class SchemaRecordFormat extends BaseOneByteHeaderRecordFormat<SchemaRecord> implements RecordFormat<SchemaRecord>
{
    // 8 bits header. 56 possible bits for property record reference. (Even high-limit format only uses 50 bits for property ids).
    public static final int RECORD_SIZE = Long.BYTES;
    private static final long RECORD_IN_USE_BIT = ((long) IN_USE_BIT) << (Long.SIZE - Byte.SIZE);
    private static final long RECORD_PROPERTY_REFERENCE_MASK = 0x00FFFFFF_FFFFFFFFL;
    private static final long NO_NEXT_PROP = Record.NO_NEXT_PROPERTY.longValue();

    public SchemaRecordFormat()
    {
        super( fixedRecordSize( RECORD_SIZE ), 0, IN_USE_BIT, StandardFormatSettings.SCHEMA_RECORD_ID_BITS );
    }

    @Override
    public SchemaRecord newRecord()
    {
        return new SchemaRecord( AbstractBaseRecord.NO_ID );
    }

    @Override
    public void read( SchemaRecord record, PageCursor cursor, RecordLoad mode, int recordSize ) throws IOException
    {
        long data = cursor.getLong();
        boolean inUse = (data & RECORD_IN_USE_BIT) != 0;
        record.initialize( inUse, mode.shouldLoad( inUse ) ? data & RECORD_PROPERTY_REFERENCE_MASK : NO_NEXT_PROP );
    }

    @Override
    public void write( SchemaRecord record, PageCursor cursor, int recordSize ) throws IOException
    {
        long data = 0;
        if ( record.inUse() )
        {
            data = RECORD_IN_USE_BIT | record.getNextProp();
        }
        cursor.putLong( data );
    }

    @Override
    public boolean isInUse( PageCursor cursor )
    {
        long data = cursor.getLong();
        return (data & RECORD_IN_USE_BIT) != 0;
    }
}
