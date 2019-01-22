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

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.format.BaseOneByteHeaderRecordFormat;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.TokenRecord;

public abstract class TokenRecordFormat<RECORD extends TokenRecord> extends BaseOneByteHeaderRecordFormat<RECORD>
{
    protected static final int BASE_RECORD_SIZE = 1/*inUse*/ + 4/*nameId*/;

    protected TokenRecordFormat( int recordSize, int idBits )
    {
        super( fixedRecordSize( recordSize ), 0, IN_USE_BIT, idBits );
    }

    @Override
    public void read( RECORD record, PageCursor cursor, RecordLoad mode, int recordSize )
    {
        byte inUseByte = cursor.getByte();
        boolean inUse = isInUse( inUseByte );
        record.setInUse( inUse );
        if ( mode.shouldLoad( inUse ) )
        {
            readRecordData( cursor, record, inUse );
        }
    }

    protected void readRecordData( PageCursor cursor, RECORD record, boolean inUse )
    {
        record.initialize( inUse, cursor.getInt() );
    }

    @Override
    public void write( RECORD record, PageCursor cursor, int recordSize )
    {
        if ( record.inUse() )
        {
            cursor.putByte( Record.IN_USE.byteValue() );
            writeRecordData( record, cursor );
        }
        else
        {
            cursor.putByte( Record.NOT_IN_USE.byteValue() );
        }
    }

    protected void writeRecordData( RECORD record, PageCursor cursor )
    {
        cursor.putInt( record.getNameId() );
    }
}
