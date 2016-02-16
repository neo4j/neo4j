/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.format.aligned;

import java.io.IOException;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.format.BaseOneByteHeaderRecordFormat;
import org.neo4j.kernel.impl.store.record.DynamicRecord;

import static org.neo4j.kernel.impl.store.format.lowlimit.DynamicRecordFormat.readData;

class DynamicRecordFormat extends BaseOneByteHeaderRecordFormat<DynamicRecord>
{
    private static final int RECORD_HEADER_SIZE = 1/*header byte*/ + 3/*# of bytes*/ + 8/*max size of next reference*/;
                                            // = 12
    private static final int START_RECORD_BIT = 0x8;

    protected DynamicRecordFormat()
    {
        super( INT_STORE_HEADER_READER, RECORD_HEADER_SIZE, IN_USE_BIT );
    }

    @Override
    public DynamicRecord newRecord()
    {
        return new DynamicRecord( -1 );
    }

    @Override
    protected void doRead( DynamicRecord record, PageCursor cursor, int recordSize, PagedFile storeFile,
            long headerByte, boolean inUse ) throws IOException
    {
        int length = cursor.getShort() | cursor.getByte() << 16;
        long next = cursor.getLong();
        boolean isStartRecord = (headerByte & START_RECORD_BIT) != 0;
        record.initialize( inUse, isStartRecord, next, -1, length );
        readData( record, cursor );
    }

    @Override
    protected void doWrite( DynamicRecord record, PageCursor cursor, int recordSize, PagedFile storeFile )
            throws IOException
    {
        assert record.getLength() < (1 << 24) - 1;
        byte headerByte = (byte) ((record.inUse() ? IN_USE_BIT : 0) |
                (record.isStartRecord() ? START_RECORD_BIT : 0));
        cursor.putByte( headerByte );
        cursor.putShort( (short) record.getLength() );
        cursor.putByte( (byte) (record.getLength() >>> 16 ) );
        cursor.putLong( record.getNextBlock() );
        cursor.putBytes( record.getData() );
    }

    @Override
    public long getNextRecordReference( DynamicRecord record )
    {
        return record.getNextBlock();
    }
}
