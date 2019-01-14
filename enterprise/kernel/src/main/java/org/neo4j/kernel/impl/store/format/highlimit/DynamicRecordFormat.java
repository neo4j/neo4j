/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.impl.store.format.highlimit;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.format.BaseOneByteHeaderRecordFormat;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.store.format.standard.DynamicRecordFormat.payloadTooBigErrorMessage;
import static org.neo4j.kernel.impl.store.format.standard.DynamicRecordFormat.readData;

/**
 * LEGEND:
 * V: variable between 3B-8B
 * D: data size
 *
 * Record format:
 * 1B   header
 * 3B   number of bytes data in this block
 * 8B   next block
 * DB   data (record size - (the above) header size)
 *
 * => 12B + data size
 */
public class DynamicRecordFormat extends BaseOneByteHeaderRecordFormat<DynamicRecord>
{
    private static final int RECORD_HEADER_SIZE = 1/*header byte*/ + 3/*# of bytes*/ + 8/*max size of next reference*/;
                                            // = 12
    private static final int START_RECORD_BIT = 0x8;

    public DynamicRecordFormat()
    {
        super( INT_STORE_HEADER_READER, RECORD_HEADER_SIZE, IN_USE_BIT, HighLimitFormatSettings.DYNAMIC_MAXIMUM_ID_BITS );
    }

    @Override
    public DynamicRecord newRecord()
    {
        return new DynamicRecord( -1 );
    }

    @Override
    public void read( DynamicRecord record, PageCursor cursor, RecordLoad mode, int recordSize )
    {
        byte headerByte = cursor.getByte();
        boolean inUse = isInUse( headerByte );
        if ( mode.shouldLoad( inUse ) )
        {
            int length = cursor.getShort() | cursor.getByte() << 16;
            if ( length > recordSize | length < 0 )
            {
                cursor.setCursorException( payloadLengthErrorMessage( record, recordSize, length ) );
                return;
            }
            long next = cursor.getLong();
            boolean isStartRecord = (headerByte & START_RECORD_BIT) != 0;
            record.initialize( inUse, isStartRecord, next, -1, length );
            readData( record, cursor );
        }
        else
        {
            record.setInUse( inUse );
        }
    }

    private String payloadLengthErrorMessage( DynamicRecord record, int recordSize, int length )
    {
        return length < 0 ?
               negativePayloadErrorMessage( record, length ) :
               payloadTooBigErrorMessage( record, recordSize, length );
    }

    private String negativePayloadErrorMessage( DynamicRecord record, int length )
    {
        return format( "DynamicRecord[%s] claims to have a negative payload of %s bytes.",
                record.getId(), length );
    }

    @Override
    public void write( DynamicRecord record, PageCursor cursor, int recordSize )
    {
        if ( record.inUse() )
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
        else
        {
            markAsUnused( cursor );
        }
    }

    @Override
    public long getNextRecordReference( DynamicRecord record )
    {
        return record.getNextBlock();
    }
}
