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
package org.neo4j.kernel.impl.store.format.standard;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.format.BaseOneByteHeaderRecordFormat;
import org.neo4j.kernel.impl.store.format.BaseRecordFormat;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.store.record.DynamicRecord.NO_DATA;

public class DynamicRecordFormat extends BaseOneByteHeaderRecordFormat<DynamicRecord>
{
    // (in_use+next high)(1 byte)+nr_of_bytes(3 bytes)+next_block(int)
    public static final int RECORD_HEADER_SIZE = 1 + 3 + 4; // = 8

    public DynamicRecordFormat()
    {
        super( INT_STORE_HEADER_READER, RECORD_HEADER_SIZE, 0x10/*the inUse bit is the lsb in the second nibble*/,
                StandardFormatSettings.DYNAMIC_RECORD_MAXIMUM_ID_BITS );
    }

    @Override
    public DynamicRecord newRecord()
    {
        return new DynamicRecord( -1 );
    }

    @Override
    public void read( DynamicRecord record, PageCursor cursor, RecordLoad mode, int recordSize, PagedFile storeFile )
    {
        /*
         * First 4b
         * [x   ,    ][    ,    ][    ,    ][    ,    ] 0: start record, 1: linked record
         * [   x,    ][    ,    ][    ,    ][    ,    ] inUse
         * [    ,xxxx][    ,    ][    ,    ][    ,    ] high next block bits
         * [    ,    ][xxxx,xxxx][xxxx,xxxx][xxxx,xxxx] nr of bytes in the data field in this record
         *
         */
        long firstInteger = cursor.getInt() & 0xFFFFFFFFL;
        boolean isStartRecord = (firstInteger & 0x80000000) == 0;
        boolean inUse = (firstInteger & 0x10000000) != 0;
        if ( mode.shouldLoad( inUse ) )
        {
            int dataSize = recordSize - getRecordHeaderSize();
            int nrOfBytes = (int) (firstInteger & 0xFFFFFF);

            /*
             * Pointer to next block 4b (low bits of the pointer)
             */
            long nextBlock = cursor.getInt() & 0xFFFFFFFFL;
            long nextModifier = (firstInteger & 0xF000000L) << 8;

            long longNextBlock = BaseRecordFormat.longFromIntAndMod( nextBlock, nextModifier );
            record.initialize( inUse, isStartRecord, longNextBlock, -1, nrOfBytes );
            if ( longNextBlock != Record.NO_NEXT_BLOCK.intValue()
                    && nrOfBytes < dataSize || nrOfBytes > dataSize )
            {
                mode.report( format( "Next block set[%d] current block illegal size[%d/%d]",
                        record.getNextBlock(), record.getLength(), dataSize ) );
            }

            readData( record, cursor );
        }
    }

    public static void readData( DynamicRecord record, PageCursor cursor )
    {
        if ( record.getLength() == 0 ) // don't go though the trouble of acquiring the window if we would read nothing
        {
            record.setData( NO_DATA );
            return;
        }

        int len = record.getLength();
        byte[] data = record.getData();
        if ( data == null || data.length != len )
        {
            data = new byte[len];
        }
        cursor.getBytes( data );
        record.setData( data );
    }

    @Override
    public void write( DynamicRecord record, PageCursor cursor, int recordSize, PagedFile storeFile )
    {
        if ( record.inUse() )
        {
            long nextBlock = record.getNextBlock();
            int highByteInFirstInteger = nextBlock == Record.NO_NEXT_BLOCK.intValue() ? 0
                    : (int) ((nextBlock & 0xF00000000L) >> 8);
            highByteInFirstInteger |= (Record.IN_USE.byteValue() << 28);
            highByteInFirstInteger |= (record.isStartRecord() ? 0 : 1) << 31;

            /*
             * First 4b
             * [x   ,    ][    ,    ][    ,    ][    ,    ] 0: start record, 1: linked record
             * [   x,    ][    ,    ][    ,    ][    ,    ] inUse
             * [    ,xxxx][    ,    ][    ,    ][    ,    ] high next block bits
             * [    ,    ][xxxx,xxxx][xxxx,xxxx][xxxx,xxxx] nr of bytes in the data field in this record
             *
             */
            int firstInteger = record.getLength();
            assert firstInteger < (1 << 24) - 1;

            firstInteger |= highByteInFirstInteger;

            cursor.putInt( firstInteger );
            cursor.putInt( (int) nextBlock );
            cursor.putBytes( record.getData() );
        }
        else
        {
            cursor.putByte( Record.NOT_IN_USE.byteValue() );
        }
    }

    @Override
    public long getNextRecordReference( DynamicRecord record )
    {
        return record.getNextBlock();
    }
}
