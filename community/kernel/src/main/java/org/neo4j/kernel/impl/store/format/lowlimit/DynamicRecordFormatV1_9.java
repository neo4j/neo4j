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
package org.neo4j.kernel.impl.store.format.lowlimit;

import java.io.IOException;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.format.BaseRecordFormat;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;

import static java.lang.String.format;

import static org.neo4j.kernel.impl.store.format.lowlimit.DynamicRecordFormat.readData;

class DynamicRecordFormatV1_9 extends BaseRecordFormat<DynamicRecord>
{
    // (in_use+next high)(1 byte)+nr_of_bytes(3 bytes)+next_block(int)
    public static final int RECORD_HEADER_SIZE = 1 + 3 + 4; // = 8

    DynamicRecordFormatV1_9()
    {
        super( INT_STORE_HEADER_READER, RECORD_HEADER_SIZE, 0x10 );
    }

    @Override
    public DynamicRecord newRecord()
    {
        return new DynamicRecord( -1 );
    }

    @Override
    public void read( DynamicRecord record, PageCursor cursor, RecordLoad mode, int recordSize, PagedFile storeFile )
            throws IOException
    {
        /*
        *
        * [   x,    ][    ,    ][    ,    ][    ,    ] inUse
        * [    ,xxxx][    ,    ][    ,    ][    ,    ] high next block bits
        * [    ,    ][xxxx,xxxx][xxxx,xxxx][xxxx,xxxx] nr of bytes
        *
        */
       long firstInteger = cursor.getUnsignedInt();
       int inUseByte = (int) ( ( firstInteger & 0xF0000000 ) >> 28 );
       boolean inUse = inUseByte == Record.IN_USE.intValue();
       if ( mode.shouldLoad( inUse ) )
       {
           int dataSize = recordSize - getRecordHeaderSize();
           int nrOfBytes = (int) ( firstInteger & 0xFFFFFF );

           long nextBlock = cursor.getUnsignedInt();
           long nextModifier = ( firstInteger & 0xF000000L ) << 8;

           long longNextBlock = longFromIntAndMod( nextBlock, nextModifier );
           record.initialize( inUse, true, longNextBlock, -1, nrOfBytes );
           if ( longNextBlock != Record.NO_NEXT_BLOCK.intValue()
               && nrOfBytes < dataSize || nrOfBytes > dataSize )
           {
               mode.report( format( "Next block set[%d] current block illegal size[%d/%d]",
                       record.getNextBlock(), record.getLength(), dataSize ) );
           }

           readData( record, cursor );
       }
    }

    @Override
    public void write( DynamicRecord record, PageCursor cursor, int recordSize, PagedFile storeFile ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getNextRecordReference( DynamicRecord record )
    {
        return record.getNextBlock();
    }
}
