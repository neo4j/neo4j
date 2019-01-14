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
package org.neo4j.io.pagecache.randomharness;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.neo4j.io.pagecache.PageCursor;

public class PageCountRecordFormat extends RecordFormat
{
    @Override
    public int getRecordSize()
    {
        return 16;
    }

    @Override
    public Record createRecord( File file, int recordId )
    {
        return new PageCountRecord( recordId, getRecordSize() );
    }

    @Override
    public Record readRecord( PageCursor cursor ) throws IOException
    {
        int offset = cursor.getOffset();
        byte[] bytes = new byte[getRecordSize()];
        do
        {
            cursor.setOffset( offset );
            cursor.getBytes( bytes );
        }
        while ( cursor.shouldRetry() );
        return new PageCountRecord( bytes );
    }

    @Override
    public Record zeroRecord()
    {
        return new PageCountRecord( 0, getRecordSize() );
    }

    @Override
    public void write( Record record, PageCursor cursor )
    {
        PageCountRecord r = (PageCountRecord) record;
        int shorts = getRecordSize() / 2;
        for ( int i = 0; i < shorts; i++ )
        {
            cursor.putShort( r.getRecordId() );
        }
    }

    private static final class PageCountRecord implements Record
    {
        private final byte[] bytes;
        private final ByteBuffer buf;

        PageCountRecord( int recordId, int recordSize )
        {
            if ( recordId > Short.MAX_VALUE )
            {
                throw new IllegalArgumentException(
                        "Record ID greater than Short.MAX_VALUE: " + recordId );
            }
            if ( recordSize < 2 )
            {
                throw new IllegalArgumentException(
                        "Record size must be positive: " + recordSize );
            }
            if ( recordSize % 2 != 0 )
            {
                throw new IllegalArgumentException(
                        "Record size must be even: " + recordSize );
            }
            bytes = new byte[recordSize];
            buf = ByteBuffer.wrap( bytes );
            for ( int i = 0; i < bytes.length; i += 2 )
            {
                buf.putShort( (short) recordId );
            }
        }

        PageCountRecord( byte[] bytes )
        {
            if ( bytes.length == 0 )
            {
                throw new IllegalArgumentException( "Bytes cannot be empty" );
            }
            if ( bytes.length % 2 != 0 )
            {
                throw new IllegalArgumentException(
                        "Record size must be even: " + bytes.length );
            }
            byte first = bytes[0];
            for ( byte b : bytes )
            {
                if ( b != first )
                {
                    throw new IllegalArgumentException(
                            "All bytes must be the same: " + Arrays.toString( bytes ) );
                }
            }
            this.bytes = bytes;
            this.buf = ByteBuffer.wrap( bytes );
        }

        public short getRecordId()
        {
            return buf.getShort( 0 );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            PageCountRecord that = (PageCountRecord) o;

            return Arrays.equals( bytes, that.bytes );

        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode( bytes );
        }

        @Override
        public String toString()
        {
            return "PageCountRecord[" +
                   "bytes=" + Arrays.toString( bytes ) +
                   ']';
        }
    }
}
