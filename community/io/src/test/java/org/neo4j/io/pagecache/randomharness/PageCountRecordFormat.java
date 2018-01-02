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
package org.neo4j.io.pagecache.randomharness;

import java.io.File;
import java.io.IOException;
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
        for ( int i = 0; i < getRecordSize(); i++ )
        {
            cursor.putByte( r.getRecordId() );
        }
    }

    private static final class PageCountRecord implements Record
    {
        private final byte[] bytes;

        public PageCountRecord( int recordId, int recordSize )
        {
            if ( recordId > Byte.MAX_VALUE )
            {
                throw new IllegalArgumentException(
                        "Record ID greater than Byte.MAX_VALUE: " + recordId );
            }
            if ( recordSize < 1 )
            {
                throw new IllegalArgumentException(
                        "Record size must be positive: " + recordSize );
            }
            bytes = new byte[recordSize];
            Arrays.fill( bytes, (byte) recordId );
        }

        public PageCountRecord( byte[] bytes )
        {
            if ( bytes.length == 0 )
            {
                throw new IllegalArgumentException( "Bytes cannot be empty" );
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
        }

        public byte getRecordId()
        {
            return bytes[0];
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            { return true; }
            if ( o == null || getClass() != o.getClass() )
            { return false; }

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
