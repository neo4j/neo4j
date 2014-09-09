/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.io.pagecache.stress;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.io.pagecache.PageCursor;

public class StubPageCursor implements PageCursor
{
    private final List<Long> countsAndChecksum;
    private int offset;

    private StubPageCursor( ArrayList<Long> countsAndChecksum )
    {
        this.countsAndChecksum = countsAndChecksum;
    }

    public static StubPageCursor create( long... counts )
    {
        ArrayList<Long> countsAndChecksum = new ArrayList<>( counts.length + 1 );

        long checksum = 0;

        for ( long count : counts )
        {
            countsAndChecksum.add( count );
            checksum += count;
        }

        countsAndChecksum.add( checksum );

        return new StubPageCursor( countsAndChecksum );
    }

    @Override
    public byte getByte()
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public void putByte( byte value )
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public long getLong()
    {
        return countsAndChecksum.get( offset++ );
    }

    @Override
    public void putLong( long value )
    {
        countsAndChecksum.set( offset++, value );
    }

    @Override
    public int getInt()
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public void putInt( int value )
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public long getUnsignedInt()
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public void getBytes( byte[] data )
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public void putBytes( byte[] data )
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public short getShort()
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public void putShort( short value )
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public void setOffset( int offset )
    {
        this.offset = offset / 8;
    }

    @Override
    public int getOffset()
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public long getCurrentPageId()
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public void rewind() throws IOException
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public boolean next() throws IOException
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public boolean next( long pageId ) throws IOException
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public void close()
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public boolean shouldRetry() throws IOException
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    public long getCount( int recordNumber, int counterNumber )
    {
        assertThat( "multiple records/ pages not supported", recordNumber, is( 0 ) );

        return countsAndChecksum.get( counterNumber );
    }

    public long getChecksum( int recordNumber )
    {
        assertThat( "multiple records/ pages not supported", recordNumber, is( 0 ) );

        return countsAndChecksum.get( countsAndChecksum.size() - 1 );
    }

    public PageCursor resetOffset()
    {
        offset = 0;
        return this;
    }
}
