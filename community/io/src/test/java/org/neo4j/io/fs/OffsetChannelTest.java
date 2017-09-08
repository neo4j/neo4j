/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.io.fs;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OffsetChannelTest
{
    private long offset = 128;
    private StoreChannel actual = mock( StoreChannel.class );

    private OffsetChannel channel = new OffsetChannel( actual, offset );

    private ByteBuffer buf = ByteBuffer.allocate( 0 );
    private ByteBuffer[] buffers = new ByteBuffer[0];

    @Test
    public void tryLock() throws Exception
    {
        channel.tryLock();
        verify( actual ).tryLock();
    }

    @Test
    public void write() throws Exception
    {
        channel.write( buf );
        verify( actual ).write( buf );
    }

    @Test
    public void writeWithPosition() throws Exception
    {
        long position = 500;
        channel.write( buf, position );
        verify( actual ).write( buf, position + offset );
    }

    @Test
    public void writeAll() throws Exception
    {
        channel.writeAll( buf );
        verify( actual ).writeAll( buf );
    }

    @Test
    public void writeAllWithPosition() throws Exception
    {
        long position = 500;
        channel.writeAll( buf, position );
        verify( actual ).writeAll( buf, position + offset );
    }

    @Test
    public void read() throws Exception
    {
        channel.read( buf );
        verify( actual ).read( buf );
    }

    @Test
    public void force() throws Exception
    {
        channel.force( false );
        verify( actual ).force( false );
    }

    @Test
    public void readWithPosition() throws Exception
    {
        long position = 500;
        channel.read( buf, position );
        verify( actual ).read( buf, position + offset );
    }

    @Test
    public void position() throws Exception
    {
        long position = 500;
        when( actual.position() ).thenReturn( position );
        assertEquals( position - offset, channel.position() );
        verify( actual ).position();
    }

    @Test
    public void positionWithPosition() throws Exception
    {
        long position = 500;
        channel.position( position );
        verify( actual ).position( 500 + offset );
    }

    @Test
    public void size() throws Exception
    {
        long size = 256;
        when( actual.size() ).thenReturn( size );
        assertEquals( 256 - offset, channel.size() );
        verify( actual ).size();
    }

    @Test
    public void truncate() throws Exception
    {
        long size = 256;
        channel.truncate( size );
        verify( actual ).truncate( size + offset );
    }

    @Test
    public void flush() throws Exception
    {
        channel.flush();
        verify( actual ).flush();
    }

    @Test
    public void writeMultiple() throws Exception
    {
        channel.write( buffers );
        verify( actual ).write( buffers );
    }

    @Test
    public void writeMultipleExtended() throws Exception
    {
        int off = 16;
        int len = 32;
        channel.write( buffers, off, len );
        verify( actual ).write( buffers, off, len );
    }

    @Test
    public void readMultiple() throws Exception
    {
        channel.read( buffers );
        verify( actual ).read( buffers );
    }

    @Test
    public void readMultipleExtended() throws Exception
    {
        int off = 16;
        int len = 32;
        channel.read( buffers, off, len );
        verify( actual ).read( buffers, off, len );
    }

    @Test
    public void isOpen() throws Exception
    {
        channel.isOpen();
        verify( actual ).isOpen();
    }

    @Test
    public void close() throws Exception
    {
        channel.close();
        verify( actual ).close();
    }
}
