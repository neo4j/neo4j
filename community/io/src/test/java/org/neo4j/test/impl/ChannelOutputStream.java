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
package org.neo4j.test.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.StoreChannel;

public class ChannelOutputStream extends OutputStream
{
    private final StoreChannel channel;
    private final ByteBuffer buffer = ByteBuffer.allocateDirect( 8096 );

    public ChannelOutputStream( StoreChannel channel, boolean append ) throws IOException
    {
        this.channel = channel;
        if ( append )
            this.channel.position( this.channel.size() );
    }

    @Override
    public void write( int b ) throws IOException
    {
        buffer.clear();
        buffer.put( (byte) b );
        buffer.flip();
        channel.write( buffer );
    }

    @Override
    public void write( byte[] b ) throws IOException
    {
        write( b, 0, b.length );
    }

    @Override
    public void write( byte[] b, int off, int len ) throws IOException
    {
        int written = 0, index = off;
        while ( written < len )
        {
            buffer.clear();
            buffer.put( b, index, Math.min( len-written, buffer.capacity() ) );
            buffer.flip();
            written += channel.write( buffer );
        }
    }

    @Override
    public void close() throws IOException
    {
        channel.close();
    }
}
