/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.test.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.io.memory.ScopedBuffer;
import org.neo4j.memory.MemoryTracker;

import static org.neo4j.io.ByteUnit.KibiByte;

public class ChannelOutputStream extends OutputStream
{
    private final StoreChannel channel;
    private final ScopedBuffer scopedBuffer;
    private final ByteBuffer buffer;

    public ChannelOutputStream( StoreChannel channel, boolean append, MemoryTracker memoryTracker ) throws IOException
    {
        this.scopedBuffer = new HeapScopedBuffer( 8, KibiByte, memoryTracker );
        this.buffer = scopedBuffer.getBuffer();
        this.channel = channel;
        if ( append )
        {
            this.channel.position( this.channel.size() );
        }
    }

    @Override
    public void write( int b ) throws IOException
    {
        buffer.clear();
        buffer.put( (byte) b );
        buffer.flip();
        channel.writeAll( buffer );
    }

    @Override
    public void write( byte[] b ) throws IOException
    {
        write( b, 0, b.length );
    }

    @Override
    public void write( byte[] b, int off, int len ) throws IOException
    {
        int written = 0;
        while ( written < len )
        {
            buffer.clear();
            buffer.put( b, off + written, Math.min( len - written, buffer.capacity() ) );
            buffer.flip();
            written += channel.write( buffer );
        }
    }

    @Override
    public void close() throws IOException
    {
        scopedBuffer.close();
        channel.close();
    }
}
