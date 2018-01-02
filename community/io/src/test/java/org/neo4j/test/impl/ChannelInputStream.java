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
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.StoreChannel;

public class ChannelInputStream extends InputStream
{
    private final StoreChannel channel;
    private final ByteBuffer buffer = ByteBuffer.allocateDirect( 8096 );
    private int position;

    public ChannelInputStream( StoreChannel channel )
    {
        this.channel = channel;
    }

    @Override
    public int read() throws IOException
    {
        buffer.clear();
        buffer.limit( 1 );
        while ( buffer.hasRemaining())
        {
            int read = channel.read( buffer );

            if ( read == -1 )
            {
                return -1;
            }
        }
        buffer.flip();
        position++;
        // Return the *unsigned* byte value as an integer
        return buffer.get() & 0x000000FF;
    }

    @Override
    public int read( byte[] b, int off, int len ) throws IOException
    {
        // TODO implement properly
        return super.read( b, off, len );
    }

    @Override
    public int available() throws IOException
    {
        return (int) (position - channel.size());
    }

    @Override
    public void close() throws IOException
    {
        channel.close();
    }
}
