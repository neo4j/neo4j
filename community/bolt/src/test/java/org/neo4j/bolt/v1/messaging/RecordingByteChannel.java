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
package org.neo4j.bolt.v1.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public class RecordingByteChannel implements WritableByteChannel, ReadableByteChannel
{
    private final ByteBuffer buffer = ByteBuffer.allocate( 64 * 1024 );
    private int writePosition;
    private int readPosition;
    private boolean eof;

    @Override
    public boolean isOpen()
    {
        return true;
    }

    @Override
    public void close()
    {

    }

    @Override
    public int write( ByteBuffer src )
    {
        buffer.position( writePosition );
        int originalPosition = writePosition;

        buffer.put( src );

        writePosition = buffer.position();
        return writePosition - originalPosition;
    }

    @Override
    public int read( ByteBuffer dst )
    {
        if ( readPosition == writePosition )
        {
            return eof ? -1 : 0;
        }
        buffer.position( readPosition );
        int originalPosition = readPosition;
        int originalLimit = buffer.limit();

        buffer.limit( Math.min( buffer.position() + (dst.limit() - dst.position()), writePosition ) );
        dst.put( buffer );

        readPosition = buffer.position();
        buffer.limit( originalLimit );
        return readPosition - originalPosition;
    }

    public byte[] getBytes()
    {
        byte[] bytes = new byte[buffer.position()];
        buffer.position( 0 );
        buffer.get( bytes );
        return bytes;
    }

    /**
     * Mark this buffer as ended. Once whatever is currently unread in it is consumed,
     * it will start yielding -1 responses.
     */
    public void eof()
    {
        eof = true;
    }
}
