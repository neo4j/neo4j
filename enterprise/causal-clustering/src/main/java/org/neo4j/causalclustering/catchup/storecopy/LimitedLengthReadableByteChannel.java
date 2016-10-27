/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public class LimitedLengthReadableByteChannel implements ReadableByteChannel
{
    private final ReadableByteChannel channel;
    private final long lengthLimit;
    private long position = 0;

    public LimitedLengthReadableByteChannel( ReadableByteChannel channel, long lengthLimit )
    {
        this.channel = channel;
        this.lengthLimit = lengthLimit;
    }

    @Override
    public int read( ByteBuffer dst ) throws IOException
    {
        long bytesLeftInFile = lengthLimit - position;
        if ( bytesLeftInFile == 0 )
        {
            return -1;
        }

        int bytesLeftInBuffer = dst.limit() - dst.position();
        int maxReadable = (int) Math.min( bytesLeftInFile, bytesLeftInBuffer );
        dst.limit( dst.position() + maxReadable);
        int read = channel.read( dst );
        if ( read < 0 )
        {
            return read;
        }

        position += read;

        return read;
    }

    @Override
    public boolean isOpen()
    {
        return channel.isOpen();
    }

    @Override
    public void close() throws IOException
    {
        channel.close();
    }
}
