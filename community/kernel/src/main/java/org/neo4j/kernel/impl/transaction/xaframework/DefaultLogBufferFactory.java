/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.IOException;
import java.nio.channels.FileChannel;

public class DefaultLogBufferFactory implements LogBufferFactory
{
    public LogBuffer create( FileChannel fileChannel )
        throws IOException
    {
        return new DirectMappedLogBuffer( fileChannel );
    }
    
    public FileChannel combine( FileChannel fileChannel, LogBuffer logBuffer ) throws IOException
    {
        // Opening up another FileChannel to an already opened
        // file with such a buffer won't be able see the latest changes so
        // it needs to be wrapped in a FileChannel temporarily combining those two
        // (the opened FileChannel and the write buffer).
        
        // We don't need synchronization on the ByteBuffer here because
        // this thread which will read from it won't have to see changes
        // (appends) made to it and its underlying byte array is final anyway
        // (HeapByteBuffer). Maybe a bad assumption? But nice to skip
        // synchronization.
        CloseableByteBuffer byteBuffer = ((DirectMappedLogBuffer)logBuffer).getBuffer();
        byteBuffer.flip();
        return new BufferedReadableByteChannel( fileChannel, byteBuffer );
    }
}
