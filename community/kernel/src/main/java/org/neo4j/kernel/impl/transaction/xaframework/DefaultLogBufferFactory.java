/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import static java.lang.Boolean.parseBoolean;
import static org.neo4j.kernel.Config.USE_MEMORY_MAPPED_BUFFERS;
import static org.neo4j.kernel.Config.osIsWindows;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Map;

public abstract class DefaultLogBufferFactory implements LogBufferFactory
{
    public static LogBufferFactory create( Map<?, ?> config )
    {
        String configValue = config != null ?
                (String) config.get( USE_MEMORY_MAPPED_BUFFERS ) : null;
        boolean memoryMapped = parseBoolean( configValue );
        if ( !memoryMapped || osIsWindows() )
        {
            // If on Windows or memory mapping is turned off the DirectMappedLogBuffer
            // is used and opening up another FileChannel to an already opened
            // file with such a buffer won't be able see the latest changes so
            // it needs to be wrapped in a FileChannel temporarily combining those two
            // (the opened FileChannel and the write buffer).
            return new LogBufferFactory()
            {
                public LogBuffer create( FileChannel fileChannel )
                    throws IOException
                {
                    return new DirectMappedLogBuffer( fileChannel );
                }
                
                public FileChannel combine( FileChannel fileChannel, LogBuffer logBuffer ) throws IOException
                {
                    // We don't need synchronization on the ByteBuffer here because
                    // this thread which will read from it won't have to see changes
                    // (appends) made to it and its underlying byte array is final anyway
                    // (HeapByteBuffer). Maybe a bad assumption? But nice to skip
                    // synchronization.
                    CloseableByteBuffer byteBuffer = ((DirectMappedLogBuffer)logBuffer).getBuffer();
                    byteBuffer.flip();
                    return new BufferedReadableByteChannel( fileChannel, byteBuffer );
                }
            };
        }
        return new DefaultLogBufferFactory() {};
    }
    
    @Override
    public LogBuffer create( FileChannel fileChannel ) throws IOException
    {
        return new MemoryMappedLogBuffer( fileChannel );
    }

    @Override
    public FileChannel combine( FileChannel fileChannel, LogBuffer logBuffer ) throws IOException
    {
        return fileChannel;
    }
}
