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
package org.neo4j.kernel.impl.transaction.log.entry;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.IllegalLogFormatException;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;

public class LogHeaderReader
{
    public static LogHeader readLogHeader( FileSystemAbstraction fileSystem, File file ) throws IOException
    {
        try ( StoreChannel channel = fileSystem.open( file, "r" ) )
        {
            return readLogHeader( ByteBuffer.allocateDirect( 100 * 1000 ), channel, true );
        }
    }

    public static LogHeader readLogHeader( ByteBuffer buffer, ReadableByteChannel channel, boolean strict )
            throws IOException
    {
        buffer.clear();
        buffer.limit( LOG_HEADER_SIZE );

        int read = channel.read( buffer );
        if ( read != LOG_HEADER_SIZE )
        {
            if ( strict )
            {
                throw new IOException( "Unable to read log version and last committed tx" );
            }
            return null;
        }
        buffer.flip();
        long encodedLogVersions = buffer.getLong();
        byte logFormatVersion = decodeLogFormatVersion( encodedLogVersions );
        long logVersion = decodeLogVersion( encodedLogVersions );
        long previousCommittedTx = buffer.getLong();
        return new LogHeader( logFormatVersion, logVersion, previousCommittedTx );
    }

    public static long decodeLogVersion( long encLogVersion )
            throws IllegalLogFormatException
    {
        return (encLogVersion & 0x00FFFFFFFFFFFFFFL);
    }

    public static byte decodeLogFormatVersion( long encLogVersion )
    {
        return (byte) ((encLogVersion >> 56) & 0xFF);
    }
}
