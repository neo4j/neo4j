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

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.WritableLogChannel;

import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_VERSION;

public class LogHeaderWriter
{
    public static void writeLogHeader( WritableLogChannel channel, long logVersion, long previousCommittedTxId )
            throws IOException
    {
        channel.putLong( encodeLogVersion( logVersion ) );
        channel.putLong( previousCommittedTxId );
    }

    public static ByteBuffer writeLogHeader( ByteBuffer buffer, long logVersion, long previousCommittedTxId )
    {
        buffer.clear();
        buffer.putLong( encodeLogVersion( logVersion ) );
        buffer.putLong( previousCommittedTxId );
        buffer.flip();
        return buffer;
    }

    public static void writeLogHeader( FileSystemAbstraction fileSystem, File file, long logVersion,
                                       long previousLastCommittedTxId ) throws IOException
    {
        try ( StoreChannel channel = fileSystem.open( file, "rw" ) )
        {
            writeLogHeader( channel, logVersion, previousLastCommittedTxId );
        }
    }

    public static void writeLogHeader( StoreChannel channel, long logVersion, long previousLastCommittedTxId )
            throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate( LOG_HEADER_SIZE );
        writeLogHeader( buffer, logVersion, previousLastCommittedTxId );
        channel.write( buffer );
    }

    public static long encodeLogVersion( long logVersion )
    {
        return logVersion | (((long) CURRENT_FORMAT_VERSION) << 56);
    }
}
