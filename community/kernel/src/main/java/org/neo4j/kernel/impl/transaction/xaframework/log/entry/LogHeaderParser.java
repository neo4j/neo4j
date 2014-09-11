/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework.log.entry;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.xaframework.IllegalLogFormatException;
import org.neo4j.kernel.impl.transaction.xaframework.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.xaframework.WritableLogChannel;

import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogVersions.CURRENT_LOG_VERSION;
import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogVersions.OLDEST_STILL_SUPPORTED_LOG_VERSION;

public class LogHeaderParser
{
    private static final short CURRENT_FORMAT_VERSION = CURRENT_LOG_VERSION & 0xFF;
    private static final short OLDEST_STILL_SUPPORTED__FORMAT_VERSION = OLDEST_STILL_SUPPORTED_LOG_VERSION & 0xFF;
    public static final int LOG_HEADER_SIZE = 16;

    public static LogHeader readLogHeader( ReadableLogChannel channel ) throws IOException
    {
        long encodedLogVersions = channel.getLong();
        byte logFormatVersion = decodeLogFormatVersion( encodedLogVersions );
        long logVersion = decodeLogVersion( logFormatVersion, encodedLogVersions, true );
        long previousCommittedTx = channel.getLong();
        return new LogHeader( logFormatVersion, logVersion, previousCommittedTx );
    }

    public static LogHeader readLogHeader( FileSystemAbstraction fileSystem, File file ) throws IOException
    {
        try ( StoreChannel channel = fileSystem.open( file, "r" ) )
        {
            return readLogHeader( ByteBuffer.allocateDirect( 100 * 1000 ), channel, true );
        }
    }

    /**
     * @return long[] {logVersion, lastCommittedTxIdOfPreviousLog, logFormatVersion}
     */
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
        long logVersion = decodeLogVersion( logFormatVersion, encodedLogVersions, strict );
        long previousCommittedTx = buffer.getLong();
        return new LogHeader( logFormatVersion, logVersion, previousCommittedTx );
    }

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
            ByteBuffer buffer = ByteBuffer.allocate( LOG_HEADER_SIZE );
            writeLogHeader( buffer, logVersion, previousLastCommittedTxId );
            channel.write( buffer );
        }
    }

    public static long encodeLogVersion( long logVersion )
    {
        return logVersion | (((long) CURRENT_FORMAT_VERSION) << 56);
    }

    public static long decodeLogVersion( byte logFormatVersion, long encLogVersion, boolean strict )
            throws IllegalLogFormatException
    {
        if ( strict && !isSupportLogFormat( logFormatVersion ) )
        {
            throw new IllegalLogFormatException( CURRENT_FORMAT_VERSION, logFormatVersion );
        }
        return (encLogVersion & 0x00FFFFFFFFFFFFFFL);
    }

    public static byte decodeLogFormatVersion( long encLogVersion )
    {
        return (byte) ((encLogVersion >> 56) & 0xFF);
    }

    private static boolean isSupportLogFormat( byte logFormatVersion )
    {
        if ( logFormatVersion > CURRENT_FORMAT_VERSION )
        {
            return false;
        }

        return logFormatVersion >= OLDEST_STILL_SUPPORTED__FORMAT_VERSION && logFormatVersion <= CURRENT_FORMAT_VERSION;

    }
}
