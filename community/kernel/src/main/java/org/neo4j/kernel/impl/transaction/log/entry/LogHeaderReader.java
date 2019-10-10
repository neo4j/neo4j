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
package org.neo4j.kernel.impl.transaction.log.entry;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.storageengine.api.StoreId;

import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_VERSION_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.LOG_HEADER_SIZE_3_5;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.LOG_HEADER_SIZE_4_0;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.LOG_VERSION_3_5;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.LOG_VERSION_4_0;

public class LogHeaderReader
{
    private LogHeaderReader()
    {
    }

    public static LogHeader readLogHeader( FileSystemAbstraction fileSystem, File file ) throws IOException
    {
        return readLogHeader( fileSystem, file, true );
    }

    public static LogHeader readLogHeader( FileSystemAbstraction fileSystem, File file, boolean strict ) throws IOException
    {
        try ( StoreChannel channel = fileSystem.read( file ) )
        {
            return readLogHeader( ByteBuffers.allocate( CURRENT_FORMAT_LOG_HEADER_SIZE ), channel, strict, file );
        }
    }

    /**
     * Reads the header of a log. Data will be read from {@code channel} using supplied {@code buffer}
     * as to allow more controlled allocation.
     *
     * @param buffer {@link ByteBuffer} to read into. Passed in to allow control over allocation.
     * @param channel {@link ReadableByteChannel} to read from, typically a channel over a file containing the data.
     * @param strict if {@code true} then will fail with {@link IncompleteLogHeaderException} on incomplete
     * header, i.e. if there's not enough data in the channel to even read the header. If {@code false} then
     * the return value will instead be {@code null}.
     * @param fileForAdditionalErrorInformationOrNull when in {@code strict} mode the exception can be
     * amended with information about which file the channel represents, if any. Purely for better forensics
     * ability.
     * @return {@link LogHeader} containing the log header data from the {@code channel}.
     * @throws IOException if unable to read from {@code channel}
     * @throws IncompleteLogHeaderException if {@code strict} and not enough data could be read
     */
    public static LogHeader readLogHeader( ByteBuffer buffer, ReadableByteChannel channel, boolean strict,
            File fileForAdditionalErrorInformationOrNull ) throws IOException
    {
        // Decode first part of the header that contains the version
        if ( !safeRead( buffer, channel, LOG_HEADER_VERSION_SIZE, strict, fileForAdditionalErrorInformationOrNull ) )
        {
            return null;
        }

        long encodedLogVersions = buffer.getLong();
        if ( encodedLogVersions == 0 )
        {
            // Since the format version is a non-zero number, we know we are reading a pre-allocated file
            return null;
        }

        byte logFormatVersion = decodeLogFormatVersion( encodedLogVersions );
        long logVersion = decodeLogVersion( encodedLogVersions );

        // The header's total length differs from versions
        if ( logFormatVersion == LOG_VERSION_3_5 )
        {
            if ( !safeRead( buffer, channel, Long.BYTES, strict, fileForAdditionalErrorInformationOrNull ) )
            {
                return null;
            }
            long previousCommittedTx = buffer.getLong();
            return new LogHeader( logFormatVersion, logVersion, previousCommittedTx, LOG_HEADER_SIZE_3_5 );
        }
        if ( logFormatVersion == LOG_VERSION_4_0 )
        {
            if ( !safeRead( buffer, channel, LOG_HEADER_SIZE_4_0 - LOG_HEADER_VERSION_SIZE, strict, fileForAdditionalErrorInformationOrNull ) )
            {
                return null;
            }
            long previousCommittedTx = buffer.getLong();
            StoreId storeId = new StoreId( buffer.getLong(), buffer.getLong(), buffer.getLong(), buffer.getLong(), buffer.getLong() );
            buffer.getLong(); // reserved
            return new LogHeader( logFormatVersion, logVersion, previousCommittedTx, storeId, LOG_HEADER_SIZE_4_0 );
        }

        throw new IOException( "Unrecognized transaction log format version: " + logFormatVersion );
    }

    /**
     * Try to read the {@code size} of bytes, and throw if {@code strict} is true.
     * @return true if all of the bytes were successfully read.
     */
    private static boolean safeRead( ByteBuffer buffer, ReadableByteChannel channel, int size, boolean strict,
            File fileForAdditionalErrorInformationOrNull )
            throws IOException
    {
        buffer.clear();
        buffer.limit( size );
        int read = channel.read( buffer );
        if ( read != size )
        {
            if ( strict )
            {
                if ( fileForAdditionalErrorInformationOrNull != null )
                {
                    throw new IncompleteLogHeaderException( fileForAdditionalErrorInformationOrNull, read, size );
                }
                throw new IncompleteLogHeaderException( read, size );
            }
            return false;
        }
        buffer.flip();
        return true;
    }

    static long decodeLogVersion( long encLogVersion )
    {
        return encLogVersion & 0x00FFFFFFFFFFFFFFL;
    }

    static byte decodeLogFormatVersion( long encLogVersion )
    {
        return (byte) ((encLogVersion >> 56) & 0xFF);
    }
}
