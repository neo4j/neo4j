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
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;

import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;

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
        try ( StoreChannel channel = fileSystem.open( file, OpenMode.READ ) )
        {
            return readLogHeader( ByteBuffer.allocate( LOG_HEADER_SIZE ), channel, strict, file );
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
        buffer.clear();
        buffer.limit( LOG_HEADER_SIZE );

        int read = channel.read( buffer );
        if ( read != LOG_HEADER_SIZE )
        {
            if ( strict )
            {
                if ( fileForAdditionalErrorInformationOrNull != null )
                {
                    throw new IncompleteLogHeaderException( fileForAdditionalErrorInformationOrNull, read );
                }
                throw new IncompleteLogHeaderException( read );
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

    static long decodeLogVersion( long encLogVersion )
    {
        return encLogVersion & 0x00FFFFFFFFFFFFFFL;
    }

    static byte decodeLogFormatVersion( long encLogVersion )
    {
        return (byte) ((encLogVersion >> 56) & 0xFF);
    }
}
