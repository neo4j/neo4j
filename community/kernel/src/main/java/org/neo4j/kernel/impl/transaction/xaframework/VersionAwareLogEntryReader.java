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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.nioneo.xa.CommandReader;
import org.neo4j.kernel.impl.nioneo.xa.CommandReaderFactory;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;

/**
 * Version aware implementation of LogEntryReader
 * Starting with Neo4j version 2.1, log entries are prefixed with a version. This allows for Neo4j instances of
 * different versions to exchange transaction data, either directly or via logical logs. This implementation of
 * LogEntryReader makes use of the version information to deserialize command entries that hold commands created
 * with previous versions of Neo4j. Support for this comes from the required {@link org.neo4j.kernel.impl.nioneo.xa.CommandReaderFactory} which can
 * provide deserializers for Commands given the version.
 */
public class VersionAwareLogEntryReader implements LogEntryReader<ReadableLogChannel>
{
    private static final short CURRENT_FORMAT_VERSION = ( LogEntry.CURRENT_LOG_VERSION) & 0xFF;
    static final int LOG_HEADER_SIZE = 16;

    private final CommandReaderFactory commandReaderFactory;
    private final LogPositionMarker positionMarker = new LogPositionMarker();

    public VersionAwareLogEntryReader( CommandReaderFactory commandReaderFactory )
    {
        this.commandReaderFactory = commandReaderFactory;
    }

    public static long[] readLogHeader( FileSystemAbstraction fileSystem, File file ) throws IOException
    {
        try ( StoreChannel channel = fileSystem.open( file, "r" ) )
        {
            return readLogHeader( ByteBuffer.allocateDirect( 100*1000 ), channel, true );
        }
    }

    /**
     * @return long[] {logVersion, lastCommittedTxIdOfPreviousLog}
     */
    public static long[] readLogHeader( ByteBuffer buffer, ReadableByteChannel channel,
            boolean strict ) throws IOException
    {
        buffer.clear();
        buffer.limit( LOG_HEADER_SIZE );

        if ( channel.read( buffer ) != LOG_HEADER_SIZE )
        {
            if ( strict )
            {
                throw new IOException( "Unable to read log version and last committed tx" );
            }
            return null;
        }
        buffer.flip();
        long version = buffer.getLong();
        long previousCommittedTx = buffer.getLong();
        long logFormatVersion = ( version >> 56 ) & 0xFF;
        if ( (strict && CURRENT_FORMAT_VERSION != logFormatVersion ) || CURRENT_FORMAT_VERSION < logFormatVersion )
        {
            throw new IllegalLogFormatException( CURRENT_FORMAT_VERSION, logFormatVersion );
        }
        version = version & 0x00FFFFFFFFFFFFFFL;
        return new long[] { version, previousCommittedTx };
    }

    public static ByteBuffer writeLogHeader( ByteBuffer buffer, long logVersion,
            long previousCommittedTxId )
    {
        buffer.clear();
        buffer.putLong( logVersion | ( ( (long) CURRENT_FORMAT_VERSION ) << 56 ) );
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

    @Override
    public LogEntry readLogEntry( ReadableLogChannel channel ) throws IOException
    {
        try
        {
            /*
             * This is a hack particular to switching from unversioned to versioned LogEntries. Negative bytes
             * at the beginning indicate that the indeed exists version info and must be consumed before we get
             * to the actual type. But, for rolling upgrades from unversioned entries, that does not exist, so the
             * first byte is the actual type. That is a mismatch that can be resolved externally, hence this conditional
             * extra byte read. After 2.1 is released we can remove it.
             */

            // TODO this is wasteful. Turn this around so that LogPosition is mutable and pass it in,
            // or introduce a LogPositionMark that is mutable and that can create LogPosition when requested.
            channel.getCurrentPosition( positionMarker );
            byte version = channel.get();
            byte type = channel.get();

            switch ( type )
            {
                case LogEntry.TX_START:
                    return readTxStartEntry( version, channel, positionMarker.newPosition() );
                case LogEntry.TX_1P_COMMIT:
                    return readTxOnePhaseCommitEntry( version, channel );
                case LogEntry.COMMAND:
                    return readTxCommandEntry( version, channel );
                case LogEntry.EMPTY:
                    return null;
                default:
                    throw new IOException( "Unknown entry[" + type + "]" );
            }
        }
        catch ( ReadPastEndException e )
        {
            return null;
        }
    }

    private LogEntry.Start readTxStartEntry( byte version, ReadableLogChannel channel,
            LogPosition position ) throws IOException
    {
        int masterId = channel.getInt();
        int authorId = channel.getInt();
        long timeWritten = channel.getLong();
        long latestCommittedTxWhenStarted = channel.getLong();
        int additionalHeaderLength = channel.getInt();
        byte[] additionalHeader = new byte[additionalHeaderLength];
        channel.get( additionalHeader, additionalHeaderLength );
        return new LogEntry.Start( version, masterId, authorId, timeWritten, latestCommittedTxWhenStarted,
                additionalHeader, position );
    }

    private LogEntry.OnePhaseCommit readTxOnePhaseCommitEntry( byte version, ReadableLogChannel channel )
            throws IOException
    {
        return new LogEntry.OnePhaseCommit( version, channel.getLong(), channel.getLong() );
    }

    private LogEntry.Command readTxCommandEntry( byte version, ReadableLogChannel channel )
            throws IOException
    {
        CommandReader commandReader = commandReaderFactory.newInstance( version );
        Command command = commandReader.read( channel );
        if ( command == null )
        {
            return null;
        }
        return new LogEntry.Command( version, command );
    }
}
