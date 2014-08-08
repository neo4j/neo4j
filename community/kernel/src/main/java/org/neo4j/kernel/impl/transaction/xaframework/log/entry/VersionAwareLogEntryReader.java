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
import org.neo4j.kernel.impl.nioneo.xa.CommandReader;
import org.neo4j.kernel.impl.nioneo.xa.CommandReaderFactory;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.transaction.xaframework.IllegalLogFormatException;
import org.neo4j.kernel.impl.transaction.xaframework.LogPosition;
import org.neo4j.kernel.impl.transaction.xaframework.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.xaframework.ReadPastEndException;
import org.neo4j.kernel.impl.transaction.xaframework.ReadableLogChannel;

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
    public static final int LOG_HEADER_SIZE = 16;

    private final CommandReaderFactory commandReaderFactory;
    private final LogPositionMarker positionMarker = new LogPositionMarker();

    public VersionAwareLogEntryReader()
    {
        this( new CommandReaderFactory.Default() );
    }

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
    public static long[] readLogHeader( ByteBuffer buffer, ReadableByteChannel channel, boolean strict )
            throws IOException
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
        long logVersion = decodeLogVersion(  buffer.getLong(), strict );
        long previousCommittedTx = buffer.getLong();
        return new long[] { logVersion, previousCommittedTx };
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

    static long encodeLogVersion(long logVersion) {
        return logVersion | ( ( (long) CURRENT_FORMAT_VERSION ) << 56 );
    }

    static long decodeLogVersion(long encLogVersion, boolean strict) throws IllegalLogFormatException
    {
        final long logFormatVersion = ( encLogVersion >> 56 ) & 0xFF;
        if ( (strict && CURRENT_FORMAT_VERSION != logFormatVersion ) || CURRENT_FORMAT_VERSION < logFormatVersion )
        {
            throw new IllegalLogFormatException( CURRENT_FORMAT_VERSION, logFormatVersion );
        }
        return encLogVersion & 0x00FFFFFFFFFFFFFFL;
    }

    @Override
    public LogEntry readLogEntry( ReadableLogChannel channel ) throws IOException
    {
        try
        {
            channel.getCurrentPosition( positionMarker );

            /*
             * This is a hack particular to switching from unversioned to versioned LogEntries. Negative bytes
             * at the beginning indicate that the indeed exists version info and must be consumed before we get
             * to the actual type. But, for rolling upgrades from unversioned entries, that does not exist, so the
             * first byte is the actual type. That is a mismatch that can be resolved externally, hence this conditional
             * extra byte read. After 2.1 is released we can remove it.
             */
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
                    throw new IOException( "Unknown entry[" + type + "] at position " + positionMarker.newPosition() +
                            " and entry version " + version );
            }
        }
        catch ( ReadPastEndException e )
        {
            return null;
        }
    }

    private LogEntryStart readTxStartEntry( byte version, ReadableLogChannel channel, LogPosition position )
            throws IOException
    {
        int masterId = channel.getInt();
        int authorId = channel.getInt();
        long timeWritten = channel.getLong();
        long latestCommittedTxWhenStarted = channel.getLong();
        int additionalHeaderLength = channel.getInt();
        byte[] additionalHeader = new byte[additionalHeaderLength];
        channel.get( additionalHeader, additionalHeaderLength );
        return new LogEntryStart( version, masterId, authorId, timeWritten, latestCommittedTxWhenStarted,
                additionalHeader, position );
    }

    private OnePhaseCommit readTxOnePhaseCommitEntry( byte version, ReadableLogChannel channel )
            throws IOException
    {
        long txId = channel.getLong();
        long timeWritten = channel.getLong();
        return new OnePhaseCommit( version, txId, timeWritten );
    }

    private LogEntryCommand readTxCommandEntry( byte version, ReadableLogChannel channel )
            throws IOException
    {
        CommandReader commandReader = commandReaderFactory.newInstance( version );
        Command command = commandReader.read( channel );
        if ( command == null )
        {
            return null;
        }
        return new LogEntryCommand( version, command );
    }
}
