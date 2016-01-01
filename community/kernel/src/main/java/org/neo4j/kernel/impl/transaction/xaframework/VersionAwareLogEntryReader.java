/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import javax.transaction.xa.Xid;

import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.nioneo.xa.XaCommandReader;
import org.neo4j.kernel.impl.nioneo.xa.XaCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.XidImpl;
import org.neo4j.kernel.impl.util.IoPrimitiveUtils;

/**
 * Version aware implementation of LogEntryReader
 * Starting with Neo4j version 2.1, log entries are prefixed with a version. This allows for Neo4j instances of
 * different versions to exchange transaction data, either directly or via logical logs. This implementation of
 * LogEntryReader makes use of the version information to deserialize command entries that hold commands created
 * with previous versions of Neo4j. Support for this comes from the required {@link XaCommandReaderFactory} which can
 * provide deserializers for Commands given the version.
 */
public class VersionAwareLogEntryReader implements LogEntryReader<ReadableByteChannel>
{
    private static final short CURRENT_FORMAT_VERSION = ( LogEntry.CURRENT_LOG_VERSION) & 0xFF;
    static final int LOG_HEADER_SIZE = 16;

    private ByteBuffer byteBuffer;
    private final XaCommandReaderFactory commandReaderFactory;

    public VersionAwareLogEntryReader( ByteBuffer byteBuffer, XaCommandReaderFactory commandReaderFactory )
    {
        this.byteBuffer = byteBuffer;
        this.commandReaderFactory = commandReaderFactory;
    }

    public static long[] readLogHeader( FileSystemAbstraction fileSystem, File file ) throws IOException
    {
        StoreChannel channel = fileSystem.open( file, "r" );
        try
        {
            return readLogHeader( ByteBuffer.allocateDirect( 100*1000 ), channel, true );
        }
        finally
        {
            channel.close();
        }
    }

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
        return new long[] { version, previousCommittedTx, logFormatVersion };
    }

    public LogEntry readLogEntry( ReadableByteChannel channel ) throws IOException
    {
        /*
         * This is a hack particular to switching from unversioned to versioned LogEntries. Negative bytes
         * at the beginning indicate that the indeed exists version info and must be consumed before we get
         * to the actual type. But, for rolling upgrades from unversioned entries, that does not exist, so the
         * first byte is the actual type. That is a mismatch that can be resolved externally, hence this conditional
         * extra byte read. After 2.1 is released we can remove it.
         */

        byte version = 0;

        byteBuffer.clear();
        byteBuffer.limit( 1 );
        if ( channel.read( byteBuffer ) != byteBuffer.limit() )
        {
            return null;
        }
        byteBuffer.flip();

        byte type = byteBuffer.get();

        if ( type < 0 )
        {
            byteBuffer.clear();
            byteBuffer.limit( 1 );
            if ( channel.read( byteBuffer ) != byteBuffer.limit() )
            {
                return null;
            }
            byteBuffer.flip();

            version = type;
            type = byteBuffer.get();
        }

        try
        {
            switch ( type )
            {
                case LogEntry.TX_START:
                    return readTxStartEntry( version, channel );
                case LogEntry.TX_PREPARE:
                    return readTxPrepareEntry( version, channel );
                case LogEntry.TX_1P_COMMIT:
                    return readTxOnePhaseCommitEntry( version, channel );
                case LogEntry.TX_2P_COMMIT:
                    return readTxTwoPhaseCommitEntry( version, channel );
                case LogEntry.COMMAND:
                    return readTxCommandEntry( version, channel );
                case LogEntry.DONE:
                    return readTxDoneEntry( version, channel );
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

    private LogEntry.Start readTxStartEntry( byte version, ReadableByteChannel channel ) throws IOException, ReadPastEndException
    {
        // TODO can these arrays be reused, given that each LogEntryReader manages only one start entry at a time
        byte globalIdLength = readNextByte( channel );
        byte branchIdLength = readNextByte( channel );
        byte globalId[] = readIntoArray( channel, globalIdLength );
        byte branchId[] = readIntoArray( channel, branchIdLength );
        int identifier = readNextInt( channel );
        @SuppressWarnings("unused")
        int formatId = readNextInt( channel );
        int masterId = readNextInt( channel );
        int myId = readNextInt( channel );
        long timeWritten = readNextLong( channel );
        long latestCommittedTxWhenStarted = readNextLong( channel );

        // re-create the transaction
        Xid xid = new XidImpl( globalId, branchId );
        return new LogEntry.Start( xid, identifier, version, masterId, myId, -1, timeWritten, latestCommittedTxWhenStarted );
    }

    private LogEntry.Prepare readTxPrepareEntry( byte version, ReadableByteChannel channel ) throws IOException, ReadPastEndException
    {
        return new LogEntry.Prepare( readNextInt( channel ), version, readNextLong( channel ) );
    }

    private LogEntry.OnePhaseCommit readTxOnePhaseCommitEntry( byte version, ReadableByteChannel channel )
            throws IOException, ReadPastEndException
    {
        return new LogEntry.OnePhaseCommit( readNextInt( channel ), version, readNextLong( channel ),
                readNextLong( channel ) );
    }

    private LogEntry.Done readTxDoneEntry( byte version, ReadableByteChannel channel )
            throws IOException, ReadPastEndException
    {
        return new LogEntry.Done( readNextInt( channel ), version );
    }

    private LogEntry.TwoPhaseCommit readTxTwoPhaseCommitEntry( byte version, ReadableByteChannel channel )
            throws IOException, ReadPastEndException
    {
        return new LogEntry.TwoPhaseCommit( readNextInt( channel ), version, readNextLong( channel ), readNextLong( channel ) );
    }

    private LogEntry.Command readTxCommandEntry( byte version, ReadableByteChannel channel )
            throws IOException, ReadPastEndException
    {
        int identifier = readNextInt( channel );
        XaCommandReader commandReader = commandReaderFactory.newInstance( version, byteBuffer );
        XaCommand command = commandReader.read( channel );
        if ( command == null )
        {
            return null;
        }
        return new LogEntry.Command( identifier, version, command );
    }

    private int readNextInt( ReadableByteChannel channel )
            throws IOException, ReadPastEndException
    {
        return readIntoBufferAndFlip( channel, 4 ).getInt();
    }

    private long readNextLong( ReadableByteChannel channel )
            throws IOException, ReadPastEndException
    {
        return readIntoBufferAndFlip( channel, 8 ).getLong();
    }

    public byte readNextByte( ReadableByteChannel channel )
            throws IOException, ReadPastEndException
    {
        return readIntoBufferAndFlip( channel, 1 ).get();
    }

    private ByteBuffer readIntoBufferAndFlip( ReadableByteChannel channel,
            int numberOfBytes ) throws IOException, ReadPastEndException
    {
        if ( IoPrimitiveUtils.readAndFlip( channel, byteBuffer, numberOfBytes ) )
        {
            return byteBuffer;
        }
        throw new ReadPastEndException();
    }

    private byte[] readIntoArray( ReadableByteChannel channel, int numberOfBytes )
            throws IOException, ReadPastEndException
    {
        byte[] result = new byte[ numberOfBytes ];
        if ( IoPrimitiveUtils.readAndFlip( channel, byteBuffer, numberOfBytes ) )
        {
            byteBuffer.get( result );
            return result;
        }
        throw new ReadPastEndException();
    }
}
