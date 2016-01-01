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
package org.neo4j.kernel.impl.storemigration.legacystore.v20;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import javax.transaction.xa.Xid;

import org.neo4j.kernel.impl.storemigration.legacystore.LegacyLogIoUtil;
import org.neo4j.kernel.impl.transaction.XidImpl;
import org.neo4j.kernel.impl.transaction.xaframework.IllegalLogFormatException;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.ReadPastEndException;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;

public class Legacy20LogIoUtil implements LegacyLogIoUtil
{
    private static final short LEGACY_FORMAT_VERSION = ((byte) 3) & 0xFF;
    static final int LOG_HEADER_SIZE = 16;

    private final CommandReader commandReader;

    public Legacy20LogIoUtil( CommandReader commandReader )
    {
        this.commandReader = commandReader;
    }

    @Override
    public long[] readLogHeader( ByteBuffer buffer, ReadableByteChannel channel,
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
        long logFormatVersion = (version >> 56) & 0xFF;
        if ( LEGACY_FORMAT_VERSION != logFormatVersion )
        {
            throw new IllegalLogFormatException( LEGACY_FORMAT_VERSION, logFormatVersion );
        }
        version = version & 0x00FFFFFFFFFFFFFFL;
        return new long[]{version, previousCommittedTx};
    }

    @Override
    public LogEntry readEntry( ByteBuffer buffer, ReadableByteChannel channel ) throws IOException
    {
        try
        {
            return readLogEntry( buffer, channel );
        }
        catch ( ReadPastEndException e )
        {
            return null;
        }
    }

    private LogEntry readLogEntry( ByteBuffer buffer, ReadableByteChannel channel )
            throws IOException, ReadPastEndException
    {
        byte entry = readNextByte( buffer, channel );
        switch ( entry )
        {
            case LogEntry.TX_START:
                return readTxStartEntry( buffer, channel );
            case LogEntry.TX_PREPARE:
                return readTxPrepareEntry( buffer, channel );
            case LogEntry.TX_1P_COMMIT:
                return readTxOnePhaseCommitEntry( buffer, channel );
            case LogEntry.TX_2P_COMMIT:
                return readTxTwoPhaseCommitEntry( buffer, channel );
            case LogEntry.COMMAND:
                return readTxCommandEntry( buffer, channel );
            case LogEntry.DONE:
                return readTxDoneEntry( buffer, channel );
            case LogEntry.EMPTY:
                return null;
            default:
                throw new IOException( "Unknown entry[" + entry + "]" );
        }
    }

    private LogEntry.Start readTxStartEntry( ByteBuffer buf,
                                             ReadableByteChannel channel ) throws IOException,
            ReadPastEndException
    {
        byte globalIdLength = readNextByte( buf, channel );
        byte branchIdLength = readNextByte( buf, channel );
        byte globalId[] = new byte[globalIdLength];
        readIntoBufferAndFlip( ByteBuffer.wrap( globalId ), channel, globalIdLength );
        byte branchId[] = new byte[branchIdLength];
        readIntoBufferAndFlip( ByteBuffer.wrap( branchId ), channel, branchIdLength );
        int identifier = readNextInt( buf, channel );
        @SuppressWarnings("unused")
        int formatId = readNextInt( buf, channel );
        int masterId = readNextInt( buf, channel );
        int myId = readNextInt( buf, channel );
        long timeWritten = readNextLong( buf, channel );
        long latestCommittedTxWhenStarted = readNextLong( buf, channel );

        // re-create the transaction
        Xid xid = new XidImpl( globalId, branchId );
        return new LogEntry.Start( xid, identifier, masterId, myId, -1, timeWritten, latestCommittedTxWhenStarted );
    }

    private LogEntry.Prepare readTxPrepareEntry( ByteBuffer buf,
                                                 ReadableByteChannel channel ) throws IOException,
            ReadPastEndException
    {
        return new LogEntry.Prepare( readNextInt( buf, channel ), readNextLong( buf, channel ) );
    }

    private LogEntry.OnePhaseCommit readTxOnePhaseCommitEntry( ByteBuffer buf,
                                                               ReadableByteChannel channel ) throws
            IOException, ReadPastEndException
    {
        return new LogEntry.OnePhaseCommit( readNextInt( buf, channel ),
                readNextLong( buf, channel ), readNextLong( buf, channel ) );
    }

    private LogEntry.Done readTxDoneEntry( ByteBuffer buf,
                                           ReadableByteChannel channel ) throws IOException, ReadPastEndException
    {
        return new LogEntry.Done( readNextInt( buf, channel ) );
    }

    private LogEntry.TwoPhaseCommit readTxTwoPhaseCommitEntry( ByteBuffer buf,
                                                               ReadableByteChannel channel ) throws
            IOException, ReadPastEndException
    {
        return new LogEntry.TwoPhaseCommit( readNextInt( buf, channel ),
                readNextLong( buf, channel ), readNextLong( buf, channel ) );
    }

    private LogEntry.Command readTxCommandEntry( ByteBuffer buf, ReadableByteChannel channel )
            throws IOException, ReadPastEndException
    {
        int identifier = readNextInt( buf, channel );
        XaCommand command = commandReader.readCommand( channel, buf );
        if ( command == null )
        {
            return null;
        }
        return new LogEntry.Command( identifier, command );
    }


    private int readNextInt( ByteBuffer buf, ReadableByteChannel channel )
            throws IOException, ReadPastEndException
    {
        return readIntoBufferAndFlip( buf, channel, 4 ).getInt();
    }

    private long readNextLong( ByteBuffer buf, ReadableByteChannel channel )
            throws IOException, ReadPastEndException
    {
        return readIntoBufferAndFlip( buf, channel, 8 ).getLong();
    }

    private byte readNextByte( ByteBuffer buf, ReadableByteChannel channel )
            throws IOException, ReadPastEndException
    {
        return readIntoBufferAndFlip( buf, channel, 1 ).get();
    }

    private ByteBuffer readIntoBufferAndFlip( ByteBuffer buf, ReadableByteChannel channel,
                                              int numberOfBytes ) throws IOException, ReadPastEndException
    {
        buf.clear();
        buf.limit( numberOfBytes );
        if ( channel.read( buf ) != buf.limit() )
        {
            throw new ReadPastEndException();
        }
        buf.flip();
        return buf;
    }
}
