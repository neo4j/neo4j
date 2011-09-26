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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import javax.transaction.xa.Xid;

import org.neo4j.kernel.impl.transaction.XidImpl;

public class LogIoUtils
{
    private static final short CURRENT_FORMAT_VERSION = ( LogEntry.CURRENT_VERSION ) & 0xFF;
    static final int LOG_HEADER_SIZE = 16;

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
        if ( CURRENT_FORMAT_VERSION != logFormatVersion )
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

    public static LogEntry readEntry( ByteBuffer buffer, ReadableByteChannel channel,
            XaCommandFactory cf ) throws IOException
    {
        try
        {
            return readLogEntry( buffer, channel, cf );
        }
        catch ( ReadPastEndException e )
        {
            return null;
        }
    }

    public static LogEntry readLogEntry( ByteBuffer buffer, ReadableByteChannel channel, XaCommandFactory cf )
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
                return readTxCommandEntry( buffer, channel, cf );
            case LogEntry.DONE:
                return readTxDoneEntry( buffer, channel );
            case LogEntry.EMPTY:
                return null;
            default:
                throw new IOException( "Unknown entry[" + entry + "]" );
        }
    }

    private static LogEntry.Start readTxStartEntry( ByteBuffer buf,
            ReadableByteChannel channel ) throws IOException, ReadPastEndException
    {
        byte globalIdLength = readNextByte( buf, channel );
        byte branchIdLength = readNextByte( buf, channel );
        byte globalId[] = new byte[globalIdLength];
        readIntoBufferAndFlip( ByteBuffer.wrap( globalId ), channel, globalIdLength );
        byte branchId[] = new byte[branchIdLength];
        readIntoBufferAndFlip( ByteBuffer.wrap( branchId ), channel, branchIdLength );
        int identifier = readNextInt( buf, channel );
        int formatId = readNextInt( buf, channel );

        // re-create the transaction
        Xid xid = new XidImpl( globalId, branchId, formatId );
        return new LogEntry.Start( xid, identifier, -1 );
    }

    private static LogEntry.Prepare readTxPrepareEntry( ByteBuffer buf,
            ReadableByteChannel channel ) throws IOException, ReadPastEndException
    {
        return new LogEntry.Prepare( readNextInt( buf, channel ) );
    }

    private static LogEntry.OnePhaseCommit readTxOnePhaseCommitEntry( ByteBuffer buf,
            ReadableByteChannel channel ) throws IOException, ReadPastEndException
    {
        return new LogEntry.OnePhaseCommit( readNextInt( buf, channel ),
                readNextLong( buf, channel ), readNextInt( buf, channel ) );
    }

    private static LogEntry.Done readTxDoneEntry( ByteBuffer buf,
            ReadableByteChannel channel ) throws IOException, ReadPastEndException
    {
        return new LogEntry.Done( readNextInt( buf, channel ) );
    }

    private static LogEntry.TwoPhaseCommit readTxTwoPhaseCommitEntry( ByteBuffer buf,
            ReadableByteChannel channel ) throws IOException, ReadPastEndException
    {
        return new LogEntry.TwoPhaseCommit( readNextInt( buf, channel ),
                readNextLong( buf, channel ), readNextInt( buf, channel ) );
    }

    private static LogEntry.Command readTxCommandEntry(
            ByteBuffer buf, ReadableByteChannel channel, XaCommandFactory cf )
            throws IOException, ReadPastEndException
    {
        int identifier = readNextInt( buf, channel );
        XaCommand command = cf.readCommand( channel, buf );
        if ( command == null )
        {
            return null;
        }
        return new LogEntry.Command( identifier, command );
    }

    public static void writeLogEntry( LogEntry entry, LogBuffer buffer )
        throws IOException
    {
        if ( entry instanceof LogEntry.Command )
        {
            writeCommand( buffer, entry.getIdentifier(), ((LogEntry.Command) entry).getXaCommand() );
        }
        else if ( entry instanceof LogEntry.Start )
        {
            writeStart( buffer, entry.getIdentifier(), ( (LogEntry.Start) entry ).getXid() );
        }
        else if ( entry instanceof LogEntry.Done )
        {
            writeDone( buffer, entry.getIdentifier() );
        }
        else if ( entry instanceof LogEntry.OnePhaseCommit )
        {
            LogEntry.Commit commit = (LogEntry.Commit) entry;
            writeCommit( false, buffer, commit.getIdentifier(), commit.getTxId(),
                    commit.getMasterId() );
        }
        else if ( entry instanceof LogEntry.Prepare )
        {
            writePrepare( buffer, entry.getIdentifier() );
        }
        else if ( entry instanceof LogEntry.TwoPhaseCommit )
        {
            LogEntry.Commit commit = (LogEntry.Commit) entry;
            writeCommit( true, buffer, commit.getIdentifier(), commit.getTxId(),
                    commit.getMasterId() );
        }
    }

    public static void writePrepare( LogBuffer buffer, int identifier ) throws IOException
    {
        buffer.put( LogEntry.TX_PREPARE ).putInt( identifier );
    }

    public static void writeCommit( boolean twoPhase, LogBuffer buffer, int identifier, long txId,
            int masterId ) throws IOException
    {
        buffer.put( twoPhase ? LogEntry.TX_2P_COMMIT : LogEntry.TX_1P_COMMIT )
              .putInt( identifier ).putLong( txId ).putInt( masterId );
    }

    public static void writeDone( LogBuffer buffer, int identifier ) throws IOException
    {
        buffer.put( LogEntry.DONE ).putInt( identifier );
    }

    public static void writeDone( ByteBuffer buffer, int identifier )
    {
        buffer.put( LogEntry.DONE ).putInt( identifier );
    }

    public static void writeStart( LogBuffer buffer, int identifier, Xid xid )
            throws IOException
    {
        byte globalId[] = xid.getGlobalTransactionId();
        byte branchId[] = xid.getBranchQualifier();
        int formatId = xid.getFormatId();
        buffer.put( LogEntry.TX_START ).put( (byte) globalId.length ).put(
                (byte) branchId.length ).put( globalId ).put( branchId ).putInt( identifier ).putInt(
                formatId );
    }

    public static void writeCommand( LogBuffer buffer, int identifier, XaCommand command )
            throws IOException
    {
        buffer.put( LogEntry.COMMAND ).putInt( identifier );
        command.writeToFile( buffer );
    }

    private static int readNextInt( ByteBuffer buf, ReadableByteChannel channel )
            throws IOException, ReadPastEndException
    {
        return readIntoBufferAndFlip( buf, channel, 4 ).getInt();
    }

    private static long readNextLong( ByteBuffer buf, ReadableByteChannel channel )
            throws IOException, ReadPastEndException
    {
        return readIntoBufferAndFlip( buf, channel, 8 ).getLong();
    }

    public static byte readNextByte( ByteBuffer buf, ReadableByteChannel channel )
            throws IOException, ReadPastEndException
    {
        return readIntoBufferAndFlip( buf, channel, 1 ).get();
    }

    private static ByteBuffer readIntoBufferAndFlip( ByteBuffer buf, ReadableByteChannel channel,
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

    public static void moveAllLogicalLogs( File storeDir, String subDirectoryName )
    {
        File subdir = new File( storeDir, subDirectoryName );
        subdir.mkdir();
        for ( File file : storeDir.listFiles() )
        {
            if ( file.getName().contains( "nioneo_logical.log.v" ) )
            {
                file.renameTo( new File( subdir, file.getName() ) );
            }
        }
    }
}