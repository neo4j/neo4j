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
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;

import javax.transaction.xa.Xid;

import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.xa.XaCommandReader;
import org.neo4j.kernel.impl.transaction.XidImpl;
import org.neo4j.kernel.impl.util.IoPrimitiveUtils;

public class LogEntryReaderv1 implements LogEntryReader<ReadableByteChannel>
{
    private static final short CURRENT_FORMAT_VERSION = ( LogEntry.CURRENT_VERSION ) & 0xFF;
    static final int LOG_HEADER_SIZE = 16;

    public static long[] readLogHeader( FileSystemAbstraction fileSystem, File file ) throws IOException
    {
        FileChannel channel = fileSystem.open( file, "r" );
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

    public byte readVersion( ReadableByteChannel channel ) throws IOException
    {
        try
        {
            return readNextByte( channel );
        }
        catch ( ReadPastEndException e )
        {
            throw new IOException( e );
        }
    }

    public LogEntry readLogEntry( byte type, ReadableByteChannel channel ) throws IOException
    {
        try
        {
            switch ( type )
            {
                case LogEntry.TX_START:
                    return readTxStartEntry( channel );
                case LogEntry.TX_PREPARE:
                    return readTxPrepareEntry( channel );
                case LogEntry.TX_1P_COMMIT:
                    return readTxOnePhaseCommitEntry( channel );
                case LogEntry.TX_2P_COMMIT:
                    return readTxTwoPhaseCommitEntry( channel );
                case LogEntry.COMMAND:
                    return readTxCommandEntry( channel );
                case LogEntry.DONE:
                    return readTxDoneEntry( channel );
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

    private LogEntry.Start readTxStartEntry( ReadableByteChannel channel ) throws IOException, ReadPastEndException
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
        return new LogEntry.Start( xid, identifier, masterId, myId, -1, timeWritten, latestCommittedTxWhenStarted );
    }

    private LogEntry.Prepare readTxPrepareEntry( ReadableByteChannel channel ) throws IOException, ReadPastEndException
    {
        return new LogEntry.Prepare( readNextInt( channel ), readNextLong( channel ) );
    }

    private LogEntry.OnePhaseCommit readTxOnePhaseCommitEntry( ReadableByteChannel channel )
            throws IOException, ReadPastEndException
    {
        return new LogEntry.OnePhaseCommit( readNextInt( channel ),
                readNextLong( channel ), readNextLong( channel ) );
    }

    private LogEntry.Done readTxDoneEntry( ReadableByteChannel channel )
            throws IOException, ReadPastEndException
    {
        return new LogEntry.Done( readNextInt( channel ) );
    }

    private LogEntry.TwoPhaseCommit readTxTwoPhaseCommitEntry( ReadableByteChannel channel )
            throws IOException, ReadPastEndException
    {
        return new LogEntry.TwoPhaseCommit( readNextInt( channel ), readNextLong( channel ), readNextLong( channel ) );
    }

    private LogEntry.Command readTxCommandEntry( ReadableByteChannel channel )
            throws IOException, ReadPastEndException
    {
        int identifier = readNextInt( channel );
        XaCommand command = commandReader.read( channel );
        if ( command == null )
        {
            return null;
        }
        return new LogEntry.Command( identifier, command );
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
//        byteBuffer.clear();
//        byteBuffer.limit( numberOfBytes );
//        if ( channel.read( byteBuffer ) != byteBuffer.limit() )
//        {
//            throw new ReadPastEndException();
//        }
//        byteBuffer.flip();
//        return byteBuffer;
        if ( IoPrimitiveUtils.readAndFlip( channel, byteBuffer, numberOfBytes ) )
        {
            return byteBuffer;
        }
        throw new IOException("fuck");
    }

    private byte[] readIntoArray( ReadableByteChannel channel, int numberOfBytes )
            throws IOException, ReadPastEndException
    {
        byte[] result = new byte[ numberOfBytes ];
//        byteBuffer.clear();
//        byteBuffer.limit( numberOfBytes );
//        if ( channel.read( byteBuffer ) != byteBuffer.limit() )
//        {
//            throw new ReadPastEndException();
//        }
//        byteBuffer.flip();
        if ( IoPrimitiveUtils.readAndFlip( channel, byteBuffer, numberOfBytes ) )
        {
            byteBuffer.get( result );
            return result;
        }
        throw new IOException("fuck");
    }

    private ByteBuffer byteBuffer;
    private XaCommandReader commandReader;

    public void setCommandReader( XaCommandReader commandReader )
    {
        this.commandReader = commandReader;
    }

    public void setByteBuffer( ByteBuffer byteBuffer )
    {
        this.byteBuffer = byteBuffer;
    }
}