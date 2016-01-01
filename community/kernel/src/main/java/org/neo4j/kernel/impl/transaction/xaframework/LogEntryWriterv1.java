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

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.transaction.xa.Xid;

import org.neo4j.kernel.impl.nioneo.xa.XaCommandWriter;

public class LogEntryWriterv1 implements LogEntryWriter
{
    private static final short CURRENT_FORMAT_VERSION = ( LogEntry.CURRENT_LOG_VERSION) & 0xFF;
    static final int LOG_HEADER_SIZE = 16;

    public static ByteBuffer writeLogHeader( ByteBuffer buffer, long logVersion,
            long previousCommittedTxId )
    {
        buffer.clear();
        buffer.putLong( logVersion | ( ( (long) CURRENT_FORMAT_VERSION ) << 56 ) );
        buffer.putLong( previousCommittedTxId );
        buffer.flip();
        return buffer;
    }

    public void writeLogEntry( LogEntry entry, LogBuffer buffer ) throws IOException
    {
        if ( entry.getVersion() == LogEntry.CURRENT_LOG_ENTRY_VERSION )
        {
            buffer.put( entry.getVersion() );
        }
        switch ( entry.getType() )
        {
            case LogEntry.TX_START:
                writeStart( entry.getIdentifier(), ((LogEntry.Start) entry).getXid(),
                        ((LogEntry.Start) entry).getMasterId(), ((LogEntry.Start) entry).getLocalId(),
                        ((LogEntry.Start) entry).getTimeWritten(),
                        ((LogEntry.Start) entry).getLastCommittedTxWhenTransactionStarted(), buffer );
                break;
            case LogEntry.COMMAND:
                writeCommand( entry.getIdentifier(), ((LogEntry.Command) entry).getXaCommand(), buffer );
                break;
            case LogEntry.TX_PREPARE:
                writePrepare( entry.getIdentifier(), ((LogEntry.Prepare) entry).getTimeWritten(), buffer );
                break;
            case LogEntry.TX_1P_COMMIT:
                LogEntry.Commit commit1PC = (LogEntry.Commit) entry;
                writeCommit( false, commit1PC.getIdentifier(), commit1PC.getTxId(),
                        ((LogEntry.OnePhaseCommit) entry).getTimeWritten(), buffer );
                break;
            case LogEntry.TX_2P_COMMIT:
                LogEntry.Commit commit2PC = (LogEntry.Commit) entry;
                writeCommit( true, commit2PC.getIdentifier(), commit2PC.getTxId(),
                        ((LogEntry.TwoPhaseCommit) entry).getTimeWritten(), buffer );
                break;
            case LogEntry.DONE:
                writeDone( entry.getIdentifier(), buffer );
                break;
            default:
                throw new IllegalArgumentException("Unknown entry type " + entry.getType() );

        }
    }

    private void writePrepare( int identifier, long timeWritten, LogBuffer logBuffer ) throws IOException
    {
        logBuffer.put( LogEntry.TX_PREPARE ).putInt( identifier ).putLong( timeWritten );
    }

    private void writeCommit( boolean twoPhase, int identifier, long txId,
            long timeWritten, LogBuffer logBuffer ) throws IOException
    {
        logBuffer.put( twoPhase ? LogEntry.TX_2P_COMMIT : LogEntry.TX_1P_COMMIT )
              .putInt( identifier ).putLong( txId ).putLong( timeWritten );
    }

    private void writeDone( int identifier, LogBuffer logBuffer ) throws IOException
    {
        logBuffer.put( LogEntry.DONE ).putInt( identifier );
    }

    private void writeStart( int identifier, Xid xid, int masterId, int myId, long timeWritten,
                                   long latestCommittedTxWhenStarted, LogBuffer logBuffer )
            throws IOException
    {
        byte globalId[] = xid.getGlobalTransactionId();
        byte branchId[] = xid.getBranchQualifier();
        int formatId = xid.getFormatId();
        logBuffer
              .put( LogEntry.TX_START )
              .put( (byte) globalId.length )
              .put( (byte) branchId.length )
              .put( globalId ).put( branchId )
              .putInt( identifier )
              .putInt( formatId )
              .putInt( masterId )
              .putInt( myId )
              .putLong( timeWritten )
              .putLong( latestCommittedTxWhenStarted );
    }

    private void writeCommand( int identifier, XaCommand command, LogBuffer logBuffer )
            throws IOException
    {
        logBuffer.put( LogEntry.COMMAND ).putInt( identifier );
        commandWriter.write( command, logBuffer );
    }

    private XaCommandWriter commandWriter;

    public void setCommandWriter( XaCommandWriter commandWriter )
    {
        this.commandWriter = commandWriter;
    }
}