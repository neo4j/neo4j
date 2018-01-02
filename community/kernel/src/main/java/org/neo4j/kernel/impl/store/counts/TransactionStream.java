/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.counts;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.IOCursor;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;

import static javax.transaction.xa.Xid.MAXBQUALSIZE;
import static javax.transaction.xa.Xid.MAXGTRIDSIZE;

import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel.DEFAULT_READ_AHEAD_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;

class TransactionStream
{
    private final LogFile[] files;
    private final FileSystemAbstraction fs;
    private final boolean isContiguous;
    private final LogFile firstFile;

    public TransactionStream( FileSystemAbstraction fs, File path ) throws IOException
    {
        this.fs = fs;
        { // read metadata from all files in the directory
            ByteBuffer buffer = ByteBuffer.allocateDirect( 9 + MAXGTRIDSIZE + MAXBQUALSIZE * 10 );
            File[] files = fs.listFiles( path, new TxFileFilter( fs ) );
            ArrayList<LogFile> logFiles = new ArrayList<>( files.length );
            for ( File file : files )
            {
                LogFile logFile = new LogFile( fs, file, buffer );
                if ( logFile.lastTxId > 0 )
                {
                    logFiles.add( logFile );
                }
            }
            this.files = logFiles.toArray( new LogFile[logFiles.size()] );
        }
        Arrays.sort( files );
        { // verify the read metadata
            LogFile first = null, last = null;
            boolean isContiguous = true;
            for ( LogFile file : files )
            {
                if ( last == null )
                {
                    first = file;
                }
                else if ( last.lastTxId < file.header.lastCommittedTxId )
                {
                    isContiguous = false;
                }
                last = file;
            }
            if ( first == null )
            {
                throw new IOException( path + " does not contain any log files" );
            }
            this.firstFile = first;
            this.isContiguous = isContiguous;
        }
    }

    public boolean isContiguous()
    {
        return isContiguous;
    }

    public long firstTransactionId()
    {
        return firstFile.header.lastCommittedTxId;
    }

    public IOCursor<CommittedTransactionRepresentation> cursor() throws IOException
    {
        return new Cursor();
    }

    public String rangeString( String prefixForEach )
    {
        StringBuilder result = new StringBuilder();
        for ( LogFile file : files )
        {
            file.rangeString( result.append( prefixForEach ) );
        }
        return result.toString();
    }

    private class Cursor implements IOCursor<CommittedTransactionRepresentation>
    {
        private final ByteBuffer buffer = ByteBuffer.allocateDirect( 9 + MAXGTRIDSIZE + MAXBQUALSIZE * 10 );
        private int file;
        private CommittedTransactionRepresentation current;
        private IOCursor<LogEntry> entries;

        @Override
        public CommittedTransactionRepresentation get()
        {
            if ( current == null )
            {
                throw new IllegalStateException( "Cursor not initialized." );
            }
            return current;
        }

        @Override
        public boolean next() throws IOException
        {
            while ( null == (current = readTransaction( entries )) )
            {
                if ( entries != null )
                {
                    entries.close();
                }
                if ( null == (entries = openNext()) )
                {
                    return false;
                }
            }
            return true;
        }

        private IOCursor<LogEntry> openNext() throws IOException
        {
            return file >= files.length ? null : logEntryCursor( files[file++], buffer );
        }

        @Override
        public void close() throws IOException
        {
            if ( entries != null )
            {
                entries.close();
            }
            file = files.length;
        }

        private CommittedTransactionRepresentation readTransaction( IOCursor<LogEntry> entries ) throws IOException
        {
            if ( entries == null )
            {
                return null;
            }
            LogEntryStart start = null;
            List<Command> commands = new ArrayList<>();
            while ( entries.next() )
            {
                LogEntry entry = entries.get();
                if ( entry instanceof LogEntryStart )
                {
                    start = (LogEntryStart) entry;
                    commands.clear();
                }
                else if ( entry instanceof LogEntryCommit && start != null )
                {
                    LogEntryCommit commit = (LogEntryCommit) entry;
                    if ( commit.getTxId() > files[file].cap )
                    {
                        return null;
                    }
                    PhysicalTransactionRepresentation transaction = new PhysicalTransactionRepresentation(
                            commands );
                    transaction.setHeader( start.getAdditionalHeader(), start.getMasterId(),
                                           start.getLocalId(), start.getTimeWritten(),
                                           start.getLastCommittedTxWhenTransactionStarted(),
                                           commit.getTimeWritten(), -1 );
                    return new CommittedTransactionRepresentation( start, transaction, commit );
                }
                else
                {
                    commands.add( entry.<LogEntryCommand>as().getXaCommand() );
                }
            }
            return null;
        }
    }

    private static class LogFile implements Comparable<LogFile>
    {
        private final LogHeader header;
        private final File file;
        private final long lastTxId;
        long cap = Long.MAX_VALUE;

        LogFile( FileSystemAbstraction fs, File file, ByteBuffer buffer ) throws IOException
        {
            this.file = file;
            try ( StoreChannel channel = fs.open( file, "r" ) )
            {
                header = readLogHeader( buffer, channel, false );

                try ( IOCursor<LogEntry> cursor = logEntryCursor( channel, header ) )
                {
                    long txId = header.lastCommittedTxId;
                    while ( cursor.next() )
                    {
                        LogEntry entry = cursor.get();
                        if ( entry instanceof LogEntryCommit )
                        {
                            txId = ((LogEntryCommit) entry).getTxId();
                        }
                    }
                    this.lastTxId = txId;
                }
            }
            finally
            {
                buffer.clear();
            }
        }

        @Override
        public String toString()
        {
            return rangeString( new StringBuilder() ).toString();
        }

        @Override
        public int hashCode()
        {
            return header.hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( obj instanceof LogFile )
            {
                LogFile that = (LogFile) obj;
                return file.equals( that.file ) && header.equals( that.header );
            }
            return false;
        }

        @Override
        public int compareTo( LogFile that )
        {
            int cmp = Long.compare( this.header.lastCommittedTxId, that.header.lastCommittedTxId );
            if ( cmp == 0 )
            {
                cmp = Long.compare( this.lastTxId, that.lastTxId );
            }
            return cmp;
        }

        StringBuilder rangeString( StringBuilder target )
        {
            return target.append( ']' ).append( header.lastCommittedTxId )
                         .append( ", " ).append( lastTxId ).append( ']' );
        }
    }

    private IOCursor<LogEntry> logEntryCursor( LogFile file, ByteBuffer buffer ) throws IOException
    {
        StoreChannel channel = fs.open( file.file, "r" );
        try
        {
            LogHeader header = readLogHeader( buffer, channel, false );
            if ( !header.equals( file.header ) )
            {
                throw new IOException( "Files have changed on disk." );
            }
        }
        finally
        {
            buffer.clear();
        }
        return logEntryCursor( channel, file.header );
    }

    private static IOCursor<LogEntry> logEntryCursor( StoreChannel channel, LogHeader header ) throws IOException
    {
        return new LogEntryCursor( new ReadAheadLogChannel(
                new PhysicalLogVersionedStoreChannel( channel, header.logVersion, header.logFormatVersion ),
                NO_MORE_CHANNELS, DEFAULT_READ_AHEAD_SIZE ) );
    }

    private static class TxFileFilter implements FilenameFilter
    {

        private final FileSystemAbstraction fs;

        TxFileFilter( FileSystemAbstraction fs )
        {
            this.fs = fs;
        }

        @Override
        public boolean accept( File dir, String name )
        {
            File file = new File( dir, name );
            if ( fs.fileExists( file ) && !fs.isDirectory( file ) )
            {
                if ( name.startsWith( PhysicalLogFile.DEFAULT_NAME ) && !name.contains( "active" ) )
                {
                    return true;
                }
            }
            return false;
        }
    }
}
