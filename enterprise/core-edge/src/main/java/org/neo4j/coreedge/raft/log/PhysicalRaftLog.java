/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.raft.log;

import static java.lang.String.format;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.cursor.IOCursor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.FlushablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.LogHeaderCache;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.LoggingLogFileMonitor;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotationImpl;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class PhysicalRaftLog implements RaftLog, Lifecycle
{
    public static final String BASE_FILE_NAME = "raft.log";
    public static final String DIRECTORY_NAME = "raft-log";

    private final PhysicalLogFile logFile;
    private final ChannelMarshal<ReplicatedContent> marshal;
    private final Supplier<DatabaseHealth> databaseHealthSupplier;
    private final Log log;
    private LogRotation logRotation;
    private FlushablePositionAwareChannel writer;
    private final RaftLogMetadataCache metadataCache;
    private final AtomicLong appendIndex = new AtomicLong( -1 );
    private long commitIndex = -1;
    private final LogPositionMarker positionMarker = new LogPositionMarker();
    private long term = -1;

    private final RaftEntryStore entryStore;

    private final PhysicalLogFiles logFiles;

    public PhysicalRaftLog( FileSystemAbstraction fileSystem, File directory, long rotateAtSize,
                            int entryCacheSize, int headerCacheSize, PhysicalLogFile.Monitor monitor,
                            ChannelMarshal<ReplicatedContent> marshal, Supplier<DatabaseHealth> databaseHealthSupplier,
                            LogProvider logProvider )
    {
        this.marshal = marshal;
        this.databaseHealthSupplier = databaseHealthSupplier;
        this.log = logProvider.getLog( getClass() );

        directory.mkdirs();

        logFiles = new PhysicalLogFiles( directory, BASE_FILE_NAME, fileSystem );
        LogVersionRepository logVersionRepository = new FilenameBasedLogVersionRepository( logFiles );

        logFile = new PhysicalLogFile( fileSystem, logFiles, rotateAtSize,
                appendIndex::get, logVersionRepository, monitor, new LogHeaderCache( headerCacheSize ) );

        this.metadataCache = new RaftLogMetadataCache( entryCacheSize );
        this.entryStore = new PhysicalRaftEntryStore( logFile, metadataCache, marshal );
    }

    @Override
    public long append( RaftLogEntry entry ) throws IOException
    {
        if ( entry.term() >= term )
        {
            term = entry.term();
        }
        else
        {
            throw new IllegalStateException( format( "Non-monotonic term %d for in entry %s in term %d",
                    entry.term(), entry.toString(), term ) );
        }

        appendIndex.incrementAndGet();
        LogPositionMarker entryStartPosition = writer.getCurrentPosition( positionMarker );
        writer.put( RecordType.APPEND.value );
        writer.putLong( appendIndex.get() );
        writer.putLong( entry.term() );
        marshal.marshal( entry.content(), writer );
        metadataCache.cacheMetadata( appendIndex.get(), entry.term(), entryStartPosition.newPosition() );
        writer.prepareForFlush().flush();

        logRotation.rotateLogIfNeeded( LogAppendEvent.NULL );
        return appendIndex.get();
    }

    @Override
    public void truncate( long fromIndex ) throws IOException
    {
        if ( fromIndex <= commitIndex )
        {
            throw new IllegalArgumentException( format( "cannot truncate (%d) before the commit index (%d)",
                    fromIndex, commitIndex ) );
        }

        if ( appendIndex.get() >= fromIndex )
        {
            appendIndex.set( fromIndex - 1 );

            writer.put( RecordType.TRUNCATE.value );
            writer.putLong( fromIndex );
            writer.prepareForFlush().flush();
            logRotation.rotateLogIfNeeded( LogAppendEvent.NULL );
        }
        term = readEntryTerm( appendIndex.get() );
    }

    @Override
    public void commit( long newCommitIndex ) throws IOException
    {
        if ( appendIndex.get() == -1 || commitIndex == appendIndex.get() )
        {
            return;
        }

        long actualNewCommitIndex = Math.min( newCommitIndex, appendIndex.get() );

        while ( commitIndex < actualNewCommitIndex )
        {
            commitIndex++;
        }
        writer.put( RecordType.COMMIT.value );
        writer.putLong( commitIndex );
        writer.prepareForFlush().flush();
        logRotation.rotateLogIfNeeded( LogAppendEvent.NULL );
    }

    @Override
    public long appendIndex()
    {
        return appendIndex.get();
    }

    @Override
    public long commitIndex()
    {
        return commitIndex;
    }

    @Override
    public IOCursor<RaftLogEntry> getEntryCursor( long fromIndex ) throws IOException
    {
        final IOCursor<RaftLogAppendRecord> inner = entryStore.getEntriesFrom( fromIndex );
        return new IOCursor<RaftLogEntry>()
        {
            @Override
            public boolean next() throws IOException
            {
                return inner.next();
            }

            @Override
            public void close() throws IOException
            {
                inner.close();
            }

            @Override
            public RaftLogEntry get()
            {
                return inner.get().getLogEntry();
            }
        };
    }

    @Override
    public RaftLogEntry readLogEntry( long logIndex ) throws IOException
    {
        try ( IOCursor<RaftLogAppendRecord> entriesFrom = entryStore.getEntriesFrom( logIndex ) )
        {
            while ( entriesFrom.next() )
            {
                RaftLogAppendRecord raftLogAppendRecord = entriesFrom.get();
                if ( raftLogAppendRecord.getLogIndex() == logIndex )
                {
                    return raftLogAppendRecord.getLogEntry();
                }
                else if ( raftLogAppendRecord.getLogIndex() > logIndex )
                {
                    throw new IllegalStateException( format( "Asked for index %d but got up to %d without " +
                            "finding it.", logIndex, raftLogAppendRecord.getLogIndex() ) );
                }
            }
        }
        return null;
    }

    @Override
    public long readEntryTerm( long logIndex ) throws IOException
    {
        // -1 is not an existing log index, but represents the beginning of the log. It is a valid value to request the
        // term for, and the term is -1.
        if( logIndex == -1 || ( logIndex > appendIndex.get() ) )
        {
            return -1;
        }
        long resultTerm = -1;
        RaftLogMetadataCache.RaftLogEntryMetadata metadata = metadataCache.getMetadata( logIndex );
        if ( metadata != null )
        {
            resultTerm = metadata.getEntryTerm();
        }
        else
        {
            RaftLogEntry raftLogEntry = readLogEntry( logIndex );
            if ( raftLogEntry != null )
            {
                resultTerm = raftLogEntry.term();
            }

        }
        return resultTerm;
    }

    @Override
    public boolean entryExists( long logIndex ) throws IOException
    {
        return appendIndex.get() >= logIndex;
    }

    @Override
    public void init() throws Throwable
    {
        logFile.init();
    }

    @Override
    public void start() throws Throwable
    {
        this.logRotation =
                new LogRotationImpl( new LoggingLogFileMonitor( log ), logFile,  databaseHealthSupplier.get() );

        logFile.start();
        restoreIndexes();
        this.writer = logFile.getWriter();
    }

    private void restoreIndexes() throws IOException
    {
        long lowestLogVersion = logFiles.getLowestLogVersion();
        ReadableLogChannel reader = logFile.getReader( new LogPosition( lowestLogVersion, LogHeader.LOG_HEADER_SIZE ) );
        try ( RaftRecordCursor<ReadableLogChannel> recordCursor = new RaftRecordCursor<>( reader, marshal ) )
        {
            while ( recordCursor.next() )
            {
                RaftLogRecord record = recordCursor.get();
                switch ( record.getType() )
                {
                    case COMMIT:
                        commitIndex = record.getLogIndex();
                        break;
                    case APPEND:
                        appendIndex.set( record.getLogIndex() );
                        break;
                    case TRUNCATE:
                        long truncateAtIndex = record.getLogIndex() - 1; // we must restore append/commit at before this index
                        appendIndex.set( truncateAtIndex );
                        commitIndex = Math.min( commitIndex, truncateAtIndex );
                        break;
                    default:
                        throw new IllegalStateException( "Record is of unknown type " + record );
                }
            }
        }
        log.info( "Restored commit index at %d, append index at %d, started at version %d (largest is %d)",
                commitIndex, appendIndex.get(), lowestLogVersion, logFiles.getHighestLogVersion() );
    }

    @Override
    public void stop() throws Throwable
    {
        logFile.stop();
        this.writer = null;
    }

    @Override
    public void shutdown() throws Throwable
    {
        logFile.shutdown();
    }

    public enum RecordType
    {
        APPEND( (byte) 0 ), COMMIT( (byte) 1 ), TRUNCATE( (byte) 2 );

        private final byte value;

        RecordType( byte value )
        {
            this.value = value;
        }

        public byte value()
        {
            return value;
        }

        public static RecordType forValue( byte value )
        {
            switch ( value )
            {
                case 0:
                    return APPEND;
                case 1:
                    return COMMIT;
                case 2:
                    return TRUNCATE;
                default:
                    throw new IllegalArgumentException( "Value " + value + " is not a known entry type" );
            }
        }
    }

}
