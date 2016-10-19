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
package org.neo4j.causalclustering.core.consensus.log.segmented;

import java.io.File;
import java.io.IOException;
import java.time.Clock;

import org.neo4j.causalclustering.core.consensus.log.EntryRecord;
import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogCursor;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import org.neo4j.cursor.IOCursor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.neo4j.kernel.impl.util.JobScheduler.SchedulingStrategy.POOLED;

/**
 * The segmented RAFT log is an append only log supporting the operations required to support
 * the RAFT consensus algorithm.
 * <p>
 * A RAFT log must be able to append new entries, but also truncate not yet committed entries,
 * prune out old compacted entries and skip to a later starting point.
 * <p>
 * The RAFT log consists of a sequence of individual log files, called segments, with
 * the following format:
 * <p>
 * [HEADER] [ENTRY]*
 * <p>
 * So a header with zero or more entries following it. Each segment file contains a consecutive
 * sequence of appended entries. The operations of truncating and skipping in the log is implemented
 * by switching to the next segment file, called the next version. A new segment file is also started
 * when the threshold for a particular file has been reached.
 */
public class SegmentedRaftLog extends LifecycleAdapter implements RaftLog
{
    private final int READER_POOL_MAX_AGE = 1; // minutes

    private final FileSystemAbstraction fileSystem;
    private final File directory;
    private final long rotateAtSize;
    private final ChannelMarshal<ReplicatedContent> contentMarshal;
    private final FileNames fileNames;
    private final JobScheduler scheduler;
    private final Log log;

    private boolean needsRecovery;
    private final LogProvider logProvider;
    private final SegmentedRaftLogPruner pruner;

    private State state;
    private final ReaderPool readerPool;
    private JobScheduler.JobHandle readerPoolPruner;

    public SegmentedRaftLog( FileSystemAbstraction fileSystem, File directory, long rotateAtSize,
            ChannelMarshal<ReplicatedContent> contentMarshal, LogProvider logProvider, int readerPoolSize, Clock clock,
            JobScheduler scheduler, CoreLogPruningStrategy pruningStrategy )
    {
        this.fileSystem = fileSystem;
        this.directory = directory;
        this.rotateAtSize = rotateAtSize;
        this.contentMarshal = contentMarshal;
        this.logProvider = logProvider;
        this.scheduler = scheduler;

        this.fileNames = new FileNames( directory );
        this.readerPool = new ReaderPool( readerPoolSize, logProvider, fileNames, fileSystem, clock );
        this.pruner = new SegmentedRaftLogPruner( pruningStrategy );
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public synchronized void start() throws IOException, DamagedLogStorageException, DisposedException
    {
        if ( !directory.exists() && !directory.mkdirs() )
        {
            throw new IOException( "Could not create: " + directory );
        }

        state = new RecoveryProtocol( fileSystem, fileNames, readerPool, contentMarshal, logProvider ).run();
        log.info( "log started with recovered state %s", state );

        readerPoolPruner = scheduler.scheduleRecurring( new JobScheduler.Group( "reader-pool-pruner", POOLED ),
                () -> readerPool.prune( READER_POOL_MAX_AGE, MINUTES ), READER_POOL_MAX_AGE, READER_POOL_MAX_AGE, MINUTES );
    }

    @Override
    public synchronized void stop() throws Throwable
    {
        readerPoolPruner.cancel( false );
        readerPool.close();
        state.segments.close();
    }

    @Override
    public synchronized long append( RaftLogEntry... entries ) throws IOException
    {
        ensureOk();

        try
        {
            for ( RaftLogEntry entry : entries )
            {
                state.appendIndex++;
                state.terms.append( state.appendIndex, entry.term() );
                state.segments.last().write( state.appendIndex, entry );
            }
            state.segments.last().flush();
        }
        catch ( Throwable e )
        {
            needsRecovery = true;
            throw e;
        }

        if ( state.segments.last().position() >= rotateAtSize )
        {
            rotateSegment( state.appendIndex, state.appendIndex, state.terms.latest() );
        }

        return state.appendIndex;
    }

    private void ensureOk()
    {
        if ( needsRecovery )
        {
            throw new IllegalStateException( "Raft log requires recovery" );
        }
    }

    @Override
    public synchronized void truncate( long fromIndex ) throws IOException
    {
        if ( state.appendIndex < fromIndex )
        {
            throw new IllegalArgumentException( "Cannot truncate at index " + fromIndex + " when append index is " +
                                                state.appendIndex );
        }

        long newAppendIndex = fromIndex - 1;
        long newTerm = readEntryTerm( newAppendIndex );
        truncateSegment( state.appendIndex, newAppendIndex, newTerm );

        state.appendIndex = newAppendIndex;
        state.terms.truncate( fromIndex );
    }

    private void rotateSegment( long prevFileLastIndex, long prevIndex, long prevTerm ) throws IOException
    {
        state.segments.last().closeWriter();
        state.segments.rotate( prevFileLastIndex, prevIndex, prevTerm );
    }

    private void truncateSegment( long prevFileLastIndex, long prevIndex, long prevTerm ) throws IOException
    {
        state.segments.last().closeWriter();
        state.segments.truncate( prevFileLastIndex, prevIndex, prevTerm );
    }

    private void skipSegment( long prevFileLastIndex, long prevIndex, long prevTerm ) throws IOException
    {
        state.segments.last().closeWriter();
        state.segments.skip( prevFileLastIndex, prevIndex, prevTerm );
    }

    @Override
    public long appendIndex()
    {
        return state.appendIndex;
    }

    @Override
    public long prevIndex()
    {
        return state.prevIndex;
    }

    @Override
    public RaftLogCursor getEntryCursor( long fromIndex ) throws IOException
    {
        final IOCursor<EntryRecord> inner = new EntryCursor( state.segments, fromIndex );
        return new SegmentedRaftLogCursor( fromIndex, inner );
    }

    @Override
    public synchronized long skip( long newIndex, long newTerm ) throws IOException
    {
        log.info( "Skipping from {index: %d, term: %d} to {index: %d, term: %d}",
                state.appendIndex, state.terms.latest(), newIndex, newTerm );

        if ( state.appendIndex < newIndex )
        {
            skipSegment( state.appendIndex, newIndex, newTerm );
            state.terms.skip( newIndex, newTerm );

            state.prevIndex = newIndex;
            state.prevTerm = newTerm;
            state.appendIndex = newIndex;
        }

        return state.appendIndex;
    }

    private RaftLogEntry readLogEntry( long logIndex ) throws IOException
    {
        try ( IOCursor<EntryRecord> cursor = new EntryCursor( state.segments, logIndex ) )
        {
            return cursor.next() ? cursor.get().logEntry() : null;
        }
    }

    @Override
    public long readEntryTerm( long logIndex ) throws IOException
    {
        if ( logIndex > state.appendIndex )
        {
            return -1;
        }
        long term = state.terms.get( logIndex );
        if ( term == -1 && logIndex >= state.prevIndex )
        {
            RaftLogEntry entry = readLogEntry( logIndex );
            term = (entry != null) ? entry.term() : -1;
        }
        return term;
    }

    @Override
    public long prune( long safeIndex ) throws IOException
    {
        long pruneIndex = pruner.getIndexToPruneFrom( safeIndex, state.segments );
        SegmentFile oldestNotDisposed = state.segments.prune( pruneIndex );

        long newPrevIndex = oldestNotDisposed.header().prevIndex();
        long newPrevTerm = oldestNotDisposed.header().prevTerm();

        if ( newPrevIndex > state.prevIndex )
        {
            state.prevIndex = newPrevIndex;
        }

        if ( newPrevTerm > state.prevTerm )
        {
            state.prevTerm = newPrevTerm;
        }

        state.terms.prune( state.prevIndex );

        return state.prevIndex;
    }
}
