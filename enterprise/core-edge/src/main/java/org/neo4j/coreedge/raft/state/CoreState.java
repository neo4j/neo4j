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
package org.neo4j.coreedge.raft.state;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.neo4j.coreedge.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.coreedge.catchup.storecopy.core.CoreStateType;
import org.neo4j.coreedge.discovery.CoreServerSelectionException;
import org.neo4j.coreedge.raft.RaftStateMachine;
import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.log.RaftLogCursor;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.log.monitoring.RaftLogCommitIndexMonitor;
import org.neo4j.coreedge.raft.log.pruning.LogPruner;
import org.neo4j.coreedge.raft.log.segmented.InFlightMap;
import org.neo4j.coreedge.raft.replication.DistributedOperation;
import org.neo4j.coreedge.raft.replication.ProgressTracker;
import org.neo4j.coreedge.raft.replication.session.GlobalSessionTrackerState;
import org.neo4j.coreedge.raft.replication.tx.CoreReplicatedContent;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.edge.CoreServerSelectionStrategy;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

public class CoreState extends LifecycleAdapter implements RaftStateMachine, LogPruner
{
    private static final long NOTHING = -1;
    private final RaftLog raftLog;
    private final StateStorage<Long> lastFlushedStorage;
    private final int flushEvery;
    private final ProgressTracker progressTracker;
    private final StateStorage<Long> lastApplyingStorage;
    private final StateStorage<GlobalSessionTrackerState<CoreMember>> sessionStorage;
    private final Supplier<DatabaseHealth> dbHealth;
    private final InFlightMap<Long,RaftLogEntry> inFlightMap;
    private final Log log;
    private final CoreStateApplier applier;
    private final CoreServerSelectionStrategy selectionStrategy;
    private final CoreStateDownloader downloader;
    private final RaftLogCommitIndexMonitor commitIndexMonitor;
    private final OperationBatcher batcher;

    private GlobalSessionTrackerState<CoreMember> sessionState = new GlobalSessionTrackerState<>();
    private CoreStateMachines coreStateMachines;

    private long lastApplied = NOTHING;
    private long lastSeenCommitIndex = NOTHING;
    private long lastFlushed = NOTHING;

    public CoreState(
            RaftLog raftLog,
            int maxBatchSize,
            int flushEvery,
            Supplier<DatabaseHealth> dbHealth,
            LogProvider logProvider,
            ProgressTracker progressTracker,
            StateStorage<Long> lastFlushedStorage,
            StateStorage<Long> lastApplyingStorage,
            StateStorage<GlobalSessionTrackerState<CoreMember>> sessionStorage,
            CoreServerSelectionStrategy selectionStrategy,
            CoreStateApplier applier,
            CoreStateDownloader downloader,
            InFlightMap<Long,RaftLogEntry> inFlightMap,
            Monitors monitors )
    {
        this.raftLog = raftLog;
        this.lastFlushedStorage = lastFlushedStorage;
        this.flushEvery = flushEvery;
        this.progressTracker = progressTracker;
        this.lastApplyingStorage = lastApplyingStorage;
        this.sessionStorage = sessionStorage;
        this.applier = applier;
        this.downloader = downloader;
        this.selectionStrategy = selectionStrategy;
        this.log = logProvider.getLog( getClass() );
        this.dbHealth = dbHealth;
        this.inFlightMap = inFlightMap;
        this.commitIndexMonitor = monitors.newMonitor( RaftLogCommitIndexMonitor.class, getClass() );
        this.batcher = new OperationBatcher( maxBatchSize );
    }

    synchronized void setStateMachine( CoreStateMachines coreStateMachines )
    {
        this.coreStateMachines = coreStateMachines;
    }

    public void skip( long lastApplied )
    {
        this.lastApplied = this.lastFlushed = lastApplied;
        log.info( format( "Skipping lastApplied index forward to %d", lastApplied ) );
    }

    @Override
    public synchronized void notifyCommitted( long commitIndex )
    {
        assert this.lastSeenCommitIndex <= commitIndex;
        if ( this.lastSeenCommitIndex < commitIndex )
        {
            this.lastSeenCommitIndex = commitIndex;
            submitApplyJob( commitIndex );
            commitIndexMonitor.commitIndex( commitIndex );
        }
    }

    private void submitApplyJob( long lastToApply )
    {
        applier.submit( ( status ) -> () -> {
            try ( LogEntrySupplier logEntrySupplier = new LogEntrySupplier() )
            {
                lastApplyingStorage.persistStoreData( lastToApply );
                for ( long logIndex = lastApplied + 1; !status.isCancelled() && logIndex <= lastToApply; logIndex++ )
                {
                    RaftLogEntry entry = logEntrySupplier.get( logIndex );
                    if ( entry == null )
                    {
                        throw new IllegalStateException( "Committed log entry must exist." );
                    }

                    if ( entry.content() instanceof DistributedOperation )
                    {
                        DistributedOperation distributedOperation = (DistributedOperation) entry.content();
                        progressTracker.trackReplication( distributedOperation );
                        batcher.add( logIndex, distributedOperation );
                    }
                    else
                    {
                        batcher.flush();
                    }
                }
                batcher.flush();
            }
            catch ( Throwable e )
            {
                log.error( "Failed to apply up to index " + lastToApply, e );
                dbHealth.get().panic( e );
            }
        } );
    }

    private class OperationBatcher
    {
        private List<DistributedOperation> batch;
        private int maxBatchSize;
        private long lastIndex;

        OperationBatcher( int maxBatchSize )
        {
            this.batch = new ArrayList<>( maxBatchSize );
            this.maxBatchSize = maxBatchSize;
        }

        private void add( long index, DistributedOperation operation ) throws Exception
        {
            if ( batch.size() > 0 )
            {
                assert index == (lastIndex + 1);
            }

            batch.add( operation );
            lastIndex = index;

            if ( batch.size() == maxBatchSize )
            {
                flush();
            }
        }

        private void flush() throws Exception
        {
            if ( batch.size() == 0 )
            {
                return;
            }

            long startIndex = lastIndex - batch.size() + 1;
            handleOperations( startIndex, batch );
            lastApplied = lastIndex;

            batch.clear();
            maybeFlush();
        }
    }

    private class LogEntrySupplier implements AutoCloseable
    {
        private RaftLogCursor cursor;
        private boolean useInFlightMap = true;

        public RaftLogEntry get( long logIndex ) throws IOException
        {
            RaftLogEntry entry = null;

            if ( useInFlightMap )
            {
                entry = inFlightMap.retrieve( logIndex );
            }

            if ( entry == null )
            {
                useInFlightMap = false;
                entry = getUsingCursor( logIndex );
            }

            inFlightMap.unregister( logIndex );

            return entry;
        }

        private RaftLogEntry getUsingCursor( long logIndex ) throws IOException
        {
            if ( cursor == null )
            {
                cursor = raftLog.getEntryCursor( logIndex );
            }

            if ( cursor.next() )
            {
                assert cursor.index() == logIndex;
                return cursor.get();
            }
            else
            {
                return null;
            }
        }

        @Override
        public void close() throws Exception
        {
            if ( cursor != null )
            {
                cursor.close();
            }
        }
    }

    @Override
    public synchronized void notifyNeedFreshSnapshot()
    {
        try
        {
            downloadSnapshot( selectionStrategy.coreServer() );
        }
        catch ( CoreServerSelectionException | InterruptedException | StoreCopyFailedException e )
        {
            log.error( "Failed to download snapshot", e );
        }
    }

    /**
     * Compacts the core state.
     *
     * @throws IOException
     */
    public void compact() throws IOException
    {
        raftLog.prune( lastFlushed );
    }

    /**
     * Attempts to download a fresh snapshot from another core instance.
     *
     * @param source The source address to attempt a download of a snapshot from.
     */
    public synchronized void downloadSnapshot( AdvertisedSocketAddress source )
            throws InterruptedException, StoreCopyFailedException
    {
        applier.sync( true );
        downloader.downloadSnapshot( source, this );
    }

    private void handleOperations( long commandIndex, List<DistributedOperation> operations )
    {
        try ( CommandDispatcher dispatcher = coreStateMachines.commandDispatcher() )
        {
            for ( DistributedOperation operation : operations )
            {
                if ( !sessionState.validateOperation( operation.globalSession(), operation.operationId() ) )
                {
                    continue;
                }

                CoreReplicatedContent command = (CoreReplicatedContent) operation.content();
                command.dispatch( dispatcher, commandIndex,
                        result -> progressTracker.trackResult( operation, result ) );

                sessionState.update( operation.globalSession(), operation.operationId(), commandIndex );
                commandIndex++;
            }
        }
    }

    private void maybeFlush() throws IOException
    {
        if ( (lastApplied - lastFlushed) > flushEvery )
        {
            flush();
        }
    }

    private void flush() throws IOException
    {
        coreStateMachines.flush();
        sessionStorage.persistStoreData( sessionState );
        lastFlushedStorage.persistStoreData( lastApplied );
        lastFlushed = lastApplied;
    }

    @Override
    public synchronized void start() throws IOException, InterruptedException
    {
        lastFlushed = lastApplied = lastFlushedStorage.getInitialState();
        log.info( format( "Restoring last applied index to %d", lastApplied ) );
        sessionState = sessionStorage.getInitialState();

        submitApplyJob( lastApplyingStorage.getInitialState() );
        applier.sync( false );
    }

    @Override
    public synchronized void stop() throws Throwable
    {
        applier.sync( true );
        flush();
    }

    public synchronized CoreSnapshot snapshot() throws IOException, InterruptedException
    {
        applier.sync( false );

        long prevIndex = lastApplied;
        long prevTerm = raftLog.readEntryTerm( prevIndex );
        CoreSnapshot coreSnapshot = new CoreSnapshot( prevIndex, prevTerm );

        coreStateMachines.addSnapshots( coreSnapshot );
        coreSnapshot.add( CoreStateType.SESSION_TRACKER, sessionState.newInstance() );

        return coreSnapshot;
    }

    synchronized void installSnapshot( CoreSnapshot coreSnapshot )
    {
        coreStateMachines.installSnapshots( coreSnapshot );
        sessionState = coreSnapshot.get( CoreStateType.SESSION_TRACKER );
    }

    @Override
    public void prune() throws IOException
    {
        compact();
    }
}
