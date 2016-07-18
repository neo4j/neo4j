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

import org.neo4j.coreedge.SessionTracker;
import org.neo4j.coreedge.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.coreedge.discovery.CoreServerSelectionException;
import org.neo4j.coreedge.raft.RaftStateMachine;
import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.log.monitoring.RaftLogCommitIndexMonitor;
import org.neo4j.coreedge.raft.log.pruning.LogPruner;
import org.neo4j.coreedge.raft.log.segmented.InFlightMap;
import org.neo4j.coreedge.raft.replication.DistributedOperation;
import org.neo4j.coreedge.raft.replication.ProgressTracker;
import org.neo4j.coreedge.raft.replication.tx.CoreReplicatedContent;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.edge.CoreServerSelectionStrategy;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.Math.max;
import static java.lang.String.format;

public class CoreState extends LifecycleAdapter implements RaftStateMachine, LogPruner
{
    private static final long NOTHING = -1;
    private final RaftLog raftLog;
    private final StateStorage<Long> lastFlushedStorage;
    private final int flushEvery;
    private final ProgressTracker progressTracker;
    private final SessionTracker sessionTracker;
    private final Supplier<DatabaseHealth> dbHealth;
    private final InFlightMap<Long,RaftLogEntry> inFlightMap;
    private final Log log;
    private final CoreStateApplier applier;
    private final CoreServerSelectionStrategy someoneElse;
    private final CoreStateDownloader downloader;
    private final RaftLogCommitIndexMonitor commitIndexMonitor;
    private final OperationBatcher batcher;

    private CoreStateMachines coreStateMachines;

    private long lastApplied = NOTHING;
    private long lastSeenCommitIndex = NOTHING;
    private long lastFlushed = NOTHING;

    public CoreState(
            CoreStateMachines coreStateMachines, RaftLog raftLog,
            int maxBatchSize,
            int flushEvery,
            Supplier<DatabaseHealth> dbHealth,
            LogProvider logProvider,
            ProgressTracker progressTracker,
            StateStorage<Long> lastFlushedStorage,
            SessionTracker sessionTracker,
            CoreServerSelectionStrategy someoneElse,
            CoreStateApplier applier,
            CoreStateDownloader downloader,
            InFlightMap<Long, RaftLogEntry> inFlightMap,
            Monitors monitors )
    {
        this.coreStateMachines = coreStateMachines;
        this.raftLog = raftLog;
        this.lastFlushedStorage = lastFlushedStorage;
        this.flushEvery = flushEvery;
        this.progressTracker = progressTracker;
        this.sessionTracker = sessionTracker;
        this.someoneElse = someoneElse;
        this.applier = applier;
        this.downloader = downloader;
        this.log = logProvider.getLog( getClass() );
        this.dbHealth = dbHealth;
        this.inFlightMap = inFlightMap;
        this.commitIndexMonitor = monitors.newMonitor( RaftLogCommitIndexMonitor.class, getClass() );
        this.batcher = new OperationBatcher( maxBatchSize );
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
            try ( InFlightLogEntryReader logEntrySupplier = new InFlightLogEntryReader( raftLog, inFlightMap, true ) )
            {
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
                        lastApplied = logIndex;
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

    public synchronized long lastApplied()
    {
        return lastApplied;
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

    @Override
    public synchronized void notifyNeedFreshSnapshot()
    {
        try
        {
            downloadSnapshot( someoneElse.coreServer() );
        }
        catch ( CoreServerSelectionException e )
        {
            log.error( "Failed to select server", e );
        }
    }

    public void compact() throws IOException
    {
        raftLog.prune( lastFlushed );
    }

    /**
     * Attempts to download a fresh snapshot from another core instance.
     *
     * @param source The source address to attempt a download of a snapshot from.
     */
    public synchronized void downloadSnapshot( CoreMember source )
    {
        try
        {
            applier.sync( true );
            downloader.downloadSnapshot( source, this );
        }
        catch ( InterruptedException | StoreCopyFailedException e )
        {
            log.error( "Failed to download snapshot", e );
        }
    }

    private void handleOperations( long commandIndex, List<DistributedOperation> operations )
    {
        try ( CommandDispatcher dispatcher = coreStateMachines.commandDispatcher() )
        {
            for ( DistributedOperation operation : operations )
            {
                if ( !sessionTracker.validateOperation( operation.globalSession(), operation.operationId() ) )
                {
                    commandIndex++;
                    continue;
                }

                CoreReplicatedContent command = (CoreReplicatedContent) operation.content();
                command.dispatch( dispatcher, commandIndex,
                        result -> progressTracker.trackResult( operation, result ) );

                sessionTracker.update( operation.globalSession(), operation.operationId(), commandIndex );
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
        sessionTracker.flush();
        lastFlushedStorage.persistStoreData( lastApplied );
        lastFlushed = lastApplied;
    }

    @Override
    public synchronized void start() throws IOException, InterruptedException
    {
        lastFlushed = lastApplied = lastFlushedStorage.getInitialState();
        log.info( format( "Restoring last applied index to %d", lastApplied ) );
        sessionTracker.start();

        /* Considering the order in which state is flushed, the state machines will
         * always be furthest ahead and indicate the furthest possible state to
         * which we must replay to reach a consistent state. */
        long lastPossiblyApplying = max( coreStateMachines.getLastAppliedIndex(), sessionTracker.getLastAppliedIndex() );

        if ( lastPossiblyApplying > lastApplied )
        {
            log.info( "Recovering up to: " + lastPossiblyApplying );
            submitApplyJob( lastPossiblyApplying );
            applier.sync( false );
        }
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
        sessionTracker.addSnapshots( coreSnapshot );

        return coreSnapshot;
    }

    synchronized void installSnapshot( CoreSnapshot coreSnapshot )
    {
        coreStateMachines.installSnapshots( coreSnapshot );
        long snapshotPrevIndex = coreSnapshot.prevIndex();
        try
        {
            if ( snapshotPrevIndex > 1 )
            {
                raftLog.skip( snapshotPrevIndex, coreSnapshot.prevTerm() );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        this.lastApplied = this.lastFlushed = snapshotPrevIndex;
        log.info( format( "Skipping lastApplied index forward to %d", snapshotPrevIndex ) );

        sessionTracker.installSnapshots( coreSnapshot );
    }

    @Override
    public void prune() throws IOException
    {
        compact();
    }
}
