/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.state;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import org.neo4j.causalclustering.SessionTracker;
import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import org.neo4j.causalclustering.core.consensus.log.monitoring.RaftLogCommitIndexMonitor;
import org.neo4j.causalclustering.core.replication.DistributedOperation;
import org.neo4j.causalclustering.core.replication.ProgressTracker;
import org.neo4j.causalclustering.core.state.machines.tx.CoreReplicatedContent;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import org.neo4j.causalclustering.helper.StatUtil;
import org.neo4j.function.ThrowingAction;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.Math.max;
import static java.lang.String.format;

public class CommandApplicationProcess
{
    private static final long NOTHING = -1;
    private final RaftLog raftLog;
    private final int flushEvery;
    private final ProgressTracker progressTracker;
    private final SessionTracker sessionTracker;
    private final Supplier<DatabaseHealth> dbHealth;
    private final InFlightCache inFlightCache;
    private final Log log;
    private final CoreState coreState;
    private final RaftLogCommitIndexMonitor commitIndexMonitor;
    private final CommandBatcher batcher;
    private final StatUtil.StatContext batchStat;

    private long lastFlushed = NOTHING;
    private int pauseCount = 1; // we are created in the paused state
    private Thread applierThread;
    private final ApplierState applierState = new ApplierState();

    public CommandApplicationProcess(
            RaftLog raftLog,
            int maxBatchSize,
            int flushEvery,
            Supplier<DatabaseHealth> dbHealth,
            LogProvider logProvider,
            ProgressTracker progressTracker,
            SessionTracker sessionTracker,
            CoreState coreState,
            InFlightCache inFlightCache,
            Monitors monitors )
    {
        this.raftLog = raftLog;
        this.flushEvery = flushEvery;
        this.progressTracker = progressTracker;
        this.sessionTracker = sessionTracker;
        this.log = logProvider.getLog( getClass() );
        this.dbHealth = dbHealth;
        this.coreState = coreState;
        this.inFlightCache = inFlightCache;
        this.commitIndexMonitor = monitors.newMonitor( RaftLogCommitIndexMonitor.class, getClass() );
        this.batcher = new CommandBatcher( maxBatchSize, this::applyBatch );
        this.batchStat = StatUtil.create( "BatchSize", log, 4096, true );
    }

    void notifyCommitted( long commitIndex )
    {
        applierState.notifyCommitted( commitIndex );
    }

    private class ApplierState
    {
        // core applier state, synchronized by ApplierState monitor
        private long lastSeenCommitIndex = NOTHING;

        // owned by applier
        private volatile long lastApplied = NOTHING;
        private volatile boolean panic;

        private volatile boolean keepRunning = true; // clear to shutdown the apply job

        private synchronized long getLastSeenCommitIndex()
        {
            return lastSeenCommitIndex;
        }

        void panic()
        {
            panic = true;
            keepRunning = false;
        }

        synchronized void setKeepRunning( boolean keepRunning )
        {
            if ( panic )
            {
                throw new IllegalStateException( "The applier has panicked" );
            }

            this.keepRunning = keepRunning;
            notifyAll();
        }

        synchronized long awaitJob()
        {
            while ( lastApplied >= lastSeenCommitIndex && keepRunning )
            {
                ignoringInterrupts( this::wait );
            }
            return lastSeenCommitIndex;
        }

        synchronized void notifyCommitted( long commitIndex )
        {
            if ( lastSeenCommitIndex < commitIndex )
            {
                lastSeenCommitIndex = commitIndex;
                commitIndexMonitor.commitIndex( commitIndex );
                notifyAll();
            }
        }
    }

    private void applyJob()
    {
        while ( applierState.keepRunning )
        {
            try
            {
                applyUpTo( applierState.awaitJob() );
            }
            catch ( Throwable e )
            {
                applierState.panic();
                log.error( "Failed to apply", e );
                dbHealth.get().panic( e );
                return; // LET THREAD DIE
            }
        }
    }

    private void applyUpTo( long applyUpToIndex ) throws Exception
    {
        try ( InFlightLogEntryReader logEntrySupplier = new InFlightLogEntryReader( raftLog, inFlightCache, true ) )
        {
            for ( long logIndex = applierState.lastApplied + 1; applierState.keepRunning && logIndex <= applyUpToIndex; logIndex++ )
            {
                RaftLogEntry entry = logEntrySupplier.get( logIndex );
                if ( entry == null )
                {
                    throw new IllegalStateException( format( "Committed log entry at index %d must exist.", logIndex ) );
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
                    // since this last entry didn't get in the batcher we need to update the lastApplied:
                    applierState.lastApplied = logIndex;
                }
            }
            batcher.flush();
        }
    }

    public long lastApplied()
    {
        return applierState.lastApplied;
    }

    /**
     * The applier must be paused when installing a snapshot.
     *
     * @param coreSnapshot The snapshot to install.
     */
    void installSnapshot( CoreSnapshot coreSnapshot )
    {
        assert pauseCount > 0;
        applierState.lastApplied = lastFlushed = coreSnapshot.prevIndex();
    }

    synchronized long lastFlushed()
    {
        return lastFlushed;
    }

    private void applyBatch( long lastIndex, List<DistributedOperation> batch ) throws Exception
    {
        if ( batch.size() == 0 )
        {
            return;
        }

        batchStat.collect( batch.size() );

        long startIndex = lastIndex - batch.size() + 1;
        long lastHandledIndex = handleOperations( startIndex, batch );
        assert lastHandledIndex == lastIndex;
        applierState.lastApplied = lastIndex;

        maybeFlushToDisk();
    }

    private long handleOperations( long commandIndex, List<DistributedOperation> operations )
    {
        try ( CommandDispatcher dispatcher = coreState.commandDispatcher() )
        {
            for ( DistributedOperation operation : operations )
            {
                if ( !sessionTracker.validateOperation( operation.globalSession(), operation.operationId() ) )
                {
                    if ( log.isDebugEnabled() )
                    {
                        log.debug( "Skipped an invalid distributed operation: " + operation + ". Session tracker state: " + sessionTracker.snapshot() );
                    }
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
        return commandIndex - 1;
    }

    private void maybeFlushToDisk() throws IOException
    {
        if ( (applierState.lastApplied - lastFlushed) > flushEvery )
        {
            coreState.flush( applierState.lastApplied );
            lastFlushed = applierState.lastApplied;
        }
    }

    public synchronized void start() throws Exception
    {
        // TODO: check None/Partial/Full here, because this is the first level which can
        // TODO: bootstrapping RAFT can also be performed from here.

        if ( lastFlushed == NOTHING )
        {
            lastFlushed = coreState.getLastFlushed();
        }
        applierState.lastApplied = lastFlushed;

        log.info( format( "Restoring last applied index to %d", lastFlushed ) );
        sessionTracker.start();

        /* Considering the order in which state is flushed, the state machines will
         * always be furthest ahead and indicate the furthest possible state to
         * which we must replay to reach a consistent state. */
        long lastPossiblyApplying = max( coreState.getLastAppliedIndex(), applierState.getLastSeenCommitIndex() );

        if ( lastPossiblyApplying > applierState.lastApplied )
        {
            log.info( "Applying up to: " + lastPossiblyApplying );
            applyUpTo( lastPossiblyApplying );
        }

        resumeApplier( "startup" );
    }

    public synchronized void stop() throws IOException
    {
        pauseApplier( "shutdown" );
        coreState.flush( applierState.lastApplied );
    }

    private void spawnApplierThread()
    {
        applierState.setKeepRunning( true );
        applierThread = new Thread( this::applyJob, "core-state-applier" );
        applierThread.start();
    }

    private void stopApplierThread()
    {
        applierState.setKeepRunning( false );
        ignoringInterrupts( () -> applierThread.join() );
    }

    public synchronized void pauseApplier( String reason )
    {
        if ( pauseCount < 0 )
        {
            throw new IllegalStateException( "Unmatched pause/resume" );
        }

        pauseCount++;
        log.info( format( "Pausing due to %s (count = %d)", reason, pauseCount ) );

        if ( pauseCount == 1 )
        {
            stopApplierThread();
        }
    }

    public synchronized void resumeApplier( String reason )
    {
        if ( pauseCount <= 0 )
        {
            throw new IllegalStateException( "Unmatched pause/resume" );
        }

        pauseCount--;
        log.info( format( "Resuming after %s (count = %d)", reason, pauseCount ) );

        if ( pauseCount == 0 )
        {
            spawnApplierThread();
        }
    }

    /**
     * We do not expect the interrupt system to be used here,
     * so we ignore them and log a warning.
     */
    private void ignoringInterrupts( ThrowingAction<InterruptedException> action )
    {
        try
        {
            action.apply();
        }
        catch ( InterruptedException e )
        {
            log.warn( "Unexpected interrupt", e );
        }
    }
}
