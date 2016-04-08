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
import java.util.function.Supplier;

import org.neo4j.coreedge.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.coreedge.catchup.storecopy.core.CoreStateType;
import org.neo4j.coreedge.discovery.CoreServerSelectionException;
import org.neo4j.coreedge.raft.RaftStateMachine;
import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.log.RaftLogCompactedException;
import org.neo4j.coreedge.raft.log.RaftLogCursor;
import org.neo4j.coreedge.raft.replication.DistributedOperation;
import org.neo4j.coreedge.raft.replication.ProgressTracker;
import org.neo4j.coreedge.raft.replication.session.GlobalSessionTrackerState;
import org.neo4j.coreedge.raft.replication.tx.CoreReplicatedContent;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.edge.CoreServerSelectionStrategy;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class CoreState extends LifecycleAdapter implements RaftStateMachine
{

    private static final long NOTHING = -1;
    private CoreStateMachines coreStateMachines;
    private final RaftLog raftLog;
    private final StateStorage<Long> lastFlushedStorage;
    private final int flushEvery;
    private final ProgressTracker progressTracker;
    private GlobalSessionTrackerState<CoreMember> sessionState = new GlobalSessionTrackerState<>();
    private final StateStorage<Long> lastApplyingStorage;
    private final StateStorage<GlobalSessionTrackerState<CoreMember>> sessionStorage;
    private final Supplier<DatabaseHealth> dbHealth;
    private final Log log;

    private long lastApplied = NOTHING;
    private long lastSeenCommitIndex = NOTHING;

    private long lastFlushed = NOTHING;
    private final CoreStateApplier applier;
    private final CoreServerSelectionStrategy selectionStrategy;

    private final CoreStateDownloader downloader;

    public CoreState(
            RaftLog raftLog,
            int flushEvery,
            Supplier<DatabaseHealth> dbHealth,
            LogProvider logProvider,
            ProgressTracker progressTracker,
            StateStorage<Long> lastFlushedStorage,
            StateStorage<Long> lastApplyingStorage,
            StateStorage<GlobalSessionTrackerState<CoreMember>> sessionStorage,
            CoreServerSelectionStrategy selectionStrategy,
            CoreStateApplier applier,
            CoreStateDownloader downloader )
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
    }

    public synchronized void setStateMachine( CoreStateMachines coreStateMachines, long lastApplied )
    {
        this.coreStateMachines = coreStateMachines;
        this.lastApplied = this.lastFlushed = lastApplied;
    }

    @Override
    public synchronized void notifyCommitted( long commitIndex )
    {
        if ( this.lastSeenCommitIndex != commitIndex )
        {
            this.lastSeenCommitIndex = commitIndex;
            submitApplyJob( commitIndex );
        }
    }

    private void submitApplyJob( long lastToApply )
    {
        applier.submit( ( status ) -> () -> {
            try ( RaftLogCursor cursor = raftLog.getEntryCursor( lastApplied + 1 ) )
            {
                lastApplyingStorage.persistStoreData( lastToApply );

                while ( cursor.next() && cursor.index() <= lastToApply )
                {
                    if ( cursor.get().content() instanceof DistributedOperation )
                    {
                        DistributedOperation distributedOperation = (DistributedOperation) cursor.get().content();

                        progressTracker.trackReplication( distributedOperation );
                        handleOperation( cursor.index(), distributedOperation );

                        maybeFlush();
                    }

                    assert cursor.index() == (lastApplied + 1);
                    lastApplied = cursor.index();

                    if ( status.isCancelled() )
                    {
                        return;
                    }
                }
            }
            catch ( Throwable e )
            {
                log.error( "Failed to apply up to index " + lastToApply, e );
                dbHealth.get().panic( e );
            }
        } );
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
        try
        {
            raftLog.prune( lastFlushed );
        }
        catch ( RaftLogCompactedException e )
        {
            log.warn( "Log already pruned?", e );
        }
    }

    /**
     * Attempts to download a fresh snapshot from another core instance.
     *
     * @param source The source address to attempt a download of a snapshot from.
     */
    public synchronized void downloadSnapshot( AdvertisedSocketAddress source ) throws InterruptedException, StoreCopyFailedException
    {
        if ( !applier.sync( true ) )
        {
            throw new StoreCopyFailedException( "Failed to synchronize with executor" );
        }

        downloader.downloadSnapshot( source, this );
    }

    private void handleOperation( long commandIndex, DistributedOperation operation ) throws IOException
    {
        if ( !sessionState.validateOperation( operation.globalSession(), operation.operationId() ) )
        {
            return;
        }

        CoreReplicatedContent command = (CoreReplicatedContent) operation.content();
        command.dispatch( coreStateMachines, commandIndex )
                .ifPresent( result -> progressTracker.trackResult( operation, result ) );

        sessionState.update( operation.globalSession(), operation.operationId(), commandIndex );
    }

    private void maybeFlush() throws IOException
    {
        if ( lastApplied % this.flushEvery == 0 )
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
    public synchronized void start() throws IOException, RaftLogCompactedException, InterruptedException
    {
        lastFlushed = lastApplied = lastFlushedStorage.getInitialState();
        sessionState = sessionStorage.getInitialState();

        submitApplyJob( lastApplyingStorage.getInitialState() );
        applier.sync( false );
    }

    @Override
    public synchronized void stop() throws Throwable
    {
        if ( applier.sync( true ) )
        {
            flush();
        }
    }

    public synchronized CoreSnapshot snapshot() throws IOException, RaftLogCompactedException
    {
        long prevIndex = lastApplied;
        long prevTerm = raftLog.readEntryTerm( prevIndex );
        CoreSnapshot coreSnapshot = new CoreSnapshot( prevIndex, prevTerm );

        coreStateMachines.addSnapshots( coreSnapshot );
        coreSnapshot.add( CoreStateType.SESSION_TRACKER, sessionState.newInstance() );

        return coreSnapshot;
    }

    public synchronized void installSnapshot( CoreSnapshot coreSnapshot )
    {
        coreStateMachines.installSnapshots( coreSnapshot );
        sessionState = coreSnapshot.get( CoreStateType.SESSION_TRACKER );
    }
}
