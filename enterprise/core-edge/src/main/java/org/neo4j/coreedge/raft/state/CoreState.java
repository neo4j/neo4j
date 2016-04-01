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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.coreedge.catchup.storecopy.core.RaftStateType;
import org.neo4j.coreedge.raft.log.RaftLogCompactedException;
import org.neo4j.coreedge.raft.log.RaftLogCursor;
import org.neo4j.coreedge.raft.log.ReadableRaftLog;
import org.neo4j.coreedge.raft.replication.DistributedOperation;
import org.neo4j.coreedge.raft.replication.ProgressTracker;
import org.neo4j.coreedge.raft.replication.session.GlobalSessionTrackerState;
import org.neo4j.coreedge.raft.replication.tx.CoreReplicatedContent;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.concurrent.TimeUnit.HOURS;

public class CoreState extends LifecycleAdapter
{
    private static final long NOTHING = -1;

    private CoreStateMachines coreStateMachines;
    private final ReadableRaftLog raftLog;
    private final StateStorage<Long> lastFlushedStorage;
    private final int flushEvery;
    private final ProgressTracker progressTracker;
    private GlobalSessionTrackerState<CoreMember> sessionState = new GlobalSessionTrackerState<>();
    private final StateStorage<Long> lastApplyingStorage;
    private final StateStorage<GlobalSessionTrackerState<CoreMember>> sessionStorage;
    private final Supplier<DatabaseHealth> dbHealth;
    private final Log log;
    private long lastApplied = NOTHING;

    private ExecutorService executor;

    private long lastSeenCommitIndex = NOTHING;
    private long lastFlushed = NOTHING;

    public CoreState(
            ReadableRaftLog raftLog,
            ExecutorService executor,
            int flushEvery,
            Supplier<DatabaseHealth> dbHealth,
            LogProvider logProvider,
            ProgressTracker progressTracker,
            StateStorage<Long> lastFlushedStorage,
            StateStorage<Long> lastApplyingStorage,
            StateStorage<GlobalSessionTrackerState<CoreMember>> sessionStorage )
    {
        this.raftLog = raftLog;
        this.lastFlushedStorage = lastFlushedStorage;
        this.flushEvery = flushEvery;
        this.progressTracker = progressTracker;
        this.lastApplyingStorage = lastApplyingStorage;
        this.sessionStorage = sessionStorage;
        this.log = logProvider.getLog( getClass() );
        this.dbHealth = dbHealth;
        this.executor = executor;
    }

    public void setStateMachine( CoreStateMachines coreStateMachines, long lastApplied )
    {
        this.coreStateMachines = coreStateMachines;
        this.lastApplied = this.lastFlushed = lastApplied;
    }

    public synchronized void notifyCommitted( long commitIndex )
    {
        if ( this.lastSeenCommitIndex != commitIndex )
        {
            this.lastSeenCommitIndex = commitIndex;
            executor.execute( () -> {
                try
                {
                    applyUpTo( commitIndex );
                }
                catch ( Throwable e )
                {
                    log.error( "Failed to apply up to index " + commitIndex, e );
                    dbHealth.get().panic( e );
                }
            } );
        }
    }

    private void applyUpTo( long lastToApply ) throws IOException, RaftLogCompactedException
    {
        try ( RaftLogCursor cursor = raftLog.getEntryCursor( lastApplied + 1 ) )
        {
            lastApplyingStorage.persistStoreData( lastToApply );

            while ( cursor.next() && lastApplied < lastToApply )
            {
                if( cursor.get().content() instanceof DistributedOperation )
                {
                    DistributedOperation distributedOperation = (DistributedOperation) cursor.get().content();

                    progressTracker.trackReplication( distributedOperation );
                    handleOperation( lastApplied + 1, distributedOperation );
                    lastApplied++;

                    maybeFlush();
                }
            }
        }
    }

    private void handleOperation( long commandIndex, DistributedOperation operation ) throws IOException
    {
        if( !sessionState.validateOperation( operation.globalSession(), operation.operationId() ) )
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
            coreStateMachines.flush();
            sessionStorage.persistStoreData( sessionState );
            lastFlushedStorage.persistStoreData( lastApplied );
            lastFlushed = lastApplied;
        }
    }

    @Override
    public synchronized void start() throws IOException, RaftLogCompactedException
    {
        lastFlushed = lastApplied = lastFlushedStorage.getInitialState();
        sessionState = sessionStorage.getInitialState();

        applyUpTo( lastApplyingStorage.getInitialState() );
    }

    @Override
    public void stop() throws Throwable
    {
        executor.shutdown();
        executor.awaitTermination( 1, HOURS );
    }

    public long lastFlushed()
    {
        return lastFlushed;
    }

    public synchronized Map<RaftStateType,Object> snapshot()
    {
        Map<RaftStateType,Object> snapshots = coreStateMachines.snapshots();
        snapshots.put( RaftStateType.SESSION_TRACKER, sessionState );
        return snapshots;
    }

    public synchronized void installSnapshots( HashMap<RaftStateType,Object> snapshots )
    {
        coreStateMachines.installSnapshots( snapshots );
        sessionState = (GlobalSessionTrackerState<CoreMember>) snapshots.get( RaftStateType.SESSION_TRACKER );
    }
}
