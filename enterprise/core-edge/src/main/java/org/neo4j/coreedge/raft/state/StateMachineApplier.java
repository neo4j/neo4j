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
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import org.neo4j.coreedge.raft.log.RaftLogCompactedException;
import org.neo4j.coreedge.raft.log.RaftLogCursor;
import org.neo4j.coreedge.raft.log.ReadableRaftLog;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.System.currentTimeMillis;

public class StateMachineApplier extends LifecycleAdapter implements Supplier<StateMachine>
{
    public static final long NOTHING = -1;

    private StateMachine stateMachine;
    private final ReadableRaftLog raftLog;
    private final StateStorage<LastAppliedState> lastAppliedStorage;
    private final int flushEvery;
    private final Supplier<DatabaseHealth> dbHealth;
    private final Log log;
    private long lastApplied = NOTHING;

    private Executor executor;

    private long commitIndex = NOTHING;
    private long lastFlushed = NOTHING;

    public StateMachineApplier(
            ReadableRaftLog raftLog,
            StateStorage<LastAppliedState> lastAppliedStorage,
            Executor executor,
            int flushEvery,
            Supplier<DatabaseHealth> dbHealth,
            LogProvider logProvider )
    {
        this.raftLog = raftLog;
        this.lastAppliedStorage = lastAppliedStorage;
        this.flushEvery = flushEvery;
        this.log = logProvider.getLog( getClass() );
        this.dbHealth = dbHealth;
        this.executor = executor;
    }

    public void setStateMachine( StateMachine stateMachine, long lastApplied )
    {
        this.stateMachine = stateMachine;
        this.lastApplied = this.lastFlushed = lastApplied;
    }

    @Override
    public StateMachine get()
    {
        return stateMachine;
    }

    public synchronized void notifyUpdate()
    {
        long commitIndex = raftLog.commitIndex();
        if ( this.commitIndex != commitIndex )
        {
            this.commitIndex = commitIndex;
            executor.execute( () -> {
                try
                {
                    applyUpTo( commitIndex );
                }
                catch ( Exception e )
                {
                    log.error( "Failed to apply up to index " + commitIndex, e );
                    dbHealth.get().panic( e );
                }
            } );
        }
    }

    private void applyUpTo( long commitIndex ) throws IOException, RaftLogCompactedException
    {
        try ( RaftLogCursor cursor = raftLog.getEntryCursor( lastApplied + 1 ) )
        {
            while ( cursor.next() && lastApplied < commitIndex )
            {
                long indexToApply = lastApplied + 1;
                stateMachine.applyCommand( cursor.get().content(), indexToApply );

                lastApplied = indexToApply;

                if ( indexToApply % this.flushEvery == 0 )
                {
                    stateMachine.flush();
                    lastAppliedStorage.persistStoreData( new LastAppliedState( lastApplied ) );
                    lastFlushed = lastApplied;
                }
            }
        }
    }

    @Override
    public synchronized void start() throws IOException, RaftLogCompactedException
    {
        lastFlushed = lastApplied = lastAppliedStorage.getInitialState().get();
        log.info( "Replaying commands from index %d to index %d", lastApplied, raftLog.commitIndex() );

        long start = currentTimeMillis();
        applyUpTo( raftLog.commitIndex() );
        log.info( "Replay done, took %d ms", currentTimeMillis() - start );
    }

    public long lastFlushed()
    {
        return lastFlushed;
    }
}
