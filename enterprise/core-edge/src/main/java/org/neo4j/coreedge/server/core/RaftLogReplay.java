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
package org.neo4j.coreedge.server.core;

import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.StateMachine;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;

public class RaftLogReplay extends LifecycleAdapter
{
    private final StateMachine stateMachine;
    private final RaftLog raftLog;
    private final int flushAfter;
    private final Log log;

    public RaftLogReplay( StateMachine stateMachine, RaftLog raftLog, LogProvider logProvider, int flushAfter )
    {
        this.stateMachine = stateMachine;
        this.raftLog = raftLog;
        this.flushAfter = flushAfter;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void start() throws Throwable
    {
        long start = currentTimeMillis();
        /*
         * Since all state machines and replicated content listeners persist their state, we can skip all entries that
         * have been committed successfully.
         * However, looking at how commit() does its thing, we probably face a race with a crash in between updating
         * the commit index and having notified all replicated content listeners. We should probably invert the order
         * there. For this reason, and having the (assumed/required) idempotent property of replicated content listeners,
         * we still replay the last committed entry because, since we do one entry at a time, that may be the only one
         * that has not been applied against all listeners.
         * This change is effectively equivalent to truncating/compacting the raft log.
         */
        long index = max( 0, raftLog.commitIndex() - 1 - flushAfter ); // new instances have a commit index of -1, which should be ignored
        log.info( "Starting replay at index %d", index );
        for(; index <= raftLog.commitIndex(); index++ )
        {
            ReplicatedContent content = raftLog.readEntryContent( index );
            stateMachine.applyCommand( content, index );
            log.info( "Index %d replayed as committed", index );
        }

        log.info( "Replay done, took %d ms", currentTimeMillis() - start );
    }
}
