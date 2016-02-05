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

import org.junit.Test;

import org.neo4j.coreedge.raft.ReplicatedInteger;
import org.neo4j.coreedge.raft.log.InMemoryRaftLog;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.state.StateMachine;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class RaftLogReplayTest
{
    @Test
    public void shouldReplayLastCommittedEntry() throws Throwable
    {
        // given
        StateMachine stateMachine = mock( StateMachine.class );
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        raftLog.append( new RaftLogEntry( 0, ReplicatedInteger.valueOf( 0 ) ) );
        raftLog.append( new RaftLogEntry( 0, ReplicatedInteger.valueOf( 1 ) ) );
        raftLog.append( new RaftLogEntry( 0, ReplicatedInteger.valueOf( 2 ) ) );
        raftLog.commit( 2 );
        raftLog.append( new RaftLogEntry( 0, ReplicatedInteger.valueOf( 3 ) ) );

        RaftLogReplay replayer = new RaftLogReplay( stateMachine, raftLog, NullLogProvider.getInstance(), 1 );

        // when
        replayer.start();

        // then
        verify( stateMachine ).applyCommand( ReplicatedInteger.valueOf( 2 ), 2 );
    }
}