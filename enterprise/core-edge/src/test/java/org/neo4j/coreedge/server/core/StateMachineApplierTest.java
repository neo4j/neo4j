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

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

import org.junit.Test;

import org.neo4j.coreedge.catchup.storecopy.core.RaftStateType;
import org.neo4j.coreedge.raft.log.InMemoryRaftLog;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.InMemoryStateStorage;
import org.neo4j.coreedge.raft.state.LastAppliedState;
import org.neo4j.coreedge.raft.state.StateMachine;
import org.neo4j.coreedge.raft.state.StateMachineApplier;
import org.neo4j.kernel.internal.DatabaseHealth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static org.neo4j.coreedge.raft.ReplicatedInteger.valueOf;
import static org.neo4j.logging.NullLogProvider.getInstance;

public class StateMachineApplierTest
{
    @Test
    public void shouldApplyCommittedCommands() throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        StateMachine stateMachine = mock( StateMachine.class );
        InMemoryStateStorage<LastAppliedState> lastApplied = new InMemoryStateStorage<>( new LastAppliedState( -1 ) );
        StateMachineApplier applier = new StateMachineApplier( raftLog, lastApplied,
                Runnable::run, 10, health(), getInstance() );
        applier.setStateMachine( stateMachine, -1 );

        raftLog.append( new RaftLogEntry( 0, valueOf( 0 ) ) );
        raftLog.commit( 0 );
        applier.start();

        // when
        applier.notifyUpdate();

        // then
        verify( stateMachine ).applyCommand( valueOf( 0 ), 0 );
    }

    @Test
    public void shouldNotApplyAnythingIfNothingIsCommitted() throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        StateMachine stateMachine = mock( StateMachine.class );
        InMemoryStateStorage<LastAppliedState> lastApplied = new InMemoryStateStorage<>( new LastAppliedState( -1 ) );
        StateMachineApplier applier = new StateMachineApplier( raftLog, lastApplied,
                Runnable::run, 10, health(), getInstance() );

        raftLog.append( new RaftLogEntry( 0, valueOf( 0 ) ) );
        applier.start();

        // when
        applier.notifyUpdate();

        // then
        verify( stateMachine, times( 0 ) ).applyCommand( valueOf( 0 ), 0 );
    }

    @Test
    public void startShouldApplyCommittedButNotYetAppliedCommands() throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        StateMachine stateMachine = mock( StateMachine.class );
        InMemoryStateStorage<LastAppliedState> lastApplied = new InMemoryStateStorage<>( new LastAppliedState( -1 ) );
        StateMachineApplier applier = new StateMachineApplier( raftLog, lastApplied,
                Runnable::run, 10, health(), getInstance() );

        applier.setStateMachine( stateMachine, -1 );

        raftLog.append( new RaftLogEntry( 0, valueOf( 0 ) ) );
        raftLog.append( new RaftLogEntry( 0, valueOf( 1 ) ) );
        raftLog.append( new RaftLogEntry( 0, valueOf( 2 ) ) );
        raftLog.append( new RaftLogEntry( 0, valueOf( 3 ) ) );
        raftLog.commit( 3 );

        lastApplied.persistStoreData( new LastAppliedState( 1 ) );

        // when
        applier.start();

        // then
        verify( stateMachine ).applyCommand( valueOf( 2 ), 2 );
        verify( stateMachine ).applyCommand( valueOf( 3 ), 3 );
        verifyNoMoreInteractions( stateMachine );
    }

    @Test
    public void shouldPeriodicallyFlushStateMachines() throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        StateMachine stateMachine = mock( StateMachine.class );
        InMemoryStateStorage<LastAppliedState> lastApplied = new InMemoryStateStorage<>( new LastAppliedState( -1 ) );
        StateMachineApplier applier = new StateMachineApplier( raftLog, lastApplied,
                Runnable::run, 5, health(), getInstance() );

        applier.setStateMachine( stateMachine, -1 );

        for ( int i = 0; i < 50; i++ )
        {
            raftLog.append( new RaftLogEntry( 0, valueOf( i ) ) );
        }
        raftLog.commit( 49 );

        // when
        applier.start();

        // then
        verify( stateMachine, times( 10 ) ).flush();
    }

    @Test
    public void shouldPeriodicallyStoreLastAppliedState() throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        StateMachine stateMachine = mock( StateMachine.class );
        InMemoryStateStorage<LastAppliedState> lastApplied = new InMemoryStateStorage<>( new LastAppliedState( -1 ) );
        StateMachineApplier applier = new StateMachineApplier( raftLog, lastApplied,
                Runnable::run, 5, health(), getInstance() );
        applier.setStateMachine( stateMachine, -1 );

        for ( int i = 0; i < 50; i++ )
        {
            raftLog.append( new RaftLogEntry( 0, valueOf( i ) ) );
        }
        raftLog.commit( 49 );

        // when
        applier.start();

        // then
        assertEquals( 45L, lastApplied.getInitialState().get() );
    }

    @Test
    public void shouldPanicIfUnableToApply() throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();

        StateMachine stateMachine = new FailingStateMachine();

        InMemoryStateStorage<LastAppliedState> lastApplied = new InMemoryStateStorage<>( new LastAppliedState( -1 ) );

        Supplier<DatabaseHealth> healthSupplier = health();
        DatabaseHealth health = mock( DatabaseHealth.class );
        when( healthSupplier.get() ).thenReturn( health );

        StateMachineApplier applier = new StateMachineApplier( raftLog, lastApplied,
                Runnable::run, 5, healthSupplier, getInstance() );
        applier.setStateMachine( stateMachine, -1 );

        raftLog.append( new RaftLogEntry( 0, valueOf( 1 ) ) );
        raftLog.commit( 0 );

        // when
        applier.notifyUpdate();

        // then
        verify( health ).panic( anyObject() );
    }

    @Test
    public void shouldNotStartIfUnableToApplyOnStartUp() throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();

        StateMachine stateMachine = new FailingStateMachine();

        InMemoryStateStorage<LastAppliedState> lastApplied = new InMemoryStateStorage<>( new LastAppliedState( -1 ) );

        Supplier<DatabaseHealth> healthSupplier = health();
        DatabaseHealth health = mock( DatabaseHealth.class );
        when( healthSupplier.get() ).thenReturn( health );

        StateMachineApplier applier = new StateMachineApplier( raftLog, lastApplied,
                Runnable::run, 5, healthSupplier, getInstance() );
        applier.setStateMachine( stateMachine, -1 );

        raftLog.append( new RaftLogEntry( 0, valueOf( 1 ) ) );
        raftLog.commit( 0 );

        // when
        try
        {
            applier.start();
            fail( "Should have thrown IllegalStateException" );
        }
        catch ( IllegalStateException ignored )
        {
            // expected
        }
    }

    @SuppressWarnings("unchecked")
    private Supplier<DatabaseHealth> health()
    {
        return mock( Supplier.class );
    }

    private static class FailingStateMachine implements StateMachine
    {
        @Override
        public void applyCommand( ReplicatedContent content, long logIndex )
        {
            throw new IllegalStateException();
        }

        @Override
        public void flush() throws IOException
        {
            // do nothing
        }

        @Override
        public Map<RaftStateType, Object> snapshot()
        {
            return null;
        }

        @Override
        public void installSnapshot( Map<RaftStateType, Object> snapshot )
        {
            // do nothing
        }
    }
}
