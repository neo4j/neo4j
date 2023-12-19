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
package org.neo4j.causalclustering.core.state.machines.id;

import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.consensus.state.ExposedRaftState;
import org.neo4j.causalclustering.identity.MemberId;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IdReusabilityConditionTest
{
    private RaftMachine raftMachine = mock( RaftMachine.class );
    private ExposedRaftState state = mock( ExposedRaftState.class );
    private MemberId myself;
    private CommandIndexTracker commandIndexTracker = mock( CommandIndexTracker.class );
    private IdReusabilityCondition idReusabilityCondition;

    @Before
    public void setUp()
    {
        when( raftMachine.state() ) .thenReturn( state );
        myself = new MemberId( UUID.randomUUID() );
        idReusabilityCondition = new IdReusabilityCondition( commandIndexTracker, raftMachine, myself );
    }

    @Test
    public void shouldReturnFalseAsDefault()
    {
        assertFalse( idReusabilityCondition.getAsBoolean() );
    }

    @Test
    public void shouldNeverReuseWhenNotLeader()
    {
        MemberId someoneElse = new MemberId( UUID.randomUUID() );

        idReusabilityCondition.onLeaderSwitch( new LeaderInfo( someoneElse, 1 ));
        assertFalse( idReusabilityCondition.getAsBoolean() );
    }

    @Test
    public void shouldNotReturnTrueWithPendingTransactions()
    {
        assertFalse( idReusabilityCondition.getAsBoolean() );

        when( commandIndexTracker.getAppliedCommandIndex() ).thenReturn( 2L ); // gap-free
        when( state.lastLogIndexBeforeWeBecameLeader() ).thenReturn( 5L );

        idReusabilityCondition.onLeaderSwitch( new LeaderInfo( myself, 1 ) );

        assertFalse( idReusabilityCondition.getAsBoolean() );
        assertFalse( idReusabilityCondition.getAsBoolean() );
        assertFalse( idReusabilityCondition.getAsBoolean() );

        verify( commandIndexTracker, times( 3 ) ).getAppliedCommandIndex();
        verify( state ).lastLogIndexBeforeWeBecameLeader();
    }

    @Test
    public void shouldOnlyReturnTrueWhenOldTransactionsBeenApplied()
    {
        assertFalse( idReusabilityCondition.getAsBoolean() );

        when( commandIndexTracker.getAppliedCommandIndex() ).thenReturn( 2L, 5L, 6L ); // gap-free
        when( state.lastLogIndexBeforeWeBecameLeader() ).thenReturn( 5L );

        idReusabilityCondition.onLeaderSwitch( new LeaderInfo( myself, 1 ) );

        assertFalse( idReusabilityCondition.getAsBoolean() );
        assertFalse( idReusabilityCondition.getAsBoolean() );
        assertTrue( idReusabilityCondition.getAsBoolean() );

        verify( commandIndexTracker, times( 3 ) ).getAppliedCommandIndex();
        verify( state ).lastLogIndexBeforeWeBecameLeader();
    }

    @Test
    public void shouldNotReuseIfReelection()
    {
        assertFalse( idReusabilityCondition.getAsBoolean() );

        when( commandIndexTracker.getAppliedCommandIndex() ).thenReturn( 2L, 5L, 6L ); // gap-free
        when( state.lastLogIndexBeforeWeBecameLeader() ).thenReturn( 5L );

        idReusabilityCondition.onLeaderSwitch( new LeaderInfo( myself, 1 ) );

        assertFalse( idReusabilityCondition.getAsBoolean() );
        assertFalse( idReusabilityCondition.getAsBoolean() );
        assertTrue( idReusabilityCondition.getAsBoolean() );

        MemberId someoneElse = new MemberId( UUID.randomUUID() );
        idReusabilityCondition.onLeaderSwitch( new LeaderInfo( someoneElse, 1 ) );

        assertFalse( idReusabilityCondition.getAsBoolean() );
    }
}
