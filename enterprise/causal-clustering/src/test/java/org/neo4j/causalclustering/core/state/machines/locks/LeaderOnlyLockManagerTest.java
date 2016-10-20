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
package org.neo4j.causalclustering.core.state.machines.locks;

import org.junit.Test;

import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.replication.DirectReplicator;
import org.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.storageengine.api.lock.AcquireLockTimeoutException;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;

@SuppressWarnings("unchecked")
public class LeaderOnlyLockManagerTest
{
    @Test
    public void shouldIssueLocksOnLeader() throws Exception
    {
        // given
        MemberId me = member( 0 );

        ReplicatedLockTokenStateMachine replicatedLockStateMachine =
                new ReplicatedLockTokenStateMachine( new InMemoryStateStorage( new ReplicatedLockTokenState() ) );

        DirectReplicator replicator = new DirectReplicator( replicatedLockStateMachine );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( me );
        Locks locks = mock( Locks.class );
        when( locks.newClient() ).thenReturn( mock( Locks.Client.class ) );

        LeaderOnlyLockManager lockManager =
                new LeaderOnlyLockManager( me, replicator, leaderLocator, locks, replicatedLockStateMachine );

        // when
        lockManager.newClient().acquireExclusive( ResourceTypes.NODE, 0L );

        // then
    }

    @Test
    public void shouldNotIssueLocksOnNonLeader() throws Exception
    {
        // given
        MemberId me = member( 0 );
        MemberId leader = member( 1 );

        ReplicatedLockTokenStateMachine replicatedLockStateMachine =
                new ReplicatedLockTokenStateMachine( new InMemoryStateStorage( new ReplicatedLockTokenState() ) );
        DirectReplicator replicator = new DirectReplicator( replicatedLockStateMachine );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( leader );
        Locks locks = mock( Locks.class );
        when( locks.newClient() ).thenReturn( mock( Locks.Client.class ) );

        LeaderOnlyLockManager lockManager =
                new LeaderOnlyLockManager( me, replicator, leaderLocator, locks, replicatedLockStateMachine );

        // when
        Locks.Client lockClient = lockManager.newClient();
        try
        {
            lockClient.acquireExclusive( ResourceTypes.NODE, 0L );
            fail( "Should have thrown exception" );
        }
        catch ( AcquireLockTimeoutException e )
        {
            // expected
        }
    }
}
