/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.state.machines.locks;

import org.junit.Test;

import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.replication.DirectReplicator;
import org.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.storageengine.api.lock.AcquireLockTimeoutException;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;

@SuppressWarnings( "unchecked" )
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
        Locks.Client client = mock( Locks.Client.class );
        when( locks.newClient() ).thenReturn( client );

        LeaderOnlyLockManager lockManager =
                new LeaderOnlyLockManager( me, replicator, leaderLocator, locks, replicatedLockStateMachine );

        // when
        lockManager.newClient().acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 0L );

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
        Locks.Client client = mock( Locks.Client.class );
        when( locks.newClient() ).thenReturn( client );

        LeaderOnlyLockManager lockManager =
                new LeaderOnlyLockManager( me, replicator, leaderLocator, locks, replicatedLockStateMachine );

        // when
        Locks.Client lockClient = lockManager.newClient();
        try
        {
            lockClient.acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 0L );
            fail( "Should have thrown exception" );
        }
        catch ( AcquireLockTimeoutException e )
        {
            // expected
        }
    }
}
