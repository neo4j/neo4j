package org.neo4j.coreedge.server.core.locks;

import org.junit.Test;

import org.neo4j.coreedge.raft.LeaderLocator;
import org.neo4j.coreedge.raft.replication.StubReplicator;
import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.storageengine.api.lock.AcquireLockTimeoutException;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.coreedge.server.RaftTestMember.member;

@SuppressWarnings( "unchecked" )
public class LeaderOnlyLockManagerTest
{
    @Test
    public void shouldIssueLocksOnLeader() throws Exception
    {
        // given
        RaftTestMember me = member( 0 );
        StubReplicator replicator = new StubReplicator();
        ReplicatedLockStateMachine<RaftTestMember> replicatedLockStateMachine = new ReplicatedLockStateMachine<>(me, replicator );
        LeaderLocator<RaftTestMember> leaderLocator = mock( LeaderLocator.class );
        when(leaderLocator.getLeader()).thenReturn( me );
        Locks locks = mock( Locks.class );
        when( locks.newClient() ).thenReturn( mock( Locks.Client.class ) );
        LeaderOnlyLockManager<RaftTestMember> lockManager = new LeaderOnlyLockManager<>( me, replicator, leaderLocator, locks, replicatedLockStateMachine );

        // when
        lockManager.newClient().acquireExclusive( ResourceTypes.NODE, 0L );

        // then
    }

    @Test
    public void shouldNotIssueLocksOnNonLeader() throws Exception
    {
        // given
        RaftTestMember me = member( 0 );
        RaftTestMember leader = member( 1 );
        StubReplicator replicator = new StubReplicator();
        ReplicatedLockStateMachine<RaftTestMember> replicatedLockStateMachine = new ReplicatedLockStateMachine<>(me, replicator );
        LeaderLocator<RaftTestMember> leaderLocator = mock( LeaderLocator.class );
        when(leaderLocator.getLeader()).thenReturn( leader );
        Locks locks = mock( Locks.class );
        when( locks.newClient() ).thenReturn( mock( Locks.Client.class ) );
        LeaderOnlyLockManager<RaftTestMember> lockManager = new LeaderOnlyLockManager<>( me, replicator, leaderLocator, locks, replicatedLockStateMachine );

        // when
        Locks.Client lockClient = lockManager.newClient();
        try
        {
            lockClient.acquireExclusive( ResourceTypes.NODE, 0L );
            fail("Should have thrown exception");
        }
        catch ( AcquireLockTimeoutException e )
        {
            // expected
        }
    }
}
