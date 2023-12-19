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
package org.neo4j.cluster.protocol.election;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import org.junit.Test;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectInputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectOutputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AcceptorInstanceStore;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context.MultiPaxosContext;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterContext;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ElectionContextTest
{
    @Test
    public void testElectionOkNoFailed()
    {
        Set<InstanceId> failed = new HashSet<>();

        baseTestForElectionOk( failed, false );
    }

    @Test
    public void testElectionOkLessThanQuorumFailed()
    {
        Set<InstanceId> failed = new HashSet<>();
        failed.add( new InstanceId( 1 ) );

        baseTestForElectionOk( failed, false );
    }

    @Test
    public void testElectionNotOkMoreThanQuorumFailed()
    {
        Set<InstanceId> failed = new HashSet<>();
        failed.add( new InstanceId( 1 ) );
        failed.add( new InstanceId( 2 ) );

        baseTestForElectionOk( failed, true );
    }

    @Test
    public void testElectionNotOkQuorumFailedTwoInstances()
    {
        Set<InstanceId> failed = new HashSet<>();
        failed.add( new InstanceId( 2 ) );

        Map<InstanceId, URI> members = new HashMap<>();
        members.put( new InstanceId( 1 ), URI.create( "server1" ) );
        members.put( new InstanceId( 2 ), URI.create( "server2" ) );

        Config config = mock( Config.class );
        when( config.get( ClusterSettings.max_acceptors ) ).thenReturn( 10 );

        ClusterConfiguration clusterConfiguration = mock( ClusterConfiguration.class );
        when( clusterConfiguration.getMembers() ).thenReturn( members );

        ClusterContext clusterContext = mock( ClusterContext.class );
        when( clusterContext.getConfiguration() ).thenReturn( clusterConfiguration );

        MultiPaxosContext context = new MultiPaxosContext( new InstanceId( 1 ),
                Iterables.iterable( new ElectionRole( "coordinator" ) ),
                clusterConfiguration, mock( Executor.class ), NullLogProvider.getInstance(),
                mock( ObjectInputStreamFactory.class ), mock( ObjectOutputStreamFactory.class ),
                mock( AcceptorInstanceStore.class ), mock( Timeouts.class ),
                mock( ElectionCredentialsProvider.class), config );

        context.getHeartbeatContext().getFailed().addAll( failed );

        ElectionContext toTest = context.getElectionContext();

        assertFalse( toTest.electionOk() );
    }

    @Test
    public void testElectionNotOkQuorumFailedFourInstances()
    {
        Set<InstanceId> failed = new HashSet<>();
        failed.add( new InstanceId( 2 ) );
        failed.add( new InstanceId( 3 ) );

        Map<InstanceId, URI> members = new HashMap<>();
        members.put( new InstanceId( 1 ), URI.create( "server1" ) );
        members.put( new InstanceId( 2 ), URI.create( "server2" ) );
        members.put( new InstanceId( 3 ), URI.create( "server3" ) );
        members.put( new InstanceId( 4 ), URI.create( "server4" ) );

        Config config = mock( Config.class );
        when( config.get( ClusterSettings.max_acceptors ) ).thenReturn( 10 );

        ClusterConfiguration clusterConfiguration = mock( ClusterConfiguration.class );
        when( clusterConfiguration.getMembers() ).thenReturn( members );

        ClusterContext clusterContext = mock( ClusterContext.class );
        when( clusterContext.getConfiguration() ).thenReturn( clusterConfiguration );

        MultiPaxosContext context =
                new MultiPaxosContext( new InstanceId( 1 ), Iterables.iterable( new ElectionRole( "coordinator" ) ),
                        clusterConfiguration, mock( Executor.class ), NullLogProvider.getInstance(),
                        mock( ObjectInputStreamFactory.class ), mock( ObjectOutputStreamFactory.class ),
                        mock( AcceptorInstanceStore.class ), mock( Timeouts.class ),
                        mock( ElectionCredentialsProvider.class ), config );

        context.getHeartbeatContext().getFailed().addAll( failed );

        ElectionContext toTest = context.getElectionContext();

        assertFalse( toTest.electionOk() );
    }

    @Test
    public void testElectionNotOkQuorumFailedFiveInstances()
    {
        Set<InstanceId> failed = new HashSet<>();
        failed.add( new InstanceId( 2 ) );
        failed.add( new InstanceId( 3 ) );
        failed.add( new InstanceId( 4 ) );

        Map<InstanceId, URI> members = new HashMap<>();
        members.put( new InstanceId( 1 ), URI.create( "server1" ) );
        members.put( new InstanceId( 2 ), URI.create( "server2" ) );
        members.put( new InstanceId( 3 ), URI.create( "server3" ) );
        members.put( new InstanceId( 4 ), URI.create( "server4" ) );
        members.put( new InstanceId( 5 ), URI.create( "server5" ) );

        Config config = mock( Config.class );
        when( config.get( ClusterSettings.max_acceptors ) ).thenReturn( 10 );

        ClusterConfiguration clusterConfiguration = mock( ClusterConfiguration.class );
        when( clusterConfiguration.getMembers() ).thenReturn( members );

        ClusterContext clusterContext = mock( ClusterContext.class );
        when( clusterContext.getConfiguration() ).thenReturn( clusterConfiguration );

        MultiPaxosContext context =
                new MultiPaxosContext( new InstanceId( 1 ), Iterables.iterable( new ElectionRole( "coordinator" ) ),
                        clusterConfiguration, mock( Executor.class ), NullLogProvider.getInstance(),
                        mock( ObjectInputStreamFactory.class ), mock( ObjectOutputStreamFactory.class ),
                        mock( AcceptorInstanceStore.class ), mock( Timeouts.class ),
                        mock( ElectionCredentialsProvider.class ), config );

        context.getHeartbeatContext().getFailed().addAll( failed );

        ElectionContext toTest = context.getElectionContext();

        assertFalse( toTest.electionOk() );
    }

    @Test
    public void testInstanceWithLowestIdFailedIsNotConsideredTheElector()
    {
        // Given
        // A cluster of 5 of which the two lowest instances are failed
        Set<InstanceId> failed = new HashSet<>();
        failed.add( new InstanceId( 1 ) );
        failed.add( new InstanceId( 2 ) );

        // This is the instance that must discover that it is the elector and whose state machine we'll test
        InstanceId lowestNonFailedInstanceId = new InstanceId( 3 );

        Map<InstanceId, URI> members = new HashMap<>();
        members.put( new InstanceId( 1 ), URI.create( "server1" ) );
        members.put( new InstanceId( 2 ), URI.create( "server2" ) );
        members.put( lowestNonFailedInstanceId, URI.create( "server3" ) );
        members.put( new InstanceId( 4 ), URI.create( "server4" ) );
        members.put( new InstanceId( 5 ), URI.create( "server5" ) );

        Config config = Config.defaults();
        ClusterConfiguration clusterConfiguration = mock( ClusterConfiguration.class );
        when( clusterConfiguration.getMembers() ).thenReturn( members );
        when( clusterConfiguration.getMemberIds() ).thenReturn( members.keySet() );

        ClusterContext clusterContext = mock( ClusterContext.class );
        when( clusterContext.getConfiguration() ).thenReturn( clusterConfiguration );

        MultiPaxosContext context = new MultiPaxosContext( lowestNonFailedInstanceId, Iterables.iterable(
                new ElectionRole( "coordinator" ) ), clusterConfiguration,
                mock( Executor.class ), NullLogProvider.getInstance(),
                mock( ObjectInputStreamFactory.class ), mock( ObjectOutputStreamFactory.class ),
                mock( AcceptorInstanceStore.class ), mock( Timeouts.class ),
                mock( ElectionCredentialsProvider.class), config );

        ElectionContext toTest = context.getElectionContext();
        // Sanity check that before learning about lowest failed ids it does not consider itself the elector
        assertFalse( toTest.isElector() );

        // When
        // The lowest numbered alive instance receives word about other failed instances
        context.getHeartbeatContext().getFailed().addAll( failed );

        // Then
        // It should realise it is the elector (lowest instance id alive)
        assertTrue( toTest.isElector() );
    }

    @Test
    public void twoVotesFromSameInstanceForSameRoleShouldBeConsolidated()
    {
        // Given
        final String coordinatorRole = "coordinator";
        HeartbeatContext heartbeatContext = mock(HeartbeatContext.class);
        when( heartbeatContext.getFailed() ).thenReturn( Collections.emptySet() );

        Map<InstanceId, URI> members = new HashMap<>();
        members.put( new InstanceId( 1 ), URI.create( "server1" ) );
        members.put( new InstanceId( 2 ), URI.create( "server2" ) );
        members.put( new InstanceId( 3 ), URI.create( "server3" ) );

        Config config = mock( Config.class );
        when( config.get( ClusterSettings.max_acceptors ) ).thenReturn( 10 );

        ClusterConfiguration clusterConfiguration = mock( ClusterConfiguration.class );
        when( clusterConfiguration.getMembers() ).thenReturn( members );

        ClusterContext clusterContext = mock( ClusterContext.class );
        when( clusterContext.getConfiguration() ).thenReturn( clusterConfiguration );

        MultiPaxosContext context =
                new MultiPaxosContext( new InstanceId( 1 ), Iterables.iterable( new ElectionRole( coordinatorRole ) ),
                        clusterConfiguration, mock( Executor.class ), NullLogProvider.getInstance(),
                        mock( ObjectInputStreamFactory.class ), mock( ObjectOutputStreamFactory.class ),
                        mock( AcceptorInstanceStore.class ), mock( Timeouts.class ),
                        mock( ElectionCredentialsProvider.class ), config );

        ElectionContext toTest = context.getElectionContext();

        // When
        toTest.startElectionProcess( coordinatorRole );
        toTest.voted( coordinatorRole, new InstanceId( 1 ), new IntegerElectionCredentials( 100 ), ClusterContext.NO_ELECTOR_VERSION );
        toTest.voted( coordinatorRole, new InstanceId( 2 ), new IntegerElectionCredentials( 100 ), ClusterContext.NO_ELECTOR_VERSION );
        toTest.voted( coordinatorRole, new InstanceId( 2 ), new IntegerElectionCredentials( 101 ), ClusterContext.NO_ELECTOR_VERSION );

        // Then
        assertNull( toTest.getElectionWinner( coordinatorRole ) );
        assertEquals( 2, toTest.getVoteCount( coordinatorRole ) );
    }

    @Test
    public void electionBeingForgottenMustIncreaseElectionId()
    {
        // Given
        final String coordinatorRole = "coordinator";
        HeartbeatContext heartbeatContext = mock(HeartbeatContext.class);
        when( heartbeatContext.getFailed() ).thenReturn( Collections.emptySet() );

        Config config = mock( Config.class );
        when( config.get( ClusterSettings.max_acceptors ) ).thenReturn( 10 );

        ElectionContext context =
                new MultiPaxosContext( new InstanceId( 1 ), Iterables.iterable( new ElectionRole( coordinatorRole ) ),
                        mock( ClusterConfiguration.class ), mock( Executor.class ), NullLogProvider.getInstance(),
                        mock( ObjectInputStreamFactory.class ), mock( ObjectOutputStreamFactory.class ),
                        mock( AcceptorInstanceStore.class ), mock( Timeouts.class ),
                        mock( ElectionCredentialsProvider.class ), config ).getElectionContext();

        ElectionContext.VoteRequest voteRequestBefore = context.voteRequestForRole( new ElectionRole( coordinatorRole ) );
        context.forgetElection( coordinatorRole );
        ElectionContext.VoteRequest voteRequestAfter = context.voteRequestForRole( new ElectionRole( coordinatorRole ) );
        assertEquals( voteRequestBefore.getVersion() + 1, voteRequestAfter.getVersion() );
    }

    @Test
    public void voteFromPreviousSuccessfulElectionMustNotBeCounted()
    {
        // Given
        final String coordinatorRole = "coordinator";
        HeartbeatContext heartbeatContext = mock(HeartbeatContext.class);
        when( heartbeatContext.getFailed() ).thenReturn( Collections.emptySet() );

        Config config = mock( Config.class );
        when( config.get( ClusterSettings.max_acceptors ) ).thenReturn( 10 );

        ElectionContext context =
                new MultiPaxosContext( new InstanceId( 1 ), Iterables.iterable( new ElectionRole( coordinatorRole ) ),
                        mock( ClusterConfiguration.class ), mock( Executor.class ), NullLogProvider.getInstance(),
                        mock( ObjectInputStreamFactory.class ), mock( ObjectOutputStreamFactory.class ),
                        mock( AcceptorInstanceStore.class ), mock( Timeouts.class ),
                        mock( ElectionCredentialsProvider.class ), config ).getElectionContext();

        // When
        ElectionContext.VoteRequest voteRequestBefore = context.voteRequestForRole( new ElectionRole( coordinatorRole ) );
        context.forgetElection( coordinatorRole );

        // Then
        assertFalse( context.voted( coordinatorRole, new InstanceId( 2 ), null, voteRequestBefore.getVersion() - 1 ) );
    }

    @Test
    public void instanceFailingShouldHaveItsVotesInvalidated()
    {
        // Given
        final String role1 = "coordinator1";
        final String role2 = "coordinator2";
        InstanceId me = new InstanceId( 1 );
        InstanceId failingInstance = new InstanceId( 2 );
        InstanceId otherInstance = new InstanceId( 3 );

        Config config = mock( Config.class );
        when( config.get( ClusterSettings.max_acceptors ) ).thenReturn( 10 );

        ClusterConfiguration clusterConfiguration = mock( ClusterConfiguration.class );
        List<InstanceId> clusterMemberIds = new LinkedList<>();
        clusterMemberIds.add( failingInstance );
        clusterMemberIds.add( otherInstance );
        clusterMemberIds.add( me );
        when( clusterConfiguration.getMemberIds() ).thenReturn( clusterMemberIds );

        MultiPaxosContext context = new MultiPaxosContext( me, Iterables.iterable(
                new ElectionRole( role1 ), new ElectionRole( role2 ) ), clusterConfiguration, Runnable::run, NullLogProvider.getInstance(),
                mock( ObjectInputStreamFactory.class ), mock( ObjectOutputStreamFactory.class ),
                mock( AcceptorInstanceStore.class ), mock( Timeouts.class ), mock( ElectionCredentialsProvider.class ),
                config );

        HeartbeatContext heartbeatContext = context.getHeartbeatContext();
        ElectionContext electionContext = context.getElectionContext();

        electionContext.startElectionProcess( role1 );
        electionContext.startElectionProcess( role2 );

        electionContext.voted( role1, failingInstance, mock( ElectionCredentials.class ), 2 );
        electionContext.voted( role2, failingInstance, mock( ElectionCredentials.class ), 2 );

        electionContext.voted( role1, otherInstance, mock( ElectionCredentials.class ), 2 );
        electionContext.voted( role2, otherInstance, mock( ElectionCredentials.class ), 2 );

        heartbeatContext.suspect( failingInstance );

        assertEquals( 1, electionContext.getVoteCount( role1 ) );
        assertEquals( 1, electionContext.getVoteCount( role2 ) );
    }

    @Test
    public void failedElectorRejoiningMustHaveItsVersionFromVoteRequestsSetTheElectorVersion()
    {
        // Given
        final String role1 = "coordinator1";
        InstanceId me = new InstanceId( 1 );
        InstanceId failingInstance = new InstanceId( 2 );
        InstanceId forQuorum = new InstanceId( 3 );

        Config config = mock( Config.class );
        when( config.get( ClusterSettings.max_acceptors ) ).thenReturn( 10 );

        ClusterConfiguration clusterConfiguration = mock( ClusterConfiguration.class );
        List<InstanceId> clusterMemberIds = new LinkedList<>();
        clusterMemberIds.add( failingInstance );
        clusterMemberIds.add( me );
        clusterMemberIds.add( forQuorum );
        when( clusterConfiguration.getMemberIds() ).thenReturn( clusterMemberIds );

        MultiPaxosContext context = new MultiPaxosContext( me, Iterables.iterable(
                new ElectionRole( role1 ) ), clusterConfiguration, Runnable::run, NullLogProvider.getInstance(),
                mock( ObjectInputStreamFactory.class ), mock( ObjectOutputStreamFactory.class ),
                mock( AcceptorInstanceStore.class ), mock( Timeouts.class ), mock( ElectionCredentialsProvider.class ),
                config );

        HeartbeatContext heartbeatContext = context.getHeartbeatContext();
        ClusterContext clusterContext = context.getClusterContext();

        clusterContext.setLastElector( failingInstance );
        clusterContext.setLastElectorVersion( 8 );

        // When the elector fails
        heartbeatContext.suspicions( forQuorum, Collections.singleton( failingInstance ) );
        heartbeatContext.suspect( failingInstance );

        // Then the elector is reset to defaults
        assertEquals( clusterContext.getLastElector(), InstanceId.NONE );
        assertEquals( clusterContext.getLastElectorVersion(), ClusterContext.NO_ELECTOR_VERSION );

        // When the elector comes back with an election result
        clusterContext.elected( role1, forQuorum, failingInstance, 9 );

        // Then the result is actually respected
        assertEquals( clusterContext.getLastElector(), failingInstance );
        assertEquals( clusterContext.getLastElectorVersion(), 9 );
  }

    /*
     * This assumes an instance leaves the cluster normally and then rejoins, without any elections in between. The
     * expected result is that it will succeed in sending election results.
     */
    @Test
    public void electorLeavingAndRejoiningWithNoElectionsInBetweenMustStillHaveElectionsGoThrough()
    {
        // Given
        final String role1 = "coordinator1";
        InstanceId me = new InstanceId( 1 );
        InstanceId leavingInstance = new InstanceId( 2 );
        InstanceId forQuorum = new InstanceId( 3 );

        Config config = mock( Config.class );
        when( config.get( ClusterSettings.max_acceptors ) ).thenReturn( 10 );

        ClusterConfiguration clusterConfiguration = mock( ClusterConfiguration.class );
        List<InstanceId> clusterMemberIds = new LinkedList<>();
        clusterMemberIds.add( leavingInstance );
        clusterMemberIds.add( me );
        clusterMemberIds.add( forQuorum );
        when( clusterConfiguration.getMemberIds() ).thenReturn( clusterMemberIds );

        MultiPaxosContext context = new MultiPaxosContext( me,
                Iterables.iterable( new ElectionRole( role1 ) ), clusterConfiguration, Runnable::run,
                NullLogProvider.getInstance(), mock( ObjectInputStreamFactory.class ), mock( ObjectOutputStreamFactory.class ),
                mock( AcceptorInstanceStore.class ), mock( Timeouts.class ), mock( ElectionCredentialsProvider.class ),
                config );

        ClusterContext clusterContext = context.getClusterContext();

        clusterContext.setLastElector( leavingInstance );
        clusterContext.setLastElectorVersion( 8 );

        // When the elector leaves the cluster
        clusterContext.left( leavingInstance );

        // Then the elector is reset to defaults
        assertEquals( clusterContext.getLastElector(), InstanceId.NONE );
        assertEquals( clusterContext.getLastElectorVersion(), ClusterContext.NO_ELECTOR_VERSION );

        // When the elector comes back with an election result
          // We don't need to join, election results do not check for elector membership
        clusterContext.elected( role1, forQuorum, leavingInstance, 9 );

        // Then the result is actually respected
        assertEquals( clusterContext.getLastElector(), leavingInstance );
        assertEquals( clusterContext.getLastElectorVersion(), 9 );
    }

    private void baseTestForElectionOk( Set<InstanceId> failed, boolean moreThanQuorum )
    {
        Map<InstanceId, URI> members = new HashMap<>();
        members.put( new InstanceId( 1 ), URI.create( "server1" ) );
        members.put( new InstanceId( 2 ), URI.create( "server2" ) );
        members.put( new InstanceId( 3 ), URI.create( "server3" ) );

        Config config = mock( Config.class );
        when( config.get( ClusterSettings.max_acceptors ) ).thenReturn( 10 );

        ClusterConfiguration clusterConfiguration = mock( ClusterConfiguration.class );
        when( clusterConfiguration.getMembers() ).thenReturn( members );

        ClusterContext clusterContext = mock( ClusterContext.class );
        when( clusterContext.getConfiguration() ).thenReturn( clusterConfiguration );

        MultiPaxosContext context =
                new MultiPaxosContext( new InstanceId( 1 ), Iterables.iterable( new ElectionRole( "coordinator" ) ),
                        clusterConfiguration, mock( Executor.class ), NullLogProvider.getInstance(),
                        mock( ObjectInputStreamFactory.class ), mock( ObjectOutputStreamFactory.class ),
                        mock( AcceptorInstanceStore.class ), mock( Timeouts.class ),
                        mock( ElectionCredentialsProvider.class ), config );

        context.getHeartbeatContext().getFailed().addAll( failed );

        ElectionContext toTest = context.getElectionContext();

        assertEquals( moreThanQuorum, !toTest.electionOk() );
    }
}
