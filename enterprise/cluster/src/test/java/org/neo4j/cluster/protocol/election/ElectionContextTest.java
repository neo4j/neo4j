/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import org.mockito.Matchers;
import org.mockito.Mockito;
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
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;

import static junit.framework.Assert.assertNull;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ElectionContextTest
{
    @Test
    public void testElectionOkNoFailed()
    {
        Set<InstanceId> failed = new HashSet<InstanceId>();

        baseTestForElectionOk( failed, false );
    }

    @Test
    public void testElectionOkLessThanQuorumFailed()
    {
        Set<InstanceId> failed = new HashSet<InstanceId>();
        failed.add( new InstanceId( 1 ) );

        baseTestForElectionOk( failed, false );
    }

    @Test
    public void testElectionNotOkMoreThanQuorumFailed()
    {
        Set<InstanceId> failed = new HashSet<InstanceId>();
        failed.add( new InstanceId( 1 ) );
        failed.add( new InstanceId( 2 ) );

        baseTestForElectionOk( failed, true );
    }

    @Test
    public void testElectionNotOkQuorumFailedTwoInstances()
    {
        Set<InstanceId> failed = new HashSet<InstanceId>();
        failed.add( new InstanceId( 2 ) );

        Map<InstanceId, URI> members = new HashMap<InstanceId, URI>();
        members.put( new InstanceId( 1 ), URI.create( "server1" ) );
        members.put( new InstanceId( 2 ), URI.create( "server2" ) );

        ClusterConfiguration clusterConfiguration = mock( ClusterConfiguration.class );
        when( clusterConfiguration.getMembers() ).thenReturn( members );

        ClusterContext clusterContext = mock( ClusterContext.class );
        when( clusterContext.getConfiguration() ).thenReturn( clusterConfiguration );

        MultiPaxosContext context = new MultiPaxosContext( new InstanceId(1), Iterables.<ElectionRole, ElectionRole>iterable(
                new ElectionRole( "coordinator" ) ), clusterConfiguration,
                Mockito.mock(Executor.class), Mockito.mock(Logging.class),
                Mockito.mock( ObjectInputStreamFactory.class), Mockito.mock( ObjectOutputStreamFactory.class),
                Mockito.mock( AcceptorInstanceStore.class), Mockito.mock( Timeouts.class) );

        context.getHeartbeatContext().getFailed().addAll( failed );

        ElectionContext toTest = context.getElectionContext();

        assertFalse( toTest.electionOk() );
    }

    @Test
    public void testElectionNotOkQuorumFailedFourInstances()
    {
        Set<InstanceId> failed = new HashSet<InstanceId>();
        failed.add( new InstanceId( 2 ) );
        failed.add( new InstanceId( 3 ) );

        Map<InstanceId, URI> members = new HashMap<InstanceId, URI>();
        members.put( new InstanceId( 1 ), URI.create( "server1" ) );
        members.put( new InstanceId( 2 ), URI.create( "server2" ) );
        members.put( new InstanceId( 3 ), URI.create( "server3" ) );
        members.put( new InstanceId( 4 ), URI.create( "server4" ) );

        ClusterConfiguration clusterConfiguration = mock( ClusterConfiguration.class );
        when( clusterConfiguration.getMembers() ).thenReturn( members );

        ClusterContext clusterContext = mock( ClusterContext.class );
        when( clusterContext.getConfiguration() ).thenReturn( clusterConfiguration );

        MultiPaxosContext context = new MultiPaxosContext( new InstanceId(1), Iterables.<ElectionRole, ElectionRole>iterable(
                new ElectionRole( "coordinator" ) ), clusterConfiguration,
                Mockito.mock(Executor.class), Mockito.mock(Logging.class),
                Mockito.mock( ObjectInputStreamFactory.class), Mockito.mock( ObjectOutputStreamFactory.class),
                Mockito.mock( AcceptorInstanceStore.class), Mockito.mock( Timeouts.class) );

        context.getHeartbeatContext().getFailed().addAll( failed );

        ElectionContext toTest = context.getElectionContext();

        assertFalse( toTest.electionOk() );
    }

    @Test
    public void testElectionNotOkQuorumFailedFiveInstances()
    {
        Set<InstanceId> failed = new HashSet<InstanceId>();
        failed.add( new InstanceId( 2 ) );
        failed.add( new InstanceId( 3 ) );
        failed.add( new InstanceId( 4 ) );

        Map<InstanceId, URI> members = new HashMap<InstanceId, URI>();
        members.put( new InstanceId( 1 ), URI.create( "server1" ) );
        members.put( new InstanceId( 2 ), URI.create( "server2" ) );
        members.put( new InstanceId( 3 ), URI.create( "server3" ) );
        members.put( new InstanceId( 4 ), URI.create( "server4" ) );
        members.put( new InstanceId( 5 ), URI.create( "server5" ) );

        ClusterConfiguration clusterConfiguration = mock( ClusterConfiguration.class );
        when( clusterConfiguration.getMembers() ).thenReturn( members );

        ClusterContext clusterContext = mock( ClusterContext.class );
        when( clusterContext.getConfiguration() ).thenReturn( clusterConfiguration );

        MultiPaxosContext context = new MultiPaxosContext( new InstanceId(1), Iterables.<ElectionRole, ElectionRole>iterable(
                new ElectionRole( "coordinator" ) ), clusterConfiguration,
                Mockito.mock(Executor.class), Mockito.mock(Logging.class),
                Mockito.mock( ObjectInputStreamFactory.class), Mockito.mock( ObjectOutputStreamFactory.class),
                Mockito.mock( AcceptorInstanceStore.class), Mockito.mock( Timeouts.class) );

        context.getHeartbeatContext().getFailed().addAll( failed );

        ElectionContext toTest = context.getElectionContext();

        assertFalse( toTest.electionOk() );
    }

    @Test
    public void twoVotesFromSameInstanceForSameRoleShouldBeConsolidated() throws Exception
    {
        // Given
        final String coordinatorRole = "coordinator";
        HeartbeatContext heartbeatContext = mock(HeartbeatContext.class);
        when( heartbeatContext.getFailed() ).thenReturn( Collections.<InstanceId>emptySet() );

        Map<InstanceId, URI> members = new HashMap<InstanceId, URI>();
        members.put( new InstanceId( 1 ), URI.create( "server1" ) );
        members.put( new InstanceId( 2 ), URI.create( "server2" ) );
        members.put( new InstanceId( 3 ), URI.create( "server3" ) );

        ClusterConfiguration clusterConfiguration = mock( ClusterConfiguration.class );
        when( clusterConfiguration.getMembers() ).thenReturn( members );

        ClusterContext clusterContext = mock( ClusterContext.class );
        when( clusterContext.getConfiguration() ).thenReturn( clusterConfiguration );

        Logging logging = Mockito.mock( Logging.class );
        when ( logging.getMessagesLog( Matchers.<Class>any() ) ).thenReturn( mock( StringLogger.class ) );

        MultiPaxosContext context = new MultiPaxosContext( new InstanceId(1), Iterables.<ElectionRole, ElectionRole>iterable(
                        new ElectionRole( coordinatorRole ) ), clusterConfiguration,
                        Mockito.mock(Executor.class), logging,
                        Mockito.mock( ObjectInputStreamFactory.class), Mockito.mock( ObjectOutputStreamFactory.class),
                Mockito.mock( AcceptorInstanceStore.class), Mockito.mock( Timeouts.class) );

        ElectionContext toTest = context.getElectionContext();

        // When
        toTest.startElectionProcess( coordinatorRole );
        toTest.voted( coordinatorRole, new InstanceId( 1 ), new IntegerElectionCredentials( 100 ), -1 );
        toTest.voted( coordinatorRole, new InstanceId( 2 ), new IntegerElectionCredentials( 100 ), -1 );
        toTest.voted( coordinatorRole, new InstanceId( 2 ), new IntegerElectionCredentials( 101 ), -1 );

        // Then
        assertNull( toTest.getElectionWinner( coordinatorRole ) );
        assertEquals( 2, toTest.getVoteCount( coordinatorRole ) );
    }

    @Test
    public void electionBeingForgottenMustIncreaseElectionId() throws Exception
    {
        // Given
        final String coordinatorRole = "coordinator";
        HeartbeatContext heartbeatContext = mock(HeartbeatContext.class);
        when( heartbeatContext.getFailed() ).thenReturn( Collections.<InstanceId>emptySet() );

        Logging logging = Mockito.mock( Logging.class );
        when ( logging.getMessagesLog( Matchers.<Class>any() ) ).thenReturn( mock( StringLogger.class ) );

        ElectionContext context = new MultiPaxosContext( new InstanceId(1), Iterables.<ElectionRole, ElectionRole>iterable(
                new ElectionRole( coordinatorRole ) ),  mock( ClusterConfiguration.class ),
                Mockito.mock(Executor.class), logging,
                Mockito.mock( ObjectInputStreamFactory.class), Mockito.mock( ObjectOutputStreamFactory.class),
                Mockito.mock( AcceptorInstanceStore.class), Mockito.mock( Timeouts.class) ).getElectionContext();

        ElectionContext.VoteRequest voteRequestBefore = context.voteRequestForRole( new ElectionRole( coordinatorRole ) );
        context.forgetElection( coordinatorRole );
        ElectionContext.VoteRequest voteRequestAfter = context.voteRequestForRole( new ElectionRole( coordinatorRole ) );
        assertEquals( voteRequestBefore.getVersion() + 1, voteRequestAfter.getVersion() );
    }

    @Test
    public void voteFromPreviousSuccessfulElectionMustNotBeCounted() throws Exception
    {
        // Given
        final String coordinatorRole = "coordinator";
        HeartbeatContext heartbeatContext = mock(HeartbeatContext.class);
        when( heartbeatContext.getFailed() ).thenReturn( Collections.<InstanceId>emptySet() );

        Logging logging = Mockito.mock( Logging.class );
        when ( logging.getMessagesLog( Matchers.<Class>any() ) ).thenReturn( mock( StringLogger.class ) );

        ElectionContext context = new MultiPaxosContext( new InstanceId(1), Iterables.<ElectionRole, ElectionRole>iterable(
                new ElectionRole( coordinatorRole ) ),  mock( ClusterConfiguration.class ),
                Mockito.mock(Executor.class), logging,
                Mockito.mock( ObjectInputStreamFactory.class), Mockito.mock( ObjectOutputStreamFactory.class),
                Mockito.mock( AcceptorInstanceStore.class), Mockito.mock( Timeouts.class) ).getElectionContext();

        // When
        ElectionContext.VoteRequest voteRequestBefore = context.voteRequestForRole( new ElectionRole( coordinatorRole ) );
        context.forgetElection( coordinatorRole );

        // Then
        assertFalse( context.voted( coordinatorRole, new InstanceId( 2 ), null, voteRequestBefore.getVersion() - 1 ) );
    }

    @Test
    public void instanceFailingShouldHaveItsVotesInvalidated() throws Exception
    {
        // Given
        final String role1 = "coordinator1";
        final String role2 = "coordinator2";
        InstanceId me = new InstanceId( 1 );
        InstanceId failingInstance = new InstanceId( 2 );
        InstanceId otherInstance = new InstanceId( 3 );

        Logging logging = Mockito.mock( Logging.class );
        when ( logging.getMessagesLog( Matchers.<Class>any() ) ).thenReturn( mock( StringLogger.class ) );

        ClusterConfiguration clusterConfiguration = mock( ClusterConfiguration.class );
        List<InstanceId> clusterMemberIds = new LinkedList<InstanceId>();
        clusterMemberIds.add( failingInstance );
        clusterMemberIds.add( otherInstance );
        clusterMemberIds.add( me );
        when( clusterConfiguration.getMemberIds() ).thenReturn( clusterMemberIds );

        MultiPaxosContext context = new MultiPaxosContext( me, Iterables.<ElectionRole, ElectionRole>iterable(
                new ElectionRole( role1 ), new ElectionRole( role2 ) ), clusterConfiguration,
                new Executor()
                {
                    @Override
                    public void execute( Runnable command )
                    {
                        command.run();
                    }
                }, logging,
                Mockito.mock( ObjectInputStreamFactory.class), Mockito.mock( ObjectOutputStreamFactory.class),
                Mockito.mock( AcceptorInstanceStore.class), Mockito.mock( Timeouts.class) );

        HeartbeatContext heartbeatContext = context.getHeartbeatContext();
        ElectionContext electionContext = context.getElectionContext();

        electionContext.startElectionProcess( role1 );
        electionContext.startElectionProcess( role2 );

        electionContext.voted( role1, failingInstance, mock( Comparable.class ), 2 );
        electionContext.voted( role2, failingInstance, mock( Comparable.class ), 2 );

        electionContext.voted( role1, otherInstance, mock( Comparable.class ), 2 );
        electionContext.voted( role2, otherInstance, mock( Comparable.class ), 2 );

        heartbeatContext.suspect( failingInstance );

        assertEquals( 1, electionContext.getVoteCount( role1 ) );
        assertEquals( 1, electionContext.getVoteCount( role2 ) );
    }

    private void baseTestForElectionOk( Set<InstanceId> failed, boolean moreThanQuorum )
    {
        Map<InstanceId, URI> members = new HashMap<InstanceId, URI>();
        members.put( new InstanceId( 1 ), URI.create( "server1" ) );
        members.put( new InstanceId( 2 ), URI.create( "server2" ) );
        members.put( new InstanceId( 3 ), URI.create( "server3" ) );

        ClusterConfiguration clusterConfiguration = mock( ClusterConfiguration.class );
        when( clusterConfiguration.getMembers() ).thenReturn( members );

        ClusterContext clusterContext = mock( ClusterContext.class );
        when( clusterContext.getConfiguration() ).thenReturn( clusterConfiguration );

        MultiPaxosContext context = new MultiPaxosContext( new InstanceId(1), Iterables.<ElectionRole, ElectionRole>iterable(
                        new ElectionRole( "coordinator" ) ), clusterConfiguration,
                        Mockito.mock(Executor.class), Mockito.mock(Logging.class),
                        Mockito.mock( ObjectInputStreamFactory.class), Mockito.mock( ObjectOutputStreamFactory.class),
                Mockito.mock( AcceptorInstanceStore.class), Mockito.mock( Timeouts.class) );

        context.getHeartbeatContext().getFailed().addAll( failed );

        ElectionContext toTest = context.getElectionContext();

        assertEquals( moreThanQuorum, !toTest.electionOk() );
    }
}
