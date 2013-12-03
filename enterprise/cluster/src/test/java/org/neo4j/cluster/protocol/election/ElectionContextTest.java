/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.MultiPaxosContext;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterContext;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;

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
        toTest.voted( coordinatorRole, new InstanceId( 1 ), new IntegerElectionCredentials( 100 ) );
        toTest.voted( coordinatorRole, new InstanceId( 2 ), new IntegerElectionCredentials( 100 ) );
        toTest.voted( coordinatorRole, new InstanceId( 2 ), new IntegerElectionCredentials( 101 ) );

        // Then
        assertNull( toTest.getElectionWinner( coordinatorRole ) );
        assertEquals( 2, toTest.getVoteCount( coordinatorRole ) );
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

    private static final class IntegerElectionCredentials implements ElectionCredentials
    {
        private final int credential;

        private IntegerElectionCredentials( int credential )
        {
            this.credential = credential;
        }

        @Override
        public int compareTo( Object o )
        {
            return o instanceof IntegerElectionCredentials
                    ? Integer.valueOf(credential).compareTo(Integer.valueOf(( (IntegerElectionCredentials) o).credential)) : 0;
        }
    }
}
