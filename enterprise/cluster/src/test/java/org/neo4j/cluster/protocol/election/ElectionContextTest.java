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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.mockito.Matchers;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterContext;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.util.StringLogger;

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
    public void testElectionOkMoreThanQuorumFailed()
    {
        Set<InstanceId> failed = new HashSet<InstanceId>();
        failed.add( new InstanceId( 1 ) );
        failed.add( new InstanceId( 2 ) );

        baseTestForElectionOk( failed, true );
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
        when ( clusterContext.getLogger( Matchers.<Class>any() ) ).thenReturn( mock( StringLogger.class ) );

        ElectionContext toTest = new ElectionContext( Iterables.<ElectionRole, ElectionRole>iterable(
                new ElectionRole( coordinatorRole ) ), clusterContext, heartbeatContext );

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
        HeartbeatContext heartbeatContext = mock(HeartbeatContext.class);
        when( heartbeatContext.getFailed() ).thenReturn( failed );

        Map<InstanceId, URI> members = new HashMap<InstanceId, URI>();
        members.put( new InstanceId( 1 ), URI.create( "server1" ) );
        members.put( new InstanceId( 2 ), URI.create( "server2" ) );
        members.put( new InstanceId( 3 ), URI.create( "server3" ) );

        ClusterConfiguration clusterConfiguration = mock( ClusterConfiguration.class );
        when( clusterConfiguration.getMembers() ).thenReturn( members );

        ClusterContext clusterContext = mock( ClusterContext.class );
        when( clusterContext.getConfiguration() ).thenReturn( clusterConfiguration );

        ElectionContext toTest = new ElectionContext( Iterables.<ElectionRole, ElectionRole>iterable(
                new ElectionRole("coordinator") ), clusterContext, heartbeatContext );

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
