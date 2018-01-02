/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cluster.protocol.cluster;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.VerifyInstanceConfiguration;

public class InstanceIdTest
        extends ClusterMockTest
{
    @Test
    public void nodeTriesToJoinAnotherNodeWithSameServerId() throws InterruptedException, ExecutionException,
            TimeoutException, URISyntaxException
    {
        testCluster( new int[] { 1, 1 }, new VerifyInstanceConfiguration[]
                {
                new VerifyInstanceConfiguration( Collections.<URI>emptyList(), Collections.<String, InstanceId>emptyMap(),
                                        Collections.<InstanceId>emptySet() ),
                new VerifyInstanceConfiguration( Collections.<URI>emptyList(), Collections.<String, InstanceId>emptyMap(),
                                        Collections.<InstanceId>emptySet() )
                },
                DEFAULT_NETWORK(), new ClusterTestScriptDSL().
                rounds( 600 ).
                join( 100, 1, 1, 2 ).
                join( 100, 2, 1, 2 ).
                message( 500, "*** All nodes tried to start, should be in failed mode" )
                );
    }

    @Test
    public void nodeTriesToJoinRunningClusterWithExistingServerId() throws InterruptedException, ExecutionException,
            TimeoutException, URISyntaxException
    {
        List<URI> correctMembers = new ArrayList<>();
        correctMembers.add( URI.create( "server1" ) );
        correctMembers.add( URI.create( "server2" ) );
        correctMembers.add( URI.create( "server3" ) );

        Map<String, InstanceId> roles = new HashMap<>();
        roles.put( "coordinator", new InstanceId( 1 ) );

        testCluster( new int[] {1, 2, 3, 3},
                new VerifyInstanceConfiguration[]{
                new VerifyInstanceConfiguration( correctMembers, roles, Collections.<InstanceId>emptySet() ),
                new VerifyInstanceConfiguration( correctMembers, roles, Collections.<InstanceId>emptySet() ),
                new VerifyInstanceConfiguration( correctMembers, roles, Collections.<InstanceId>emptySet() ),
                new VerifyInstanceConfiguration( Collections.<URI>emptyList(), Collections.<String, InstanceId>emptyMap(),
                        Collections.<InstanceId>emptySet() )}, DEFAULT_NETWORK(), new ClusterTestScriptDSL().
                rounds( 600 ).
                join( 100, 1, 1 ).
                join( 100, 2, 1 ).
                join( 100, 3, 1 ).
                join( 5000, 4, 1 ).
                message( 0, "*** Conflicting node tried to join" )
                );
    }

    @Test
    public void substituteFailedNode() throws InterruptedException, ExecutionException, TimeoutException,
            URISyntaxException
    {
        List<URI> correctMembers = new ArrayList<>();
        correctMembers.add( URI.create( "server1" ) );
        correctMembers.add( URI.create( "server2" ) );
        correctMembers.add( URI.create( "server4" ) );

        List<URI> wrongMembers = new ArrayList<>();
        wrongMembers.add( URI.create( "server1" ) );
        wrongMembers.add( URI.create( "server2" ) );
        wrongMembers.add( URI.create( "server3" ) );

        Map<String, InstanceId> roles = new HashMap<>();
        roles.put( "coordinator", new InstanceId( 1 ) );

        Set<InstanceId> clusterMemberFailed = new HashSet<>();
        Set<InstanceId> isolatedMemberFailed = new HashSet<>();
        isolatedMemberFailed.add( new InstanceId( 1 ) ); // will never receive heartbeats again from 1,2 so they are failed
        isolatedMemberFailed.add( new InstanceId( 2 ) );

        testCluster( new int[]{ 1, 2, 3, 3 },
                new VerifyInstanceConfiguration[]{
                        new VerifyInstanceConfiguration( correctMembers, roles, clusterMemberFailed ),
                        new VerifyInstanceConfiguration( correctMembers, roles, clusterMemberFailed ),
                        new VerifyInstanceConfiguration( wrongMembers, roles, isolatedMemberFailed ),
                        new VerifyInstanceConfiguration( correctMembers, roles, clusterMemberFailed )},
                DEFAULT_NETWORK(),
                new ClusterTestScriptDSL().
                rounds( 8000 ).
                join( 100, 1, 1 ).
                join( 100, 2, 1 ).
                join( 100, 3, 1 ).
                down( 3000, 3 ).
                join( 1000, 4, 1, 2, 3 )
        );
    }

    @Test
    public void substituteFailedNodeAndFailedComesOnlineAgain() throws InterruptedException, ExecutionException, TimeoutException,
            URISyntaxException
    {
        List<URI> correctMembers = new ArrayList<>();
        correctMembers.add( URI.create( "server1" ) );
        correctMembers.add( URI.create( "server2" ) );
        correctMembers.add( URI.create( "server4" ) );

        List<URI> badMembers = new ArrayList<>();
        badMembers.add( URI.create( "server1" ) );
        badMembers.add( URI.create( "server2" ) );
        badMembers.add( URI.create( "server3" ) );

        Map<String, InstanceId> roles = new HashMap<>();
        roles.put( "coordinator", new InstanceId( 1 ) );

        Set<InstanceId> clusterMemberFailed = new HashSet<>(); // no failures
        Set<InstanceId> isolatedMemberFailed = new HashSet<>();
        isolatedMemberFailed.add( new InstanceId( 1 ) ); // will never receive heartbeats again from 1,2 so they are failed
        isolatedMemberFailed.add( new InstanceId( 2 ) );


        testCluster( new int[]{1, 2, 3, 3},
                new VerifyInstanceConfiguration[]{
                        new VerifyInstanceConfiguration( correctMembers, roles, clusterMemberFailed ),
                        new VerifyInstanceConfiguration( correctMembers, roles, clusterMemberFailed ),
                        new VerifyInstanceConfiguration( badMembers, roles, isolatedMemberFailed ),
                        new VerifyInstanceConfiguration( correctMembers, roles, clusterMemberFailed )},
                DEFAULT_NETWORK(),
                new ClusterTestScriptDSL().
                        rounds( 800 ).
                        join( 100, 1, 1 ).
                        join( 100, 2, 1 ).
                        join( 100, 3, 1 ).
                        down( 3000, 3 ).
                        join( 1000, 4, 1, 2, 3 ).
                        up( 1000, 3 )
        );
    }
}
