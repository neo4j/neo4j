/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.scenarios;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.TimeoutException;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.HazelcastDiscoveryServiceFactory;
import org.neo4j.graphdb.Node;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;
import static org.neo4j.graphdb.Label.label;

public class WillNotBecomeLeaderIT
{
    @Rule
    public final ClusterRule clusterRule =
            new ClusterRule( getClass() ).withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 0 )
                    .withDiscoveryServiceFactory( new HazelcastDiscoveryServiceFactory() )
                    .withSharedCoreParam( CausalClusteringSettings.multi_dc_license, "true" );

    @Rule
    public ExpectedException exceptionMatcher = ExpectedException.none();

    @Test
    public void clusterShouldNotElectNewLeader() throws Exception
    {
        // given
        int leaderId = 0;

        clusterRule.withInstanceCoreParam( CausalClusteringSettings.refuse_to_be_leader, x ->
        {
            if ( x == leaderId )
            {
                return "false";
            }
            else
            {
                return "true";
            }
        } );

        Cluster cluster = clusterRule.createCluster();
        cluster.start();
        assertEquals( leaderId, cluster.awaitLeader().serverId() );

        cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        // When
        cluster.removeCoreMemberWithMemberId( leaderId );

        // Then
        try
        {
            cluster.awaitLeader(10, SECONDS);

            fail( "Should not have elected a leader" );
        }
        catch ( TimeoutException ex )
        {
            // Successful
        }
    }
}
