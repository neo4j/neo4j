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
package org.neo4j.coreedge.scenarios;

import org.junit.Rule;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.CoreClusterMember;
import org.neo4j.coreedge.raft.roles.Role;
import org.neo4j.coreedge.server.CoreEdgeClusterSettings;
import org.neo4j.coreedge.server.core.CoreGraphDatabase;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.coreedge.ClusterRule;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;

import static org.neo4j.coreedge.server.CoreEdgeClusterSettings.raft_log_pruning_frequency;
import static org.neo4j.coreedge.server.CoreEdgeClusterSettings.raft_log_pruning_strategy;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class CoreToCoreCopySnapshotIT
{
    private static final int TIMEOUT_MS = 5000;

    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() )
            .withNumberOfCoreMembers( 3 )
            .withNumberOfEdgeMembers( 0 );

    @Test
    public void shouldBeAbleToDownloadFreshEmptySnapshot() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        CoreClusterMember leader = cluster.awaitLeader( TIMEOUT_MS );

        // when
        CoreClusterMember follower = cluster.awaitCoreMemberWithRole( TIMEOUT_MS, Role.FOLLOWER );
        follower.coreState().downloadSnapshot( leader.id() );

        // then
        assertEquals( DbRepresentation.of( leader.database() ), DbRepresentation.of( follower.database() ) );
    }

    @Test
    public void shouldBeAbleToDownloadSmallFreshSnapshot() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        CoreClusterMember source = cluster.coreTx( ( db, tx ) -> {
            Node node = db.createNode();
            node.setProperty( "hej", "svej" );
            tx.success();
        } );

        // when
        CoreClusterMember follower = cluster.awaitCoreMemberWithRole( TIMEOUT_MS, Role.FOLLOWER );

        follower.coreState().downloadSnapshot( source.id() );

        // then
        assertEquals( DbRepresentation.of( source.database() ), DbRepresentation.of( follower.database() ) );
    }

    @Test
    public void shouldBeAbleToDownloadLargerFreshSnapshot() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        CoreClusterMember source = cluster.coreTx( ( db, tx ) -> {
            createData( db, 1000 );
            tx.success();
        } );

        // when
        CoreClusterMember follower = cluster.awaitCoreMemberWithRole( TIMEOUT_MS, Role.FOLLOWER );
        follower.coreState().downloadSnapshot( source.id() );

        // then
        assertEquals( DbRepresentation.of( source.database() ), DbRepresentation.of( follower.database() ) );
    }

    @Test
    public void shouldBeAbleToDownloadToNewInstanceAfterPruning() throws Exception
    {
        // given
        Map<String,String> params = stringMap( CoreEdgeClusterSettings.state_machine_flush_window_size.name(), "1",
                CoreEdgeClusterSettings.raft_log_pruning_strategy.name(), "3 entries",
                CoreEdgeClusterSettings.raft_log_rotation_size.name(), "1K" );

        Cluster cluster = clusterRule.withSharedCoreParams( params ).startCluster();

        CoreClusterMember leader = cluster.coreTx( ( db, tx ) -> {
            createData( db, 10000 );
            tx.success();
        } );

        // when
        for ( CoreClusterMember coreDb : cluster.coreMembers() )
        {
            coreDb.coreState().compact();
        }

        cluster.removeCoreMember( leader ); // to force a change of leader
        leader = cluster.awaitLeader();

        int newDbId = 3;
        cluster.addCoreMemberWithId( newDbId, 3 ).start();
        CoreGraphDatabase newDb = cluster.getCoreMemberById( newDbId ).database();

        // then
        assertEquals( DbRepresentation.of( leader.database() ), DbRepresentation.of( newDb ) );
    }

    @Test
    public void shouldBeAbleToDownloadToRejoinedInstanceAfterPruning() throws Exception
    {
        // given
        Map<String,String> coreParams = stringMap();
        coreParams.put( raft_log_pruning_strategy.name(), "keep_none" );
        coreParams.put( raft_log_pruning_frequency.name(), "100ms" );
        int numberOfTransactions = 100;

        //Start the cluster and accumulate some log files.
        Cluster cluster = clusterRule.withSharedCoreParams( coreParams ).startCluster();

        CoreClusterMember leader = cluster.awaitCoreMemberWithRole( TIMEOUT_MS, Role.LEADER );
        int followersLastLog = getMostRecentLogIdOn( leader );
        while ( followersLastLog < 2 )
        {
            doSomeTransactions( cluster, numberOfTransactions );
            followersLastLog = getMostRecentLogIdOn( leader );
        }

        CoreClusterMember follower = cluster.awaitCoreMemberWithRole( TIMEOUT_MS, Role.FOLLOWER );
        follower.shutdown();

        /**
         * After a follower is shutdown, wait until we accumulate some logs such that the oldest log is older than
         * the most recent log when the follower was removed. We can then be sure that the follower won't be able
         * to catch up to the leader without a snapshot.
         */

        //when
        int leadersOldestLog = getOldestLogIdOn( leader );
        while ( leadersOldestLog < followersLastLog + 10 )
        {
            doSomeTransactions( cluster, numberOfTransactions );
            leadersOldestLog = getOldestLogIdOn( leader );
        }

        //then
        assertThat( leadersOldestLog, greaterThan( followersLastLog ) );
        //The cluster member should join. Otherwise this line will hang forever.
        follower.start();
    }

    static void createData( GraphDatabaseService db, int size )
    {
        for ( int i = 0; i < size; i++ )
        {
            Node node1 = db.createNode();
            Node node2 = db.createNode();

            node1.setProperty( "hej", "svej" );
            node2.setProperty( "tjabba", "tjena" );

            Relationship rel = node1.createRelationshipTo( node2, RelationshipType.withName( "halla" ) );
            rel.setProperty( "this", "that" );
        }
    }

    private int getOldestLogIdOn( CoreClusterMember clusterMember ) throws TimeoutException
    {
        return clusterMember.getLogFileNames().firstKey().intValue();
    }

    private int getMostRecentLogIdOn( CoreClusterMember clusterMember ) throws TimeoutException
    {
        return clusterMember.getLogFileNames().lastKey().intValue();
    }

    private void doSomeTransactions( Cluster cluster, int count )
    {
        try
        {
            for ( int i = 0; i < count; i++ )
            {
                cluster.coreTx( ( db, tx ) -> {
                    Node node = db.createNode();
                    node.setProperty( "that's a bam", string( 1024 ) );
                    tx.success();
                } );

            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private String string( int numberOfCharacters )
    {
        StringBuilder s = new StringBuilder();
        for ( int i = 0; i < numberOfCharacters; i++ )
        {
            s.append( String.valueOf( i ) );
        }
        return s.toString();
    }

}
