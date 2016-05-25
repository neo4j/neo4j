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

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.TestOnlyDiscoveryServiceFactory;
import org.neo4j.coreedge.raft.log.segmented.FileNames;
import org.neo4j.coreedge.raft.roles.Role;
import org.neo4j.coreedge.server.CoreEdgeClusterSettings;
import org.neo4j.coreedge.server.core.CoreGraphDatabase;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.rule.TargetDirectory;

import static java.util.Collections.emptyMap;
import static junit.framework.TestCase.assertEquals;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;
import static org.neo4j.coreedge.raft.log.segmented.SegmentedRaftLog.SEGMENTED_LOG_DIRECTORY_NAME;
import static org.neo4j.coreedge.server.CoreEdgeClusterSettings.raft_log_pruning_frequency;
import static org.neo4j.coreedge.server.CoreEdgeClusterSettings.raft_log_pruning_strategy;
import static org.neo4j.coreedge.server.core.EnterpriseCoreEditionModule.CLUSTER_STATE_DIRECTORY_NAME;

public class CoreToCoreCopySnapshotIT
{
    @Rule
    public final TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTest( getClass() );

    private Cluster cluster;
    private int TIMEOUT_MS = 5000;

    @After
    public void shutdown() throws ExecutionException, InterruptedException
    {
        if ( cluster != null )
        {
            cluster.shutdown();
            cluster = null;
        }
    }

    @Test
    public void shouldBeAbleToDownloadFreshEmptySnapshot() throws Exception
    {
        // given
        File dbDir = dir.directory();
        cluster = Cluster.start( dbDir, 3, 0, new TestOnlyDiscoveryServiceFactory() );

        CoreGraphDatabase leader = cluster.awaitLeader( TIMEOUT_MS );

        // when
        CoreGraphDatabase follower = cluster.awaitCoreGraphDatabaseWithRole( TIMEOUT_MS, Role.FOLLOWER );
        follower.downloadSnapshot( leader.id().getCoreAddress() );

        // then
        assertEquals( DbRepresentation.of( leader ), DbRepresentation.of( follower ) );
    }

    @Test
    public void shouldBeAbleToDownloadSmallFreshSnapshot() throws Exception
    {
        // given
        File dbDir = dir.directory();
        cluster = Cluster.start( dbDir, 3, 0, new TestOnlyDiscoveryServiceFactory() );

        CoreGraphDatabase source = cluster.coreTx( ( db, tx ) -> {
            Node node = db.createNode();
            node.setProperty( "hej", "svej" );
            tx.success();
        } );

        // when
        CoreGraphDatabase follower = cluster.awaitCoreGraphDatabaseWithRole( TIMEOUT_MS, Role.FOLLOWER );
        follower.downloadSnapshot( source.id().getCoreAddress() );

        // then
        assertEquals( DbRepresentation.of( source ), DbRepresentation.of( follower ) );
    }

    @Test
    public void shouldBeAbleToDownloadLargerFreshSnapshot() throws Exception
    {
        // given
        File dbDir = dir.directory();
        cluster = Cluster.start( dbDir, 3, 0, new TestOnlyDiscoveryServiceFactory() );

        CoreGraphDatabase source = cluster.coreTx( ( db, tx ) -> {
            createData( db, 1000 );
            tx.success();
        } );

        // when
        CoreGraphDatabase follower = cluster.awaitCoreGraphDatabaseWithRole( TIMEOUT_MS, Role.FOLLOWER );
        follower.downloadSnapshot( source.id().getCoreAddress() );

        // then
        assertEquals( DbRepresentation.of( source ), DbRepresentation.of( follower ) );
    }

    @Test
    public void shouldBeAbleToDownloadToNewInstanceAfterPruning() throws Exception
    {
        // given
        File dbDir = dir.directory();
        Map<String,String> params = stringMap( CoreEdgeClusterSettings.state_machine_flush_window_size.name(), "1",
                CoreEdgeClusterSettings.raft_log_pruning_strategy.name(), "3 entries",
                CoreEdgeClusterSettings.raft_log_rotation_size.name(), "1K" );

        cluster = Cluster.start( dbDir, 3, 0, params, new TestOnlyDiscoveryServiceFactory() );

        CoreGraphDatabase source = cluster.coreTx( ( db, tx ) -> {
            createData( db, 10000 );
            tx.success();
        } );

        // when
        for ( CoreGraphDatabase coreDb : cluster.coreServers() )
        {
            coreDb.compact();
        }

        int newDbId = 3;
        cluster.addCoreServerWithServerId( newDbId, 4 );
        CoreGraphDatabase newDb = cluster.getCoreServerById( 3 );

        // then
        assertEquals( DbRepresentation.of( source ), DbRepresentation.of( newDb ) );
    }

    @Test
    public void shouldBeAbleToDownloadToRejoinedInstanceAfterPruning() throws Exception
    {
        // given
        File dbDir = dir.directory();

        Map<String,String> coreParams = stringMap();
        coreParams.put( raft_log_pruning_strategy.name(), "keep_none" );
        coreParams.put( raft_log_pruning_frequency.name(), "100ms" );
        int numberOfTransactions = 100;

        //Start the cluster and accumulate some log files.
        try ( Cluster cluster = Cluster
                .start( dbDir, 3, 0, new TestOnlyDiscoveryServiceFactory(), coreParams, emptyMap(),
                        StandardV3_0.NAME ) )
        {

            CoreGraphDatabase leader = cluster.awaitCoreGraphDatabaseWithRole( TIMEOUT_MS, Role.LEADER );
            int followersLastLog = getMostRecentLogIdOn( leader );
            while ( followersLastLog < 2 )
            {
                doSomeTransactions( cluster, numberOfTransactions );
                followersLastLog = getMostRecentLogIdOn( leader );
            }

            CoreGraphDatabase follower = cluster.awaitCoreGraphDatabaseWithRole( TIMEOUT_MS, Role.FOLLOWER );
            follower.shutdown();
            Config config = follower.getDependencyResolver().resolveDependency( Config.class );

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
            cluster.addCoreServerWithServerId( config.get( CoreEdgeClusterSettings.server_id ), 3 );
        }
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

    private Integer getOldestLogIdOn( CoreGraphDatabase clusterMember ) throws TimeoutException
    {
        return getLogFileNames( clusterMember ).firstKey().intValue();
    }

    private Integer getMostRecentLogIdOn( CoreGraphDatabase clusterMember ) throws TimeoutException
    {
        return getLogFileNames( clusterMember ).lastKey().intValue();
    }

    private SortedMap<Long,File> getLogFileNames( CoreGraphDatabase clusterMember )
    {
        File clusterDir = new File( clusterMember.getStoreDir(), CLUSTER_STATE_DIRECTORY_NAME );
        File logFilesDir = new File( clusterDir, SEGMENTED_LOG_DIRECTORY_NAME );
        return new FileNames( logFilesDir ).getAllFiles( new DefaultFileSystemAbstraction(), null );
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
        StringBuffer s = new StringBuffer();
        for ( int i = 0; i < numberOfCharacters; i++ )
        {
            s.append( String.valueOf( i ) );
        }
        return s.toString();
    }
}
