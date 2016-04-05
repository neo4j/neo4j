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

import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.raft.roles.Role;
import org.neo4j.coreedge.server.CoreEdgeClusterSettings;
import org.neo4j.coreedge.server.core.CoreGraphDatabase;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.TargetDirectory;

import static junit.framework.TestCase.assertEquals;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class CoreToCoreCopySnapshotIT
{
    @Rule
    public final TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTest( getClass() );

    private Cluster cluster;
    private int TIMEOUT_MS = 5000;

    @After
    public void shutdown()
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
        cluster = Cluster.start( dbDir, 3, 0 );

        CoreGraphDatabase leader = cluster.awaitLeader( TIMEOUT_MS );

        // when
        CoreGraphDatabase follower = cluster.awaitCoreGraphDatabaseWithRole( 5000, Role.FOLLOWER );
        follower.downloadSnapshot( leader.id().getCoreAddress() );

        // then
        assertEquals( DbRepresentation.of( leader ), DbRepresentation.of( follower ) );
    }

    @Test
    public void shouldBeAbleToDownloadSmallFreshSnapshot() throws Exception
    {
        // given
        File dbDir = dir.directory();
        cluster = Cluster.start( dbDir, 3, 0 );

        CoreGraphDatabase source =
                cluster.coreTx( ( db, tx ) -> {
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
        cluster = Cluster.start( dbDir, 3, 0 );

        CoreGraphDatabase source =
                cluster.coreTx( ( db, tx ) -> {
                    createStore( db, 1000 );
                    tx.success();
                } );

        // when
        CoreGraphDatabase follower = cluster.awaitCoreGraphDatabaseWithRole( TIMEOUT_MS, Role.FOLLOWER );
        follower.downloadSnapshot( source.id().getCoreAddress() );

        // then
        assertEquals( DbRepresentation.of( source ), DbRepresentation.of( follower ) );
    }

    @Test
    public void shouldBeAbleToDownloadAfterPruning() throws Exception
    {
        // given
        File dbDir = dir.directory();
        Map<String,String> params = stringMap(
                CoreEdgeClusterSettings.state_machine_flush_window_size.name(), "1",
                CoreEdgeClusterSettings.raft_log_pruning.name(), "3 entries",
                CoreEdgeClusterSettings.raft_log_rotation_size.name(), "1K" );

        cluster = Cluster.start( dbDir, 3, 0, params );

        CoreGraphDatabase source =
                cluster.coreTx( ( db, tx ) -> {
                    createStore( db, 10000 );
                    tx.success();
                } );

        // when
        cluster.coreServers().forEach( CoreGraphDatabase::compact );
        int newDbId = 3;
        cluster.addCoreServerWithServerId( newDbId, 4 );
        CoreGraphDatabase newDb = cluster.getCoreServerById( 3 );

        // then
        assertEquals( DbRepresentation.of( source ), DbRepresentation.of( newDb ) );
    }

    private void createStore( GraphDatabaseService db, int size )
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
}
