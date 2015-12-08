/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.neo4j.cluster.InstanceId;
import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.server.CoreEdgeClusterSettings;
import org.neo4j.coreedge.server.core.CoreGraphDatabase;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.test.TargetDirectory;
import org.neo4j.tooling.GlobalGraphOperations;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static junit.framework.TestCase.fail;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.neo4j.cluster.ClusterSettings.server_id;
import static org.neo4j.coreedge.server.CoreEdgeClusterSettings.raft_advertised_address;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.IteratorUtil.asList;
import static org.neo4j.test.Assert.assertEventually;

public class CoreServerReplicationIT
{
    @Rule
    public final TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTest( getClass() );

    private Cluster cluster;

    @After
    public void shutdown()
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    public void shouldReplicateTransactionToCoreServers() throws Exception
    {
        // given
        File dbDir = dir.directory();
        cluster = Cluster.start( dbDir, 3, 0 );

        // when
        GraphDatabaseService coreDB = cluster.findLeader( 5000 );

        try ( Transaction tx = coreDB.beginTx() )
        {
            Node node = coreDB.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        }

        // then
        for ( final CoreGraphDatabase db : cluster.coreServers() )
        {
            try ( Transaction tx = db.beginTx() )
            {
                ThrowingSupplier<Long, Exception> nodeCount = () -> count( db.getAllNodes() );

                Config config = db.getDependencyResolver().resolveDependency( Config.class );

                assertEventually( "node to appear on core server " + config.get( raft_advertised_address ), nodeCount,
                        greaterThan(  0L ), 15, SECONDS );

                for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
                {
                    assertEquals( "baz_bat", node.getProperty( "foobar" ) );
                }

                tx.success();
            }
        }
    }

    @Test
    public void shouldReplicateTransactionToCoreServersAddedAfterInitialStartUp() throws Exception
    {
        // given
        File dbDir = dir.directory();
        cluster = Cluster.start( dbDir, 3, 0 );

        cluster.addCoreServerWithServerId( 3, 4 );

        // when
        GraphDatabaseService coreDB = cluster.findLeader( 5000 );

        try ( Transaction tx = coreDB.beginTx() )
        {
            Node node = coreDB.createNode();
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        }

        cluster.addCoreServerWithServerId( 4, 5 );

        coreDB = cluster.findLeader( 5000 );

        try ( Transaction tx = coreDB.beginTx() )
        {
            Node node = coreDB.createNode();
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        }

        // then
        for ( final CoreGraphDatabase db : cluster.coreServers() )
        {
            try ( Transaction tx = db.beginTx() )
            {
                ThrowingSupplier<Long, Exception> nodeCount = () -> count( db.getAllNodes() );

                Config config = db.getDependencyResolver().resolveDependency( Config.class );

                assertEventually( "node to appear on core server " + config.get( HaSettings.ha_server ), nodeCount,
                        equalTo( 2L ), 3, SECONDS );

                for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
                {
                    assertEquals( "baz_bat", node.getProperty( "foobar" ) );
                }

                tx.success();
            }
        }
    }

    @Test
    public void shouldReplicateTransactionAfterOneOriginalServerRemovedFromCluster() throws Exception
    {
        File dbDir = dir.directory();
        cluster = Cluster.start( dbDir, 3, 0 );
        CoreGraphDatabase leader = cluster.findLeader( 5000 );
        try ( Transaction tx = leader.beginTx() )
        {
            Node node = leader.createNode();
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        }

        cluster.removeCoreServer( leader );
        final GraphDatabaseService newLeader = cluster.findLeader( 5000 );
        ThrowingSupplier<Boolean, Exception> creationSuccess = () -> {
            try ( Transaction tx = newLeader.beginTx() )
            {
                Node node = newLeader.createNode();
                node.setProperty( "foobar", "baz_bat" );
                tx.success();
                return true;

            }
            catch ( Exception e )
            {
                // ignore temporary failures
            }
            return false;
        };

        assertEventually( "node could be created after server removed", creationSuccess, is( true ), 1, MINUTES );

        // then
        for ( final CoreGraphDatabase db : cluster.coreServers() )
        {
            try ( Transaction tx = db.beginTx() )
            {
                ThrowingSupplier<Long, Exception> nodeCount = () -> count( db.getAllNodes() );

                Config config = db.getDependencyResolver().resolveDependency( Config.class );

                assertEventually( "node to appear on core server " + config.get( HaSettings.ha_server ), nodeCount,
                        equalTo( 2L ), 1, MINUTES );

                for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
                {
                    assertEquals( "baz_bat", node.getProperty( "foobar" ) );
                }

                tx.success();
            }
        }
    }

    @Test
    public void shouldReplicateToCoreServersAddedAfterInitialTransactions() throws Exception
    {
        // given
        File dbDir = dir.directory();
        cluster = Cluster.start( dbDir, 3, 0 );

        // when
        for ( int i = 0; i < 15; i++ )
        {
            CoreGraphDatabase leader = cluster.findLeader( 5000 );

            try ( Transaction tx = leader.beginTx() )
            {
                Node node = leader.createNode();
                node.setProperty( "foobar", "baz_bat" );
                tx.success();
            }
        }

        cluster.addCoreServerWithServerId( 3, 4 );
        cluster.addCoreServerWithServerId( 4, 5 );

        // then
        for ( final CoreGraphDatabase db : cluster.coreServers() )
        {
            try ( Transaction tx = db.beginTx() )
            {
                ThrowingSupplier<Long, Exception> nodeCount = () -> count( db.getAllNodes() );

                Config config = db.getDependencyResolver().resolveDependency( Config.class );

                assertEventually( "node to appear on core server " + config.get( CoreEdgeClusterSettings
                                .raft_listen_address ), nodeCount,
                        is( 15L ), 1, MINUTES );

                for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
                {
                    DependencyResolver dependencyResolver = db.getDependencyResolver();
                    InstanceId id = dependencyResolver.resolveDependency( Config.class ).get( server_id );
                    assertEquals( "Assertion fails on server: " + id, "baz_bat", node.getProperty( "foobar" ) );
                }

                tx.success();
            }
        }
    }

    @Test
    @Ignore("Currently we only support writing on the leader")
    public void shouldReplicateTransactionFromAnyCoreServer() throws Exception
    {
        // given
        File dbDir = dir.directory();
        cluster = Cluster.start( dbDir, 3, 0 );

        // when
        Set<CoreGraphDatabase> coreServers = cluster.coreServers();

        ExecutorService executor = Executors.newFixedThreadPool( coreServers.size() );
        try
        {
            for ( final CoreGraphDatabase coreServer : coreServers )
            {
                executor.submit( () -> {
                    try ( Transaction tx = coreServer.beginTx() )
                    {
                        coreServer.createNode();
                        tx.success();
                    }
                } );
            }
        }
        finally
        {
            executor.shutdown();
            if ( !executor.awaitTermination( 1, TimeUnit.MINUTES ) )
            {
                fail( "Initial transactions not finished within reasonable time." );
            }
        }

        for ( final CoreGraphDatabase db : coreServers )
        {
            try ( Transaction tx = db.beginTx() )
            {
                ThrowingSupplier<Long, Exception> nodeCount = () -> count( db.getAllNodes() );

                Config config = db.getDependencyResolver().resolveDependency( Config.class );
                assertEventually( "node to appear on core server " +
                                config.get( CoreEdgeClusterSettings.transaction_listen_address ), nodeCount,
                        is( (long) coreServers.size() ), 1, MINUTES );
                tx.success();
            }
        }
    }

    @Test
    @Ignore("Currently we only support writing on the leader")
    public void shouldAtomicallyCreatePropertyTokenAcrossCluster() throws Exception
    {
        // given
        final int NUMBER_OF_SERVERS = 3;
        final int NODES_PER_SERVER = 2;

        final File dbDir = dir.directory();

        cluster = Cluster.start( dbDir, NUMBER_OF_SERVERS, 0 );
        Set<CoreGraphDatabase> coreServers = cluster.coreServers();

        // when
        ExecutorService executor = Executors.newFixedThreadPool( NUMBER_OF_SERVERS );
        try
        {
            for ( final CoreGraphDatabase coreServer : coreServers )
            {
                executor.submit( () -> {
                    boolean success = false;
                    while ( !success )
                    {
                        try
                        {
                            try ( Transaction tx = coreServer.beginTx() )
                            {
                                Node node1 = coreServer.createNode( label( "label1" ) );
                                node1.setProperty( "key1", "value1" );
                                Node node2 = coreServer.createNode( label( "label2" ) );
                                node2.setProperty( "key2", "value2" );

                                node1.createRelationshipTo( node2, withName( "relType1" ) );
                                tx.success();
                            }
                            success = true;
                        }
                        catch ( Exception e )
                        {
                            e.printStackTrace();
                        }
                    }
                } );
            }
        }
        finally
        {
            executor.shutdown();
            if ( !executor.awaitTermination( 1, TimeUnit.MINUTES ) )
            {
                fail( "Initial transactions not finished within reasonable time." );
            }
        }

        // then
        for ( final CoreGraphDatabase db : cluster.coreServers() )
        {
            try ( Transaction tx = db.beginTx() )
            {
                assertEventually( "Node count", () -> count( db.getAllNodes() ),
                        is( (long) NUMBER_OF_SERVERS * NODES_PER_SERVER ), 1, MINUTES );

                ResourceIterator<Node> nodes = db.findNodes( label( "label1" ) );
                while ( nodes.hasNext() )
                {
                    Node node = nodes.next();
                    assertEquals( "value1", node.getProperty( "key1" ) );
                    Iterable<Label> labels = node.getSingleRelationship( withName( "relType1" ), Direction.OUTGOING )
                            .getEndNode().getLabels();
                    assertEquals( singletonList( label( "label2" ) ), asList( labels ) );
                }

                tx.success();
            }
        }
    }
}
