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
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Set;

import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.server.edge.EdgeGraphDatabase;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import static java.io.File.pathSeparator;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.test.Assert.assertEventually;

public class EdgeServerReplicationIT
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
    public void shouldNotBeAbleToWriteToEdge() throws Exception
    {
        // given
        cluster = Cluster.start( dir.directory(), 3, 1);

        GraphDatabaseService edgeDB = cluster.findAnEdgeServer();

        // when (write should fail)
        boolean transactionFailed = false;
        try ( Transaction tx = edgeDB.beginTx() )
        {
            Node node = edgeDB.createNode();
            node.setProperty( "foobar", "baz_bat" );
            node.addLabel( Label.label( "Foo" ) );
            tx.success();
        }
        catch ( TransactionFailureException e )
        {
            // expected
            transactionFailed = true;
        }

        assertTrue( transactionFailed );
    }

    @Test
    public void allServersBecomeAvailable() throws Exception
    {
        // given
        cluster = Cluster.start( dir.directory(), 3, 1);

        // then
        for ( final EdgeGraphDatabase edgeGraphDatabase : cluster.edgeServers() )
        {
            ThrowingSupplier<Boolean, Exception> availability = () -> edgeGraphDatabase.isAvailable( 0 );
            assertEventually( "edge server becomes available", availability, is( true ), 10, SECONDS );
        }
    }

    @Test
    public void shouldEventuallyPullTransactionDownToAllEdgeServers() throws Exception
    {
        // given
        cluster = Cluster.start( dir.directory(), 3, 0);
        int nodesBeforeEdgeServerStarts = 1;

        // when
        GraphDatabaseService coreDB = cluster.findLeader( 5000 );
        try ( Transaction tx = coreDB.beginTx() )
        {
            for ( int i = 0; i < nodesBeforeEdgeServerStarts; i++ )
            {
                Node node = coreDB.createNode();
                node.setProperty( "foobar", "baz_bat" );
            }
            tx.success();
        }

        cluster.addEdgeServerWithFileLocation( 0 );

        // when
        try ( Transaction tx = coreDB.beginTx() )
        {
            Node node = coreDB.createNode();
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        }

        // then
        Set<EdgeGraphDatabase> edgeGraphDatabases = cluster.edgeServers();
        for ( final GraphDatabaseService edgeDB : edgeGraphDatabases )
        {
            try ( Transaction tx = edgeDB.beginTx() )
            {
                ThrowingSupplier<Long, Exception> nodeCount = () -> Iterables.count( edgeDB.getAllNodes() );
                assertEventually( "node to appear on edge server", nodeCount, is( nodesBeforeEdgeServerStarts + 1l ),
                        2, MINUTES );

                for ( Node node : GlobalGraphOperations.at( edgeDB ).getAllNodes() )
                {
                    assertEquals( "baz_bat", node.getProperty( "foobar" ) );
                }

                tx.success();
            }
        }
    }

    @Test
    public void shouldShutdownRatherThanPullUpdatesFromCoreServerWithDifferentStoreIfServerHasData() throws Exception
    {

        File edgeDatabaseStoreFileLocation = createExistingEdgeStore( dir.directory().getAbsolutePath() +
                pathSeparator + "edgeStore" );

        cluster = Cluster.start( dir.directory(), 3, 0);

        GraphDatabaseService coreDB = this.cluster.findLeader( 5000 );

        try ( Transaction tx = coreDB.beginTx() )
        {
            for ( int i = 0; i < 10; i++ )
            {
                Node node = coreDB.createNode();
                node.setProperty( "foobar", "baz_bat" );
            }
            tx.success();
        }

        try
        {
            cluster.addEdgeServerWithFileLocation( edgeDatabaseStoreFileLocation );
            fail();
        }
        catch ( Throwable required )
        {
            // Lifecycle should throw exception, server should not start.
        }
    }

    private File createExistingEdgeStore( String path )
    {
        File dir = new File( path );
        dir.mkdirs();

        GraphDatabaseService db = new TestGraphDatabaseFactory()
                .newEmbeddedDatabase( Cluster.edgeSeverStoreDirectory( dir, 1966 ) );

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }

        db.shutdown();

        return dir;
    }
}
