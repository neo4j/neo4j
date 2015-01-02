/**
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
package org.neo4j.ha;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.cluster.ClusterSettings.cluster_server;
import static org.neo4j.cluster.ClusterSettings.initial_hosts;
import static org.neo4j.cluster.ClusterSettings.server_id;
import static org.neo4j.kernel.ha.HaSettings.ha_server;
import static org.neo4j.kernel.ha.HaSettings.state_switch_timeout;

import java.io.File;

import org.junit.After;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.test.TargetDirectory;
import org.neo4j.tooling.GlobalGraphOperations;

public class ForeignStoreIdIT
{
    @Test
    public void emptyForeignDbShouldJoinAfterHavingItsEmptyDbDeleted() throws Exception
    {
        // GIVEN
        // -- one instance running
        firstInstance = new HighlyAvailableGraphDatabaseFactory()
                .newHighlyAvailableDatabaseBuilder( DIR.cleanDirectory( "1" ).getAbsolutePath() )
                .setConfig( server_id, "1" )
                .setConfig( cluster_server, "127.0.0.1:5001" )
                .setConfig( ha_server, "127.0.0.1:6031" )
                .setConfig( initial_hosts, "127.0.0.1:5001" )
                .newGraphDatabase();
        // -- another instance preparing to join with a store with a different store ID
        String foreignDbStoreDir = createAnotherStore( DIR.cleanDirectory( "2" ), 0 );

        // WHEN
        // -- the other joins
        foreignInstance = new HighlyAvailableGraphDatabaseFactory()
                .newHighlyAvailableDatabaseBuilder( foreignDbStoreDir )
                .setConfig( server_id, "2" )
                .setConfig( initial_hosts, "127.0.0.1:5001" )
                .setConfig( cluster_server, "127.0.0.1:5002" )
                .setConfig( ha_server, "127.0.0.1:6032" )
                .newGraphDatabase();
        // -- and creates a node
        long foreignNode = createNode( foreignInstance, "foreigner" );

        // THEN
        // -- that node should arrive at the master
        assertEquals( foreignNode, findNode( firstInstance, "foreigner" ) );
    }

    @Test
    public void nonEmptyForeignDbShouldNotBeAbleToJoin() throws Exception
    {
        // GIVEN
        // -- one instance running
        firstInstance = new HighlyAvailableGraphDatabaseFactory()
                .newHighlyAvailableDatabaseBuilder( DIR.cleanDirectory( "1" ).getAbsolutePath() )
                .setConfig( server_id, "1" )
                .setConfig( initial_hosts, "127.0.0.1:5001" )
                .setConfig( cluster_server, "127.0.0.1:5001" )
                .setConfig( ha_server, "127.0.0.1:6041" )
                .newGraphDatabase();
        createNodes( firstInstance, 3, "first" );
        // -- another instance preparing to join with a store with a different store ID
        String foreignDbStoreDir = createAnotherStore( DIR.cleanDirectory( "2" ), 1 );

        // WHEN
        // -- the other joins
        foreignInstance = new HighlyAvailableGraphDatabaseFactory()
                .newHighlyAvailableDatabaseBuilder( foreignDbStoreDir )
                .setConfig( server_id, "2" )
                .setConfig( initial_hosts, "127.0.0.1:5001" )
                .setConfig( cluster_server, "127.0.0.1:5002" )
                .setConfig( ha_server, "127.0.0.1:6042" )
                .setConfig( state_switch_timeout, "5s" )
                .newGraphDatabase();

        try
        {
            // THEN
            // -- that node should arrive at the master
            createNode( foreignInstance, "foreigner" );
            fail( "Shouldn't be able to create a node, since it shouldn't have joined" );
        }
        catch ( Exception e )
        {
            // Good
        }
    }
    
    private final TargetDirectory DIR = TargetDirectory.forTest( getClass() );
    private GraphDatabaseService firstInstance, foreignInstance;
    
    @After
    public void after() throws Exception
    {
        if ( foreignInstance != null )
            foreignInstance.shutdown();
        if ( firstInstance != null )
            firstInstance.shutdown();
    }
    
    private long findNode( GraphDatabaseService db, String name )
    {
        Transaction transaction = db.beginTx();

        try
        {
            for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
                if ( name.equals( node.getProperty( "name", null ) ) )
                    return node.getId();
            fail( "Didn't find node '" + name + "' in " + db );
            return -1; // will never happen
        }
        finally
        {
            transaction.finish();
        }
    }

    private String createAnotherStore( File directory, int transactions )
    {
        String storeDir = directory.getAbsolutePath();
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        createNodes( db, transactions, "node" );
        db.shutdown();
        return storeDir;
    }

    private void createNodes( GraphDatabaseService db, int transactions, String prefix )
    {
        for ( int i = 0; i < transactions; i++ )
            createNode( db, prefix + i );
    }

    private long createNode( GraphDatabaseService db, String name )
    {
        Transaction tx = db.beginTx();
        try
        {
            Node node = db.createNode();
            node.setProperty( "name", name );
            tx.success();
            return node.getId();
        }
        finally
        {
            tx.finish();
        }
    }
}
