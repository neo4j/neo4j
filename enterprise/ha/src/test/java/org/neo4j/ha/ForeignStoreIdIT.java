/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.ha;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.cluster.ClusterSettings.cluster_server;
import static org.neo4j.cluster.ClusterSettings.initial_hosts;
import static org.neo4j.cluster.ClusterSettings.server_id;
import static org.neo4j.kernel.ha.HaSettings.ha_server;
import static org.neo4j.kernel.ha.HaSettings.state_switch_timeout;

public class ForeignStoreIdIT
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    private GraphDatabaseService firstInstance;
    private GraphDatabaseService foreignInstance;

    @After
    public void after()
    {
        if ( foreignInstance != null )
        {
            foreignInstance.shutdown();
        }
        if ( firstInstance != null )
        {
            firstInstance.shutdown();
        }
    }

    @Test
    public void emptyForeignDbShouldJoinAfterHavingItsEmptyDbDeleted()
    {
        // GIVEN
        // -- one instance running
        int firstInstanceClusterPort = PortAuthority.allocatePort();
        firstInstance = new TestHighlyAvailableGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( testDirectory.directory( "1" ) )
                .setConfig( server_id, "1" )
                .setConfig( cluster_server, "127.0.0.1:" + firstInstanceClusterPort )
                .setConfig( ha_server, "127.0.0.1:" + PortAuthority.allocatePort() )
                .setConfig( initial_hosts, "127.0.0.1:" + firstInstanceClusterPort )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .newGraphDatabase();
        // -- another instance preparing to join with a store with a different store ID
        File foreignDbStoreDir = createAnotherStore( testDirectory.directory( "2" ), 0 );

        // WHEN
        // -- the other joins
        foreignInstance = new TestHighlyAvailableGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( foreignDbStoreDir )
                .setConfig( server_id, "2" )
                .setConfig( initial_hosts, "127.0.0.1:" + firstInstanceClusterPort )
                .setConfig( cluster_server, "127.0.0.1:" + PortAuthority.allocatePort() )
                .setConfig( ha_server, "127.0.0.1:" + PortAuthority.allocatePort() )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .newGraphDatabase();
        // -- and creates a node
        long foreignNode = createNode( foreignInstance, "foreigner" );

        // THEN
        // -- that node should arrive at the master
        assertEquals( foreignNode, findNode( firstInstance, "foreigner" ) );
    }

    @Test
    public void nonEmptyForeignDbShouldNotBeAbleToJoin()
    {
        // GIVEN
        // -- one instance running
        int firstInstanceClusterPort = PortAuthority.allocatePort();
        firstInstance = new TestHighlyAvailableGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( testDirectory.directory( "1" ) )
                .setConfig( server_id, "1" )
                .setConfig( initial_hosts, "127.0.0.1:" + firstInstanceClusterPort )
                .setConfig( cluster_server, "127.0.0.1:" + firstInstanceClusterPort )
                .setConfig( ha_server, "127.0.0.1:" + PortAuthority.allocatePort() )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .newGraphDatabase();
        createNodes( firstInstance, 3, "first" );
        // -- another instance preparing to join with a store with a different store ID
        File foreignDbStoreDir = createAnotherStore( testDirectory.directory( "2" ), 1 );

        // WHEN
        // -- the other joins
        foreignInstance = new TestHighlyAvailableGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( foreignDbStoreDir )
                .setConfig( server_id, "2" )
                .setConfig( initial_hosts, "127.0.0.1:" + firstInstanceClusterPort )
                .setConfig( cluster_server, "127.0.0.1:" + PortAuthority.allocatePort() )
                .setConfig( ha_server, "127.0.0.1:" + PortAuthority.allocatePort() )
                .setConfig( state_switch_timeout, "5s" )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
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

    private long findNode( GraphDatabaseService db, String name )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            for ( Node node : db.getAllNodes() )
            {
                if ( name.equals( node.getProperty( "name", null ) ) )
                {
                    return node.getId();
                }
            }
            fail( "Didn't find node '" + name + "' in " + db );
            return -1; // will never happen
        }
    }

    private File createAnotherStore( File directory, int transactions )
    {
        GraphDatabaseService db = new EnterpriseGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( directory )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .newGraphDatabase();
        createNodes( db, transactions, "node" );
        db.shutdown();
        return directory;
    }

    private void createNodes( GraphDatabaseService db, int transactions, String prefix )
    {
        for ( int i = 0; i < transactions; i++ )
        {
            createNode( db, prefix + i );
        }
    }

    private long createNode( GraphDatabaseService db, String name )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.setProperty( "name", name );
            tx.success();
            return node.getId();
        }
    }
}
