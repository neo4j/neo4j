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
package org.neo4j.kernel.ha;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.test.LoggerRule;
import org.neo4j.test.TargetDirectory;

/**
 * TODO
 */
public class TestPaxosClusterEvents
{
    @Rule
    public LoggerRule logger = new LoggerRule();

    List<HighlyAvailableGraphDatabase> databases = new ArrayList<HighlyAvailableGraphDatabase>();

    public TargetDirectory dir = TargetDirectory.forTest( getClass() );

    @Before
    public void setup()
            throws IOException
    {
        HighlyAvailableGraphDatabase initialDatabase =
                (HighlyAvailableGraphDatabase) new HighlyAvailableGraphDatabaseFactory().
                        newHighlyAvailableDatabaseBuilder( dir.directory( "1", true ).getAbsolutePath() )
                        .setConfig( ClusterSettings.cluster_server, "127.0.0.1:5001" )
                        .setConfig( ClusterSettings.server_id, "1" )
                        .setConfig( HaSettings.ha_server, "localhost:6361" )
                        .newGraphDatabase();
        databases.add( initialDatabase );

        for ( int i = 0; i < 2; i++ )
        {
            int serverId = i + 2;
            HighlyAvailableGraphDatabase database = (HighlyAvailableGraphDatabase) new
                    HighlyAvailableGraphDatabaseFactory().
                    newHighlyAvailableDatabaseBuilder( dir.directory( "" + serverId, true ).getAbsolutePath() )
                    .setConfig( ClusterSettings.cluster_server, "localhost:" + (5002 + i) )
                    .setConfig( ClusterSettings.initial_hosts, "127.0.0.1:5001" )
                    .setConfig( ClusterSettings.server_id, serverId + "" )
                    .setConfig( HaSettings.ha_server, ":" + (6362 + i) )
                    .newGraphDatabase();

            databases.add( database );
        }

        logger.getLogger().info( "*** All nodes started" );
    }

    @Test
    public void testHa()
            throws InterruptedException
    {
        long nodeId = 0;
        Transaction tx = null;
        try
        {
            tx = databases.get( 1 ).beginTx();

            Node node = databases.get( 1 ).createNode();
            node.setProperty( "Hello", "World" );
            nodeId = node.getId();

            tx.success();
        }
        catch ( Throwable ex )
        {
            ex.printStackTrace();
            Assert.fail();
        }
        finally
        {
            tx.finish();
        }

        while ( true )
        {
            try
            {
                String value = databases.get( 0 ).getNodeById( nodeId ).getProperty( "Hello" ).toString();
                logger.getLogger().info( "Hello=" + value );
                Assert.assertEquals( "World", value );
                break;
            }
            catch ( Exception e )
            {
                Thread.sleep( 1000 );
            }
        }
    }

    @After
    public void tearDown()
    {
        for ( HighlyAvailableGraphDatabase database : databases )
        {
            database.shutdown();
        }
    }
}
