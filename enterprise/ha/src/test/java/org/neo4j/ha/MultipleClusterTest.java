/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import java.io.File;

import junit.framework.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.test.LoggerRule;
import org.neo4j.test.ha.ClusterManager;

/**
 * Verify that we can run multiple clusters simultaneously
 */
public class MultipleClusterTest
{
    @Rule
    public LoggerRule logging = new LoggerRule();

    @Test
    public void runTwoClusters() throws Throwable
    {
        File root = new File( "target/cluster" );

        ClusterManager clusterManager = new ClusterManager( getClass().getResource( "/twoclustertest.xml" ).toURI(),
                root, MapUtil.stringMap() );
        clusterManager.start();

        long cluster1;
        {
            GraphDatabaseService master = clusterManager.getMaster( "neo4j.ha" );
            logging.getLogger().info( "CREATE NODE" );
            Transaction tx = master.beginTx();
            Node node = master.createNode();
            node.setProperty( "cluster", "neo4j.ha" );
            cluster1 = node.getId();
            logging.getLogger().info( "CREATED NODE" );
            tx.success();
            tx.finish();
        }

        long cluster2;
        {
            GraphDatabaseService master = clusterManager.getMaster( "neo4j.ha2" );
            logging.getLogger().info( "CREATE NODE" );
            Transaction tx = master.beginTx();
            Node node = master.createNode();
            node.setProperty( "cluster", "neo4j.ha2" );
            cluster2 = node.getId();
            logging.getLogger().info( "CREATED NODE" );
            tx.success();
            tx.finish();
        }

        // Verify properties in all cluster nodes
        for ( HighlyAvailableGraphDatabase highlyAvailableGraphDatabase : clusterManager.getCluster( "neo4j.ha" ) )
        {
            highlyAvailableGraphDatabase.getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();
            Assert.assertEquals( "neo4j.ha", highlyAvailableGraphDatabase.getNodeById( cluster1 ).getProperty(
                    "cluster" ) );
        }

        for ( HighlyAvailableGraphDatabase highlyAvailableGraphDatabase : clusterManager.getCluster( "neo4j.ha2" ) )
        {
            highlyAvailableGraphDatabase.getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();
            Assert.assertEquals( "neo4j.ha2", highlyAvailableGraphDatabase.getNodeById( cluster2 ).getProperty(
                    "cluster" ) );
        }

        clusterManager.stop();
    }

}
