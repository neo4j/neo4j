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
package org.neo4j.ha;

import java.io.File;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.UpdatePullerClient;
import org.neo4j.test.LoggerRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.ClusterManager;
import org.neo4j.test.ha.ClusterManager.ManagedCluster;

import static org.junit.Assert.assertEquals;

import static org.neo4j.test.ha.ClusterManager.fromXml;

/**
 * Verify that we can run multiple clusters simultaneously
 */
@Ignore("Fails too often")
public class MultipleClusterTest
{
    @Rule
    public LoggerRule logging = new LoggerRule();

    @Test
    public void runTwoClusters() throws Throwable
    {
        File root = TargetDirectory.forTest( getClass() ).cleanDirectory( "cluster" );

        ClusterManager clusterManager = new ClusterManager(
                fromXml( getClass().getResource( "/twoclustertest.xml" ).toURI() ), root, MapUtil.stringMap() );

        try
        {
            clusterManager.start();
            ManagedCluster cluster1 = clusterManager.getCluster( "neo4j.ha" );

            long cluster1NodeId;
            {
                GraphDatabaseService master = cluster1.getMaster();
                logging.getLogger().info( "CREATE NODE" );
                Transaction tx = master.beginTx();
                Node node = master.createNode();
                node.setProperty( "cluster", "neo4j.ha" );
                cluster1NodeId = node.getId();
                logging.getLogger().info( "CREATED NODE" );
                tx.success();
                tx.finish();
            }

            ManagedCluster cluster2 = clusterManager.getCluster( "neo4j.ha2" );
            long cluster2NodeId;
            {
                GraphDatabaseService master = cluster2.getMaster();
                logging.getLogger().info( "CREATE NODE" );
                Transaction tx = master.beginTx();
                Node node = master.createNode();
                node.setProperty( "cluster", "neo4j.ha2" );
                cluster2NodeId = node.getId();
                logging.getLogger().info( "CREATED NODE" );
                tx.success();
                tx.finish();
            }

            // Verify properties in all cluster nodes
            for ( HighlyAvailableGraphDatabase highlyAvailableGraphDatabase : cluster1.getAllMembers() )
            {
                highlyAvailableGraphDatabase.getDependencyResolver().resolveDependency( UpdatePullerClient.class ).pullUpdates();

                Transaction transaction = highlyAvailableGraphDatabase.beginTx();
                assertEquals( "neo4j.ha", highlyAvailableGraphDatabase.getNodeById( cluster1NodeId ).getProperty(
                        "cluster" ) );
                transaction.finish();
            }

            for ( HighlyAvailableGraphDatabase highlyAvailableGraphDatabase : cluster2.getAllMembers() )
            {
                highlyAvailableGraphDatabase.getDependencyResolver().resolveDependency( UpdatePullerClient.class ).pullUpdates();

                Transaction transaction = highlyAvailableGraphDatabase.beginTx();
                assertEquals( "neo4j.ha2", highlyAvailableGraphDatabase.getNodeById( cluster2NodeId ).getProperty(
                        "cluster" ) );
                transaction.finish();
            }
        }
        finally
        {
            clusterManager.stop();
        }
    }

}
