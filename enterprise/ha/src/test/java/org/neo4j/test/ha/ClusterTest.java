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
package org.neo4j.test.ha;

import static org.neo4j.test.ha.ClusterManager.fromXml;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.test.LoggerRule;
import org.neo4j.test.TargetDirectory;

public class ClusterTest
{
    @Rule
    public LoggerRule logging = new LoggerRule();

    @Test
    public void testCluster() throws Throwable
    {
        ClusterManager clusterManager = new ClusterManager( fromXml( getClass().getResource( "/threeinstances.xml" ).toURI() ),
                TargetDirectory.forTest( getClass() ).directory( "testCluster", true ), MapUtil.stringMap());
        clusterManager.start();
        
        GraphDatabaseAPI master = clusterManager.getDefaultCluster().getMaster();
        Transaction tx = master.beginTx();
        master.createNode();
        tx.success();
        tx.finish();

        clusterManager.stop();
    }

    @Test
    public void testArbiterStartsFirstAndThenTwoInstancesJoin() throws Throwable
    {
        ClusterManager clusterManager = new ClusterManager( ClusterManager.clusterWithAdditionalArbiters( 2, 1 ),
                TargetDirectory.forTest( getClass() ).directory( "testCluster", true ), MapUtil.stringMap());
        clusterManager.start();

        GraphDatabaseAPI master = clusterManager.getDefaultCluster().getMaster();
        Transaction tx = master.beginTx();
        master.createNode();
        tx.success();
        tx.finish();

        clusterManager.stop();
    }

    @Test(expected = RuntimeException.class)
    public void testInstancesWithConflictingPorts() throws Throwable
    {
        ClusterManager clusterManager = null;
        try
        {
            clusterManager = new ClusterManager(
                    fromXml( getClass().getResource( "/threeinstancesconflictingports.xml" ).toURI() ),
                    TargetDirectory.forTest( getClass() ).directory( "testClusterConflictingPorts", true ),
                    MapUtil.stringMap() );
            clusterManager.start();
        }
        finally
        {
            clusterManager.stop();
        }
    }

    @Test
    public void given4instanceclusterWhenMasterGoesDownThenElectNewMaster() throws Throwable
    {
        ClusterManager clusterManager = new ClusterManager( fromXml( getClass().getResource( "/fourinstances.xml" ).toURI() ),
                TargetDirectory.forTest( getClass() ).directory( "4instances", true ), MapUtil.stringMap() );
        clusterManager.start();

        logging.getLogger().info( "STOPPING MASTER" );
        clusterManager.getDefaultCluster().getMaster().stop();
        logging.getLogger().info( "STOPPED MASTER" );

        Thread.sleep( 30000 ); // OMG!!!! My Eyes!!!! It Burns Us!!!!

        GraphDatabaseService master = clusterManager.getCluster( "neo4j.ha" ).getMaster();
        logging.getLogger().info( "CREATE NODE" );
        Transaction tx = master.beginTx();
        master.createNode();
        logging.getLogger().info( "CREATED NODE" );
        tx.success();
        tx.finish();

        logging.getLogger().info( "STOPPING CLUSTER" );
        clusterManager.stop();
    }
}
