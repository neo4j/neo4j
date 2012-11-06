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

package org.neo4j.test.ha;

import static org.neo4j.test.ha.ClusterManager.fromXml;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.test.LoggerRule;

public class ClusterTest
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public LoggerRule logging = new LoggerRule();

    @Test
    public void testCluster() throws Throwable
    {
        ClusterManager clusterManager = new ClusterManager( fromXml( getClass().getResource( "/threeinstances.xml" ).toURI() ),
                folder.getRoot(), MapUtil.stringMap() );
        clusterManager.start();
        
        GraphDatabaseService master = clusterManager.getDefaultCluster().getMaster();
        logging.getLogger().info( "CREATE NODE" );
        Transaction tx = master.beginTx();
        master.createNode();
        logging.getLogger().info( "CREATED NODE" );
        tx.success();
        tx.finish();

        clusterManager.stop();
    }

    @Test
    public void given4instanceclusterWhenMasterGoesDownThenElectNewMaster() throws Throwable
    {
        ClusterManager clusterManager = new ClusterManager( fromXml( getClass().getResource( "/fourinstances.xml" ).toURI() ),
                folder.getRoot(), MapUtil.stringMap() );
        clusterManager.start();

        logging.getLogger().info( "STOPPING MASTER" );
        clusterManager.getDefaultCluster().getMaster().stop();
        logging.getLogger().info( "STOPPED MASTER" );

        Thread.sleep( 60000 );

/*
        GraphDatabaseService master = clusterManager.getMaster( "neo4j.ha" );
        logging.getLogger().info( "CREATE NODE" );
        Transaction tx = master.beginTx();
        master.createNode();
        logging.getLogger().info( "CREATED NODE" );
        tx.success();
        tx.finish();
*/

        logging.getLogger().info( "STOPPING CLUSTER" );
        clusterManager.stop();
    }
}
