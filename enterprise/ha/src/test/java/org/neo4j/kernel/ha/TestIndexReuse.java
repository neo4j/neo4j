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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.TargetDirectory.forTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;

public class TestIndexReuse
{
    @Test
    public void testIndexCanBeReusedAfterInternalRestart() throws Exception
    {
        String indexName = "indexTEST", key = "testkey", value = "datest";
        IndexManager indexManager = slave.index();
        Index<Node> nodeIndex = indexManager.forNodes( indexName );
        Node node = createIndexedNode( slave, nodeIndex, key, value );
        
        switchMaster();

        // Use the index manager again
        assertTrue( indexManager.existsForNodes( indexName ) );
        assertNotNull( indexManager.forNodes( "new index" ) );
        
        // Use the index again
        assertEquals( node, nodeIndex.get( key, value ).next() );
    }

    private final TargetDirectory dir = forTest( getClass() );
    private LocalhostZooKeeperCluster zoo;
    private HighlyAvailableGraphDatabase master, slave;
    
    @Before
    public void before() throws Exception
    {
        zoo = LocalhostZooKeeperCluster.singleton().clearDataAndVerifyConnection();

        master = new HighlyAvailableGraphDatabase( dir.directory( "master", true ).getAbsolutePath(), stringMap(
                HaSettings.server_id.name(), "0", HaSettings.server.name(), "localhost:" + 6666,
                HaSettings.coordinators.name(), zoo.getConnectionString(), HaSettings.pull_interval.name(),
                0 + "ms", HaSettings.tx_push_strategy.name(), HaSettings.TxPushStrategySetting.fixed , HaSettings.tx_push_factor.name(), "1") );
        
        createNode( master );
        Thread.sleep( 5000 );
        
        slave = new HighlyAvailableGraphDatabase( dir.directory( "slave", true ).getAbsolutePath(), stringMap(
                HaSettings.server_id.name(), "1", HaSettings.server.name(), "localhost:" + 6667,
                HaSettings.coordinators.name(), zoo.getConnectionString(), HaSettings.pull_interval.name(),
                0 + "ms", HaSettings.tx_push_strategy.name(), HaSettings.TxPushStrategySetting.fixed , HaSettings.tx_push_factor.name(), "1") );
    }

    @After
    public void after() throws Exception
    {
        if ( slave != null )
            slave.shutdown();
        if ( master != null )
            master.shutdown();
    }
    
    private Node createIndexedNode( HighlyAvailableGraphDatabase db, Index<Node> nodeIndex, String key, Object value )
    {
        Transaction tx = db.beginTx();
        Node node = null;
        try
        {
            node = db.createNode();
            nodeIndex.add( node, key, value );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        return node;
    }

    private void switchMaster()
            throws InterruptedException
    {
        // kill master so we get a role switch (internal restart) on slave 
        master.shutdown();
        master = null;
        int count = 0;
        boolean success = false;
        do
        {
            try
            {
                createNode( slave );
                success = true;
            }
            catch ( Throwable t )
            {
                t.printStackTrace();
                Thread.sleep( 1000 );
            }
        } while ( !success && ++count < 10 );
        assertTrue( count < 10 );
    }

    private void createNode( HighlyAvailableGraphDatabase db )
    {
        Transaction masterTx = db.beginTx();
        try
        {
            db.createNode();
            masterTx.success();
        }
        finally
        {
            masterTx.finish();
        }
    }
}
