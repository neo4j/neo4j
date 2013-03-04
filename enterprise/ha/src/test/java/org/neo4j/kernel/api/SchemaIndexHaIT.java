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
package org.neo4j.kernel.api;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;
import static org.neo4j.cluster.ClusterSettings.default_timeout;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.ha.ClusterManager.clusterOfSize;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema.IndexState;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.ClusterManager;
import org.neo4j.test.ha.ClusterManager.ManagedCluster;

@Ignore( "Haven't been able to run this as HA just doesn't work a.t.m. (at least not on Windows)" )
public class SchemaIndexHaIT
{
    @Test
    public void creatingIndexOnMasterShouldHaveSlavesBuildItAsWell() throws Throwable
    {
        // GIVEN
        ManagedCluster cluster = startCluster( clusterOfSize( 3 ) );
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        Map<Object, Node> data = createSomeData( master );

        // WHEN
        IndexDefinition index = createIndex( master );

        // THEN
        awaitIndexOnline( index, cluster );
    }
    
    @Test
    public void creatingIndexOnSlaveShouldHaveOtherSlavesAndMasterBuiltItAsWell() throws Throwable
    {
        // GIVEN
        ManagedCluster cluster = startCluster( clusterOfSize( 3 ) );
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        Map<Object, Node> data = createSomeData( master );
        cluster.sync();
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();

        // WHEN
        IndexDefinition index = createIndex( slave );

        // THEN
        awaitIndexOnline( index, cluster );
    }
    
    // TODO schemaIndexesShouldBeIncludedWhenCopyingStoreFromMaster
    
    private final File storeDir = TargetDirectory.forTest( getClass() ).graphDbDir( true );
    private ClusterManager clusterManager;
    private final String key = "key";
    private final Label label = label( "label" );
    
    private ManagedCluster startCluster( ClusterManager.Provider provider ) throws Throwable
    {
        clusterManager = new ClusterManager( provider, storeDir, stringMap( default_timeout.name(), "1s" ) );
        clusterManager.start();
        return clusterManager.getDefaultCluster();
    }
    
    @After
    public void after() throws Throwable
    {
        if ( clusterManager != null )
            clusterManager.stop();
    }

    private Map<Object, Node> createSomeData( GraphDatabaseService db )
    {
        Map<Object, Node> result = new HashMap<Object, Node>();
        Transaction tx = db.beginTx();
        try
        {
            for ( int i = 0; i < 10; i++ )
            {
                Node node = db.createNode( label );
                Object propertyValue = i;
                node.setProperty( key, propertyValue );
                result.put( propertyValue, node );
            }
            tx.success();
            return result;
        }
        finally
        {
            tx.finish();
        }
    }

    private IndexDefinition createIndex( GraphDatabaseService db )
    {
        Transaction tx = db.beginTx();
        IndexDefinition index = db.schema().indexCreator( label ).on( key ).create();
        tx.success();
        tx.finish();
        return index;
    }

    private void awaitIndexOnline( IndexDefinition index, ManagedCluster cluster ) throws InterruptedException
    {
        for ( GraphDatabaseService db : cluster.getAllMembers() )
            awaitIndexOnline( index, db );
    }
    
    private void awaitIndexOnline( IndexDefinition index, GraphDatabaseService db ) throws InterruptedException
    {
        long timeout = System.currentTimeMillis() + SECONDS.toMillis( 60 );
        while( db.schema().getIndexState( index ) != IndexState.ONLINE )
        {
            Thread.sleep( 1 );
            if ( System.currentTimeMillis() > timeout )
            {
                fail( "Expected index to come online within a reasonable time." );
            }
        }
    }
}
