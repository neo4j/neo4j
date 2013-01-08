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
package org.neo4j.ha;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.com.Protocol.DEFAULT_FRAME_LENGTH;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.test.TargetDirectory.forTest;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.EnterpriseGraphDatabaseFactory;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.zookeeper.NoMasterException;
import org.neo4j.kernel.ha.zookeeper.ZooClient;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperBroker;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperClusterClient;
import org.neo4j.kernel.impl.cache.Cache;
import org.neo4j.kernel.impl.cache.GCResistantCacheProvider;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;

public class TestConfig
{
    private LocalhostZooKeeperCluster zoo;
    private final TargetDirectory dir = forTest( getClass() );

    @Before
    public void doBefore() throws Exception
    {
        zoo = LocalhostZooKeeperCluster.singleton().clearDataAndVerifyConnection();
    }

    @Test
    public void testZkSessionTimeout() throws Exception
    {
        long timeout = 80000; // Default is 5000
        HighlyAvailableGraphDatabase db = (HighlyAvailableGraphDatabase) new EnterpriseGraphDatabaseFactory().
            newHighlyAvailableDatabaseBuilder( dir.directory( "zkTimeout", true ).getAbsolutePath() ).
            setConfig( HaSettings.server_id, "1" ).
            setConfig( HaSettings.coordinators, zoo.getConnectionString() ).
            setConfig( HaSettings.zk_session_timeout, ""+timeout ).
            newGraphDatabase();
            
        ZooKeeperBroker broker = (ZooKeeperBroker) db.getBroker();

        // Test ZooClient

        Field f = broker.getClass().getDeclaredField( "zooClient" );
        f.setAccessible( true ); // private
        ZooClient zooClient = (ZooClient) f.get( broker );
        Method m = zooClient.getClass().getSuperclass().getDeclaredMethod(
                "getSessionTimeout" );
        m.setAccessible( true ); // protected
        Integer timeoutParsed = (Integer) m.invoke( zooClient );
        Assert.assertEquals( timeout, timeoutParsed.intValue() );

        // Test ClusterClient, reuse f, m and timeoutParsed

        f = db.getClass().getDeclaredField( "clusterClient" );
        f.setAccessible( true ); // private
        ZooKeeperClusterClient clusterClient = (ZooKeeperClusterClient) f.get( db );
        m = clusterClient.getClass().getSuperclass().getDeclaredMethod(
                "getSessionTimeout" );
        m.setAccessible( true ); // protected
        timeoutParsed = (Integer) m.invoke( clusterClient );
        Assert.assertEquals( timeout, timeoutParsed.intValue() );

        // Done, Shutdown

        db.shutdown();
    }
    
    @Test
    public void gcrCachesGetsReusedBetweenInternalRestarts() throws Exception
    {
        List<HighlyAvailableGraphDatabase> dbs = new ArrayList<HighlyAvailableGraphDatabase>();
        for ( int i = 0; i < 3; i++ )
        {
            HighlyAvailableGraphDatabase db = (HighlyAvailableGraphDatabase) new EnterpriseGraphDatabaseFactory().
                    newHighlyAvailableDatabaseBuilder( dir.directory( "gcr" + i, true ).getAbsolutePath() ).
                    setConfig( HaSettings.server_id, "" + i ).
                    setConfig( HaSettings.coordinators, zoo.getConnectionString() ).
                    setConfig( GraphDatabaseSettings.cache_type, GCResistantCacheProvider.NAME ).
                    setConfig( HaSettings.server, "localhost:" + (6361+i) ).
                    newGraphDatabase();
            dbs.add( db );
        }
        
        doTransaction( dbs.get( 0 ) );
        int master = getMaster( dbs );
        Map<Integer, Cache<?>> cacheObjects = gatherCacheObjects( dbs );
        dbs.get( master ).shutdown();
        int otherThanMaster = (master+1)%dbs.size();
        pullUpdates( dbs.get( otherThanMaster ) );
        int newMaster = getMaster( dbs );
        assertFalse( master == newMaster );
        Cache<?> newCache = first( dbs.get( newMaster ).getNodeManager().caches() );
        assertEquals( "Expected the cache instance from when it was slave and now when it's master to be the same",
                cacheObjects.get( newMaster ), newCache );
        
        for ( int i = 0; i < dbs.size(); i++ )
            if ( i != master )
                dbs.get( i ).shutdown();
    }

    @Test
    public void shouldOperateWhenChunkSizeIsSmallerThanFrameLength() throws Exception
    {
        testSimpleCommunicationWithConfiguredChunkSize( "" + (DEFAULT_FRAME_LENGTH -1) );
    }

    @Test
    public void shouldOperateWhenChunkSizeIsEqualToFrameLength() throws Exception
    {
        testSimpleCommunicationWithConfiguredChunkSize( "" + DEFAULT_FRAME_LENGTH );
    }

    @Test
    public void shouldNotOperateWhenChunkSizeIsLargerThanFrameLength() throws Exception
    {
        try
        {
            new EnterpriseGraphDatabaseFactory()
                .newHighlyAvailableDatabaseBuilder( dir.directory( "chunk_size", true ).getAbsolutePath() )
                .setConfig( HaSettings.com_chunk_size, "" + (DEFAULT_FRAME_LENGTH+1) )
                .setConfig( HaSettings.coordinators, zoo.getConnectionString() )
                .setConfig( HaSettings.server_id, "1" )
                .newGraphDatabase();
            fail( "Shouldn't be able to operate with such a high chunk size" );
        }
        catch ( IllegalArgumentException e )
        {
            assertThat( e.getMessage(), is("Chunk size 16777217 needs to be equal or less than frame length 16777216") );
        }
    }

    private void testSimpleCommunicationWithConfiguredChunkSize( String chunkSize )
    {
        GraphDatabaseService master = new EnterpriseGraphDatabaseFactory()
            .newHighlyAvailableDatabaseBuilder( dir.directory( "chunk_size1", true ).getAbsolutePath() )
            .setConfig( HaSettings.com_chunk_size, chunkSize )
            .setConfig( HaSettings.coordinators, zoo.getConnectionString() )
            .setConfig( HaSettings.server, "localhost:6361" )
            .setConfig( HaSettings.server_id, "1" )
            .newGraphDatabase();
        GraphDatabaseService slave = new EnterpriseGraphDatabaseFactory()
            .newHighlyAvailableDatabaseBuilder( dir.directory( "chunk_size2", true ).getAbsolutePath() )
            .setConfig( HaSettings.com_chunk_size, chunkSize )
            .setConfig( HaSettings.coordinators, zoo.getConnectionString() )
            .setConfig( HaSettings.server, "localhost:6362" )
            .setConfig( HaSettings.server_id, "2" )
            .newGraphDatabase();
        
        pullUpdates( (HighlyAvailableGraphDatabase) slave );
        
        // Wow, this is poor. No verification on chunk size.
        
        slave.shutdown();
        master.shutdown();
    }

    private void pullUpdates( HighlyAvailableGraphDatabase db )
    {
        try
        {
            db.pullUpdates();
        }
        catch ( NoMasterException e )
        {
            db.pullUpdates();
        }
    }

    private Map<Integer, Cache<?>> gatherCacheObjects( List<HighlyAvailableGraphDatabase> dbs )
    {
        Map<Integer, Cache<?>> map = new HashMap<Integer, Cache<?>>();
        for ( int i = 0; i < dbs.size(); i++ )
            map.put( i, first( dbs.get( i ).getNodeManager().caches() ) );
        return map;
    }

    private int getMaster( List<HighlyAvailableGraphDatabase> dbs )
    {
        for ( int i = 0; i < dbs.size(); i++ )
        {
            HighlyAvailableGraphDatabase db = dbs.get( i );
            if ( db.isMaster() )
                return i;
        }
        throw new IllegalStateException( "No master found" );
    }

    private void doTransaction( HighlyAvailableGraphDatabase db )
    {
        Transaction tx = db.beginTx();
        try
        {
            db.createNode();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }
}
