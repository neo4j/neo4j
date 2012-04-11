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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.test.TargetDirectory.forTest;
import static org.neo4j.test.ha.LocalhostZooKeeperCluster.standardZoo;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.HAGraphDb;
import org.neo4j.kernel.HaConfig;
import org.neo4j.kernel.ha.zookeeper.NoMasterException;
import org.neo4j.kernel.ha.zookeeper.ZooClient;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperBroker;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperClusterClient;
import org.neo4j.kernel.impl.cache.Cache;
import org.neo4j.kernel.impl.core.NodeManager.CacheType;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;

public class TestConfig
{
    private LocalhostZooKeeperCluster zoo;
    private final TargetDirectory dir = forTest( getClass() );

    @Before
    public void doBefore() throws Exception
    {
        zoo = standardZoo( getClass() );
    }

    @After
    public void doAfter() throws Exception
    {
        zoo.shutdown();
    }

    @Test
    public void testZkSessionTimeout() throws Exception
    {
        long timeout = 80000; // Default is 5000
        HAGraphDb db = new HAGraphDb( dir.directory( "zkTimeout" ).getAbsolutePath(),
                MapUtil.stringMap( HaConfig.CONFIG_KEY_SERVER_ID, "1",
                        HaConfig.CONFIG_KEY_COORDINATORS,
                        zoo.getConnectionString(),
                        HaConfig.CONFIG_KEY_ZK_SESSION_TIMEOUT,
                        timeout + "ms" ) );
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
        List<HAGraphDb> dbs = new ArrayList<HAGraphDb>();
        for ( int i = 0; i < 3; i++ )
        {
            Map<String, String> config = MapUtil.stringMap( HaConfig.CONFIG_KEY_SERVER_ID, "" + i,
                    HaConfig.CONFIG_KEY_COORDINATORS, zoo.getConnectionString(), Config.CACHE_TYPE,
                    CacheType.gcr.name() );
            HAGraphDb db = new HAGraphDb( dir.directory( "gcr" + i, true ).getAbsolutePath(), config );
            dbs.add( db );
        }

        doTransaction( dbs.get( 0 ) );
        int master = getMaster( dbs );
        Map<Integer, Cache<?>> cacheObjects = gatherCacheObjects( dbs );
        dbs.get( master ).shutdown();
        int otherThanMaster = ( master + 1 ) % dbs.size();
        pullUpdates( dbs.get( otherThanMaster ) );
        int newMaster = getMaster( dbs );
        assertFalse( master == newMaster );
        Cache<?> newCache = first( dbs.get( newMaster ).getConfig().getGraphDbModule().getNodeManager().caches() );
        assertEquals( "Expected the cache instance from when it was slave and now when it's master to be the same",
                cacheObjects.get( newMaster ), newCache );

        for ( int i = 0; i < dbs.size(); i++ )
            if ( i != master ) dbs.get( i ).shutdown();
    }

    private void pullUpdates( HAGraphDb db )
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

    private Map<Integer, Cache<?>> gatherCacheObjects( List<HAGraphDb> dbs )
    {
        Map<Integer, Cache<?>> map = new HashMap<Integer, Cache<?>>();
        for ( int i = 0; i < dbs.size(); i++ )
            map.put( i, first( dbs.get( i ).getConfig().getGraphDbModule().getNodeManager().caches() ) );
        return map;
    }

    private int getMaster( List<HAGraphDb> dbs )
    {
        for ( int i = 0; i < dbs.size(); i++ )
        {
            HAGraphDb db = dbs.get( i );
            if ( db.isMaster() ) return i;
        }
        throw new IllegalStateException( "No master found" );
    }

    private void doTransaction( HAGraphDb db )
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
