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
package org.neo4j.kernel.ha.zookeeper;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.HaConfig;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.ClusterEventReceiver;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;

import static org.junit.Assert.*;

public class TestZooClient
{
    private static final ClusterEventReceiver DummyClusterReceiver = new ClusterEventReceiver()
    {
        @Override
        public void reconnect( Exception cause )
        {
            StringLogger.SYSTEM.logMessage( "reconnect called", cause );
        }

        @Override
        public void newMaster( Exception cause )
        {
            StringLogger.SYSTEM.logMessage( "newMaster called", cause );
        }
    };

    @Test
    public void testWaitsForZKQuorumToComeUp() throws Exception
    {
        final long millisForSessionToExpire = 1000;
        Map<String, String> stringConfig = new HashMap<String, String>();
        stringConfig.put( HaConfig.CONFIG_KEY_COORDINATORS, "127.0.0.1:2181" );
        stringConfig.put( HaConfig.CONFIG_KEY_SERVER_ID, "1" );
        stringConfig.put( HaConfig.CONFIG_KEY_ZK_SESSION_TIMEOUT, Long.toString( millisForSessionToExpire ) );
        Config config = new Config(StringLogger.DEV_NULL, new DefaultFileSystemAbstraction(), stringConfig, Iterables.toList( Iterables.iterable( GraphDatabaseSettings.class, HaSettings.class ) ));
        
        ZooClient client = new ZooClient( "", StringLogger.SYSTEM, null, config, null, DummyClusterReceiver );

        final AtomicBoolean stop = new AtomicBoolean( false );
        Thread launchesZK = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                LocalhostZooKeeperCluster cluster = null;
                try
                {
                    Thread.sleep( ( millisForSessionToExpire ) * 2 /*twice the session timeout*/);
                    cluster = new LocalhostZooKeeperCluster( getClass(), 2181 );
                    while ( !stop.get() )
                    {
                        Thread.sleep( 150 );
                    }
                }
                catch ( Throwable e )
                {
                    e.printStackTrace();
                }
                finally
                {
                    if ( cluster != null )
                    {
                        cluster.shutdown();
                    }
                }
            }
        } );
        launchesZK.setDaemon( true );

        launchesZK.start();

        client.waitForSyncConnected( AbstractZooKeeperManager.WaitMode.STARTUP );
        client.shutdown();
        stop.set( true );
        launchesZK.join();
    }

    @Test
    public void sessionWaitSyncConnectedTimesOut() throws Exception
    {
        final long secondsForSessionToExpire = 1;
        Map<String, String> stringConfig = new HashMap<String, String>();
        stringConfig.put( HaConfig.CONFIG_KEY_COORDINATORS, "localhost:2181" );
        stringConfig.put( HaConfig.CONFIG_KEY_SERVER_ID, "1" );
        stringConfig.put( HaConfig.CONFIG_KEY_ZK_SESSION_TIMEOUT, Long.toString( secondsForSessionToExpire ) );
        Config config = new Config(StringLogger.DEV_NULL, new DefaultFileSystemAbstraction(), stringConfig, Iterables.toList( Iterables.iterable( GraphDatabaseSettings.class, HaSettings.class ) ));

        ZooClient client = new ZooClient( "", StringLogger.SYSTEM, null, config, null, DummyClusterReceiver );

        final Thread me = Thread.currentThread();
        final AtomicBoolean allOk = new AtomicBoolean( false );
        Thread wakeMeUp = new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    /*
                     * This will either timeout or will be interrupted by the main thread. The first case
                     * sucks.
                     */
                    Thread.sleep( ( secondsForSessionToExpire * 1000 ) * 2 /*twice the session timeout*/);
                    if ( !allOk.get() )
                    {
                        me.interrupt();
                    }
                }
                catch ( Exception e )
                {
                    if ( !allOk.get() )
                    {
                        throw new RuntimeException( e );
                    }
                    Thread.interrupted();
                }
            }
        } );
        wakeMeUp.setDaemon( true );
        wakeMeUp.start();
        try
        {
            client.waitForSyncConnected();
            fail( "There is no zookeeper here, it should time out within a session timeout" );
        }
        catch ( ZooKeeperTimedOutException success )
        {
            // awesome, it worked
            allOk.set( true );
        }
        wakeMeUp.interrupt();
        wakeMeUp.join();
    }
}
