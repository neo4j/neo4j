/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.logging.Logger;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.cluster.BindingListener;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.MultiPaxosServerFactory;
import org.neo4j.cluster.NetworkedServerFactory;
import org.neo4j.cluster.ProtocolServer;
import org.neo4j.cluster.StateMachines;
import org.neo4j.cluster.com.NetworkReceiver;
import org.neo4j.cluster.com.NetworkSender;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcast;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastMap;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectStreamFactory;
import org.neo4j.cluster.protocol.cluster.Cluster;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.cluster.ClusterMessage;
import org.neo4j.cluster.protocol.election.ServerIdElectionCredentialsProvider;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatMessage;
import org.neo4j.cluster.protocol.snapshot.Snapshot;
import org.neo4j.cluster.timeout.FixedTimeoutStrategy;
import org.neo4j.cluster.timeout.MessageTimeoutStrategy;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;

/**
 * TODO
 */
@Ignore
public class MultiPaxosNetworkTest
{
    @Test
    @Ignore
    public void testBroadcast()
            throws ExecutionException, InterruptedException, URISyntaxException, BrokenBarrierException
    {
        final LifeSupport life = new LifeSupport();

        MessageTimeoutStrategy timeoutStrategy = new MessageTimeoutStrategy( new FixedTimeoutStrategy( 10000 ) )
                .timeout( AtomicBroadcastMessage.broadcastTimeout, 30000 )
                .timeout( ClusterMessage.configurationTimeout, 3000 )
                .timeout( HeartbeatMessage.sendHeartbeat, 10000 )
                .relativeTimeout( HeartbeatMessage.timed_out, HeartbeatMessage.sendHeartbeat, 10000 );

        Monitors monitors = new Monitors();
        NetworkedServerFactory serverFactory = new NetworkedServerFactory( life,
                new MultiPaxosServerFactory( new ClusterConfiguration( "default", NullLogProvider.getInstance(),
                                "cluster://localhost:5001",
                                "cluster://localhost:5002",
                                "cluster://localhost:5003" ),
                        NullLogProvider.getInstance(),
                        monitors.newMonitor( StateMachines.Monitor.class )
                ),
                timeoutStrategy, NullLogProvider.getInstance(), new ObjectStreamFactory(), new ObjectStreamFactory(),
                monitors.newMonitor( NetworkReceiver.Monitor.class ), monitors.newMonitor( NetworkSender.Monitor.class ),
                monitors.newMonitor( NamedThreadFactory.Monitor.class )
        );

        ServerIdElectionCredentialsProvider serverIdElectionCredentialsProvider = new
                ServerIdElectionCredentialsProvider();
        final ProtocolServer server1 = serverFactory.newNetworkedServer( new Config( MapUtil.stringMap(
                        ClusterSettings.cluster_server.name(),
                        ":5001" ), ClusterSettings.class ),
                new InMemoryAcceptorInstanceStore(), serverIdElectionCredentialsProvider
        );
        server1.addBindingListener( serverIdElectionCredentialsProvider );

        serverIdElectionCredentialsProvider = new ServerIdElectionCredentialsProvider();
        final ProtocolServer server2 = serverFactory.newNetworkedServer( new Config( MapUtil.stringMap(
                        ClusterSettings.cluster_server.name(),
                        ":5002" ), ClusterSettings.class ), new InMemoryAcceptorInstanceStore(),
                serverIdElectionCredentialsProvider
        );
        server2.addBindingListener( serverIdElectionCredentialsProvider );

        serverIdElectionCredentialsProvider = new ServerIdElectionCredentialsProvider();
        final ProtocolServer server3 = serverFactory.newNetworkedServer( new Config( MapUtil.stringMap(
                        ClusterSettings.cluster_server.name(),
                        ":5003" ), ClusterSettings.class ), new InMemoryAcceptorInstanceStore(),
                serverIdElectionCredentialsProvider
        );
        server3.addBindingListener( serverIdElectionCredentialsProvider );

        server1.addBindingListener( new BindingListener()
        {
            @Override
            public void listeningAt( URI me )
            {
                server1.newClient( Cluster.class ).create( "default" );
            }
        } );

        server2.addBindingListener( new BindingListener()
        {
            @Override
            public void listeningAt( URI me )
            {
                server2.newClient( Cluster.class ).join( "default", me );
            }
        } );

        server3.addBindingListener( new BindingListener()
        {
            @Override
            public void listeningAt( URI me )
            {
                server3.newClient( Cluster.class ).join( "default", me );
            }
        } );

        AtomicBroadcast atomicBroadcast1 = server1.newClient( AtomicBroadcast.class );
        AtomicBroadcast atomicBroadcast2 = server2.newClient( AtomicBroadcast.class );
        AtomicBroadcast atomicBroadcast3 = server3.newClient( AtomicBroadcast.class );
        Snapshot snapshot1 = server1.newClient( Snapshot.class );
        Snapshot snapshot2 = server2.newClient( Snapshot.class );
        Snapshot snapshot3 = server3.newClient( Snapshot.class );

        final AtomicBroadcastMap<String, String> map = new AtomicBroadcastMap<String, String>( atomicBroadcast1,
                snapshot1 );
        final AtomicBroadcastMap<String, String> map2 = new AtomicBroadcastMap<String, String>( atomicBroadcast2,
                snapshot2 );
        final AtomicBroadcastMap<String, String> map3 = new AtomicBroadcastMap<String, String>( atomicBroadcast3,
                snapshot3 );

        final Semaphore semaphore = new Semaphore( -2 );

        final Logger logger = Logger.getLogger( getClass().getName() );
        server1.newClient( Cluster.class ).addClusterListener( new ClusterListener.Adapter()
        {
            @Override
            public void enteredCluster( ClusterConfiguration clusterConfiguration )
            {
                semaphore.release();
            }

            @Override
            public void joinedCluster( InstanceId instanceId, URI member )
            {
                logger.info( "1 sees join by " + instanceId + " at URI " + member );
            }
        } );

        server2.newClient( Cluster.class ).addClusterListener( new ClusterListener.Adapter()
        {
            @Override
            public void enteredCluster( ClusterConfiguration clusterConfiguration )
            {
                semaphore.release();
            }

            @Override
            public void joinedCluster( InstanceId instanceId, URI member )
            {
                logger.info( "2 sees join by " + instanceId + " at URI " + member );
            }
        } );

        server3.newClient( Cluster.class ).addClusterListener( new ClusterListener.Adapter()
        {
            @Override
            public void enteredCluster( ClusterConfiguration clusterConfiguration )
            {
                semaphore.release();
            }

            @Override
            public void joinedCluster( org.neo4j.cluster.InstanceId instanceId, URI member )
            {
                logger.info( "3 sees join by " + instanceId + " at URI " + member );
            }
        } );

        life.start();

        semaphore.acquire();

        logger.info( "Joined cluster - set data" );

        for ( int i = 0; i < 50; i++ )
        {
            map.put( "foo" + i, "bar" + i );
        }

        logger.info( "Set all values" );

        String value = map.get( "foo1" );

/*
        LoggerFactory.getLogger( getClass() ).info( "3 joins 1" );
        server3.newClient( Cluster.class ).addClusterListener( new ClusterListener.Adapter()
        {
            @Override
            public void enteredCluster( ClusterConfiguration clusterConfiguration )
            {
                LoggerFactory.getLogger( getClass() ).info( "3 entered cluster of:" + clusterConfiguration.getMembers
                () );
                semaphore.release();
            }
        } );
        server3.newClient( Cluster.class ).join( server1.getServerId() );
        semaphore.acquire();
*/

        logger.info( "Read value1" );
        Assert.assertThat( value, CoreMatchers.equalTo( "bar1" ) );

        map2.put( "foo2", "666" );

        logger.warning( "Read value2:" + map2.get( "foo1" ) );
        logger.warning( "Read value3:" + map2.get( "foo2" ) );

        logger.warning( "Read value4:" + map3.get( "foo1" ) );
        logger.warning( "Read value5:" + map3.get( "foo99" ) );
        Assert.assertThat( map3.get( "foo1" ), CoreMatchers.equalTo( "bar1" ) );
        Assert.assertThat( map3.get( "foo99" ), CoreMatchers.equalTo( "bar99" ) );

        map.close();
        map2.close();
        map3.close();

        life.stop();

    }
}
