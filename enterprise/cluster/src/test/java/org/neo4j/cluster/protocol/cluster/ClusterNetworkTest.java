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
package org.neo4j.cluster.protocol.cluster;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.MultiPaxosServerFactory;
import org.neo4j.cluster.NetworkedServerFactory;
import org.neo4j.cluster.ProtocolServer;
import org.neo4j.cluster.StateMachines;
import org.neo4j.cluster.com.NetworkReceiver;
import org.neo4j.cluster.com.NetworkSender;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InMemoryAcceptorInstanceStore;
import org.neo4j.cluster.protocol.election.ServerIdElectionCredentialsProvider;
import org.neo4j.cluster.timeout.FixedTimeoutStrategy;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.LoggerRule;

import static org.junit.Assert.assertEquals;

@RunWith(value = Parameterized.class)
public class ClusterNetworkTest
{
    @Parameterized.Parameters
    public static Collection<Object[]> data()
    {
        return Arrays.asList( new Object[][]
                {
                        {
                                3, new ClusterTestScriptDSL().
                                join( 10L, 1, 2, 3 ).
                                join( 0L, 2, 1, 3 ).
                                join( 0L, 3, 1, 2 ).
                                leave( 10000L, 3 ).
                                leave( 100L, 2 ).
                                leave( 100L, 1 )
                        },
                        {
                                // 3 nodes join and then leaves
                                3, new ClusterTestScriptDSL().
                                join( 10L, 1 ).
                                join( 10L, 2 ).
                                join( 100L, 3 ).
                                leave( 100L, 3 ).
                                leave( 100L, 2 ).
                                leave( 100L, 1 )
                        },
                        {
                                // 7 nodes join and then leaves
                                3, new ClusterTestScriptDSL().
                                join( 100L, 1 ).
                                join( 100L, 2 ).
                                join( 100L, 3 ).
                                join( 100L, 4 ).
                                join( 100L, 5 ).
                                join( 100L, 6 ).
                                join( 100L, 7 ).
                                leave( 100L, 7 ).
                                leave( 100L, 6 ).
                                leave( 100L, 5 ).
                                leave( 100L, 4 ).
                                leave( 100L, 3 ).
                                leave( 100L, 2 ).
                                leave( 100L, 1 )
                        },
                        {
                                // 1 node join, then 3 nodes try to join at roughly the same time
                                4, new ClusterTestScriptDSL().
                                join( 100L, 1 ).
                                join( 100L, 2 ).
                                join( 10L, 3 ).
                                leave( 500L, 3 ).
                                leave( 100L, 2 ).
                                leave( 100L, 1 )
                        },
                        {
                                // 2 nodes join, and then one leaves as the third joins
                                3, new ClusterTestScriptDSL().
                                join( 100L, 1 ).
                                join( 100L, 2 ).
                                leave( 90L, 2 ).
                                join( 20L, 3 )
                        },
                        {
                                3, new ClusterTestScriptRandom( 1337830212532839000L )
                        }
                } );
    }

    private static List<Cluster> servers = new ArrayList<>();
    private static List<Cluster> out = new ArrayList<>();
    private static List<Cluster> in = new ArrayList<>();

    @ClassRule
    public static LoggerRule logger = new LoggerRule( Level.OFF );

    private List<AtomicReference<ClusterConfiguration>> configurations = new ArrayList<AtomicReference<ClusterConfiguration>>();

    private ClusterTestScript script;

    private Timer timer = new Timer();

    private LifeSupport life = new LifeSupport();

    private static ExecutorService executor;

    public ClusterNetworkTest( int nrOfServers, ClusterTestScript script )
            throws URISyntaxException
    {
        this.script = script;

        out.clear();
        in.clear();

        for ( int i = 0; i < nrOfServers; i++ )
        {
            final URI uri = new URI( "neo4j://localhost:800" + (i + 1) );

            Monitors monitors = new Monitors();
            NetworkedServerFactory factory = new NetworkedServerFactory( life,
                    new MultiPaxosServerFactory( new ClusterConfiguration( "default", NullLogProvider.getInstance() ),
                            NullLogProvider.getInstance(), monitors.newMonitor( StateMachines.Monitor.class )
                    ),
                    new FixedTimeoutStrategy( 1000 ), NullLogProvider.getInstance(), new ObjectStreamFactory(), new ObjectStreamFactory(),
                    monitors.newMonitor( NetworkReceiver.Monitor.class ), monitors.newMonitor( NetworkSender.Monitor.class ), monitors.newMonitor( NamedThreadFactory.Monitor.class )
            );

            ServerIdElectionCredentialsProvider electionCredentialsProvider = new ServerIdElectionCredentialsProvider();
            ProtocolServer server = factory.newNetworkedServer(
                    new Config( MapUtil.stringMap( ClusterSettings.cluster_server.name(),
                            uri.getHost() + ":" + uri.getPort(),
                            ClusterSettings.server_id.name(), "" + i ) ),
                    new InMemoryAcceptorInstanceStore(),
                    electionCredentialsProvider
            );
            server.addBindingListener( electionCredentialsProvider );
            final Cluster cluster2 = server.newClient( Cluster.class );
            final AtomicReference<ClusterConfiguration> config2 = clusterStateListener( uri, cluster2 );

            servers.add( cluster2 );
            out.add( cluster2 );
            configurations.add( config2 );
        }

        life.start();
    }

    @Before
    public void setup()
    {
        executor = Executors.newSingleThreadExecutor( new NamedThreadFactory( "Threaded actions" ) );
    }

    @After
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testCluster()
            throws ExecutionException, InterruptedException, URISyntaxException, TimeoutException
    {
        final long start = System.currentTimeMillis();
        timer.scheduleAtFixedRate( new TimerTask()
        {
            int i = 0;

            @Override
            public void run()
            {
                long now = System.currentTimeMillis() - start;
                logger.getLogger().fine( "Round " + i + ", time:" + now );

                script.tick( now );

                if ( ++i == 1000 )
                {
                    timer.cancel();
                }
            }
        }, 0, 10 );

        // Let messages settle
        Thread.sleep( script.getLength() + 1000 );

        logger.getLogger().fine( "All nodes leave" );

        // All leave
        for ( Cluster cluster : new ArrayList<>( in ) )
        {
            logger.getLogger().fine( "Leaving:" + cluster );
            cluster.leave();
            Thread.sleep( 100 );
        }
    }

    @After
    public void shutdown()
    {
        life.shutdown();
    }

    private AtomicReference<ClusterConfiguration> clusterStateListener( final URI uri, final Cluster cluster )
    {
        final AtomicReference<ClusterConfiguration> config = new AtomicReference<>();
        cluster.addClusterListener( new ClusterListener()
        {
            @Override
            public void enteredCluster( ClusterConfiguration clusterConfiguration )
            {
                logger.getLogger().fine( uri + " entered cluster:" + clusterConfiguration.getMemberURIs() );
                config.set( new ClusterConfiguration( clusterConfiguration ) );
                in.add( cluster );
            }

            @Override
            public void joinedCluster( InstanceId instanceId, URI member )
            {
                logger.getLogger().fine( uri + " sees a join from " + instanceId + " at URI " + member.toString() );
                config.get().joined( instanceId, member );
            }

            @Override
            public void leftCluster( InstanceId instanceId, URI member )
            {
                logger.getLogger().fine( uri + " sees a leave:" + instanceId );
                config.get().left( instanceId );
            }

            @Override
            public void leftCluster()
            {
                out.add( cluster );
                config.set( null );
            }

            @Override
            public void elected( String role, InstanceId instanceId, URI electedMember )
            {
                logger.getLogger().fine( uri + " sees an election:" + instanceId +
                        "was elected as " + role + " on URI " + electedMember );
            }

            @Override
            public void unelected( String role, InstanceId instanceId, URI electedMember )
            {
                logger.getLogger().fine( uri + " sees an unelection:" + instanceId +
                        "was removed from " + role + " on URI " + electedMember );
            }
        } );
        return config;
    }

    interface ClusterTestScript
    {
        void tick( long time );

        long getLength();
    }

    private static class ClusterTestScriptDSL implements ClusterTestScript
    {
        abstract static class ClusterAction
                implements Runnable
        {
            public long time;
        }

        private Queue<ClusterAction> actions = new LinkedList<>();

        private long now = 0;

        public ClusterTestScriptDSL join( long time, final int joinServer, final int... joinServers )
        {
            ClusterAction joinAction = new ClusterAction()
            {
                @Override
                public void run()
                {
                    Cluster joinCluster = servers.get( joinServer - 1 );
                    for ( final Cluster cluster : out )
                    {
                        if ( cluster.equals( joinCluster ) )
                        {
                            out.remove( cluster );
                            logger.getLogger().fine( "Join:" + cluster.toString() );
                            if ( joinServers.length == 0 )
                            {
                                if ( in.isEmpty() )
                                {
                                    cluster.create( "default" );
                                }
                                else
                                {
                                    // Use test info to figure out who to join
                                    final Future<ClusterConfiguration> result = cluster.join( "default",
                                            URI.create( in.get( 0 ).toString() ) );
                                    executor.submit( new Runnable()
                                    {
                                        @Override
                                        public void run()
                                        {
                                            try
                                            {
                                                ClusterConfiguration clusterConfiguration = result.get();
                                                logger.getLogger().fine( "**** Cluster configuration:" +
                                                        clusterConfiguration );
                                            }
                                            catch ( Exception e )
                                            {
                                                logger.getLogger().fine( "**** Node " + joinServer + " could not " +
                                                        "join cluster:" + e
                                                        .getMessage() );
                                                out.add( cluster );
                                            }
                                        }
                                    } );
                                }
                            }
                            else
                            {
                                // List of servers to join was explicitly specified, so use that
                                URI[] instanceUris = new URI[joinServers.length];
                                for ( int i = 0; i < joinServers.length; i++ )
                                {
                                    int server = joinServers[i];
                                    instanceUris[i] = URI.create( servers.get( server - 1 ).toString() );
                                }

                                final Future<ClusterConfiguration> result = cluster.join( "default", instanceUris );
                                executor.submit( new Runnable()
                                {
                                    @Override
                                    public void run()
                                    {
                                        try
                                        {
                                            ClusterConfiguration clusterConfiguration = result.get();
                                            logger.getLogger().fine( "**** Cluster configuration:" +
                                                    clusterConfiguration );
                                        }
                                        catch ( Exception e )
                                        {
                                            if ( !(e.getCause() instanceof IllegalStateException) )
                                            {
                                                cluster.create( "default" );
                                            }
                                            else
                                            {
                                                logger.getLogger().fine( "*** Incorrectly configured cluster? "
                                                        + e.getCause().getMessage() );
                                            }
                                        }
                                    }
                                } );
                            }
                            /*
                            if ( in.isEmpty() )
                            {
                                cluster.create( "default" );
                            }
                            else
                            {
                                try
                                {
                                    cluster.join( "default", new URI( in.get( 0 ).toString() ) );
                                }
                                catch ( URISyntaxException e )
                                {
                                    e.printStackTrace();
                                }
                            }*/
                            break;
                        }
                    }
                }
            };
            joinAction.time = now + time;
            actions.offer( joinAction );
            now += time;
            return this;
        }

        public ClusterTestScriptDSL leave( long time, final int leaveServer )
        {
            ClusterAction leaveAction = new ClusterAction()
            {
                @Override
                public void run()
                {
                    Cluster leaveCluster = servers.get( leaveServer - 1 );
                    for ( Cluster cluster : in )
                    {
                        if ( cluster.equals( leaveCluster ) )
                        {
                            in.remove( cluster );
                            cluster.leave();
                            logger.getLogger().fine( "Leave:" + cluster.toString() );
                            break;
                        }
                    }
                }
            };
            leaveAction.time = now + time;
            actions.offer( leaveAction );
            now += time;
            return this;
        }

        @Override
        public void tick( long time )
        {
//            logger.getLogger().debug( actions.size()+" actions remaining" );
            while ( !actions.isEmpty() && actions.peek().time <= time )
            {
                actions.poll().run();
            }
        }

        @Override
        public long getLength()
        {
            return now;
        }
    }

    public static class ClusterTestScriptRandom
            implements ClusterTestScript
    {
        private final long seed;
        private final Random random;

        public ClusterTestScriptRandom( long seed )
        {
            if ( seed == -1 )
            {
                seed = System.nanoTime();
            }
            this.seed = seed;
            random = new Random( seed );
        }

        @Override
        public void tick( long time )
        {
            if ( time == 0 )
            {
                logger.getLogger().fine( "Random seed:" + seed );
            }

            if ( random.nextDouble() >= 0.9 )
            {
                if ( random.nextDouble() > 0.5 && !out.isEmpty() )
                {
                    int idx = random.nextInt( out.size() );
                    Cluster cluster = out.remove( idx );

                    if ( in.isEmpty() )
                    {
                        cluster.create( "default" );
                    }
                    else
                    {
                        try
                        {
                            cluster.join( "default", new URI( in.get( 0 ).toString() ) );
                        }
                        catch ( URISyntaxException e )
                        {
                            e.printStackTrace();
                        }
                    }
                    logger.getLogger().fine( "Enter cluster:" + cluster.toString() );

                }
                else if ( !in.isEmpty() )
                {
                    int idx = random.nextInt( in.size() );
                    Cluster cluster = in.remove( idx );
                    cluster.leave();
                    logger.getLogger().fine( "Leave cluster:" + cluster.toString() );
                }
            }
        }

        @Override
        public long getLength()
        {
            return 5000;
        }
    }
}
