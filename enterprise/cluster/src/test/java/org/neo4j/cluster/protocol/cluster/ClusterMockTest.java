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

package org.neo4j.cluster.protocol.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.neo4j.cluster.ConnectedStateMachines;
import org.neo4j.cluster.FixedNetworkLatencyStrategy;
import org.neo4j.cluster.MultipleFailureLatencyStrategy;
import org.neo4j.cluster.NetworkMock;
import org.neo4j.cluster.ScriptableNetworkFailureLatencyStrategy;
import org.neo4j.cluster.TestProtocolServer;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcast;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastListener;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;
import org.neo4j.cluster.protocol.atomicbroadcast.Payload;
import org.neo4j.cluster.protocol.heartbeat.Heartbeat;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatMessage;
import org.neo4j.cluster.statemachine.State;
import org.neo4j.cluster.timeout.FixedTimeoutStrategy;
import org.neo4j.cluster.timeout.MessageTimeoutStrategy;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.test.LoggerRule;

/**
 * Base class for cluster tests
 */
public class ClusterMockTest
{

    public static NetworkMock DEFAULT_NETWORK()
    {
        return new NetworkMock( 10,
                new MultipleFailureLatencyStrategy( new FixedNetworkLatencyStrategy( 10 ),
                        new ScriptableNetworkFailureLatencyStrategy() ),
                new MessageTimeoutStrategy( new FixedTimeoutStrategy( 500 ) )
                        .timeout( HeartbeatMessage.sendHeartbeat, 200 ) );
    }

    List<TestProtocolServer> servers = new ArrayList<TestProtocolServer>();
    List<Cluster> out = new ArrayList<Cluster>();
    List<Cluster> in = new ArrayList<Cluster>();

    @Rule
    public LoggerRule logger = new LoggerRule();

    public NetworkMock network;

    ClusterTestScript script;

    ExecutorService executor;

    @Before
    public void setup()
    {
        executor = Executors.newSingleThreadExecutor( new NamedThreadFactory( "Configuration output" ) );
    }

    @After
    public void tearDown()
    {
        executor.shutdownNow();
    }

    protected void testCluster( int nrOfServers, NetworkMock mock, ClusterTestScript script )
            throws ExecutionException, InterruptedException, URISyntaxException, TimeoutException
    {
        this.script = script;

        network = mock;
        servers.clear();
        out.clear();
        in.clear();

        for ( int i = 0; i < nrOfServers; i++ )
        {
            final URI uri = new URI( "server" + (i + 1) );
            TestProtocolServer server = network.addServer( uri );
            final Cluster cluster = server.newClient( Cluster.class );
            clusterStateListener( uri, cluster );

            server.newClient( Heartbeat.class ).addHeartbeatListener( new HeartbeatListener()
            {
                @Override
                public void failed( URI server )
                {
                    logger.getLogger().warn( uri + ": Failed:" + server );
                }

                @Override
                public void alive( URI server )
                {
                    logger.getLogger().debug( uri + ": Alive:" + server );
                }
            } );
            server.newClient( AtomicBroadcast.class ).addAtomicBroadcastListener( new AtomicBroadcastListener()
            {
                AtomicBroadcastSerializer serializer = new AtomicBroadcastSerializer();

                @Override
                public void receive( Payload value )
                {
                    try
                    {
                        logger.getLogger().debug( uri + " received: " + serializer.receive( value ) );
                    }
                    catch ( IOException e )
                    {
                        e.printStackTrace();
                    }
                    catch ( ClassNotFoundException e )
                    {
                        e.printStackTrace();
                    }
                }
            } );

            servers.add( server );
            out.add( cluster );
        }

        // Run test
        for ( int i = 0; i < script.rounds(); i++ )
        {
            logger.getLogger().debug( "Round " + i + ", time:" + network.getTime() );

            script.tick( network.getTime() );

            network.tick();
        }

        // Let messages settle
        network.tick( 100 );
        verifyConfigurations();

        logger.getLogger().debug( "All nodes leave" );

        // All leave
        for ( Cluster cluster : new ArrayList<Cluster>( in ) )
        {
            logger.getLogger().debug( "Leaving:" + cluster );
            cluster.leave();
            in.remove( cluster );
            network.tick( 400 );
        }

        verifyConfigurations();
    }

    private void clusterStateListener( final URI uri, final Cluster cluster )
    {
        cluster.addClusterListener( new ClusterListener()
        {
            @Override
            public void enteredCluster( ClusterConfiguration clusterConfiguration )
            {
                logger.getLogger().debug( uri + " entered cluster:" + clusterConfiguration.getMembers() );
                in.add( cluster );
            }

            @Override
            public void joinedCluster( URI member )
            {
                logger.getLogger().debug( uri + " sees a join:" + member.toString() );
            }

            @Override
            public void leftCluster( URI member )
            {
                logger.getLogger().debug( uri + " sees a leave:" + member.toString() );
            }

            @Override
            public void leftCluster()
            {
                logger.getLogger().debug( uri + " left cluster" );
                out.add( cluster );
            }

            @Override
            public void elected( String role, URI electedMember )
            {
                logger.getLogger().debug( uri + " sees an election: " + electedMember + " elected as " + role );
            }
        } );
    }

    public void verifyConfigurations()
    {
        logger.getLogger().debug( "Verify configurations" );
        List<URI> members = null;
        Map<String, URI> roles = null;
        List<URI> failed = null;
        int foundConfiguration = 0;
        List<TestProtocolServer> protocolServers = network.getServers();
        List<AssertionError> errors = new ArrayList<AssertionError>();
        for ( int j = 0; j < protocolServers.size(); j++ )
        {
            ConnectedStateMachines connectedStateMachines = protocolServers.get( j )
                    .getServer()
                    .getConnectedStateMachines();

            State<?, ?> clusterState = connectedStateMachines.getStateMachine( ClusterMessage.class ).getState();
            if ( !clusterState.equals( ClusterState.entered ) )
            {
                logger.getLogger().warn( "Instance " + (j + 1) + " is not in the cluster (" + clusterState + ")" );
                continue;
            }

            ClusterContext context = (ClusterContext) connectedStateMachines.getStateMachine( ClusterMessage.class )
                    .getContext();
            HeartbeatContext heartbeatContext = (HeartbeatContext) connectedStateMachines.getStateMachine(
                    HeartbeatMessage.class ).getContext();
            ClusterConfiguration clusterConfiguration = context.getConfiguration();
            if ( !clusterConfiguration.getMembers().isEmpty() )
            {
                logger.getLogger().debug( "   Server " + (j + 1) + ": Cluster:" + clusterConfiguration.getMembers() +
                        "," +
                        " Roles:" + clusterConfiguration.getRoles() + ", Failed:" + heartbeatContext.getFailed() );
                foundConfiguration++;
                if ( members == null )
                {
                    members = clusterConfiguration.getMembers();
                    roles = clusterConfiguration.getRoles();
                    failed = heartbeatContext.getFailed();
                }
                else
                {
                    try
                    {
                        assertEquals( "Config for server" + (j + 1) + " is wrong", new HashSet<URI>( members ),
                                new HashSet<URI>( clusterConfiguration
                                        .getMembers() ) );
                    }
                    catch ( AssertionError e )
                    {
                        errors.add( e );
                    }
                    try
                    {
                        assertEquals( "Roles for server" + (j + 1) + " is wrong", roles, clusterConfiguration
                                .getRoles() );
                    }
                    catch ( AssertionError e )
                    {
                        errors.add( e );
                    }
                    try
                    {
                        assertEquals( "Failed for server" + (j + 1) + " is wrong", failed, heartbeatContext.getFailed
                                () );
                    }
                    catch ( AssertionError e )
                    {
                        errors.add( e );
                    }
                }
            }
        }

        if ( !errors.isEmpty() )
        {
            for ( AssertionError error : errors )
            {
                logger.getLogger().error( error.toString() );
            }
            throw errors.get( 0 );
        }

        if ( foundConfiguration > 0 )
        {
            assertEquals( "Nr of found active members does not match configuration size", members.size(),
                    foundConfiguration );
        }

        assertEquals( "In:" + in + ", Out:" + out, protocolServers.size(), Iterables.count( Iterables.<Cluster,
                List<Cluster>>flatten( in, out ) ) );
    }

    public interface ClusterTestScript
    {
        int rounds();

        void tick( long time );
    }

    public class ClusterTestScriptDSL
            implements ClusterTestScript
    {
        public abstract class ClusterAction
                implements Runnable
        {
            public long time;
        }

        private Queue<ClusterAction> actions = new LinkedList<ClusterAction>();
        private AtomicBroadcastSerializer serializer = new AtomicBroadcastSerializer();

        private int rounds = 100;
        private long now = 0;

        public ClusterTestScriptDSL rounds( int n )
        {
            rounds = n;
            return this;
        }

        public ClusterTestScriptDSL join( int time, final int joinServer )
        {
            return addAction( new ClusterAction()
            {
                @Override
                public void run()
                {
                    Cluster joinCluster = servers.get( joinServer - 1 ).newClient( Cluster.class );
                    for ( final Cluster cluster : out )
                    {
                        if ( cluster.equals( joinCluster ) )
                        {
                            out.remove( cluster );
                            logger.getLogger().debug( "Join:" + cluster.toString() );
                            if ( in.isEmpty() )
                            {
                                cluster.create( "default" );
                            }
                            else
                            {
                                try
                                {
                                    final Future<ClusterConfiguration> result = cluster.join( new URI( in.get( 0 )
                                            .toString() ) );
                                    executor.submit( new Runnable()
                                    {
                                        @Override
                                        public void run()
                                        {
                                            try
                                            {
                                                ClusterConfiguration clusterConfiguration = result.get();
                                                logger.getLogger().debug( "**** Cluster configuration:" +
                                                        clusterConfiguration );
                                            }
                                            catch ( Exception e )
                                            {
                                                logger.getLogger().debug( "**** Node could not join cluster:" + e
                                                        .getMessage() );
                                                out.add( cluster );
                                            }
                                        }
                                    } );
                                }
                                catch ( URISyntaxException e )
                                {
                                    e.printStackTrace();
                                }

                            }
                            break;
                        }
                    }
                }
            }, time );
        }

        public ClusterTestScriptDSL leave( long time, final int leaveServer )
        {
            return addAction( new ClusterAction()
            {
                @Override
                public void run()
                {
                    Cluster leaveCluster = servers.get( leaveServer - 1 ).newClient( Cluster.class );
                    for ( Cluster cluster : in )
                    {
                        if ( cluster.equals( leaveCluster ) )
                        {
                            in.remove( cluster );
                            cluster.leave();
                            logger.getLogger().debug( "Leave:" + cluster.toString() );
                            break;
                        }
                    }
                }
            }, time );
        }

        public ClusterTestScriptDSL down( int time, final int serverDown )
        {
            return addAction( new ClusterAction()
            {
                @Override
                public void run()
                {
                    Cluster server = servers.get( serverDown - 1 ).newClient( Cluster.class );
                    network.getNetworkLatencyStrategy().getStrategy( ScriptableNetworkFailureLatencyStrategy.class )
                            .nodeIsDown( server.toString() );
                    logger.getLogger().debug( server + " is down" );
                }
            }, time );
        }

        public ClusterTestScriptDSL up( int time, final int serverUp )
        {
            return addAction( new ClusterAction()
            {
                @Override
                public void run()
                {
                    Cluster server = servers.get( serverUp - 1 ).newClient( Cluster.class );
                    network.getNetworkLatencyStrategy().getStrategy( ScriptableNetworkFailureLatencyStrategy.class )
                            .nodeIsUp( server
                                    .toString() );
                    logger.getLogger().debug( server + " is up" );
                }
            }, time );
        }

        public ClusterTestScriptDSL broadcast( int time, final int server, final Object value )
        {
            return addAction( new ClusterAction()
            {
                @Override
                public void run()
                {
                    AtomicBroadcast broadcast = servers.get( server - 1 ).newClient( AtomicBroadcast.class );
                    try
                    {
                        broadcast.broadcast( serializer.broadcast( value ) );
                    }
                    catch ( IOException e )
                    {
                        e.printStackTrace();
                    }
                }
            }, time );
        }

        public ClusterTestScriptDSL sleep( final int sleepTime )
        {
            return addAction( new ClusterAction()
            {
                @Override
                public void run()
                {
                    logger.getLogger().debug( "Slept for " + sleepTime );
                }
            }, sleepTime );
        }

        public ClusterTestScriptDSL message( int time, final String msg )
        {
            return addAction( new ClusterAction()
            {
                @Override
                public void run()
                {
                    logger.getLogger().debug( msg );
                }
            }, time );
        }

        public ClusterTestScriptDSL verifyConfigurations( long time )
        {
            return addAction( new ClusterAction()
            {
                @Override
                public void run()
                {
                    ClusterMockTest.this.verifyConfigurations();
                }
            }, time );
        }

        private ClusterTestScriptDSL addAction( ClusterAction action, long time )
        {
            action.time = now + time;
            actions.offer( action );
            now += time;
            return this;
        }

        @Override
        public int rounds()
        {
            return rounds;
        }

        @Override
        public void tick( long time )
        {
            while ( !actions.isEmpty() && actions.peek().time == time )
            {
                actions.poll().run();
            }
        }

        public ClusterTestScriptDSL getRoles( int time, final Map<String, URI> roles )
        {
            return addAction( new ClusterAction()
            {
                @Override
                public void run()
                {
                    ClusterMockTest.this.getRoles( roles );
                }
            }, 0 );
        }

        public ClusterTestScriptDSL verifyCoordinatorRoleSwitched( final Map<String, URI> comparedTo )
        {
            return addAction( new ClusterAction()
            {
                @Override
                public void run()
                {
                    HashMap<String, URI> roles = new HashMap<String, URI>();
                    ClusterMockTest.this.getRoles( roles );
                    URI oldCoordinator = comparedTo.get( ClusterConfiguration.COORDINATOR );
                    URI newCoordinator = roles.get( ClusterConfiguration.COORDINATOR );
                    assertNotNull( "Should have had a coordinator before bringing it down", oldCoordinator );
                    assertNotNull( "Should have a new coordinator after the previous failed", newCoordinator );
                    assertTrue( "Should have elected a new coordinator", !oldCoordinator.equals( newCoordinator ) );
                }
            }, 0 );
        }
    }

    public class ClusterTestScriptRandom
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
        public int rounds()
        {
            return 300;
        }

        @Override
        public void tick( long time )
        {
            if ( time >= (rounds() - 100) * 10 )
            {
                return;
            }

            if ( time == 0 )
            {
                logger.getLogger().debug( "Random seed:" + seed + "L" );
            }

            if ( random.nextDouble() >= 0.8 )
            {
                double inOrOut = (in.size() - out.size()) / ((double) servers.size());
                double whatToDo = random.nextDouble() + inOrOut;
                logger.getLogger().debug( "What to do:" + whatToDo );

                if ( whatToDo < 0.5 && !out.isEmpty() )
                {
                    int idx = random.nextInt( out.size() );
                    final Cluster cluster = out.remove( idx );

                    if ( in.isEmpty() )
                    {
                        cluster.create( "default" );
                    }
                    else
                    {
                        try
                        {
                            final Future<ClusterConfiguration> result = cluster.join( new URI( in.get( 0 )
                                    .toString() ) );
                            executor.submit( new Runnable()
                            {
                                @Override
                                public void run()
                                {
                                    try
                                    {
                                        ClusterConfiguration clusterConfiguration = result.get();
                                        logger.getLogger().debug( "**** Cluster configuration:" +
                                                clusterConfiguration );
                                    }
                                    catch ( Exception e )
                                    {
                                        logger.getLogger().debug( "**** Node could not join cluster:" + e
                                                .getMessage() );
                                        out.add( cluster );
                                    }
                                }
                            } );
                        }
                        catch ( URISyntaxException e )
                        {
                            e.printStackTrace();
                        }
                    }
                    logger.getLogger().debug( "Enter cluster:" + cluster.toString() );

                }
                else if ( !in.isEmpty() )
                {
                    int idx = random.nextInt( in.size() );
                    Cluster cluster = in.remove( idx );
                    cluster.leave();
                    logger.getLogger().debug( "Leave cluster:" + cluster.toString() );
                }
            }
        }
    }

    private void getRoles( Map<String, URI> roles )
    {
        List<TestProtocolServer> protocolServers = network.getServers();
        for ( int j = 0; j < protocolServers.size(); j++ )
        {
            ConnectedStateMachines connectedStateMachines = protocolServers.get( j )
                    .getServer()
                    .getConnectedStateMachines();

            State<?, ?> clusterState = connectedStateMachines.getStateMachine( ClusterMessage.class ).getState();
            if ( !clusterState.equals( ClusterState.entered ) )
            {
                logger.getLogger().warn( "Instance " + (j + 1) + " is not in the cluster (" + clusterState + ")" );
                continue;
            }

            ClusterContext context = (ClusterContext) connectedStateMachines.getStateMachine( ClusterMessage.class )
                    .getContext();
            ClusterConfiguration clusterConfiguration = context.getConfiguration();
            roles.putAll( clusterConfiguration.getRoles() );
        }
    }
}
