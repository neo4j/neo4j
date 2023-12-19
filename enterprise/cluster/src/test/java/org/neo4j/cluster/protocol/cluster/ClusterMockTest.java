/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cluster.protocol.cluster;

import org.junit.After;
import org.junit.Rule;

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
import java.util.Set;
import java.util.concurrent.Future;
import java.util.logging.Level;

import org.neo4j.cluster.FixedNetworkLatencyStrategy;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.MultipleFailureLatencyStrategy;
import org.neo4j.cluster.NetworkMock;
import org.neo4j.cluster.ScriptableNetworkFailureLatencyStrategy;
import org.neo4j.cluster.StateMachines;
import org.neo4j.cluster.TestProtocolServer;
import org.neo4j.cluster.VerifyInstanceConfiguration;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcast;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastListener;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.Payload;
import org.neo4j.cluster.protocol.heartbeat.Heartbeat;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatMessage;
import org.neo4j.cluster.statemachine.State;
import org.neo4j.cluster.timeout.FixedTimeoutStrategy;
import org.neo4j.cluster.timeout.MessageTimeoutStrategy;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.rule.LoggerRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Base class for cluster tests
 */
public class ClusterMockTest
{

    public static NetworkMock DEFAULT_NETWORK()
    {
        return new NetworkMock( NullLogService.getInstance(), new Monitors(), 10,
                new MultipleFailureLatencyStrategy( new FixedNetworkLatencyStrategy( 10 ),
                        new ScriptableNetworkFailureLatencyStrategy() ),
                new MessageTimeoutStrategy( new FixedTimeoutStrategy( 500 ) )
                        .timeout( HeartbeatMessage.sendHeartbeat, 200 )
        );
    }

    List<TestProtocolServer> servers = new ArrayList<>();
    List<Cluster> out = new ArrayList<>();
    List<Cluster> in = new ArrayList<>();
    Map<Integer, URI> members = new HashMap<>();

    @Rule
    public LoggerRule logger = new LoggerRule( Level.OFF );

    public NetworkMock network;

    ClusterTestScript script;

    @After
    public void tearDown()
    {
        logger.getLogger().fine( "Current threads" );
        for ( Map.Entry<Thread, StackTraceElement[]> threadEntry : Thread.getAllStackTraces().entrySet() )
        {
            logger.getLogger().fine( threadEntry.getKey().getName() );
            for ( StackTraceElement stackTraceElement : threadEntry.getValue() )
            {
                logger.getLogger().fine( "   " + stackTraceElement.toString() );
            }
        }

    }

    protected void testCluster( int nrOfServers, NetworkMock mock,
                                ClusterTestScript script )
            throws URISyntaxException
    {
        int[] serverIds = new int[nrOfServers];
        for ( int i = 1; i <= nrOfServers; i++ )
        {
            serverIds[i - 1] = i;
        }
        testCluster( serverIds, null, mock, script );
    }

    protected void testCluster( int[] serverIds, VerifyInstanceConfiguration[] finalConfig, NetworkMock mock,
                                ClusterTestScript script )
            throws URISyntaxException
    {
        this.script = script;

        network = mock;
        servers.clear();
        out.clear();
        in.clear();

        for ( int i = 0; i < serverIds.length; i++ )
        {
            final URI uri = new URI( "server" + (i + 1) );
            members.put( serverIds[i], uri );
            TestProtocolServer server = network.addServer( serverIds[i], uri );
            final Cluster cluster = server.newClient( Cluster.class );
            clusterStateListener( uri, cluster );

            server.newClient( Heartbeat.class ).addHeartbeatListener( new HeartbeatListener()
            {
                @Override
                public void failed( InstanceId server )
                {
                    logger.getLogger().warning( uri + ": Failed:" + server );
                }

                @Override
                public void alive( InstanceId server )
                {
                    logger.getLogger().fine( uri + ": Alive:" + server );
                }
            } );
            server.newClient( AtomicBroadcast.class ).addAtomicBroadcastListener( new AtomicBroadcastListener()
            {
                AtomicBroadcastSerializer serializer = new AtomicBroadcastSerializer( new ObjectStreamFactory(),
                        new ObjectStreamFactory() );

                @Override
                public void receive( Payload value )
                {
                    try
                    {
                        logger.getLogger().fine( uri + " received: " + serializer.receive( value ) );
                    }
                    catch ( IOException | ClassNotFoundException e )
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
            logger.getLogger().fine( "Round " + i + ", time:" + network.getTime() );

            script.tick( network.getTime() );

            network.tick();
        }

        // Let messages settle
        network.tick( 100 );
        if ( finalConfig == null )
        {
            verifyConfigurations( "final config" );
        }
        else
        {
            verifyConfigurations( finalConfig );
        }

        logger.getLogger().fine( "All nodes leave" );

        // All leave
        for ( Cluster cluster : new ArrayList<>( in ) )
        {
            logger.getLogger().fine( "Leaving:" + cluster );
            cluster.leave();
            in.remove( cluster );
            network.tick( 400 );
        }

        if ( finalConfig != null )
        {
            verifyConfigurations( finalConfig );
        }
        else
        {
            verifyConfigurations( "test over" );
        }
    }

    private void clusterStateListener( final URI uri, final Cluster cluster )
    {
        cluster.addClusterListener( new ClusterListener()
        {
            @Override
            public void enteredCluster( ClusterConfiguration clusterConfiguration )
            {
                logger.getLogger().fine( uri + " entered cluster:" + clusterConfiguration.getMemberURIs() );
                in.add( cluster );
            }

            @Override
            public void joinedCluster( InstanceId id, URI member )
            {
                logger.getLogger().fine( uri + " sees a join from " + id + " at URI " + member );
            }

            @Override
            public void leftCluster( InstanceId id, URI member )
            {
                logger.getLogger().fine( uri + " sees a leave from " + id );
            }

            @Override
            public void leftCluster()
            {
                logger.getLogger().fine( uri + " left cluster" );
                out.add( cluster );
            }

            @Override
            public void elected( String role, InstanceId id, URI electedMember )
            {
                logger.getLogger().fine(
                        uri + " sees an election: " + id + " elected as " + role + " at URI " + electedMember );
            }

            @Override
            public void unelected( String role, InstanceId instanceId, URI electedMember )
            {
                logger.getLogger().fine(
                        uri + " sees an unelection: " + instanceId + " removed from " + role + " at URI " +
                                electedMember );
            }
        } );
    }

    public void verifyConfigurations( VerifyInstanceConfiguration[] toCheckAgainst )
    {
        logger.getLogger().fine( "Verify configurations against given" );

        List<URI> members;
        Map<String, InstanceId> roles;
        Set<InstanceId> failed;

        List<AssertionError> errors = new LinkedList<>();

        List<TestProtocolServer> protocolServers = network.getServers();

        assertEquals( "You must provide a configuration for all instances",
                protocolServers.size(), toCheckAgainst.length );

        for ( int j = 0; j < protocolServers.size(); j++ )
        {
            members = toCheckAgainst[j].members;
            roles = toCheckAgainst[j].roles;
            failed = toCheckAgainst[j].failed;
            StateMachines stateMachines = protocolServers.get( j ).getServer().getStateMachines();

            State<?, ?> clusterState = stateMachines.getStateMachine( ClusterMessage.class ).getState();
            if ( !clusterState.equals( ClusterState.entered ) )
            {
                logger.getLogger().warning( "Instance " + ( j + 1 ) + " is not in the cluster (" + clusterState + ")" );
                continue;
            }

            ClusterContext context = (ClusterContext) stateMachines.getStateMachine( ClusterMessage.class )
                    .getContext();
            HeartbeatContext heartbeatContext = (HeartbeatContext) stateMachines.getStateMachine(
                    HeartbeatMessage.class ).getContext();
            ClusterConfiguration clusterConfiguration = context.getConfiguration();
            if ( !clusterConfiguration.getMemberURIs().isEmpty() )
            {
                logger.getLogger().fine( "   Server " + ( j + 1 ) + ": Cluster:" + clusterConfiguration.getMemberURIs() +
                        ", Roles:" + clusterConfiguration.getRoles() + ", Failed:" + heartbeatContext.getFailed() );
                verifyConfigurations( stateMachines, members, roles, failed, errors );
            }
        }

//        assertEquals( "In:" + in + ", Out:" + out, protocolServers.size(), Iterables.count( Iterables.<Cluster,
//                List<Cluster>>flatten( in, out ) ) );

        if ( !errors.isEmpty() )
        {
            for ( AssertionError error : errors )
            {
                logger.getLogger().severe( error.toString() );
            }
            throw errors.get( 0 );
        }
    }

    public void verifyConfigurations( String description )
    {
        logger.getLogger().fine( "Verify configurations" );

        List<URI> members = null;
        Map<String, InstanceId> roles = null;
        Set<InstanceId> failed = null;

        List<AssertionError> errors = new LinkedList<>();

        List<TestProtocolServer> protocolServers = network.getServers();

        for ( int j = 0; j < protocolServers.size(); j++ )
        {
            StateMachines stateMachines = protocolServers.get( j ).getServer().getStateMachines();

            State<?, ?> clusterState = stateMachines.getStateMachine( ClusterMessage.class ).getState();
            if ( !clusterState.equals( ClusterState.entered ) )
            {
                logger.getLogger().fine( "Instance " + ( j + 1 ) + " is not in the cluster (" + clusterState + ")" );
                continue;
            }

            ClusterContext context = (ClusterContext) stateMachines.getStateMachine( ClusterMessage.class )
                    .getContext();
            HeartbeatContext heartbeatContext = (HeartbeatContext) stateMachines.getStateMachine(
                    HeartbeatMessage.class ).getContext();
            ClusterConfiguration clusterConfiguration = context.getConfiguration();
            if ( !clusterConfiguration.getMemberURIs().isEmpty() )
            {
                logger.getLogger().fine( "   Server " + ( j + 1 ) + ": Cluster:" + clusterConfiguration.getMemberURIs() +
                        ", Roles:" + clusterConfiguration.getRoles() + ", Failed:" + heartbeatContext.getFailed() );
                if ( members == null )
                {
                    members = clusterConfiguration.getMemberURIs();
                    roles = clusterConfiguration.getRoles();
                    failed = heartbeatContext.getFailed();
                }
                else
                {
                    verifyConfigurations( stateMachines, members, roles, failed, errors );
                }
            }
        }

        assertEquals( description + ": In:" + in + ", Out:" + out, protocolServers.size(),
                Iterables.count( Iterables.flatten( in, out ) ) );

        if ( !errors.isEmpty() )
        {
            for ( AssertionError error : errors )
            {
                logger.getLogger().severe( error.toString() );
            }
            throw errors.get( 0 );
        }
    }

    private void verifyConfigurations( StateMachines stateMachines, List<URI> members, Map<String, InstanceId> roles,
                                       Set<InstanceId> failed, List<AssertionError> errors )
    {

        ClusterContext context = (ClusterContext) stateMachines.getStateMachine( ClusterMessage.class )
                .getContext();
        int myId = context.getMyId().toIntegerIndex();

        State<?, ?> clusterState = stateMachines.getStateMachine( ClusterMessage.class ).getState();
        if ( !clusterState.equals( ClusterState.entered ) )
        {
            logger.getLogger().warning( "Instance " + myId + " is not in the cluster (" + clusterState + ")" );
            return;
        }

        HeartbeatContext heartbeatContext = (HeartbeatContext) stateMachines.getStateMachine(
                HeartbeatMessage.class ).getContext();
        ClusterConfiguration clusterConfiguration = context.getConfiguration();
        try
        {
            assertEquals( "Config for server" + myId + " is wrong", new HashSet<>( members ),
                    new HashSet<>( clusterConfiguration.getMemberURIs() )
            );
        }
        catch ( AssertionError e )
        {
            errors.add( e );
        }
        try
        {
            assertEquals( "Roles for server" + myId + " is wrong", roles, clusterConfiguration
                    .getRoles() );
        }
        catch ( AssertionError e )
        {
            errors.add( e );
        }
        try
        {
            assertEquals( "Failed for server" + myId + " is wrong", failed, heartbeatContext.getFailed() );
        }
        catch ( AssertionError e )
        {
            errors.add( e );
        }
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

        private final Queue<ClusterAction> actions = new LinkedList<>();
        private final AtomicBroadcastSerializer serializer = new AtomicBroadcastSerializer( new ObjectStreamFactory()
                , new ObjectStreamFactory() );

        private int rounds = 100;
        private long now;

        public ClusterTestScriptDSL rounds( int n )
        {
            rounds = n;
            return this;
        }

        public ClusterTestScriptDSL join( int time, final int joinServer, final int... joinServers )
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
                                    URI[] toJoin = new URI[servers.size()];
                                    for ( int i = 0; i < servers.size(); i++ )
                                    {
                                        toJoin[i] = servers.get( i ).getServer().boundAt();
                                    }
                                    final Future<ClusterConfiguration> result = cluster.join( "default", toJoin );
                                    Runnable joiner = () ->
                                    {
                                        try
                                        {
                                            ClusterConfiguration clusterConfiguration = result.get();
                                            logger.getLogger().fine( "**** Cluster configuration:" +
                                                    clusterConfiguration );
                                        }
                                        catch ( Exception e )
                                        {
                                            logger.getLogger().warning( "**** Node could not join cluster:" + e
                                                    .getMessage() );
                                            out.add( cluster );
                                        }
                                    };
                                    network.addFutureWaiter( result, joiner );
                                }
                            }
                            else
                            {
                                // List of servers to join was explicitly specified, so use that
                                URI[] instanceUris = new URI[joinServers.length];
                                for ( int i = 0; i < joinServers.length; i++ )
                                {
                                    int server = joinServers[i];
                                    instanceUris[i] = URI.create( "server" + server );
                                }

                                final Future<ClusterConfiguration> result = cluster.join( "default", instanceUris );
                                Runnable joiner = () ->
                                {
                                    try
                                    {
                                        ClusterConfiguration clusterConfiguration = result.get();
                                        logger.getLogger().fine( "**** Cluster configuration:" +
                                                clusterConfiguration );
                                    }
                                    catch ( Exception e )
                                    {
                                        logger.getLogger().warning(
                                                "**** Node " + joinServer + " could not join cluster:" + e
                                                        .getMessage()
                                        );
                                        if ( !(e.getCause() instanceof IllegalStateException) )
                                        {
                                            cluster.create( "default" );
                                        }
                                        else
                                        {
                                            logger.getLogger().warning( "*** Incorrectly configured cluster? "
                                                    + e.getCause().getMessage() );
                                        }
                                    }
                                };
                                network.addFutureWaiter( result, joiner );
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
                            logger.getLogger().fine( "Leave:" + cluster.toString() );
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
                            .nodeIsDown( "server" + server.toString() );
                    logger.getLogger().fine( server + " is down" );
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
                    network.getNetworkLatencyStrategy()
                            .getStrategy( ScriptableNetworkFailureLatencyStrategy.class )
                            .nodeIsUp( "server" + server.toString() );
                    logger.getLogger().fine( server + " is up" );
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
                    logger.getLogger().fine( "Slept for " + sleepTime );
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
                    logger.getLogger().fine( msg );
                }
            }, time );
        }

        public ClusterTestScriptDSL verifyConfigurations( final String description, long time )
        {
            return addAction( new ClusterAction()
            {
                @Override
                public void run()
                {
                    ClusterMockTest.this.verifyConfigurations( description );
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

        public ClusterTestScriptDSL getRoles( final Map<String, InstanceId> roles )
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

        public ClusterTestScriptDSL verifyCoordinatorRoleSwitched( final Map<String, InstanceId> comparedTo )
        {
            return addAction( new ClusterAction()
            {
                @Override
                public void run()
                {
                    HashMap<String, InstanceId> roles = new HashMap<>();
                    ClusterMockTest.this.getRoles( roles );
                    InstanceId oldCoordinator = comparedTo.get( ClusterConfiguration.COORDINATOR );
                    InstanceId newCoordinator = roles.get( ClusterConfiguration.COORDINATOR );
                    assertNotNull( "Should have had a coordinator before bringing it down", oldCoordinator );
                    assertNotNull( "Should have a new coordinator after the previous failed", newCoordinator );
                    assertTrue( "Should have elected a new coordinator", !oldCoordinator.equals( newCoordinator ) );
                }
            }, 0 );
        }
    }

    private void getRoles( Map<String, InstanceId> roles )
    {
        List<TestProtocolServer> protocolServers = network.getServers();
        for ( int j = 0; j < protocolServers.size(); j++ )
        {
            StateMachines stateMachines = protocolServers.get( j )
                    .getServer()
                    .getStateMachines();

            State<?, ?> clusterState = stateMachines.getStateMachine( ClusterMessage.class ).getState();
            if ( !clusterState.equals( ClusterState.entered ) )
            {
                logger.getLogger().warning( "Instance " + (j + 1) + " is not in the cluster (" + clusterState + ")" );
                continue;
            }

            ClusterContext context = (ClusterContext) stateMachines.getStateMachine( ClusterMessage.class )
                    .getContext();
            ClusterConfiguration clusterConfiguration = context.getConfiguration();
            roles.putAll( clusterConfiguration.getRoles() );
        }
    }
}
