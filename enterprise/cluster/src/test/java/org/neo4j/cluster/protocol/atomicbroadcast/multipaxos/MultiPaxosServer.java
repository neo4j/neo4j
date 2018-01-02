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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Collection;

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
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastListener;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.Payload;
import org.neo4j.cluster.protocol.cluster.Cluster;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterContext;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.cluster.ClusterMessage;
import org.neo4j.cluster.protocol.election.ServerIdElectionCredentialsProvider;
import org.neo4j.cluster.protocol.heartbeat.Heartbeat;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatMessage;
import org.neo4j.cluster.timeout.FixedTimeoutStrategy;
import org.neo4j.cluster.timeout.MessageTimeoutStrategy;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;

/**
 * Multi Paxos test server
 */
public class MultiPaxosServer
{
    private AtomicBroadcastSerializer broadcastSerializer;
    private ProtocolServer server;

    public static void main( String[] args )
            throws IOException, InvocationTargetException, IllegalAccessException
    {
        new MultiPaxosServer().start();
    }

    protected Cluster cluster;
    protected AtomicBroadcast broadcast;

    public void start()
            throws IOException
    {
        broadcastSerializer = new AtomicBroadcastSerializer( new ObjectStreamFactory(), new ObjectStreamFactory() );
        final LifeSupport life = new LifeSupport();
        try
        {
            MessageTimeoutStrategy timeoutStrategy = new MessageTimeoutStrategy( new FixedTimeoutStrategy( 5000 ) )
                    .timeout( HeartbeatMessage.sendHeartbeat, 200 );

            Monitors monitors = new Monitors();
            NetworkedServerFactory serverFactory = new NetworkedServerFactory( life,
                    new MultiPaxosServerFactory( new ClusterConfiguration( "default", NullLogProvider.getInstance() ),
                            NullLogProvider.getInstance(), monitors.newMonitor( StateMachines.Monitor.class ) ),
                    timeoutStrategy, NullLogProvider.getInstance(), new ObjectStreamFactory(), new ObjectStreamFactory(),
                    monitors.newMonitor( NetworkReceiver.Monitor.class ),
                    monitors.newMonitor( NetworkSender.Monitor.class ),
                    monitors.newMonitor( NamedThreadFactory.Monitor.class )
            );

            ServerIdElectionCredentialsProvider electionCredentialsProvider = new ServerIdElectionCredentialsProvider();
            server = serverFactory.newNetworkedServer(
                    new Config( MapUtil.stringMap(), ClusterSettings.class ),
                    new InMemoryAcceptorInstanceStore(),
                    electionCredentialsProvider );
            server.addBindingListener( electionCredentialsProvider );
            server.addBindingListener( new BindingListener()
            {
                @Override
                public void listeningAt( URI me )
                {
                    System.out.println( "Listening at:" + me );
                }
            } );

            cluster = server.newClient( Cluster.class );
            cluster.addClusterListener( new ClusterListener()
            {
                @Override
                public void enteredCluster( ClusterConfiguration clusterConfiguration )
                {
                    System.out.println( "Entered cluster:" + clusterConfiguration );
                }

                @Override
                public void joinedCluster( InstanceId instanceId, URI member )
                {
                    System.out.println( "Joined cluster:" + instanceId + " (at URI " + member + ")" );
                }

                @Override
                public void leftCluster( InstanceId instanceId, URI member )
                {
                    System.out.println( "Left cluster:" + instanceId );
                }

                @Override
                public void leftCluster()
                {
                    System.out.println( "Left cluster" );
                }

                @Override
                public void elected( String role, InstanceId instanceId, URI electedMember )
                {
                    System.out.println( instanceId + " at URI " + electedMember + " was elected as " + role );
                }

                @Override
                public void unelected( String role, InstanceId instanceId, URI electedMember )
                {
                    System.out.println( instanceId + " at URI " + electedMember + " was removed from " + role );
                }
            } );

            Heartbeat heartbeat = server.newClient( Heartbeat.class );
            heartbeat.addHeartbeatListener( new HeartbeatListener()
            {
                @Override
                public void failed( InstanceId server )
                {
                    System.out.println( server + " failed" );
                }

                @Override
                public void alive( InstanceId server )
                {
                    System.out.println( server + " alive" );
                }
            } );

            broadcast = server.newClient( AtomicBroadcast.class );
            broadcast.addAtomicBroadcastListener( new AtomicBroadcastListener()
            {
                @Override
                public void receive( Payload value )
                {
                    try
                    {
                        System.out.println( broadcastSerializer.receive( value ) );
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

            life.start();

            String command;
            BufferedReader reader = new BufferedReader( new InputStreamReader( System.in ) );
            while ( !(command = reader.readLine()).equals( "quit" ) )
            {
                String[] arguments = command.split( " " );
                Method method = getCommandMethod( arguments[0] );
                if ( method != null )
                {
                    String[] realArgs = new String[arguments.length - 1];
                    System.arraycopy( arguments, 1, realArgs, 0, realArgs.length );
                    try
                    {
                        method.invoke( this, (Object[]) realArgs );
                    }
                    catch ( IllegalAccessException e )
                    {
                        e.printStackTrace();
                    }
                    catch ( IllegalArgumentException e )
                    {
                        e.printStackTrace();
                    }
                    catch ( InvocationTargetException e )
                    {
                        e.printStackTrace();
                    }
                }
            }

            cluster.leave();
        }
        finally
        {
            life.shutdown();
            System.out.println( "Done" );
        }
    }

    public void config()
    {
        ClusterConfiguration configuration = ((ClusterContext) server.getStateMachines()
                .getStateMachine( ClusterMessage.class )
                .getContext()).getConfiguration();

        Collection<InstanceId> failed = ((HeartbeatContext) server.getStateMachines().getStateMachine( HeartbeatMessage
                .class ).getContext()).getFailed();
    }

    private Method getCommandMethod( String name )
    {
        for ( Method method : MultiPaxosServer.class.getMethods() )
        {
            if ( method.getName().equals( name ) )
            {
                return method;
            }
        }
        return null;
    }
}
