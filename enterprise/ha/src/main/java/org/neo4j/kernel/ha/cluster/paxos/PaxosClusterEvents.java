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

package org.neo4j.kernel.ha.cluster.paxos;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.cluster.BindingListener;
import org.neo4j.cluster.ProtocolServer;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcast;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastListener;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;
import org.neo4j.cluster.protocol.atomicbroadcast.Payload;
import org.neo4j.cluster.protocol.cluster.Cluster;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.cluster.AbstractClusterEvents;
import org.neo4j.kernel.ha.cluster.ClusterEventListener;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * Paxos based implementation of {@link org.neo4j.kernel.ha.cluster.ClusterEvents}
 */
public class PaxosClusterEvents
        extends AbstractClusterEvents
        implements Lifecycle
{
    private URI serverClusterId;
    private Config config;
    private StringLogger logger;
    private AtomicBroadcast broadcast;
    protected AtomicBroadcastSerializer serializer;
    protected final ProtocolServer server;

    public PaxosClusterEvents( Config config, ProtocolServer server, StringLogger logger )
    {
        this.config = config;
        this.server = server;
        this.logger = logger;
        this.server.addBindingListener( new BindingListener()
        {
            @Override
            public void listeningAt( URI me )
            {
                serverClusterId = me;
                PaxosClusterEvents.this.logger.logMessage( "Listening at:" + me );
            }
        } );
    }

    protected Cluster cluster;

    @Override
    public void init()
            throws Throwable
    {
        cluster = server.newClient( Cluster.class );
        broadcast = server.newClient( AtomicBroadcast.class );

        serializer = new AtomicBroadcastSerializer();

        cluster.addClusterListener( new ClusterListener.Adapter()
        {
            ClusterConfiguration clusterConfiguration;

            @Override
            public void joinedCluster( URI member )
            {
                final URI coordinator = clusterConfiguration.getElected( ClusterConfiguration.COORDINATOR );

                if ( coordinator.equals( serverClusterId ) )
                {
                    // Reannounce that I am master, for the purpose of the new member to see this
                    try
                    {
                        broadcast.broadcast( serializer.broadcast( new MasterIsElected( serverClusterId ) ) );
                    }
                    catch ( IOException e )
                    {
                        e.printStackTrace();
                    }
                }
            }

            @Override
            public void enteredCluster( ClusterConfiguration clusterConfiguration )
            {
                this.clusterConfiguration = clusterConfiguration;
            }

            @Override
            public void elected( String role, URI electedMember )
            {
                try
                {
                    // TODO This seems like double work. We just Paxos-declared an election,
                    // and then broadcast it again??
                    if ( electedMember.equals( serverClusterId ) )
                    {
                        broadcast.broadcast( serializer.broadcast( new MasterIsElected( serverClusterId ) ) );
                    }
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            }
        } );

        broadcast.addAtomicBroadcastListener( new AtomicBroadcastListener()
        {
            @Override
            public void receive( Payload payload )
            {
                try
                {
                    final Object value = serializer.receive( payload );
                    if ( value instanceof MasterIsElected )
                    {
                        Listeners.notifyListeners( listeners, new Listeners.Notification<ClusterEventListener>()
                        {
                            @Override
                            public void notify( ClusterEventListener listener )
                            {
                                listener.masterIsElected( ((MasterIsElected) value).getMasterUri() );
                            }
                        } );
                    }
                    else if ( value instanceof MemberIsAvailable )
                    {
                        Listeners.notifyListeners( listeners, new Listeners.Notification<ClusterEventListener>()
                        {
                            @Override
                            public void notify( ClusterEventListener listener )
                            {
                                MemberIsAvailable memberIsAvailable = (MemberIsAvailable) value;
                                listener.memberIsAvailable( memberIsAvailable.getRole(),
                                        memberIsAvailable.getClusterUri(),
                                        memberIsAvailable.getInstanceUris() );
                            }
                        } );
                    }
                }
                catch ( Throwable t )
                {
                    t.printStackTrace();
                }
            }
        } );
    }

    @Override
    public void start()
            throws Throwable
    {
    }

    @Override
    public void stop()
            throws Throwable
    {
    }

    @Override
    public void shutdown()
            throws Throwable
    {
    }

    @Override
    public void memberIsAvailable( String role )
    {
        try
        {
            Payload payload = serializer.broadcast( new MemberIsAvailable( role, serverClusterId, Iterables.iterable(
                    getHaUri( serverClusterId ), getBackupUri( serverClusterId ) ) ) );
            serializer.receive( payload );
            broadcast.broadcast( payload );
        }
        catch ( Throwable e )
        {
            logger.warn( "Could not distribute member availability", e );
        }
    }

    private URI getHaUri( URI clusterUri )
    {
        try
        {
            String host = HaSettings.ha_server.getAddress( config.getParams() );
            if ( host == null )
            {
                host = clusterUri.getHost();
            }

            return new URI( "ha", null, host, HaSettings.ha_server.getPort( config.getParams() ), null,
                    "serverId=" + config.get( HaSettings.server_id ), null );
        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }
    }


    private URI getBackupUri( URI clusterUri )
    {
        try
        {
            String host = HaSettings.ha_server.getAddress( config.getParams() );
            if ( host == null )
            {
                host = clusterUri.getHost();
            }
            return new URI( "backup", null, host, OnlineBackupSettings.online_backup_port.getPort( config.getParams() ),
                    null, null, null );
        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }
    }
}
