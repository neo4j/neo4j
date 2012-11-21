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
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastListener;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;
import org.neo4j.cluster.protocol.atomicbroadcast.Payload;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.cluster.HighAvailabilityEvents;
import org.neo4j.kernel.ha.cluster.HighAvailabilityListener;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * Paxos based implementation of {@link org.neo4j.kernel.ha.cluster.HighAvailabilityEvents}
 */
public class PaxosHighAvailabilityEvents implements HighAvailabilityEvents, Lifecycle
{
    private ClusterListener clusterListener;
    private HeartbeatListener heartbeatListener;
    private AtomicBroadcastListener atomicBroadcastListener;

    public interface Configuration
    {
        String getHaServer();

        int getServerId();

        int getBackupPort();
    }
    
    public static Configuration adapt( final Config config )
    {
        return new Configuration()
        {
            @Override
            public int getServerId()
            {
                return config.get( HaSettings.server_id );
            }
            
            @Override
            public String getHaServer()
            {
                return HaSettings.ha_server.getAddressAndPortWithLocalhostDefault( config.getParams() );
            }
            
            @Override
            public int getBackupPort()
            {
                return config.get( OnlineBackupSettings.online_backup_port );
            }
        };
    }
    
    private URI serverClusterId;
    private Configuration config;
    private StringLogger logger;
    protected AtomicBroadcastSerializer serializer;
    private final ClusterClient cluster;
    protected Iterable<HighAvailabilityListener> listeners = Listeners.newListeners();
    private volatile ClusterConfiguration clusterConfiguration;

    public PaxosHighAvailabilityEvents( Configuration config, ClusterClient cluster, StringLogger logger )
    {
        this.config = config;
        this.cluster = cluster;
        this.logger = logger;
        this.cluster.addBindingListener( new BindingListener()
        {
            @Override
            public void listeningAt( URI me )
            {
                serverClusterId = me;
                PaxosHighAvailabilityEvents.this.logger.logMessage( "Listening at:" + me );
            }
        } );
    }
    
    @Override
    public void addClusterEventListener( HighAvailabilityListener listener )
    {
        listeners = Listeners.addListener( listener, listeners );
    }

    @Override
    public void removeClusterEventListener( HighAvailabilityListener listener )
    {
        listeners = Listeners.removeListener( listener, listeners );
    }

    @Override
    public void init()
            throws Throwable
    {
        serializer = new AtomicBroadcastSerializer();

        cluster.addClusterListener( clusterListener = new ClusterListener.Adapter()
        {
            @Override
            public void joinedCluster( URI member )
            {
                broadcastOurself();
            }

            @Override
            public void enteredCluster( ClusterConfiguration clusterConfiguration )
            {
                PaxosHighAvailabilityEvents.this.clusterConfiguration = clusterConfiguration;
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
                        cluster.broadcast( serializer.broadcast( new MasterIsElected( serverClusterId ) ) );
                    }
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            }
        } );
        
        cluster.addHeartbeatListener( heartbeatListener = new HeartbeatListener.Adapter()
        {
            @Override
            public void alive( URI server )
            {
                broadcastOurself();
            }
        } );

        cluster.addAtomicBroadcastListener( atomicBroadcastListener = new AtomicBroadcastListener()
        {
            @Override
            public void receive( Payload payload )
            {
                try
                {
                    final Object value = serializer.receive( payload );
                    if ( value instanceof MasterIsElected )
                    {
                        Listeners.notifyListeners( listeners, new Listeners.Notification<HighAvailabilityListener>()
                        {
                            @Override
                            public void notify( HighAvailabilityListener listener )
                            {
                                listener.masterIsElected( ((MasterIsElected) value).getMasterUri() );
                            }
                        } );
                    }
                    else if ( value instanceof MemberIsAvailable )
                    {
                        Listeners.notifyListeners( listeners, new Listeners.Notification<HighAvailabilityListener>()
                        {
                            @Override
                            public void notify( HighAvailabilityListener listener )
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
    
    private void broadcastOurself()
    {
        // Reannounce that I am master, for the purpose of the new member to see this
        try
        {
            final URI coordinator = clusterConfiguration.getElected( ClusterConfiguration.COORDINATOR );
            if ( coordinator.equals( serverClusterId ) )
            {
                cluster.broadcast( serializer.broadcast( new MasterIsElected( serverClusterId ) ) );
            }
            else
            {
                memberIsAvailable( ClusterConfiguration.SLAVE );
            }
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
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
        cluster.removeAtomicBroadcastListener( atomicBroadcastListener );
        cluster.removeClusterListener( clusterListener );
        cluster.removeHeartbeatListener( heartbeatListener );
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
            cluster.broadcast( payload );
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
            // TODO if we don't want this fallback on cluster address, then add this
            // logic to the (default) Configuration implementation?
            String host = config.getHaServer();
            return new URI( "ha", null, addressOf( host ), portOf( host ), null,
                    "serverId=" + config.getServerId(), null );
        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }
    }

    private int portOf( String addressWithOrWithoutPort )
    {
        String[] parts = addressWithOrWithoutPort.split( ":" );
        return parts.length < 2 ? -1 : Integer.parseInt( parts[1] );
    }

    private String addressOf( String addressWithOrWithoutPort )
    {
        String[] parts = addressWithOrWithoutPort.split( ":" );
        return parts[0].length() > 0 ? parts[0] : null;
    }

    private URI getBackupUri( URI clusterUri )
    {
        try
        {
            String host = config.getHaServer();
            return new URI( "backup", null, addressOf( host ), config.getBackupPort(), null, null, null );
        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }
    }
}
