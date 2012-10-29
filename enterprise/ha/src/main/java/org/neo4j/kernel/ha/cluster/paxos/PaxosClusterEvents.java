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
                return config.get( HaSettings.ha_server );
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

    public PaxosClusterEvents( Configuration config, ClusterClient cluster, StringLogger logger )
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
                PaxosClusterEvents.this.logger.logMessage( "Listening at:" + me );
            }
        } );
    }

    @Override
    public void init()
            throws Throwable
    {
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
                        cluster.broadcast( serializer.broadcast( new MasterIsElected( serverClusterId ) ) );
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
                        cluster.broadcast( serializer.broadcast( new MasterIsElected( serverClusterId ) ) );
                    }
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            }
        } );

        cluster.addAtomicBroadcastListener( new AtomicBroadcastListener()
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
            String host = getConfiguredHaAddress( clusterUri );
            return new URI( "ha", null, host, portOf( config.getHaServer() ), null,
                    "serverId=" + config.getServerId(), null );
        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }
    }

    private String getConfiguredHaAddress( URI clusterUri )
    {
        String host = addressOf( config.getHaServer() );
        return host != null ? host : clusterUri.getHost();
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
            String host = getConfiguredHaAddress( clusterUri );
            return new URI( "backup", null, host, config.getBackupPort(), null, null, null );
        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }
    }
}
