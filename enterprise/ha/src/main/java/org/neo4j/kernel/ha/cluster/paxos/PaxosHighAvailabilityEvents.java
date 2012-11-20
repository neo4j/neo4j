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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.cluster.BindingListener;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastListener;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;
import org.neo4j.cluster.protocol.atomicbroadcast.Payload;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.cluster.protocol.snapshot.SnapshotProvider;
import org.neo4j.helpers.HostnamePort;
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
    public interface Configuration
    {
        HostnamePort getHaServer();

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
            public HostnamePort getHaServer()
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
    protected Iterable<HighAvailabilityListener> listeners = Listeners.newListeners();
    private volatile ClusterConfiguration clusterConfiguration;
    private String role;
    private HighAvailabilitySnapshot snapshot = new HighAvailabilitySnapshot();

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
    public void addHighAvailabilityEventListener( HighAvailabilityListener listener )
    {
        listeners = Listeners.addListener( listener, listeners );
    }

    @Override
    public void removeHighAvailabilityEventListener( HighAvailabilityListener listener )
    {
        listeners = Listeners.removeListener( listener, listeners );
    }

    @Override
    public void init()
            throws Throwable
    {
        serializer = new AtomicBroadcastSerializer();

        cluster.addClusterListener( new ClusterListener.Adapter()
        {
            @Override
            public void joinedCluster( URI member )
            {
            }

            @Override
            public void enteredCluster( ClusterConfiguration clusterConfiguration )
            {
                PaxosHighAvailabilityEvents.this.clusterConfiguration = clusterConfiguration;

                cluster.refreshSnapshot();

                // Catch up with elections
                for ( Map.Entry<String, URI> memberRoles : clusterConfiguration.getRoles().entrySet() )
                {
                    elected( memberRoles.getKey(), memberRoles.getValue() );
                }
            }

            @Override
            public void elected( String role, final URI electedMember )
            {
                if (role.equals( ClusterConfiguration.COORDINATOR ))
                {
                    // Use the cluster coordinator as master for HA
                    Listeners.notifyListeners( listeners, new Listeners.Notification<HighAvailabilityListener>()
                    {
                        @Override
                        public void notify( HighAvailabilityListener listener )
                        {
                            listener.masterIsElected( electedMember );
                        }
                    } );
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
                    if ( value instanceof MemberIsAvailable )
                    {
                        final MemberIsAvailable memberIsAvailable = (MemberIsAvailable) value;

                        // Update snapshot
                        snapshot.availableMember( memberIsAvailable );

                        Listeners.notifyListeners( listeners, new Listeners.Notification<HighAvailabilityListener>()
                        {
                            @Override
                            public void notify( HighAvailabilityListener listener )
                            {
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

        cluster.setSnapshotProvider( new HighAvailabilitySnapshotProvider() );
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
            this.role = role;

            Iterable<URI> uris = cluster.getURIs();
            Payload payload = serializer.broadcast( new MemberIsAvailable( role, serverClusterId, uris ) );
            serializer.receive( payload );
            cluster.broadcast( payload );
            logger.warn( "Could not distribute member availability" );
        }
        catch ( Throwable e )
        {
            logger.warn( "Could not distribute member availability", e );
        }
    }

    private class HighAvailabilitySnapshotProvider implements SnapshotProvider
    {
        @Override
        public void getState( ObjectOutputStream output ) throws IOException
        {
            output.writeObject( snapshot );
        }

        @Override
        public void setState( ObjectInputStream input ) throws IOException, ClassNotFoundException
        {
            snapshot = HighAvailabilitySnapshot.class.cast(input.readObject());

            // Send current availability events to listeners
            Listeners.notifyListeners( listeners, new Listeners.Notification<HighAvailabilityListener>()
            {
                @Override
                public void notify( HighAvailabilityListener listener )
                {
                    for ( MemberIsAvailable memberIsAvailable : snapshot.getCurrentAvailableMembers() )
                    {
                        listener.memberIsAvailable( memberIsAvailable.getRole(), memberIsAvailable.getClusterUri(), memberIsAvailable.getInstanceUris() );
                    }
                }
            } );
        }
    }

    public static class HighAvailabilitySnapshot
        implements Serializable
    {
        Map<URI, MemberIsAvailable> availableMembers = new HashMap<URI, MemberIsAvailable>(  );

        public void availableMember(MemberIsAvailable memberIsAvailable)
        {
            availableMembers.put( memberIsAvailable.getClusterUri(), memberIsAvailable );
        }

        public Iterable<MemberIsAvailable> getCurrentAvailableMembers()
        {
            return availableMembers.values();
        }
    }
}
