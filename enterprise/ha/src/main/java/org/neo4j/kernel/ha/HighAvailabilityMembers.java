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

package org.neo4j.kernel.ha;

import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.getServerId;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.FilteringIterable;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberChangeEvent;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberListener;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.DataSourceRegistrationListener;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * Keeps an accurate list of alive and available slaves in the cluster.
 * If we are the master in the cluster then the {@link Slave}s will have
 * active connections to the slaves themselves, otherwise they will just
 * be something that holds information about a slave.
 */
public class HighAvailabilityMembers implements Slaves, Lifecycle
{
    private final AtomicReference<Map<URI /*Member cluster URI, not HA URI*/, Member>> members;
    protected final StringLogger msgLog;
    private StoreId storeId;
    private final Config config;
    private final LifeSupport life;
    private final ClusterClient clusterClient;

    public HighAvailabilityMembers( ClusterClient clusterClient, HighAvailabilityMemberStateMachine clusterEvents,
                                    StringLogger msgLog, Config config, XaDataSourceManager xaDsm )
    {
        this.clusterClient = clusterClient;
        this.msgLog = msgLog;
        this.config = config;
        this.life = new LifeSupport();
        this.members = new AtomicReference<Map<URI, Member>>( new HashMap<URI, Member>() );
        clusterEvents.addClusterMemberListener( new MembersListener() );
        xaDsm.addDataSourceRegistrationListener( new StoreIdSettingListener() );
        clusterClient.addClusterListener( new MembersClusterListener() );
        clusterClient.addHeartbeatListener( new MembersHeartbeatListener() );
    }

    @Override
    public void init() throws Throwable
    {
        life.init();
    }

    @Override
    public void start() throws Throwable
    {
        life.start();
    }

    @Override
    public void stop() throws Throwable
    {
        life.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        life.shutdown();
    }

    public MemberInfo[] getMembers()
    {
        Map<URI, Member> members = this.members.get();
        MemberInfo[] result = new MemberInfo[members.size()];
        int i = 0;
        for ( Member member : members.values() )
        {
            result[i++] = member;
        }
        return result;
    }

    @Override
    public Iterable<Slave> getSlaves()
    {
        Iterable<Member> aliveMembers = new FilteringIterable<Member>( members.get().values(), ALIVE_SLAVE );
        return new IterableWrapper<Slave, Member>( aliveMembers )
        {
            @Override
            protected Slave underlyingObjectToObject( Member member )
            {
                return member.slaveClient;
            }
        };
    }

    private static final Predicate<Member> ALIVE_SLAVE = new Predicate<HighAvailabilityMembers.Member>()
    {
        @Override
        public boolean accept( Member item )
        {
            return item.available && item.isSlave();
        }
    };

    public interface MemberInfo
    {
        int getServerId();

        URI getClusterUri();

        URI[] getUris();

        boolean isSlave();

        boolean isMaster();

        String getHaRole();

        String[] getClusterRoles();

        boolean isAvailable();
    }

    private class Member implements MemberInfo
    {
        private final URI clusterUri;
        private final Set<String> clusterRoles;
        private HighAvailabilityMemberState role = HighAvailabilityMemberState.PENDING; // TODO reuse that enum here really?
        private URI haUri;
        private boolean available;

        // Only assigned if I'm the master
        private SlaveClient slaveClient;

        public Member( URI clusterUri, Iterable<String> initialClusterRoles )
        {
            this.clusterRoles = new HashSet<String>( asCollection( initialClusterRoles ) );
            this.clusterUri = clusterUri;
        }

        @Override
        public boolean isSlave()
        {
            return this.role == HighAvailabilityMemberState.SLAVE;
        }

        private void setHaRole( HighAvailabilityMemberState role )
        {
            this.role = role;
        }

        @Override
        public boolean isMaster()
        {
            return this.role == HighAvailabilityMemberState.MASTER;
        }

        void setSlaveClient( SlaveClient client )
        {
            this.slaveClient = life.add( client );
        }

        void becomeAvailable( URI haUri, HighAvailabilityMemberState role )
        {
            this.haUri = haUri;
            this.available = true;
            this.role = role;
        }

        void setAlive( boolean alive )
        {
            this.available = alive;
            if ( !alive )
            {
                setHaRole( HighAvailabilityMemberState.PENDING );
            }
        }

        @Override
        public int getServerId()
        {
            return haUri != null ? HighAvailabilityModeSwitcher.getServerId( haUri ) : -1;
        }

        @Override
        public String toString()
        {
            return "Member [clusterUri=" + clusterUri + ", role=" + role + ", haUri=" + haUri +
                    ", alive=" + available + ", slaveClient=" + slaveClient + "]";
        }

        @Override
        public URI getClusterUri()
        {
            return this.clusterUri;
        }

        @Override
        public URI[] getUris()
        {
            Collection<URI> uris = new ArrayList<URI>();
            uris.add( clusterUri );
            URI haUri = this.haUri;
            if ( haUri != null )
            {
                uris.add( haUri );
            }
            return uris.toArray( new URI[0] );
        }

        @Override
        public boolean isAvailable()
        {
            return this.available;
        }

        @Override
        public String getHaRole()
        {
            if ( isMaster() )
            {
                return HighAvailabilityMemberState.MASTER.name();
            }
            if ( isSlave() )
            {
                return HighAvailabilityMemberState.SLAVE.name();
            }
            return "";
        }

        @Override
        public synchronized String[] getClusterRoles()
        {
            return clusterRoles.toArray( new String[0] );
        }

        private synchronized void addClusterRole( String role )
        {
            clusterRoles.add( role );
        }

        private synchronized void removeClusterRole( String role )
        {
            clusterRoles.remove( role );
        }
    }

    private class MembersListener extends HighAvailabilityMemberListener.Adapter
    {
        private volatile boolean instantiateFullSlaveClients;
        private volatile URI previouslyElectedMaster;

        @Override
        public void masterIsElected( HighAvailabilityMemberChangeEvent event )
        {
            instantiateFullSlaveClients = event.getNewState() == HighAvailabilityMemberState.TO_MASTER ||
                    event.getNewState() == HighAvailabilityMemberState.MASTER;
            URI masterUri = event.getServerClusterUri();
            if ( previouslyElectedMaster == null || !previouslyElectedMaster.equals( masterUri ) )
            {
                life.clear();
                previouslyElectedMaster = masterUri;
            }
        }

        @Override
        public void slaveIsAvailable( HighAvailabilityMemberChangeEvent event )
        {
            Member member = members.get().get( event.getServerClusterUri() );
            URI haUri = event.getServerHaUri();
            if ( instantiateFullSlaveClients )
            {
                SlaveClient client = new SlaveClient(
                        getServerId( haUri ),
                        haUri.getHost(), haUri.getPort(), msgLog, storeId,
                        config.get( HaSettings.max_concurrent_channels_per_slave ),
                        config.get( HaSettings.com_chunk_size ).intValue() );
                member.setSlaveClient( client );
            }
            member.becomeAvailable( haUri, HighAvailabilityMemberState.SLAVE );
        }

        @Override
        public void masterIsAvailable( HighAvailabilityMemberChangeEvent event )
        {
            // TODO anything special for master?
            Member member = members.get().get( event.getServerClusterUri() );
            member.becomeAvailable( event.getServerHaUri(), HighAvailabilityMemberState.MASTER );
        }

        @Override
        public void instanceStops( HighAvailabilityMemberChangeEvent event )
        {
            // TODO have another state for stopping?
            Member member = members.get().get( event.getServerClusterUri() );
            if ( member != null )
            {
                member.setAlive( false );
            }
        }
    }

    private class StoreIdSettingListener extends DataSourceRegistrationListener.Adapter
    {
        @Override
        public void registeredDataSource( XaDataSource ds )
        {
            if ( ds.getName().equals( Config.DEFAULT_DATA_SOURCE_NAME ) )
            {
                NeoStoreXaDataSource neoXaDs = (NeoStoreXaDataSource) ds;
                storeId = neoXaDs.getStoreId();
            }
        }
    }

    private class MembersClusterListener extends ClusterListener.Adapter
    {
        private ClusterConfiguration clusterConfiguration;

        @Override
        public void enteredCluster( ClusterConfiguration clusterConfiguration )
        {
            this.clusterConfiguration = clusterConfiguration;
            // This should be the first thing we get back, i.e.
            // the initial full cluster configuration
            Map<URI, Member> newMembers = new HashMap<URI, Member>();
            for ( URI memberClusterUri : clusterConfiguration.getMembers() )
            {
                Member member = new Member( memberClusterUri, clusterConfiguration.getRolesOf( memberClusterUri ) );
                newMembers.put( memberClusterUri, member );
            }
            members.set( newMembers );
        }

        @Override
        public void leftCluster( URI member )
        {
            memberLeft( member );
        }

        @Override
        public void leftCluster()
        {
            memberLeft( clusterClient.getServerUri() );
        }

        @Override
        public void joinedCluster( URI memberClusterUri )
        {
            boolean updated = false;
            do
            {
                Map<URI, Member> oldMembers = members.get();
                Map<URI, Member> newMembers = new HashMap<URI, Member>( oldMembers );
                newMembers.put( memberClusterUri, new Member( memberClusterUri, clusterConfiguration.getRolesOf(
                        memberClusterUri ) ) );
                updated = members.compareAndSet( oldMembers, newMembers );
            }
            while ( !updated );
        }
    }

    private class MembersHeartbeatListener implements HeartbeatListener
    {
        @Override
        public void failed( URI server )
        {
            members.get().get( server ).available = false;
        }

        @Override
        public void alive( URI server )
        {
            members.get().get( server ).available = true;
        }
    }

    private void memberLeft( URI memberClusterUri )
    {
        boolean updated = false;
        Member removedMember = null;
        do
        {
            Map<URI, Member> oldMembers = members.get();
            Map<URI, Member> newMembers = new HashMap<URI, Member>( oldMembers );
            removedMember = newMembers.remove( memberClusterUri );
            updated = members.compareAndSet( oldMembers, newMembers );
        }
        while ( !updated );
        life.remove( removedMember );
    }
}
