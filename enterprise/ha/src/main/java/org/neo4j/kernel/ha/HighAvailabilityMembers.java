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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.com.ComSettings;
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
    private final Map<URI /*Member cluster URI, not HA URI*/, Member> members;
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
        this.members = Collections.synchronizedMap( new HashMap<URI, Member>() );
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
        return this.members.values()
                .toArray( new MemberInfo[0] );
    }

    @Override
    public Iterable<Slave> getSlaves()
    {
        List<Member> members = Arrays.asList( internalGetMembers() );
        Iterable<Member> aliveMembers = new FilteringIterable<Member>( members,
                ALIVE_SLAVE );
        return new IterableWrapper<Slave, Member>( aliveMembers )
        {
            @Override
            protected Slave underlyingObjectToObject( Member member )
            {
                return member.slaveClient;
            }
        };
    }
    
    private Member[] internalGetMembers()
    {
        return members.values()
                .toArray( new Member[0] );
    }

    private Member getMember( URI uri )
    {
        Member member = members.get( uri );
        if ( member == null )
        {
            throw new IllegalStateException( "Member " + uri
                                             + " doesn't exist amongst "
                                             + members );
        }
        return member;
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
                uris.add( haUri );
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
                return HighAvailabilityMemberState.MASTER.name();
            if ( isSlave() )
                return HighAvailabilityMemberState.SLAVE.name();
            return "";
        }

        @Override
        public synchronized String[] getClusterRoles()
        {
            return clusterRoles.toArray( new String[0] );
        }

        public void becomeUnavailable()
        {
            available = false;
            this.role = HighAvailabilityMemberState.PENDING;
        }
    }

    private class MembersListener extends HighAvailabilityMemberListener.Adapter
    {
        private volatile boolean instantiateFullSlaveClients;
        private volatile URI previouslyElectedMaster;

        @Override
        public void masterIsElected( HighAvailabilityMemberChangeEvent event )
        {
            URI masterUri = event.getServerClusterUri();
            instantiateFullSlaveClients = masterUri.equals( clusterClient.getServerUri() );
            if ( previouslyElectedMaster == null || !previouslyElectedMaster.equals( masterUri ) )
            {
                life.clear();
                for ( Member member : internalGetMembers() )
                    member.becomeUnavailable();
                previouslyElectedMaster = masterUri;
            }
        }

        @Override
        public void slaveIsAvailable( HighAvailabilityMemberChangeEvent event )
        {
            Member member = getMember( event.getServerClusterUri() );
            URI haUri = event.getServerHaUri();
            if ( instantiateFullSlaveClients )
            {
                SlaveClient client = new SlaveClient(
                        getServerId( haUri ),
                        haUri.getHost(), haUri.getPort(), msgLog, storeId,
                        config.get( HaSettings.max_concurrent_channels_per_slave ),
                        config.get( ComSettings.com_chunk_size ) );
                member.setSlaveClient( client );
            }
            member.becomeAvailable( haUri, HighAvailabilityMemberState.SLAVE );
        }
        
        @Override
        public void masterIsAvailable( HighAvailabilityMemberChangeEvent event )
        {
            // TODO anything special for master?
            Member member = getMember( event.getServerClusterUri() );
            member.becomeAvailable( event.getServerHaUri(), HighAvailabilityMemberState.MASTER );
        }
        
        @Override
        public void instanceStops( HighAvailabilityMemberChangeEvent event )
        {
            // Here event.getServerClusterURI() seems to be null, so don't
            // update any member here
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
            for ( URI memberClusterUri : clusterConfiguration.getMembers() )
            {
                Member member = new Member( memberClusterUri, clusterConfiguration.getRolesOf( memberClusterUri ) );
                members.put( memberClusterUri, member );
            }
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
            members.put( memberClusterUri, new Member( memberClusterUri,
                    clusterConfiguration.getRolesOf( memberClusterUri ) ) );
        }
    }
    
    private class MembersHeartbeatListener implements HeartbeatListener
    {
        @Override
        public void failed( URI server )
        {
            getMember( server ).available = false;
        }

        @Override
        public void alive( URI server )
        {
            getMember( server ).available = true;
        }
    }

    private void memberLeft( URI memberClusterUri )
    {
        Member removedMember = members.remove( memberClusterUri );
        assert removedMember != null : "Tried to remove member that wasn't there";
        life.remove( removedMember );
    }

    // public void debug( String string )
    // {
    // System.out.println( clusterClient.getServerUri() + ":" + string );
    // System.out.println( "{" );
    // for ( Member member : internalGetMembers() )
    // System.out.println( "  " + member );
    // System.out.println( "}" );
    // }
}
