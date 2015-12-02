/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.discovery;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.GroupProperties;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.coreedge.server.CoreEdgeClusterSettings;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.ListenSocketAddress;
import org.neo4j.helpers.Listeners;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class HazelcastServerLifecycle extends LifecycleAdapter implements CoreDiscoveryService
{
    public static final String CLUSTER_SERVER = "cluster_server";
    public static final String TRANSACTION_SERVER = "transaction_server";
    public static final String SERVER_ID = "server_id";
    public static final String RAFT_SERVER = "raft_server";

    private Config config;
    private HazelcastInstance hazelcastInstance;

    private List<StartupListener> startupListeners = new ArrayList<>();
    private List<MembershipListener> membershipListeners = new ArrayList<>();
    private Map<MembershipListener, String> membershipRegistrationId = new ConcurrentHashMap<>();

    public HazelcastServerLifecycle( Config config )
    {
        this.config = config;
    }

    @Override
    public void addMembershipListener( Listener listener )
    {
        MembershipListenerAdapter hazelcastListener = new MembershipListenerAdapter( listener );
        membershipListeners.add( hazelcastListener );

        if ( hazelcastInstance != null )
        {
            String registrationId = hazelcastInstance.getCluster().addMembershipListener( hazelcastListener );
            membershipRegistrationId.put( hazelcastListener, registrationId );
        }
        listener.onTopologyChange( currentTopology() );
    }

    @Override
    public void removeMembershipListener( Listener listener )
    {
        MembershipListenerAdapter hazelcastListener = new MembershipListenerAdapter( listener );
        membershipListeners.remove( hazelcastListener );
        String registrationId = membershipRegistrationId.remove( hazelcastListener );

        if ( hazelcastInstance != null && registrationId != null )
        {
            hazelcastInstance.getCluster().removeMembershipListener( registrationId );
        }
    }

    public Set<Member> getMembers()
    {
        return hazelcastInstance.getCluster().getMembers();
    }

    @Override
    public void start() throws Throwable
    {
        hazelcastInstance = createHazelcastInstance();

        Listeners.notifyListeners( startupListeners, listener -> listener.hazelcastStarted( hazelcastInstance ) );

        for ( MembershipListener membershipListener : membershipListeners )
        {
            String registrationId = hazelcastInstance.getCluster().addMembershipListener( membershipListener );
            membershipRegistrationId.put( membershipListener, registrationId );
        }
    }

    @Override
    public void stop() throws Throwable
    {
        try
        {
            hazelcastInstance.shutdown();
        }
        catch ( Throwable ignored )
        {
            // TODO: log something if important
        }
    }

    private HazelcastInstance createHazelcastInstance()
    {
        System.setProperty( GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1" );

        JoinConfig joinConfig = new JoinConfig();
        joinConfig.getMulticastConfig().setEnabled( false );
        TcpIpConfig tcpIpConfig = joinConfig.getTcpIpConfig();
        tcpIpConfig.setEnabled( true );

        for ( AdvertisedSocketAddress address : config.get( CoreEdgeClusterSettings.initial_core_cluster_members ) )
        {
            tcpIpConfig.addMember( address.toString() );
        }

        NetworkConfig networkConfig = new NetworkConfig();
        ListenSocketAddress address = config.get( CoreEdgeClusterSettings.cluster_listen_address );
        networkConfig.setPort( address.socketAddress().getPort() );
        networkConfig.setJoin( joinConfig );
        String instanceName = String.valueOf( config.get( ClusterSettings.server_id ).toIntegerIndex() );

        com.hazelcast.config.Config c = new com.hazelcast.config.Config( instanceName );
        c.setProperty( GroupProperties.PROP_INITIAL_MIN_CLUSTER_SIZE,
                String.valueOf( minimumClusterSizeThatCanTolerateOneFaultForExpectedClusterSize() ) );
        c.setProperty( GroupProperties.PROP_LOGGING_TYPE, "none" );

        c.setNetworkConfig( networkConfig );
        c.getGroupConfig().setName( config.get( ClusterSettings.cluster_name ) );

        MemberAttributeConfig memberAttributeConfig = new MemberAttributeConfig();
        memberAttributeConfig.setIntAttribute( SERVER_ID, config.get( ClusterSettings.server_id ).toIntegerIndex() );

        memberAttributeConfig.setStringAttribute( CLUSTER_SERVER, address.toString() );

        AdvertisedSocketAddress transactionSource = config.get( CoreEdgeClusterSettings.transaction_advertised_address );
        memberAttributeConfig.setStringAttribute( TRANSACTION_SERVER, transactionSource.toString() );

        AdvertisedSocketAddress advertisedAddress = config.get( CoreEdgeClusterSettings.raft_advertised_address );
        memberAttributeConfig.setStringAttribute( RAFT_SERVER, advertisedAddress.toString() );

        c.setMemberAttributeConfig( memberAttributeConfig );

        return Hazelcast.newHazelcastInstance( c );
    }

    private Integer minimumClusterSizeThatCanTolerateOneFaultForExpectedClusterSize()
    {
        return config.get( CoreEdgeClusterSettings.expected_core_cluster_size ) / 2 + 1;
    }

    @Override
    public HazelcastClusterTopology currentTopology()
    {
        return new HazelcastClusterTopology( hazelcastInstance );
    }

    public interface StartupListener
    {
        void hazelcastStarted( HazelcastInstance hazelcastInstance );
    }

    private class MembershipListenerAdapter implements MembershipListener
    {
        private final Listener listener;

        public MembershipListenerAdapter( Listener listener )
        {
            this.listener = listener;
        }

        @Override
        public void memberAdded( MembershipEvent membershipEvent )
        {
            HazelcastClusterTopology clusterTopology = new HazelcastClusterTopology( hazelcastInstance );
            listener.onTopologyChange( clusterTopology );
        }

        @Override
        public void memberRemoved( MembershipEvent membershipEvent )
        {
            HazelcastClusterTopology clusterTopology = new HazelcastClusterTopology( hazelcastInstance );
            listener.onTopologyChange( clusterTopology );
        }

        @Override
        public void memberAttributeChanged( MemberAttributeEvent memberAttributeEvent )
        {
        }
    }
}
