/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.GroupProperties;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.coreedge.core.CoreEdgeClusterSettings;
import org.neo4j.coreedge.identity.ClusterId;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.coreedge.messaging.address.AdvertisedSocketAddress;
import org.neo4j.coreedge.messaging.address.ListenSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

class HazelcastCoreTopologyService extends LifecycleAdapter implements CoreTopologyService, MembershipListener
{
    private final Config config;
    private final MemberId myself;
    private final DiscoveredMemberRepository discoveredMemberRepository;
    private final Log log;
    private final CoreTopologyListenerService listenerService;
    private String membershipRegistrationId;

    private HazelcastInstance hazelcastInstance;

    HazelcastCoreTopologyService( Config config, MemberId myself, DiscoveredMemberRepository discoveredMemberRepository,
            LogProvider logProvider )
    {
        this.config = config;
        this.myself = myself;
        this.discoveredMemberRepository = discoveredMemberRepository;
        this.listenerService = new CoreTopologyListenerService();
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void addCoreTopologyListener( Listener listener )
    {
        listenerService.addCoreTopologyListener( listener );
        listener.onCoreTopologyChange();
    }

    @Override
    public boolean publishClusterId( ClusterId clusterId )
    {
        return HazelcastClusterTopology.casClusterId( hazelcastInstance, clusterId );
    }

    @Override
    public void memberAdded( MembershipEvent membershipEvent )
    {
        log.info( "Core member added %s", membershipEvent );
        log.info( "Current topology is %s", currentTopology() );
        notifyMembershipChange();
    }

    private void notifyMembershipChange()
    {
        Set<AdvertisedSocketAddress> members = hazelcastInstance.getCluster().getMembers().stream()
                .map( member -> new AdvertisedSocketAddress(
                        String.format( "%s:%d", member.getSocketAddress().getHostName(),
                                member.getSocketAddress().getPort() ) ) ).collect( Collectors.toSet() );
        discoveredMemberRepository.store( members );
        listenerService.notifyListeners();
    }

    @Override
    public void memberRemoved( MembershipEvent membershipEvent )
    {
        log.info( "Core member removed %s", membershipEvent );
        log.info( "Current topology is %s", currentTopology() );
        notifyMembershipChange();
    }

    @Override
    public void memberAttributeChanged( MemberAttributeEvent memberAttributeEvent )
    {
    }

    @Override
    public void start()
    {
        hazelcastInstance = createHazelcastInstance();
        log.info( "Cluster discovery service started" );
        membershipRegistrationId = hazelcastInstance.getCluster().addMembershipListener( this );
        notifyMembershipChange();
    }

    @Override
    public void stop()
    {
        try
        {
            hazelcastInstance.getCluster().removeMembershipListener( membershipRegistrationId );
            hazelcastInstance.shutdown();
        }
        catch ( Throwable e )
        {
            log.warn( "Failed to stop Hazelcast", e );
        }
    }

    private HazelcastInstance createHazelcastInstance()
    {
        System.setProperty( GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1" );

        JoinConfig joinConfig = new JoinConfig();
        joinConfig.getMulticastConfig().setEnabled( false );
        TcpIpConfig tcpIpConfig = joinConfig.getTcpIpConfig();
        tcpIpConfig.setEnabled( true );

        List<AdvertisedSocketAddress> initialMembers =
                config.get( CoreEdgeClusterSettings.initial_discovery_members );
        for ( AdvertisedSocketAddress address : initialMembers )
        {
            tcpIpConfig.addMember( address.toString() );
        }
        Set<AdvertisedSocketAddress> previouslySeenMembers = discoveredMemberRepository.previouslyDiscoveredMembers();
        for ( AdvertisedSocketAddress seenAddress : previouslySeenMembers )
        {
            tcpIpConfig.addMember( seenAddress.toString() );
        }
        log.info( String.format( "Discovering cluster with initial members: %s and previously seen members: %s",
                initialMembers, previouslySeenMembers ) );

        NetworkConfig networkConfig = new NetworkConfig();
        ListenSocketAddress hazelcastAddress = config.get( CoreEdgeClusterSettings.discovery_listen_address );
        networkConfig.setPort( hazelcastAddress.socketAddress().getPort() );
        networkConfig.setJoin( joinConfig );

        com.hazelcast.config.Config c = new com.hazelcast.config.Config();
        c.setProperty( GroupProperties.PROP_INITIAL_MIN_CLUSTER_SIZE,
                String.valueOf( minimumClusterSizeThatCanTolerateOneFaultForExpectedClusterSize() ) );
        c.setProperty( GroupProperties.PROP_LOGGING_TYPE, "none" );

        c.setNetworkConfig( networkConfig );

        MemberAttributeConfig memberAttributeConfig = HazelcastClusterTopology.buildMemberAttributes( myself, config );

        c.setMemberAttributeConfig( memberAttributeConfig );

        return Hazelcast.newHazelcastInstance( c );
    }

    private Integer minimumClusterSizeThatCanTolerateOneFaultForExpectedClusterSize()
    {
        return config.get( CoreEdgeClusterSettings.expected_core_cluster_size ) / 2 + 1;
    }

    @Override
    public ClusterTopology currentTopology()
    {
        return HazelcastClusterTopology.getClusterTopology( hazelcastInstance, log );
    }
}
