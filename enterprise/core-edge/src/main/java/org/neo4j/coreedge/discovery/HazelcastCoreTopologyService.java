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

import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.map.impl.MapListenerAdapter;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.coreedge.core.CoreEdgeClusterSettings;
import org.neo4j.coreedge.identity.ClusterId;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.coreedge.discovery.HazelcastClusterTopology.EDGE_SERVER_BOLT_ADDRESS_MAP_NAME;
import static org.neo4j.kernel.impl.util.JobScheduler.SchedulingStrategy.POOLED;

class HazelcastCoreTopologyService extends LifecycleAdapter implements CoreTopologyService
{
    private final Config config;
    private final MemberId myself;
    private final Log log;
    private final Log userLog;
    private final CoreTopologyListenerService listenerService;
    private final JobScheduler scheduler;
    private String membershipRegistrationId;
    private String mapRegistrationId;

    private JobScheduler.JobHandle jobHandle;

    private HazelcastInstance hazelcastInstance;
    private volatile EdgeTopology latestEdgeTopology;
    private volatile CoreTopology latestCoreTopology;

    HazelcastCoreTopologyService( Config config, MemberId myself, JobScheduler jobScheduler, LogProvider logProvider,
            LogProvider userLogProvider )
    {
        this.config = config;
        this.myself = myself;
        this.scheduler = jobScheduler;
        this.listenerService = new CoreTopologyListenerService();
        this.log = logProvider.getLog( getClass() );
        this.userLog = userLogProvider.getLog( getClass() );
    }

    @Override
    public void addCoreTopologyListener( Listener listener )
    {
        listenerService.addCoreTopologyListener( listener );
        listener.onCoreTopologyChange( coreServers() );
    }

    @Override
    public boolean setClusterId( ClusterId clusterId )
    {
        return HazelcastClusterTopology.casClusterId( hazelcastInstance, clusterId );
    }

    @Override
    public void start()
    {
        hazelcastInstance = createHazelcastInstance();
        log.info( "Cluster discovery service started" );
        membershipRegistrationId = hazelcastInstance.getCluster().addMembershipListener( new OurMembershipListener() );
        mapRegistrationId = hazelcastInstance.getMap( EDGE_SERVER_BOLT_ADDRESS_MAP_NAME )
                .addEntryListener( new OurEntryListener(), true );
        refreshCoreTopology();
        refreshEdgeTopology();
        listenerService.notifyListeners( coreServers() );

        try
        {
            scheduler.start();
        }
        catch ( Throwable throwable )
        {
            log.debug( "Failed to start job scheduler." );
            return;
        }

        JobScheduler.Group group = new JobScheduler.Group( "Scheduler", POOLED );
        jobHandle = this.scheduler.scheduleRecurring( group, () ->
        {
            refreshCoreTopology();
            refreshEdgeTopology();
        }, config.get( CoreEdgeClusterSettings.cluster_topology_refresh ), TimeUnit.MILLISECONDS );
    }

    @Override
    public void stop()
    {
        log.info( String.format( "HazelcastCoreTopologyService stopping and unbinding from %s",
                config.get( CoreEdgeClusterSettings.discovery_listen_address ) ) );
        try
        {
            hazelcastInstance.getCluster().removeMembershipListener( membershipRegistrationId );
            hazelcastInstance.getMap( EDGE_SERVER_BOLT_ADDRESS_MAP_NAME ).removeEntryListener( mapRegistrationId );
            hazelcastInstance.getLifecycleService().terminate();
        }
        catch ( Throwable e )
        {
            log.warn( "Failed to stop Hazelcast", e );
        }
        finally
        {
            jobHandle.cancel( true );
        }
    }

    private HazelcastInstance createHazelcastInstance()
    {
        System.setProperty( GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1" );

        JoinConfig joinConfig = new JoinConfig();
        joinConfig.getMulticastConfig().setEnabled( false );
        TcpIpConfig tcpIpConfig = joinConfig.getTcpIpConfig();
        tcpIpConfig.setEnabled( true );

        List<AdvertisedSocketAddress> initialMembers = config.get( CoreEdgeClusterSettings.initial_discovery_members );
        for ( AdvertisedSocketAddress address : initialMembers )
        {
            tcpIpConfig.addMember( address.toString() );
        }
        log.info( "Discovering cluster with initial members: " + initialMembers );

        NetworkConfig networkConfig = new NetworkConfig();
        Setting<ListenSocketAddress> discovery_listen_address = CoreEdgeClusterSettings.discovery_listen_address;
        ListenSocketAddress hazelcastAddress = config.get( discovery_listen_address );
        InterfacesConfig interfaces = new InterfacesConfig();
        interfaces.addInterface( hazelcastAddress.getHostname() );
        networkConfig.setInterfaces( interfaces );
        networkConfig.setPort( hazelcastAddress.getPort() );
        networkConfig.setJoin( joinConfig );
        networkConfig.setPortAutoIncrement( false );
        com.hazelcast.config.Config c = new com.hazelcast.config.Config();
        c.setProperty( GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS, "10000" );
        c.setProperty( GroupProperties.PROP_INITIAL_MIN_CLUSTER_SIZE,
                String.valueOf( minimumClusterSizeThatCanTolerateOneFaultForExpectedClusterSize() ) );
        c.setProperty( GroupProperties.PROP_LOGGING_TYPE, "none" );

        c.setNetworkConfig( networkConfig );

        MemberAttributeConfig memberAttributeConfig = HazelcastClusterTopology.buildMemberAttributes( myself, config );

        c.setMemberAttributeConfig( memberAttributeConfig );
        userLog.info( "Waiting for other members to join cluster before continuing..." );
        try
        {
            hazelcastInstance = Hazelcast.newHazelcastInstance( c );
        }
        catch ( HazelcastException e )
        {
            String errorMessage = String.format( "Hazelcast was unable to start with setting: %s = %s",
                    discovery_listen_address.name(), config.get( discovery_listen_address ) );
            userLog.error( errorMessage );
            log.error( errorMessage, e );
            throw new RuntimeException( e );
        }

        return hazelcastInstance;
    }

    private Integer minimumClusterSizeThatCanTolerateOneFaultForExpectedClusterSize()
    {
        return config.get( CoreEdgeClusterSettings.expected_core_cluster_size ) / 2 + 1;
    }

    @Override
    public EdgeTopology edgeServers()
    {
        return latestEdgeTopology;
    }

    @Override
    public CoreTopology coreServers()
    {
        return latestCoreTopology;
    }

    @Override
    public void refreshCoreTopology()
    {
        latestCoreTopology = HazelcastClusterTopology.getCoreTopology( hazelcastInstance, log );
        log.info( "Current core topology is %s", coreServers() );
        listenerService.notifyListeners( coreServers() );
    }

    private void refreshEdgeTopology()
    {
        latestEdgeTopology = HazelcastClusterTopology.getEdgeTopology( hazelcastInstance, log );
    }

    private class OurEntryListener extends MapListenerAdapter
    {
        @Override
        public void onEntryEvent( EntryEvent event )
        {
            refreshEdgeTopology();
        }
    }

    private class OurMembershipListener implements MembershipListener
    {
        @Override
        public void memberAdded( MembershipEvent membershipEvent )
        {
            log.info( "Core member added %s", membershipEvent );
            refreshCoreTopology();
        }

        @Override
        public void memberRemoved( MembershipEvent membershipEvent )
        {
            log.info( "Core member removed %s", membershipEvent );
            refreshCoreTopology();
        }

        @Override
        public void memberAttributeChanged( MemberAttributeEvent memberAttributeEvent )
        {
        }
    }
}
