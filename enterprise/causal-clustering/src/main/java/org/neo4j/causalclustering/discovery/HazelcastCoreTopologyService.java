/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.discovery;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.helper.RobustJobSchedulerWrapper;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

import static com.hazelcast.spi.properties.GroupProperty.INITIAL_MIN_CLUSTER_SIZE;
import static com.hazelcast.spi.properties.GroupProperty.LOGGING_TYPE;
import static com.hazelcast.spi.properties.GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static com.hazelcast.spi.properties.GroupProperty.PREFER_IPv4_STACK;
import static com.hazelcast.spi.properties.GroupProperty.WAIT_SECONDS_BEFORE_JOIN;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.disable_middleware_logging;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.discovery_listen_address;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.initial_discovery_members;
import static org.neo4j.causalclustering.discovery.HazelcastClusterTopology.extractCatchupAddressesMap;
import static org.neo4j.causalclustering.discovery.HazelcastClusterTopology.getCoreTopology;
import static org.neo4j.causalclustering.discovery.HazelcastClusterTopology.getReadReplicaTopology;
import static org.neo4j.causalclustering.discovery.HazelcastClusterTopology.refreshGroups;

class HazelcastCoreTopologyService extends LifecycleAdapter implements CoreTopologyService
{
    private static final long HAZELCAST_IS_HEALTHY_TIMEOUT_MS = TimeUnit.MINUTES.toMillis( 10 );
    private final Config config;
    private final MemberId myself;
    private final Log log;
    private final Log userLog;
    private final CoreTopologyListenerService listenerService;
    private final RobustJobSchedulerWrapper scheduler;
    private final long refreshPeriod;
    private final LogProvider logProvider;
    private final HostnameResolver hostnameResolver;
    private final TopologyServiceRetryStrategy topologyServiceRetryStrategy;

    private String membershipRegistrationId;
    private JobScheduler.JobHandle refreshJob;

    private volatile HazelcastInstance hazelcastInstance;
    private volatile ReadReplicaTopology readReplicaTopology = ReadReplicaTopology.EMPTY;
    private volatile CoreTopology coreTopology = CoreTopology.EMPTY;
    private volatile Map<MemberId,AdvertisedSocketAddress> catchupAddressMap = new HashMap<>();

    private Thread startingThread;
    private volatile boolean stopped;

    HazelcastCoreTopologyService( Config config, MemberId myself, JobScheduler jobScheduler,
            LogProvider logProvider, LogProvider userLogProvider, HostnameResolver hostnameResolver,
            TopologyServiceRetryStrategy topologyServiceRetryStrategy )
    {
        this.config = config;
        this.myself = myself;
        this.listenerService = new CoreTopologyListenerService();
        this.log = logProvider.getLog( getClass() );
        this.logProvider = logProvider;
        this.scheduler = new RobustJobSchedulerWrapper( jobScheduler, log );
        this.userLog = userLogProvider.getLog( getClass() );
        this.refreshPeriod = config.get( CausalClusteringSettings.cluster_topology_refresh ).toMillis();
        this.hostnameResolver = hostnameResolver;
        this.topologyServiceRetryStrategy = topologyServiceRetryStrategy;
    }

    @Override
    public void addCoreTopologyListener( Listener listener )
    {
        listenerService.addCoreTopologyListener( listener );
        listener.onCoreTopologyChange( coreTopology );
    }

    @Override
    public boolean setClusterId( ClusterId clusterId ) throws InterruptedException
    {
        waitOnHazelcastInstanceCreation();
        return HazelcastClusterTopology.casClusterId( hazelcastInstance, clusterId );
    }

    @Override
    public void start() throws Throwable
    {
        /*
         * We will start hazelcast in its own thread. Hazelcast blocks until the minimum cluster size is available
         * and during that block it ignores interrupts. This blocks the whole startup process and since it is the
         * main thread that controls lifecycle and the main thread is not daemon, it will block ignoring signals
         * and any shutdown attempts. The solution is to start hazelcast instance creation in its own thread which
         * we set as daemon. All subsequent uses of hazelcastInstance in this class will still block on it being
         * available (see waitOnHazelcastInstanceCreation() ) but they do so while checking for interrupt and
         * exiting if one happens. This provides us with a way to exit before hazelcastInstance creation completes.
         */
        startingThread = new Thread( () ->
        {
            log.info( "Cluster discovery service starting" );
            hazelcastInstance = createHazelcastInstance();
            // We may be interrupted by the stop method after hazelcast returns. This is courtesy and not really necessary
            if ( Thread.currentThread().isInterrupted() )
            {
                return;
            }
            membershipRegistrationId = hazelcastInstance.getCluster().addMembershipListener( new OurMembershipListener() );
            refreshJob = scheduler.scheduleRecurring( "TopologyRefresh", refreshPeriod,
                    HazelcastCoreTopologyService.this::refreshTopology );
            log.info( "Cluster discovery service started" );
        } );
        startingThread.setDaemon( true );
        startingThread.setName( "HZ Starting Thread" );
        startingThread.start();
    }

    @Override
    public void stop()
    {
        log.info( String.format( "HazelcastCoreTopologyService stopping and unbinding from %s",
                config.get( discovery_listen_address ) ) );

        // Interrupt the starting thread. Not really necessary, just cleaner exit
        startingThread.interrupt();
        // Flag to notify waiters
        stopped = true;

        if ( refreshJob != null )
        {
            scheduler.cancelAndWaitTermination( refreshJob );
        }

        if ( hazelcastInstance != null && membershipRegistrationId != null )
        {
            try
            {
                hazelcastInstance.getCluster().removeMembershipListener( membershipRegistrationId );
                hazelcastInstance.getLifecycleService().terminate();
            }
            catch ( Throwable e )
            {
                log.warn( "Failed to stop Hazelcast", e );
            }
        }
    }

    private HazelcastInstance createHazelcastInstance()
    {
        System.setProperty( WAIT_SECONDS_BEFORE_JOIN.getName(), "1" );

        JoinConfig joinConfig = new JoinConfig();
        joinConfig.getMulticastConfig().setEnabled( false );
        TcpIpConfig tcpIpConfig = joinConfig.getTcpIpConfig();
        tcpIpConfig.setEnabled( true );

        List<AdvertisedSocketAddress> initialMembers = config.get( initial_discovery_members );
        for ( AdvertisedSocketAddress address : initialMembers )
        {
            for ( AdvertisedSocketAddress advertisedSocketAddress : hostnameResolver.resolve( address ) )
            {
                tcpIpConfig.addMember( advertisedSocketAddress.toString() );
            }
        }

        ListenSocketAddress hazelcastAddress = config.get( discovery_listen_address );
        NetworkConfig networkConfig = new NetworkConfig();

        if ( !hazelcastAddress.isWildcard() )
        {
            InterfacesConfig interfaces = new InterfacesConfig();
            interfaces.addInterface( hazelcastAddress.getHostname() );
            interfaces.setEnabled( true );
            networkConfig.setInterfaces( interfaces );
        }

        additionalConfig( networkConfig, logProvider );

        networkConfig.setPort( hazelcastAddress.getPort() );
        networkConfig.setJoin( joinConfig );
        networkConfig.setPortAutoIncrement( false );

        // We'll use election_timeout as a base value to calculate HZ timeouts. We multiply by 1.5
        Long electionTimeoutMillis = config.get( CausalClusteringSettings.leader_election_timeout ).toMillis();
        Long baseHazelcastTimeoutMillis = (3 * electionTimeoutMillis) / 2;
        /*
         * Some HZ settings require the value in seconds. Adding the divider and subtracting 1 is equivalent to the
         * ceiling function for integers ( Math.ceil() returns double ). Anything < 0 will return 0, any
         * multiple of 1000 returns the result of the division by 1000, any non multiple of 1000 returns the result
         * of the division + 1. In other words, values in millis are rounded up.
         */
        long baseHazelcastTimeoutSeconds = (baseHazelcastTimeoutMillis + 1000 - 1) / 1000;

        com.hazelcast.config.Config c = new com.hazelcast.config.Config();
        c.setProperty( OPERATION_CALL_TIMEOUT_MILLIS.getName(), String.valueOf( baseHazelcastTimeoutMillis ) );
        c.setProperty( MERGE_NEXT_RUN_DELAY_SECONDS.getName(), String.valueOf( baseHazelcastTimeoutSeconds ) );
        c.setProperty( MERGE_FIRST_RUN_DELAY_SECONDS.getName(), String.valueOf( baseHazelcastTimeoutSeconds ) );
        c.setProperty( INITIAL_MIN_CLUSTER_SIZE.getName(),
                String.valueOf( minimumClusterSizeThatCanTolerateOneFaultForExpectedClusterSize() ) );

        if ( config.get( disable_middleware_logging ) )
        {
            c.setProperty( LOGGING_TYPE.getName(), "none" );
        }

        if ( hazelcastAddress.isIPv6() )
        {
            c.setProperty( PREFER_IPv4_STACK.getName(), "false" );
        }

        c.setNetworkConfig( networkConfig );

        MemberAttributeConfig memberAttributeConfig = HazelcastClusterTopology.buildMemberAttributesForCore( myself, config );

        c.setMemberAttributeConfig( memberAttributeConfig );
        logConnectionInfo( initialMembers );

        JobScheduler.JobHandle logJob = scheduler.schedule( "HazelcastHealth", HAZELCAST_IS_HEALTHY_TIMEOUT_MS,
                () -> log.warn( "The server has not been able to connect in a timely fashion to the " +
                        "cluster. Please consult the logs for more details. Rebooting the server may " +
                        "solve the problem." ) );

        try
        {
            hazelcastInstance = Hazelcast.newHazelcastInstance( c );
            logJob.cancel( true );
        }
        catch ( HazelcastException e )
        {
            String errorMessage = String.format( "Hazelcast was unable to start with setting: %s = %s",
                    discovery_listen_address.name(), config.get( discovery_listen_address ) );
            userLog.error( errorMessage );
            log.error( errorMessage, e );
            throw new RuntimeException( e );
        }

        List<String> groups = config.get( CausalClusteringSettings.server_groups );
        refreshGroups( hazelcastInstance, myself.getUuid().toString(), groups );

        return hazelcastInstance;
    }

    protected void additionalConfig( NetworkConfig networkConfig, LogProvider logProvider )
    {

    }

    private void logConnectionInfo( List<AdvertisedSocketAddress> initialMembers )
    {
        userLog.info( "My connection info: " + "[\n\tDiscovery:   listen=%s, advertised=%s," +
                        "\n\tTransaction: listen=%s, advertised=%s, " + "\n\tRaft:        listen=%s, advertised=%s, " +
                        "\n\tClient Connector Addresses: %s" + "\n]", config.get( discovery_listen_address ),
                config.get( CausalClusteringSettings.discovery_advertised_address ),
                config.get( CausalClusteringSettings.transaction_listen_address ),
                config.get( CausalClusteringSettings.transaction_advertised_address ),
                config.get( CausalClusteringSettings.raft_listen_address ),
                config.get( CausalClusteringSettings.raft_advertised_address ),
                ClientConnectorAddresses.extractFromConfig( config ) );
        userLog.info( "Discovering cluster with initial members: " + initialMembers );
        userLog.info( "Attempting to connect to the other cluster members before continuing..." );
    }

    private Integer minimumClusterSizeThatCanTolerateOneFaultForExpectedClusterSize()
    {
        return config.get( CausalClusteringSettings.expected_core_cluster_size ) / 2 + 1;
    }

    @Override
    public CoreTopology coreServers()
    {
        return coreTopology;
    }

    @Override
    public ReadReplicaTopology readReplicas()
    {
        return readReplicaTopology;
    }

    @Override
    public Optional<AdvertisedSocketAddress> findCatchupAddress( MemberId memberId )
    {
        return topologyServiceRetryStrategy.apply( memberId, this::retrieveSocketAddress, Optional::isPresent );
    }

    private Optional<AdvertisedSocketAddress> retrieveSocketAddress( MemberId memberId )
    {
        return Optional.ofNullable( catchupAddressMap.get( memberId ) );
    }

    private synchronized void refreshTopology() throws InterruptedException
    {
        refreshCoreTopology();
        refreshReadReplicaTopology();
        catchupAddressMap = extractCatchupAddressesMap( coreTopology, readReplicaTopology );
    }

    private void refreshCoreTopology() throws InterruptedException
    {
        waitOnHazelcastInstanceCreation();
        CoreTopology newCoreTopology = getCoreTopology( hazelcastInstance, config, log );
        TopologyDifference difference = coreTopology.difference( newCoreTopology );
        if ( difference.hasChanges() )
        {
            log.info( "Core topology changed %s", difference );
        }

        this.coreTopology = newCoreTopology;
        listenerService.notifyListeners( this.coreTopology );

    }

    private void refreshReadReplicaTopology() throws InterruptedException
    {
        waitOnHazelcastInstanceCreation();
        ReadReplicaTopology newReadReplicaTopology = getReadReplicaTopology( hazelcastInstance, log );

        TopologyDifference difference = readReplicaTopology.difference( newReadReplicaTopology );
        if ( difference.hasChanges() )
        {
            log.info( "Read replica topology changed %s", difference );
        }

        this.readReplicaTopology = newReadReplicaTopology;
    }

    /*
     * Waits for hazelcastInstance to be set. It also checks for the stopped flag which is probably not really
     * necessary. Nevertheless, since hazelcastInstance is created and set by a separate thread to avoid blocking
     * ( see start() ), all accesses to it must be guarded by this method.
     */
    private void waitOnHazelcastInstanceCreation() throws InterruptedException
    {
        while ( hazelcastInstance == null && !stopped )
        {
            Thread.sleep( 200 );
        }
    }

    private class OurMembershipListener implements MembershipListener
    {
        @Override
        public void memberAdded( MembershipEvent membershipEvent )
        {
            log.info( "Core member added %s", membershipEvent );
            try
            {
                refreshTopology();
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
        }

        @Override
        public void memberRemoved( MembershipEvent membershipEvent )
        {
            log.info( "Core member removed %s", membershipEvent );
            try
            {
                refreshTopology();
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
        }

        @Override
        public void memberAttributeChanged( MemberAttributeEvent memberAttributeEvent )
        {
            log.info( "Core member attribute changed %s", memberAttributeEvent );
        }
    }

}
