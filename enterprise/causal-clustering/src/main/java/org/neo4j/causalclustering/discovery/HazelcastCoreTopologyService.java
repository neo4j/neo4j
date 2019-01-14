/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.discovery;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import org.neo4j.causalclustering.helper.RobustJobSchedulerWrapper;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.Lifecycle;
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

public class HazelcastCoreTopologyService implements CoreTopologyService, Lifecycle
{
    private static final long HAZELCAST_IS_HEALTHY_TIMEOUT_MS = TimeUnit.MINUTES.toMillis( 10 );
    private static final int HAZELCAST_MIN_CLUSTER = 2;

    private final Config config;
    private final MemberId myself;
    private final Log log;
    private final Log userLog;
    private final CoreTopologyListenerService listenerService;
    private final RobustJobSchedulerWrapper scheduler;
    private final long refreshPeriod;
    private final HostnameResolver hostnameResolver;
    private final TopologyServiceRetryStrategy topologyServiceRetryStrategy;
    private final String localDBName;

    private String membershipRegistrationId;
    private JobScheduler.JobHandle refreshJob;

    private final AtomicReference<LeaderInfo> leaderInfo = new AtomicReference<>( LeaderInfo.INITIAL );
    private final AtomicReference<Optional<LeaderInfo>> stepDownInfo = new AtomicReference<>( Optional.empty() );

    private volatile HazelcastInstance hazelcastInstance;

    /* cached data updated during each refresh */
    private volatile CoreTopology coreTopology = CoreTopology.EMPTY;
    private volatile CoreTopology localCoreTopology = CoreTopology.EMPTY;
    private volatile ReadReplicaTopology readReplicaTopology = ReadReplicaTopology.EMPTY;
    private volatile ReadReplicaTopology localReadReplicaTopology = ReadReplicaTopology.EMPTY;
    private volatile Map<MemberId,AdvertisedSocketAddress> catchupAddressMap = new HashMap<>();
    private volatile Map<MemberId,RoleInfo> coreRoles = Collections.emptyMap();

    private Thread startingThread;
    private volatile boolean stopped;

    public HazelcastCoreTopologyService( Config config, MemberId myself, JobScheduler jobScheduler,
            LogProvider logProvider, LogProvider userLogProvider, HostnameResolver hostnameResolver,
            TopologyServiceRetryStrategy topologyServiceRetryStrategy )
    {
        this.config = config;
        this.myself = myself;
        this.listenerService = new CoreTopologyListenerService();
        this.log = logProvider.getLog( getClass() );
        this.scheduler = new RobustJobSchedulerWrapper( jobScheduler, log );
        this.userLog = userLogProvider.getLog( getClass() );
        this.refreshPeriod = config.get( CausalClusteringSettings.cluster_topology_refresh ).toMillis();
        this.hostnameResolver = hostnameResolver;
        this.topologyServiceRetryStrategy = topologyServiceRetryStrategy;
        this.localDBName = config.get( CausalClusteringSettings.database );
    }

    @Override
    public void addLocalCoreTopologyListener( Listener listener )
    {
        listenerService.addCoreTopologyListener( listener );
        listener.onCoreTopologyChange( localCoreServers() );
    }

    @Override
    public void removeLocalCoreTopologyListener( Listener listener )
    {
        listenerService.removeCoreTopologyListener( listener );
    }

    @Override
    public boolean setClusterId( ClusterId clusterId, String dbName ) throws InterruptedException
    {
        waitOnHazelcastInstanceCreation();
        return HazelcastClusterTopology.casClusterId( hazelcastInstance, clusterId, dbName );
    }

    @Override
    public void setLeader( LeaderInfo newLeaderInfo, String dbName )
    {
        leaderInfo.updateAndGet( currentLeaderInfo ->
        {
            if ( currentLeaderInfo.term() < newLeaderInfo.term() && localDBName.equals( dbName ) )
            {
                log.info( "Leader %s updating leader info for database %s and term %s", myself, localDBName, newLeaderInfo.term() );
                return newLeaderInfo;
            }
            else
            {
                return currentLeaderInfo;
            }
        } );
    }

    @Override
    public void handleStepDown( long term, String dbName )
    {
        LeaderInfo localLeaderInfo = leaderInfo.get();

        boolean wasLeaderForDbAndTerm =
                Objects.equals( myself, localLeaderInfo.memberId() ) &&
                localDBName.equals( dbName ) &&
                term == localLeaderInfo.term();

        if ( wasLeaderForDbAndTerm )
        {
            log.info( "Step down event detected. This topology member, with MemberId %s, was leader in term %s, now moving " +
                    "to follower.", myself, localLeaderInfo.term() );
            stepDownInfo.set( Optional.of( localLeaderInfo.stepDown() ) );
        }
    }

    @Override
    public Map<MemberId,RoleInfo> allCoreRoles()
    {
        return coreRoles;
    }

    @Override
    public String localDBName()
    {
        return localDBName;
    }

    @Override
    public void init()
    {
        // nothing to do
    }

    @Override
    public void start()
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
            // We may be interrupted by the stop method after hazelcast returns. This is courtesy and not really
            // necessary
            if ( Thread.currentThread().isInterrupted() )
            {
                return;
            }
            membershipRegistrationId =
                    hazelcastInstance.getCluster().addMembershipListener( new OurMembershipListener() );
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
            refreshJob.cancel( true );
        }

        if ( hazelcastInstance != null && membershipRegistrationId != null )
        {
            try
            {
                hazelcastInstance.getCluster().removeMembershipListener( membershipRegistrationId );
                hazelcastInstance.getLifecycleService().shutdown();
            }
            catch ( Throwable e )
            {
                log.warn( "Failed to stop Hazelcast", e );
            }
        }
    }

    @Override
    public void shutdown()
    {
        // nothing to do
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
        c.setProperty( INITIAL_MIN_CLUSTER_SIZE.getName(), String.valueOf( HAZELCAST_MIN_CLUSTER ) );

        if ( config.get( disable_middleware_logging ) )
        {
            c.setProperty( LOGGING_TYPE.getName(), "none" );
        }

        if ( hazelcastAddress.isIPv6() )
        {
            c.setProperty( PREFER_IPv4_STACK.getName(), "false" );
        }

        c.setNetworkConfig( networkConfig );

        MemberAttributeConfig memberAttributeConfig =
                HazelcastClusterTopology.buildMemberAttributesForCore( myself, config );

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

    @Override
    public CoreTopology allCoreServers()
    {
        // It is perhaps confusing (Or even error inducing) that this core Topology will always contain the cluster id
        // for the database local to the host upon which this method is called.
        // TODO: evaluate returning clusterId = null for global Topologies returned by allCoreServers()
        return coreTopology;
    }

    @Override
    public CoreTopology localCoreServers()
    {
        return localCoreTopology;
    }

    @Override
    public ReadReplicaTopology allReadReplicas()
    {
        return readReplicaTopology;
    }

    @Override
    public ReadReplicaTopology localReadReplicas()
    {
        return localReadReplicaTopology;
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

    private void refreshRoles() throws InterruptedException
    {
        waitOnHazelcastInstanceCreation();
        LeaderInfo localLeaderInfo = leaderInfo.get();
        Optional<LeaderInfo> localStepDownInfo = stepDownInfo.get();

        if ( localStepDownInfo.isPresent() )
        {
            HazelcastClusterTopology.casLeaders( hazelcastInstance, localStepDownInfo.get(), localDBName );
            stepDownInfo.compareAndSet( localStepDownInfo, Optional.empty() );
        }
        else if ( localLeaderInfo.memberId() != null && localLeaderInfo.memberId().equals( myself ) )
        {
            HazelcastClusterTopology.casLeaders( hazelcastInstance, localLeaderInfo, localDBName );
        }

        coreRoles = HazelcastClusterTopology.getCoreRoles( hazelcastInstance, allCoreServers().members().keySet() );
    }

    private synchronized void refreshTopology() throws InterruptedException
    {
        refreshCoreTopology();
        refreshReadReplicaTopology();
        refreshRoles();
        catchupAddressMap = extractCatchupAddressesMap( localCoreServers(), localReadReplicas() );
    }

    private void refreshCoreTopology() throws InterruptedException
    {
        waitOnHazelcastInstanceCreation();

        CoreTopology newCoreTopology = getCoreTopology( hazelcastInstance, config, log );
        TopologyDifference difference = coreTopology.difference( newCoreTopology );

        coreTopology = newCoreTopology;
        localCoreTopology = newCoreTopology.filterTopologyByDb( localDBName );

        if ( difference.hasChanges() )
        {
            log.info( "Core topology changed %s", difference );
            listenerService.notifyListeners( coreTopology );
        }
    }

    private void refreshReadReplicaTopology() throws InterruptedException
    {
        waitOnHazelcastInstanceCreation();

        ReadReplicaTopology newReadReplicaTopology = getReadReplicaTopology( hazelcastInstance, log );
        TopologyDifference difference = readReplicaTopology.difference( newReadReplicaTopology );

        this.readReplicaTopology = newReadReplicaTopology;
        this.localReadReplicaTopology = newReadReplicaTopology.filterTopologyByDb( localDBName );

        if ( difference.hasChanges() )
        {
            log.info( "Read replica topology changed %s", difference );
        }
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
