/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import com.hazelcast.mapreduce.Job;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.helper.RobustJobSchedulerWrapper;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

class SharedDiscoveryCoreClient extends AbstractTopologyService implements CoreTopologyService
{
    private final SharedDiscoveryService sharedDiscoveryService;
    private final MemberId member;
    private final CoreServerInfo coreServerInfo;
    private final CoreTopologyListenerService listenerService;
    private final Log log;
    private final boolean refusesToBeLeader;
    private final String localDBName;

    private MemberId localLeader;
    private long term;


    private CoreTopology coreTopology;
    private ReadReplicaTopology readReplicaTopology;

    SharedDiscoveryCoreClient( SharedDiscoveryService sharedDiscoveryService,
            MemberId member, LogProvider logProvider, Config config )
    {
        this.sharedDiscoveryService = sharedDiscoveryService;
        this.listenerService = new CoreTopologyListenerService();
        this.member = member;
        this.coreServerInfo = extractCoreServerInfo( config );
        this.log = logProvider.getLog( getClass() );
        this.refusesToBeLeader = config.get( CausalClusteringSettings.refuse_to_be_leader );
        this.term = -1L;
        this.localDBName = config.get( CausalClusteringSettings.database );
    }

    @Override
    public synchronized void addCoreTopologyListener( Listener listener )
    {
        listenerService.addCoreTopologyListener( listener );
        listener.onCoreTopologyChange( localCoreServers() );
    }

    @Override
    //TODO: Update logic here to account for dbName in the clusterIds
    public boolean setClusterId( ClusterId clusterId, String dbName )
    {
        return sharedDiscoveryService.casClusterId( clusterId, dbName );
    }

    @Override
    public Map<MemberId,RoleInfo> allCoreRoles()
    {
        return sharedDiscoveryService.getRoleMap();
    }

    @Override
    public void setLeader( MemberId memberId, String dbName, long term )
    {
        if ( this.term < term )
        {
            localLeader = memberId;
            this.term = term;
        }
    }

    @Override
    public void start() throws InterruptedException
    {
        sharedDiscoveryService.registerCoreMember( member, coreServerInfo, this );
        log.info( "Registered core server %s", member );
        sharedDiscoveryService.waitForClusterFormation();
        log.info( "Cluster formed" );
    }

    @Override
    public CoreTopology localCoreServers()
    {
        return super.localCoreServers();
    }

    @Override
    public void stop()
    {
        sharedDiscoveryService.unRegisterCoreMember( member, this );
        log.info( "Unregistered core server %s", member );
    }

    @Override
    public ReadReplicaTopology allReadReplicas()
    {
        return readReplicaTopology;
    }

    @Override
    public Optional<AdvertisedSocketAddress> findCatchupAddress( MemberId upstream )
    {
        return localCoreServers().find( upstream )
                .map( info -> Optional.of( info.getCatchupServer() ) )
                .orElseGet( () -> readReplicaTopology.find( upstream )
                        .map( ReadReplicaInfo::getCatchupServer ) );
    }

    @Override
    public CoreTopology allCoreServers()
    {
        return this.coreTopology;
    }

    @Override
    public String localDBName()
    {
        return localDBName;
    }


    synchronized void onCoreTopologyChange( CoreTopology coreTopology )
    {
        log.info( "Notified of core topology change " + coreTopology );
        this.coreTopology = coreTopology;
        listenerService.notifyListeners( coreTopology );
    }

    synchronized void onReadReplicaTopologyChange( ReadReplicaTopology readReplicaTopology )
    {
        log.info( "Notified of read replica topology change " + readReplicaTopology );
        this.readReplicaTopology = readReplicaTopology;
    }

    private void updateLeader()
    {
        sharedDiscoveryService.casLeaders( localLeader, term, localDBName );
    }

    private static CoreServerInfo extractCoreServerInfo( Config config )
    {
        AdvertisedSocketAddress raftAddress = config.get( CausalClusteringSettings.raft_advertised_address );
        AdvertisedSocketAddress transactionSource = config.get( CausalClusteringSettings.transaction_advertised_address );
        ClientConnectorAddresses clientConnectorAddresses = ClientConnectorAddresses.extractFromConfig( config );
        String dbName = config.get( CausalClusteringSettings.database );

        return new CoreServerInfo( raftAddress, transactionSource, clientConnectorAddresses, dbName );
    }

    public boolean refusesToBeLeader()
    {
        return refusesToBeLeader;
    }
}
