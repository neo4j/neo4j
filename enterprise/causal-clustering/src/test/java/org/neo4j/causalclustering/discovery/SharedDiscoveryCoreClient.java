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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

class SharedDiscoveryCoreClient extends LifecycleAdapter implements CoreTopologyService
{
    private final SharedDiscoveryService sharedDiscoveryService;
    private final MemberId member;
    private final CoreServerInfo coreServerInfo;
    private final Set<Listener> listeners = new LinkedHashSet<>();
    private final Log log;
    private final boolean refusesToBeLeader;
    private final String dbName;
    private final Map<String, MemberId> leaderMap;
    private long term;

    private CoreTopology coreTopology;
    private ReadReplicaTopology readReplicaTopology;

    SharedDiscoveryCoreClient( SharedDiscoveryService sharedDiscoveryService, MemberId member, LogProvider logProvider, Config config )
    {
        this.sharedDiscoveryService = sharedDiscoveryService;
        this.member = member;
        this.coreServerInfo = extractCoreServerInfo( config );
        this.log = logProvider.getLog( getClass() );
        this.refusesToBeLeader = config.get( CausalClusteringSettings.refuse_to_be_leader );
        this.leaderMap = new HashMap<>();
        this.term = -1L;
        this.dbName = config.get( CausalClusteringSettings.database );
    }

    @Override
    public synchronized void addCoreTopologyListener( Listener listener )
    {
        listeners.add( listener );
        listener.onCoreTopologyChange( coreTopology );
    }

    @Override
    //TODO: Update logic here to account for dbName in the clusterIds
    public boolean setClusterId( ClusterId clusterId, String dbName )
    {
        return sharedDiscoveryService.casClusterId( clusterId );
    }

    @Override
    public Map<MemberId,RoleInfo> allCoreRoles()
    {
        return sharedDiscoveryService.getRoleMap();
    }

    @Override
    public void setLeader( MemberId memberId, String dbName, long term )
    {

        //TODO: Actually do more than just dump the existing implementation from hz into here.
        //  need to think about how SharedDiscoveryService implements the ClusterOfCluster concept in its
        // own limited way - because the interfaces are forcing pollution and are making the current features
        // hard to write tests for
        MemberId previousLeaderId = leaderMap.get( dbName );

        //Only want a node to update Hazelcast if it suspects it is the new leader, in order to cut down on
        // potential for issues with erroneous overwrites. Also completely override the entire role map each
        // update, to avoid stale information.
        boolean suspectAmLeader = member.equals( memberId );
        boolean isUpdate = !Optional.ofNullable( previousLeaderId ).equals( Optional.ofNullable( memberId ) );

        if ( suspectAmLeader && isUpdate )
        {
            leaderMap.put( dbName, memberId );

            Set<MemberId> allLeaders = new HashSet<>( leaderMap.values() );
            Map<MemberId,RoleInfo> roleMap = new HashMap<>();

            CoreTopology t = coreTopology;
            for ( MemberId m : t.members().keySet() )
            {
                if ( allLeaders.contains( m ) )
                {
                    roleMap.put( m, RoleInfo.LEADER );
                }
                else
                {
                    roleMap.put( m, RoleInfo.FOLLOWER );
                }
            }

            sharedDiscoveryService.refreshRoles( roleMap );
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
    public ReadReplicaTopology localReadReplicas()
    {
        return readReplicaTopology.filterTopologyByDb( dbName );
    }

    @Override
    public Optional<AdvertisedSocketAddress> findCatchupAddress( MemberId upstream )
    {
        return coreTopology.find( upstream )
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
    public CoreTopology localCoreServers()
    {
        return coreTopology.filterTopologyByDb( dbName );
    }

    @Override
    public String localDBName()
    {
        return null;
    }

    synchronized void onCoreTopologyChange( CoreTopology coreTopology )
    {
        log.info( "Notified of core topology change " + coreTopology );
        this.coreTopology = coreTopology;
        for ( Listener listener : listeners )
        {
            listener.onCoreTopologyChange( coreTopology );
        }
    }

    synchronized void onReadReplicaTopologyChange( ReadReplicaTopology readReplicaTopology )
    {
        log.info( "Notified of read replica topology change " + readReplicaTopology );
        this.readReplicaTopology = readReplicaTopology;
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
