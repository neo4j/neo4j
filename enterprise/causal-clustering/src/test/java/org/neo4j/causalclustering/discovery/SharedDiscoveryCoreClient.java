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

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

class SharedDiscoveryCoreClient extends AbstractTopologyService implements CoreTopologyService, Comparable<SharedDiscoveryCoreClient>
{
    private final SharedDiscoveryService sharedDiscoveryService;
    private final MemberId myself;
    private final CoreServerInfo coreServerInfo;
    private final CoreTopologyListenerService listenerService;
    private final Log log;
    private final boolean refusesToBeLeader;
    private final String localDBName;

    private volatile LeaderInfo leaderInfo = LeaderInfo.INITIAL;
    private volatile CoreTopology coreTopology;
    private volatile ReadReplicaTopology readReplicaTopology;

    SharedDiscoveryCoreClient( SharedDiscoveryService sharedDiscoveryService,
            MemberId member, LogProvider logProvider, Config config )
    {
        this.sharedDiscoveryService = sharedDiscoveryService;
        this.listenerService = new CoreTopologyListenerService();
        this.myself = member;
        this.coreServerInfo = extractCoreServerInfo( config );
        this.log = logProvider.getLog( getClass() );
        this.refusesToBeLeader = config.get( CausalClusteringSettings.refuse_to_be_leader );
        this.localDBName = config.get( CausalClusteringSettings.database );
    }

    @Override
    public int compareTo( SharedDiscoveryCoreClient o )
    {
        return Optional.ofNullable( o ).map( c -> c.myself.getUuid().compareTo( this.myself.getUuid() ) ).orElse( -1 );
    }

    @Override
    public synchronized void addLocalCoreTopologyListener( Listener listener )
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
    public boolean setClusterId( ClusterId clusterId, String dbName )
    {
        return sharedDiscoveryService.casClusterId( clusterId, dbName );
    }

    @Override
    public Map<MemberId,RoleInfo> allCoreRoles()
    {
        return sharedDiscoveryService.getCoreRoles();
    }

    @Override
    public void setLeader( LeaderInfo newLeader, String dbName )
    {
        if ( this.leaderInfo.term() < newLeader.term() && newLeader.memberId() != null )
        {
            this.leaderInfo = newLeader;
            sharedDiscoveryService.casLeaders( newLeader, localDBName );
        }
    }

    @Override
    public void start() throws InterruptedException
    {
        coreTopology = sharedDiscoveryService.getCoreTopology( this );
        readReplicaTopology = sharedDiscoveryService.getReadReplicaTopology();

        sharedDiscoveryService.registerCoreMember( this );
        log.info( "Registered core server %s", myself );

        sharedDiscoveryService.waitForClusterFormation();
        log.info( "Cluster formed" );
    }

    @Override
    public void stop()
    {
        sharedDiscoveryService.unRegisterCoreMember( this );
        log.info( "Unregistered core server %s", myself );
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
        // It is perhaps confusing (Or even error inducing) that this core Topology will always contain the cluster id
        // for the database local to the host upon which this method is called.
        // TODO: evaluate returning clusterId = null for global Topologies returned by allCoreServers()
        return this.coreTopology;
    }

    @Override
    public void handleStepDown( long stepDownTerm, String dbName )
    {
        boolean wasLeaderForTerm = Objects.equals( myself, leaderInfo.memberId() ) && stepDownTerm == leaderInfo.term();
        if ( wasLeaderForTerm )
        {
            log.info( String.format( "Step down event detected. This topology member, with MemberId %s, was leader in term %s, now moving " +
                    "to follower.", myself, leaderInfo.term() ) );
            sharedDiscoveryService.casLeaders( leaderInfo.stepDown(), dbName );
        }
    }

    @Override
    public String localDBName()
    {
        return localDBName;
    }

    public MemberId getMemberId()
    {
        return myself;
    }

    public CoreServerInfo getCoreServerInfo()
    {
        return coreServerInfo;
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

    @Override
    public String toString()
    {
        return "SharedDiscoveryCoreClient{" + "myself=" + myself + ", coreServerInfo=" + coreServerInfo + ", refusesToBeLeader=" + refusesToBeLeader +
                ", localDBName='" + localDBName + '\'' + ", leaderInfo=" + leaderInfo + ", coreTopology=" + coreTopology + '}';
    }
}
