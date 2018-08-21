/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.util.Map;
import java.util.Optional;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;

class SharedDiscoveryCoreClient extends AbstractCoreTopologyService implements Comparable<SharedDiscoveryCoreClient>
{
    private final SharedDiscoveryService sharedDiscoveryService;
    private final CoreServerInfo coreServerInfo;
    private final String localDBName;
    private final boolean refusesToBeLeader;

    private volatile LeaderInfo leaderInfo = LeaderInfo.INITIAL;
    private volatile ReadReplicaTopology readReplicaTopology = ReadReplicaTopology.EMPTY;
    private volatile CoreTopology coreTopology = CoreTopology.EMPTY;
    private volatile ReadReplicaTopology localReadReplicaTopology = ReadReplicaTopology.EMPTY;
    private volatile CoreTopology localCoreTopology = CoreTopology.EMPTY;

    SharedDiscoveryCoreClient( SharedDiscoveryService sharedDiscoveryService,
            MemberId member, LogProvider logProvider, Config config )
    {
        super( config, member, logProvider, logProvider );
        this.localDBName = config.get( CausalClusteringSettings.database );
        this.sharedDiscoveryService = sharedDiscoveryService;
        this.coreServerInfo = CoreServerInfo.from( config );
        this.refusesToBeLeader = config.get( CausalClusteringSettings.refuse_to_be_leader );
    }

    @Override
    public int compareTo( SharedDiscoveryCoreClient o )
    {
        return Optional.ofNullable( o ).map( c -> c.myself.getUuid().compareTo( this.myself.getUuid() ) ).orElse( -1 );
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
    public void setLeader0( LeaderInfo newLeader )
    {
        leaderInfo =  newLeader;
        sharedDiscoveryService.casLeaders( newLeader, localDBName );
    }

    @Override
    public LeaderInfo getLeader()
    {
        return leaderInfo;
    }

    @Override
    public void init0()
    {
        // nothing to do
    }

    @Override
    public void start0() throws InterruptedException
    {
        coreTopology = sharedDiscoveryService.getCoreTopology( this );
        localCoreTopology = coreTopology.filterTopologyByDb( localDBName );
        readReplicaTopology = sharedDiscoveryService.getReadReplicaTopology();
        localReadReplicaTopology = readReplicaTopology.filterTopologyByDb( localDBName );

        sharedDiscoveryService.registerCoreMember( this );
        log.info( "Registered core server %s", myself );

        sharedDiscoveryService.waitForClusterFormation();
        log.info( "Cluster formed" );
    }

    @Override
    public void stop0()
    {
        sharedDiscoveryService.unRegisterCoreMember( this );
        log.info( "Unregistered core server %s", myself );
    }

    @Override
    public void shutdown0()
    {
        // nothing to do
    }

    @Override
    public String localDBName()
    {
        return localDBName;
    }

    @Override
    public CoreTopology allCoreServers()
    {
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
    public Optional<AdvertisedSocketAddress> findCatchupAddress( MemberId upstream )
    {
        return localCoreServers().find( upstream )
                .map( info -> Optional.of( info.getCatchupServer() ) )
                .orElseGet( () -> readReplicaTopology.find( upstream )
                        .map( ReadReplicaInfo::getCatchupServer ) );
    }

    @Override
    public void handleStepDown0( LeaderInfo steppingDown )
    {
        sharedDiscoveryService.casLeaders( steppingDown, localDBName );
    }

    public MemberId getMemberId()
    {
        return myself;
    }

    public CoreServerInfo getCoreServerInfo()
    {
        return coreServerInfo;
    }

    void onCoreTopologyChange( CoreTopology coreTopology )
    {
        log.info( "Notified of core topology change " + coreTopology );
        this.coreTopology = coreTopology;
        this.localCoreTopology = coreTopology.filterTopologyByDb( localDBName );
        listenerService.notifyListeners( coreTopology );
    }

    void onReadReplicaTopologyChange( ReadReplicaTopology readReplicaTopology )
    {
        log.info( "Notified of read replica topology change " + readReplicaTopology );
        this.readReplicaTopology = readReplicaTopology;
        this.localReadReplicaTopology = readReplicaTopology.filterTopologyByDb( localDBName );
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
