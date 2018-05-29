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

import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.state.RefuseToBeLeaderStrategy;
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

    private CoreTopology coreTopology;
    private ReadReplicaTopology readReplicaTopology;

    SharedDiscoveryCoreClient( SharedDiscoveryService sharedDiscoveryService, MemberId member, LogProvider logProvider, Config config )
    {
        this.sharedDiscoveryService = sharedDiscoveryService;
        this.member = member;
        this.coreServerInfo = extractCoreServerInfo( config );
        this.log = logProvider.getLog( getClass() );
        this.refusesToBeLeader = RefuseToBeLeaderStrategy.shouldRefuseToBeLeader( config );
    }

    @Override
    public synchronized void addCoreTopologyListener( Listener listener )
    {
        listeners.add( listener );
        listener.onCoreTopologyChange( coreTopology );
    }

    @Override
    public boolean setClusterId( ClusterId clusterId )
    {
        return sharedDiscoveryService.casClusterId( clusterId );
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
    public ReadReplicaTopology readReplicas()
    {
        return readReplicaTopology;
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
    public CoreTopology coreServers()
    {
        return coreTopology;
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

        return new CoreServerInfo( raftAddress, transactionSource, clientConnectorAddresses );
    }

    public boolean refusesToBeLeader()
    {
        return refusesToBeLeader;
    }
}
