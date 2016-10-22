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
package org.neo4j.causalclustering.discovery;

import java.util.LinkedHashSet;
import java.util.Set;

import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

class SharedDiscoveryCoreClient extends LifecycleAdapter implements CoreTopologyService
{
    private final SharedDiscoveryService sharedDiscoveryService;
    private final MemberId member;
    private final CoreAddresses coreAddresses;
    private final Set<Listener> listeners = new LinkedHashSet<>();
    private final Log log;

    private CoreTopology coreTopology;
    private ReadReplicaTopology readReplicaTopology;

    SharedDiscoveryCoreClient( SharedDiscoveryService sharedDiscoveryService, MemberId member, LogProvider logProvider, Config config )
    {
        this.sharedDiscoveryService = sharedDiscoveryService;
        this.member = member;
        this.coreAddresses = extractAddresses( config );
        this.log = logProvider.getLog( getClass() );
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
    public void refreshCoreTopology()
    {
        // do nothing
    }

    @Override
    public void start() throws InterruptedException
    {
        sharedDiscoveryService.registerCoreMember( member, coreAddresses, this );
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

    private static CoreAddresses extractAddresses( Config config )
    {
        AdvertisedSocketAddress raftAddress = config.get( CausalClusteringSettings.raft_advertised_address );
        AdvertisedSocketAddress transactionSource = config.get( CausalClusteringSettings.transaction_advertised_address );
        ClientConnectorAddresses clientConnectorAddresses = ClientConnectorAddresses.extractFromConfig( config );

        return new CoreAddresses( raftAddress, transactionSource, clientConnectorAddresses );
    }
}
