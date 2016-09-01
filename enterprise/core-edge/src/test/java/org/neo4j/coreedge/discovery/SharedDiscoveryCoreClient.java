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

import java.util.LinkedHashSet;
import java.util.Set;

import org.neo4j.coreedge.core.CoreEdgeClusterSettings;
import org.neo4j.coreedge.edge.EnterpriseEdgeEditionModule;
import org.neo4j.coreedge.identity.ClusterId;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.coreedge.messaging.address.SocketAddress;
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

    private ClusterTopology clusterTopology;

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
        listener.onCoreTopologyChange( clusterTopology );
    }

    @Override
    public boolean publishClusterId( ClusterId clusterId )
    {
        return sharedDiscoveryService.casClusterId( clusterId );
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
    public synchronized ClusterTopology currentTopology()
    {
        return clusterTopology;
    }

    synchronized void onTopologyChange( ClusterTopology clusterTopology )
    {
        log.info( "Notified of topology change" );
        this.clusterTopology = clusterTopology;
        for ( Listener listener : listeners )
        {
            listener.onCoreTopologyChange( clusterTopology );
        }
    }

    private static CoreAddresses extractAddresses( Config config )
    {
        SocketAddress raftAddress = config.get( CoreEdgeClusterSettings.raft_advertised_address );
        SocketAddress transactionSource = config.get( CoreEdgeClusterSettings.transaction_advertised_address );
        SocketAddress boltAddress = EnterpriseEdgeEditionModule.extractBoltAddress( config );

        return new CoreAddresses( raftAddress, transactionSource, boltAddress );
    }
}
