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

import org.neo4j.coreedge.server.BoltAddress;
import org.neo4j.coreedge.server.CoreEdgeClusterSettings;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.edge.EnterpriseEdgeEditionModule;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

class SharedDiscoveryCoreClient extends LifecycleAdapter implements CoreTopologyService
{
    private final SharedDiscoveryService sharedDiscoveryService;
    private final CoreMember member;
    private final BoltAddress boltAddress;
    private final Set<Listener> listeners = new LinkedHashSet<>();
    private final Log log;

    SharedDiscoveryCoreClient( Config config, SharedDiscoveryService sharedDiscoveryService, LogProvider logProvider )
    {
        this.sharedDiscoveryService = sharedDiscoveryService;
        this.member = toCoreMember( config );
        this.boltAddress = extractBoltAddress( config );
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public synchronized void addMembershipListener( Listener listener )
    {
        listeners.add( listener );
    }

    @Override
    public synchronized void removeMembershipListener( Listener listener )
    {
        listeners.remove( listener );
    }

    @Override
    public void start() throws InterruptedException
    {
        sharedDiscoveryService.registerCoreServer( member, boltAddress, this );
        log.info( "Registered core server %s", member );
        sharedDiscoveryService.waitForClusterFormation();
        log.info( "Cluster formed" );
    }

    @Override
    public void stop()
    {
        sharedDiscoveryService.unRegisterCoreServer( member, boltAddress, this );
        log.info( "Unregistered core server %s", member );
    }

    @Override
    public ClusterTopology currentTopology()
    {
        ClusterTopology topology = sharedDiscoveryService.currentTopology( this );
        log.info( "Current topology is %s", topology );
        return topology;
    }

    synchronized void onTopologyChange()
    {
        log.info( "Notified of topology change" );
        listeners.forEach( Listener::onTopologyChange );
    }

    private static CoreMember toCoreMember( Config config )
    {
        return new CoreMember(
                config.get( CoreEdgeClusterSettings.transaction_advertised_address ),
                config.get( CoreEdgeClusterSettings.raft_advertised_address ),
                EnterpriseEdgeEditionModule.extractBoltAddress( config )
        );
    }

    private static BoltAddress extractBoltAddress( Config config )
    {
        return new BoltAddress( EnterpriseEdgeEditionModule.extractBoltAddress( config ) );
    }
}
