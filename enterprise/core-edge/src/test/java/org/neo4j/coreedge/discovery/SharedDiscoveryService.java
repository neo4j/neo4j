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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.BoltAddress;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.unmodifiableSet;

public class SharedDiscoveryService implements DiscoveryServiceFactory
{
    private final Set<CoreMember> coreMembers = new HashSet<>();
    private final Set<BoltAddress> coreBoltAddresses = new HashSet<>();
    private final Set<BoltAddress> edgeBoltAddresses = new HashSet<>();
    private final List<SharedDiscoveryCoreClient> coreClients = new ArrayList<>();

    private final CountDownLatch latch = new CountDownLatch( 2 );

    @Override
    public synchronized CoreTopologyService coreDiscoveryService( Config config, LogProvider logProvider )
    {
        return new SharedDiscoveryCoreClient( config, this, logProvider );
    }

    @Override
    public synchronized EdgeTopologyService edgeDiscoveryService( Config config, LogProvider logProvider )
    {
        return new SharedDiscoveryEdgeClient( this, logProvider );
    }

    void waitForClusterFormation() throws InterruptedException
    {
        latch.await( 10, TimeUnit.SECONDS );
    }

    synchronized ClusterTopology currentTopology( SharedDiscoveryCoreClient client )
    {
        return new TestOnlyClusterTopology(
                coreClients.get( 0 ) == client,
                unmodifiableSet( coreMembers ),
                unmodifiableSet( coreBoltAddresses ),
                unmodifiableSet( edgeBoltAddresses )
        );
    }

    synchronized void registerCoreServer( CoreMember coreMember, BoltAddress boltAddress,
                                          SharedDiscoveryCoreClient client )
    {
        coreMembers.add( coreMember );
        coreBoltAddresses.add( boltAddress );
        coreClients.add( client );
        latch.countDown();
        onTopologyChange();
    }

    synchronized void unregisterCoreServer( CoreMember coreMember, BoltAddress boltAddress,
                                            SharedDiscoveryCoreClient client )
    {
        coreMembers.remove( coreMember );
        coreBoltAddresses.remove( boltAddress );
        coreClients.remove( client );
        onTopologyChange();
    }

    synchronized void registerEdgeServer( AdvertisedSocketAddress address )
    {
        edgeBoltAddresses.add( new BoltAddress( address ) );
    }

    private void onTopologyChange()
    {
        for ( SharedDiscoveryCoreClient coreClient : coreClients )
        {
            coreClient.onTopologyChange( currentTopology( coreClient ) );
        }
    }
}
