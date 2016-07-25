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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.coreedge.server.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;

public class SharedDiscoveryService implements DiscoveryServiceFactory
{
    private final Map<MemberId, CoreAddresses> coreMembers = new HashMap<>(  );
    private final Set<EdgeAddresses> edgeAddresses = new HashSet<>();
    private final List<SharedDiscoveryCoreClient> coreClients = new ArrayList<>();

    private final Lock lock = new ReentrantLock();
    private final Condition enoughMembers = lock.newCondition();

    @Override
    public CoreTopologyService coreDiscoveryService( Config config, MemberId myself, LogProvider logProvider )
    {
        return new SharedDiscoveryCoreClient( config, myself, this, logProvider );
    }

    @Override
    public EdgeTopologyService edgeDiscoveryService( Config config, LogProvider logProvider )
    {
        return new SharedDiscoveryEdgeClient( this, logProvider );
    }

    void waitForClusterFormation() throws InterruptedException
    {
        lock.lock();
        try
        {
            while ( coreMembers.size() < 2 )
            {
                enoughMembers.await( 10, TimeUnit.SECONDS );
            }
        }
        finally
        {
            lock.unlock();
        }
    }

    ClusterTopology currentTopology( SharedDiscoveryCoreClient client )
    {
        lock.lock();
        try
        {
            return new ClusterTopology(
                    coreClients.size() > 0 && coreClients.get( 0 ) == client,
                    unmodifiableMap( coreMembers ),
                    unmodifiableSet( edgeAddresses )
            );
        }
        finally
        {
            lock.unlock();
        }
    }

    void registerCoreMember( MemberId memberId, CoreAddresses coreAddresses, SharedDiscoveryCoreClient client )
    {
        lock.lock();
        try
        {
            coreMembers.put( memberId, coreAddresses );
            coreClients.add( client );
            enoughMembers.signalAll();
            coreClients.forEach( SharedDiscoveryCoreClient::onTopologyChange );
        }
        finally
        {
            lock.unlock();
        }
    }

    void unRegisterCoreMember( MemberId memberId, SharedDiscoveryCoreClient client )
    {
        lock.lock();
        try
        {
            coreMembers.remove( memberId );
            coreClients.remove( client );
            coreClients.forEach( SharedDiscoveryCoreClient::onTopologyChange );
        }
        finally
        {
            lock.unlock();
        }
    }

    void registerEdgeMember( EdgeAddresses edgeAddresses )
    {
        lock.lock();
        try
        {
            this.edgeAddresses.add( edgeAddresses );
        }
        finally
        {
            lock.unlock();
        }
    }
}
