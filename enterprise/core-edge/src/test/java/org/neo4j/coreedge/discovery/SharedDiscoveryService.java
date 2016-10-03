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

import org.neo4j.coreedge.core.consensus.schedule.DelayedRenewableTimeoutService;
import org.neo4j.coreedge.identity.ClusterId;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;

public class SharedDiscoveryService implements DiscoveryServiceFactory
{
    private final Map<MemberId,CoreAddresses> coreMembers = new HashMap<>();
    private final Set<EdgeAddresses> edgeAddresses = new HashSet<>();
    private final List<SharedDiscoveryCoreClient> coreClients = new ArrayList<>();

    private final Lock lock = new ReentrantLock();
    private final Condition enoughMembers = lock.newCondition();
    private ClusterId clusterId;

    @Override
    public CoreTopologyService coreTopologyService( Config config, MemberId myself, JobScheduler jobScheduler,
            LogProvider logProvider, LogProvider userLogProvider )
    {
        SharedDiscoveryCoreClient sharedDiscoveryCoreClient = new SharedDiscoveryCoreClient( this, myself, logProvider, config );
        sharedDiscoveryCoreClient.onCoreTopologyChange( coreTopology( sharedDiscoveryCoreClient ) );
        sharedDiscoveryCoreClient.onEdgeTopologyChange( edgeTopology() );
        return sharedDiscoveryCoreClient;
    }

    @Override
    public TopologyService edgeDiscoveryService( Config config, LogProvider logProvider,
                                                 DelayedRenewableTimeoutService timeoutService,
                                                 long edgeTimeToLiveTimeout, long edgeRefreshRate )
    {
        return new SharedDiscoveryEdgeClient( this, config, logProvider );
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

    CoreTopology coreTopology( SharedDiscoveryCoreClient client )
    {
        lock.lock();
        try
        {
            return new CoreTopology(
                    clusterId,
                    coreClients.size() > 0 && coreClients.get( 0 ) == client,
                    unmodifiableMap( coreMembers )
            );
        }
        finally
        {
            lock.unlock();
        }
    }

    EdgeTopology edgeTopology()
    {
        lock.lock();
        try
        {
            return new EdgeTopology(
                    clusterId,
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
            notifyCoreClients();
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
            notifyCoreClients();
        }
        finally
        {
            lock.unlock();
        }
    }

    private void notifyCoreClients()
    {
        for ( SharedDiscoveryCoreClient coreClient : coreClients )
        {
            coreClient.onCoreTopologyChange( coreTopology( coreClient ) );
            coreClient.onEdgeTopologyChange( edgeTopology(  ) );
        }
    }

    void registerEdgeMember( EdgeAddresses edgeAddresses )
    {
        lock.lock();
        try
        {
            this.edgeAddresses.add( edgeAddresses );
            notifyCoreClients();
        }
        finally
        {
            lock.unlock();
        }
    }

    void unRegisterEdgeMember( EdgeAddresses edgeAddresses )
    {
        lock.lock();
        try
        {
            this.edgeAddresses.remove( edgeAddresses );
            notifyCoreClients();
        }
        finally
        {
            lock.unlock();
        }
    }

    boolean casClusterId( ClusterId clusterId )
    {
        boolean success;
        lock.lock();
        try
        {
            if ( this.clusterId == null )
            {
                success = true;
                this.clusterId = clusterId;
            }
            else
            {
                success = this.clusterId.equals( clusterId );
            }

            if ( success )
            {
                notifyCoreClients();
            }
        }
        finally
        {
            lock.unlock();
        }

        return success;
    }
}
