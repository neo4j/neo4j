/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

import static java.util.Collections.unmodifiableMap;

public class SharedDiscoveryService implements DiscoveryServiceFactory
{
    private final Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
    private final Map<MemberId,ReadReplicaInfo> readReplicaInfoMap = new HashMap<>();
    private final List<SharedDiscoveryCoreClient> coreClients = new ArrayList<>();

    private final Lock lock = new ReentrantLock();
    private final Condition enoughMembers = lock.newCondition();
    private ClusterId clusterId;

    @Override
    public CoreTopologyService coreTopologyService( Config config, MemberId myself, JobScheduler jobScheduler,
            LogProvider logProvider, LogProvider userLogProvider, HostnameResolver hostnameResolver,
            TopologyServiceRetryStrategy topologyServiceRetryStrategy )
    {
        SharedDiscoveryCoreClient sharedDiscoveryCoreClient =
                new SharedDiscoveryCoreClient( this, myself, logProvider, config );
        sharedDiscoveryCoreClient.onCoreTopologyChange( coreTopology( sharedDiscoveryCoreClient ) );
        sharedDiscoveryCoreClient.onReadReplicaTopologyChange( readReplicaTopology() );
        return sharedDiscoveryCoreClient;
    }

    @Override
    public TopologyService topologyService( Config config, LogProvider logProvider,
            JobScheduler jobScheduler, MemberId myself, HostnameResolver hostnameResolver,
            TopologyServiceRetryStrategy topologyServiceRetryStrategy )
    {
        return new SharedDiscoveryReadReplicaClient( this, config, myself, logProvider );
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
            return new CoreTopology( clusterId, coreClients.size() > 0 && coreClients.get( 0 ) == client,
                    unmodifiableMap( coreMembers ) );
        }
        finally
        {
            lock.unlock();
        }
    }

    ReadReplicaTopology readReplicaTopology()
    {
        lock.lock();
        try
        {
            return new ReadReplicaTopology( unmodifiableMap( readReplicaInfoMap ) );

        }
        finally
        {
            lock.unlock();
        }
    }

    void registerCoreMember( MemberId memberId, CoreServerInfo coreServerInfo, SharedDiscoveryCoreClient client )
    {
        lock.lock();
        try
        {
            coreMembers.put( memberId, coreServerInfo );
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
            coreClient.onReadReplicaTopologyChange( readReplicaTopology() );
        }
    }

    void registerReadReplica( MemberId memberId, ReadReplicaInfo readReplicaInfo )
    {
        lock.lock();
        try
        {
            readReplicaInfoMap.put( memberId, readReplicaInfo );
            notifyCoreClients();
        }
        finally
        {
            lock.unlock();
        }
    }

    void unRegisterReadReplica( MemberId memberId )
    {
        lock.lock();
        try
        {
            readReplicaInfoMap.remove( memberId );
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
