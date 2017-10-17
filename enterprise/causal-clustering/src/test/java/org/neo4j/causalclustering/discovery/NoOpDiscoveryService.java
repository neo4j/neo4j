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

import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;

import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

/**
 * Discovery service that doesn't do anything
 */
public class NoOpDiscoveryService implements DiscoveryServiceFactory
{

    private final CoreTopologyService coreTopologyService;
    private final CoreTopology coreTopology;
    private final ReadReplicaTopology readReplicaTopology;

    public NoOpDiscoveryService()
    {
        coreTopology = new CoreTopology( new ClusterId( UUID.randomUUID() ), false, new HashMap<>(  ) );
        readReplicaTopology = new ReadReplicaTopology( new HashMap<>(  ) );
        coreTopologyService = new FakeTopologyService();
    }

    class FakeTopologyService implements CoreTopologyService
    {
        @Override
        public void addCoreTopologyListener( Listener listener )
        {
        }

        @Override
        public boolean setClusterId( ClusterId clusterId ) throws InterruptedException
        {
            return true;
        }

        @Override
        public CoreTopology coreServers()
        {
            return coreTopology;
        }

        @Override
        public ReadReplicaTopology readReplicas()
        {
            return readReplicaTopology;
        }

        @Override
        public Optional<AdvertisedSocketAddress> findCatchupAddress( MemberId upstream )
        {
            return Optional.empty();
        }

        @Override
        public void init() throws Throwable
        {
        }

        @Override
        public void start() throws Throwable
        {
        }

        @Override
        public void stop() throws Throwable
        {
        }

        @Override
        public void shutdown() throws Throwable
        {
        }
    }

    @Override
    public CoreTopologyService coreTopologyService( Config config, MemberId myself, JobScheduler jobScheduler, LogProvider logProvider,
            LogProvider userLogProvider, HostnameResolver hostnameResolver, TopologyServiceRetryStrategy topologyServiceRetryStrategy )
    {
        return coreTopologyService;
    }

    @Override
    public TopologyService topologyService( Config config, LogProvider logProvider, JobScheduler jobScheduler, MemberId myself,
            HostnameResolver hostnameResolver, TopologyServiceRetryStrategy topologyServiceRetryStrategy )
    {
        return coreTopologyService;
    }
}
