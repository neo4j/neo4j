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
package org.neo4j.causalclustering.identity;

import java.io.IOException;
import java.time.Clock;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.neo4j.causalclustering.core.state.CoreBootstrapper;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import org.neo4j.causalclustering.core.state.storage.SimpleStorage;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.function.ThrowingAction;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class ClusterBinder implements Supplier<Optional<ClusterId>>
{
    private final SimpleStorage<ClusterId> clusterIdStorage;
    private final CoreTopologyService topologyService;
    private final CoreBootstrapper coreBootstrapper;
    private final Log log;
    private final Clock clock;
    private final ThrowingAction<InterruptedException> retryWaiter;
    private final long timeoutMillis;

    private ClusterId clusterId;

    public ClusterBinder( SimpleStorage<ClusterId> clusterIdStorage, CoreTopologyService topologyService,
                            LogProvider logProvider, Clock clock, ThrowingAction<InterruptedException> retryWaiter,
                            long timeoutMillis, CoreBootstrapper coreBootstrapper )
    {
        this.clusterIdStorage = clusterIdStorage;
        this.topologyService = topologyService;
        this.coreBootstrapper = coreBootstrapper;
        this.log = logProvider.getLog( getClass() );
        this.clock = clock;
        this.retryWaiter = retryWaiter;
        this.timeoutMillis = timeoutMillis;
    }

    /**
     * The cluster binding process tries to establish a common cluster ID. If there is no common cluster ID
     * then a single instance will eventually create one and publish it through the underlying topology service.
     *
     * @throws IOException If there is an issue with I/O.
     * @throws InterruptedException If the process gets interrupted.
     * @throws TimeoutException If the process times out.
     */
    public BoundState bindToCluster() throws Throwable
    {
        if ( clusterIdStorage.exists() )
        {
            clusterId = clusterIdStorage.readState();
            publishClusterId( clusterId );
            log.info( "Already bound to cluster: " + clusterId );
            return new BoundState( clusterId );
        }

        CoreSnapshot snapshot = null;
        CoreTopology topology;
        long endTime = clock.millis() + timeoutMillis;

        do
        {
            topology = topologyService.coreServers();

            if ( topology.clusterId() != null )
            {
                clusterId = topology.clusterId();
                log.info( "Bound to cluster: " + clusterId );
            }
            else if ( topology.canBeBootstrapped() )
            {
                clusterId = new ClusterId( UUID.randomUUID() );
                snapshot = coreBootstrapper.bootstrap( topology.members().keySet() );
                log.info( String.format( "Bootstrapped with snapshot: %s and clusterId: %s", snapshot, clusterId ) );

                publishClusterId( clusterId );
            }
            else
            {
                retryWaiter.apply();
            }
        } while ( clusterId == null && clock.millis() < endTime );

        if ( clusterId == null )
        {
            throw new TimeoutException( String.format(
                    "Failed to join a cluster with members %s. Another member should have published " +
                    "a clusterId but none was detected. Please restart the cluster.", topology ) );
        }

        clusterIdStorage.writeState( clusterId );
        return new BoundState( clusterId, snapshot );
    }

    @Override
    public Optional<ClusterId> get()
    {
        return Optional.ofNullable( clusterId );
    }

    private void publishClusterId( ClusterId localClusterId ) throws BindingException, InterruptedException
    {
        boolean success = topologyService.setClusterId( localClusterId );
        if ( !success )
        {
            throw new BindingException( "Failed to publish: " + localClusterId );
        }
        else
        {
            log.info( "Published: " + localClusterId );
        }
    }
}
