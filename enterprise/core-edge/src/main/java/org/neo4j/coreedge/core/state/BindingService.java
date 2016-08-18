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
package org.neo4j.coreedge.core.state;

import java.io.IOException;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import org.neo4j.coreedge.core.state.storage.SimpleStorage;
import org.neo4j.coreedge.discovery.ClusterTopology;
import org.neo4j.coreedge.discovery.CoreTopologyService;
import org.neo4j.coreedge.identity.ClusterId;
import org.neo4j.function.ThrowingAction;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

class BindingService extends LifecycleAdapter
{
    private final SimpleStorage<ClusterId> clusterIdStorage;
    private final CoreTopologyService topologyService;
    private final Log log;
    private final Clock clock;
    private final ThrowingAction<InterruptedException> retryWaiter;
    private final long timeoutMillis;

    private ClusterId boundClusterId;

    BindingService( SimpleStorage<ClusterId> clusterIdStorage, CoreTopologyService topologyService, LogProvider logProvider, Clock clock, ThrowingAction<InterruptedException> retryWaiter, long timeoutMillis )
    {
        this.clusterIdStorage = clusterIdStorage;
        this.topologyService = topologyService;
        this.log = logProvider.getLog( getClass() );
        this.clock = clock;
        this.retryWaiter = retryWaiter;
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public void start() throws Throwable
    {
        boundClusterId = bindToCluster();
    }

    /**
     * Returns the bound cluster ID. If the binding service fails to bind, then it will fail to start.
     */
    ClusterId clusterId()
    {
        return Objects.requireNonNull( boundClusterId, "You must not ask for the cluster ID before the binding service has been started." );
    }

    /**
     * The cluster binding process tries to establish a common cluster ID. If there is no common cluster ID
     * then a single instance will eventually create one and publish it through the underlying topology service.
     *
     * @return The common cluster ID.
     * @throws IOException If there is an issue with I/O.
     * @throws InterruptedException If the process gets interrupted.
     * @throws TimeoutException If the process times out.
     */
    private ClusterId bindToCluster() throws IOException, InterruptedException, TimeoutException, BindingException
    {
        ClusterId localClusterId = clusterIdStorage.exists() ? clusterIdStorage.readState() : null;
        BindingProcess binder = new BindingProcess( localClusterId, log );

        long endTime = clock.millis() + timeoutMillis;

        ClusterTopology topology = topologyService.currentTopology();
        ClusterId commonClusterId;

        while ( (commonClusterId = binder.attempt( topology )) == null )
        {
            if ( clock.millis() < endTime )
            {
                retryWaiter.apply();
                topology = topologyService.currentTopology();
            }
            else
            {
                throw new TimeoutException( "Failed binding to cluster in time." );
            }
        }

        if ( localClusterId == null )
        {
            clusterIdStorage.writeState( commonClusterId );
        }

        if ( topology.canBeBootstrapped() )
        {
            boolean success = topologyService.publishClusterId( commonClusterId );
            if ( !success )
            {
                throw new BindingException( "Failed to publish: " + commonClusterId );
            }
            else
            {
                log.info( "Published: " + commonClusterId );
            }
        }

        return commonClusterId;
    }
}
