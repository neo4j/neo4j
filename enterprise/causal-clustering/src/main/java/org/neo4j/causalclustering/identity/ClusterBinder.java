/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.neo4j.causalclustering.core.state.CoreBootstrapper;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import org.neo4j.causalclustering.core.state.storage.SimpleStorage;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.function.ThrowingAction;
import org.neo4j.kernel.impl.util.CappedLogger;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

public class ClusterBinder implements Supplier<Optional<ClusterId>>
{
    private final SimpleStorage<ClusterId> clusterIdStorage;
    private final CoreTopologyService topologyService;
    private final CoreBootstrapper coreBootstrapper;
    private final Log log;
    private final CappedLogger cappedLog;
    private final Clock clock;
    private final ThrowingAction<InterruptedException> retryWaiter;
    private final long timeoutMillis;
    private final String dbName;
    private final int minCoreHosts;

    private ClusterId clusterId;

    public ClusterBinder( SimpleStorage<ClusterId> clusterIdStorage, CoreTopologyService topologyService,
                            LogProvider logProvider, Clock clock, ThrowingAction<InterruptedException> retryWaiter,
                            long timeoutMillis, CoreBootstrapper coreBootstrapper, String dbName, int minCoreHosts )
    {
        this.clusterIdStorage = clusterIdStorage;
        this.topologyService = topologyService;
        this.coreBootstrapper = coreBootstrapper;
        this.log = logProvider.getLog( getClass() );
        this.cappedLog = new CappedLogger( log ).setTimeLimit( 5, TimeUnit.SECONDS, clock );
        this.clock = clock;
        this.retryWaiter = retryWaiter;
        this.timeoutMillis = timeoutMillis;
        this.dbName = dbName;
        this.minCoreHosts = minCoreHosts;
    }

    /**
     * This method verifies if the local topology being returned by the discovery service is a viable cluster.
     * If true, then a) the topology is sufficiently large to form a cluster; & b) this host can bootstrap for
     * its configured database.
     *
     * @param coreTopology the present state of the local topology, as reported by the discovery service.
     * @return Whether or not coreTopology, in its current state, can form a viable cluster
     */
    private boolean isViableCluster( CoreTopology coreTopology )
    {
        int memberCount = coreTopology.members().size();
        if ( memberCount < minCoreHosts )
        {
            String message = "Waiting for %d members. Currently discovered %d members: %s. ";
            cappedLog.info( format( message, minCoreHosts, memberCount, coreTopology.members() ) );
            return false;
        }
        else if ( !coreTopology.canBeBootstrapped() )
        {
            String message = "Discovered sufficient members (%d) but waiting for bootstrap by other instance.";
            cappedLog.info( format( message, memberCount ) );
            return false;
        }
        else
        {
            return true;
        }
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
            topology = topologyService.localCoreServers();

            if ( topology.clusterId() != null )
            {
                clusterId = topology.clusterId();
                log.info( "Bound to cluster: " + clusterId );
            }
            else if ( isViableCluster( topology ) )
            {
                clusterId = new ClusterId( UUID.randomUUID() );
                snapshot = coreBootstrapper.bootstrap( topology.members().keySet() );
                log.info( format( "Bootstrapped with snapshot: %s and clusterId: %s", snapshot, clusterId ) );

                publishClusterId( clusterId );
            }
            else
            {
                retryWaiter.apply();
            }
        } while ( clusterId == null && clock.millis() < endTime );

        if ( clusterId == null )
        {
            throw new TimeoutException( format(
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
        boolean success = topologyService.setClusterId( localClusterId, dbName );
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
