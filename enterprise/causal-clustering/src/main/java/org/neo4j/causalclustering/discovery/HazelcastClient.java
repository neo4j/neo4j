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
package org.neo4j.causalclustering.discovery;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;

import java.util.function.Function;

import org.neo4j.causalclustering.core.consensus.schedule.RenewableTimeoutService;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.causalclustering.discovery.HazelcastClusterTopology.READ_REPLICA_BOLT_ADDRESS_MAP_NAME;

class HazelcastClient extends LifecycleAdapter implements TopologyService
{
    static final RenewableTimeoutService.TimeoutName REFRESH_READ_REPLICA = () -> "Refresh Read Replica";
    private final Log log;
    private final ClientConnectorAddresses connectorAddresses;
    private final HazelcastConnector connector;
    private final RenewableTimeoutService renewableTimeoutService;
    private HazelcastInstance hazelcastInstance;
    private RenewableTimeoutService.RenewableTimeout readReplicaRefreshTimer;
    private final long readReplicaTimeToLiveTimeout;
    private final long readReplicaRefreshRate;

    HazelcastClient( HazelcastConnector connector, LogProvider logProvider, Config config,
                     RenewableTimeoutService renewableTimeoutService, long readReplicaTimeToLiveTimeout, long readReplicaRefreshRate )
    {
        this.connector = connector;
        this.renewableTimeoutService = renewableTimeoutService;
        this.readReplicaRefreshRate = readReplicaRefreshRate;
        this.log = logProvider.getLog( getClass() );
        this.connectorAddresses = ClientConnectorAddresses.extractFromConfig( config );
        this.readReplicaTimeToLiveTimeout = readReplicaTimeToLiveTimeout;
    }

    @Override
    public CoreTopology coreServers()
    {
        try
        {
            return retry( ( hazelcastInstance ) ->
                    HazelcastClusterTopology.getCoreTopology( hazelcastInstance, log ) );
        }
        catch ( Exception e )
        {
            log.info( "Failed to read cluster topology from Hazelcast. Continuing with empty (disconnected) topology. "
                    + "Connection will be reattempted on next polling attempt.", e );
            return CoreTopology.EMPTY;
        }
    }

    @Override
    public void start() throws Throwable
    {
        readReplicaRefreshTimer = renewableTimeoutService.create( REFRESH_READ_REPLICA, readReplicaRefreshRate, 0, timeout -> {
            timeout.renew();
            retry( this::addReadReplica );
        } );
    }

    private Object addReadReplica( HazelcastInstance hazelcastInstance )
    {
        String uuid = hazelcastInstance.getLocalEndpoint().getUuid();
        String addresses = connectorAddresses.toString();

        log.debug( "Adding read replica into cluster (%s -> %s)", uuid, addresses  );

        return hazelcastInstance.getMap( READ_REPLICA_BOLT_ADDRESS_MAP_NAME )
                .put( uuid, addresses, readReplicaTimeToLiveTimeout, MILLISECONDS );
    }

    @Override
    public synchronized void stop() throws Throwable
    {
        if ( hazelcastInstance != null )
        {
            try
            {
                String uuid = hazelcastInstance.getLocalEndpoint().getUuid();
                hazelcastInstance.getMap( READ_REPLICA_BOLT_ADDRESS_MAP_NAME ).remove( uuid );
                hazelcastInstance.shutdown();
            }
            catch ( Throwable t )
            {
                // Hazelcast is not able to stop correctly sometimes and throws a bunch of different exceptions
                // let's simply log the current problem but go on with our shutdown
                log.warn( "Unable to shutdown Hazelcast", t );
            }
        }

        readReplicaRefreshTimer.cancel();
    }

    private synchronized <T> T retry( Function<HazelcastInstance, T> hazelcastOperation )
    {
        boolean attemptedConnection = false;
        HazelcastInstanceNotActiveException exception = null;

        while ( !attemptedConnection )
        {
            if ( hazelcastInstance == null )
            {
                attemptedConnection = true;
                hazelcastInstance = connector.connectToHazelcast();
            }

            try
            {
                return hazelcastOperation.apply( hazelcastInstance );
            }
            catch ( HazelcastInstanceNotActiveException e )
            {
                hazelcastInstance = null;
                exception = e;
            }
        }
        throw exception;
    }
}
