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

import java.util.function.Function;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;

import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

import static org.neo4j.coreedge.discovery.HazelcastClusterTopology.EDGE_SERVERS;

class HazelcastClient extends LifecycleAdapter implements EdgeTopologyService
{
    private final Log log;
    private final HazelcastConnector connector;
    private HazelcastInstance hazelcastInstance;

    HazelcastClient( HazelcastConnector connector, LogProvider logProvider )
    {
        this.connector = connector;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public ClusterTopology currentTopology()
    {
        try
        {
            return retry( ( hazelcastInstance ) ->
                    HazelcastClusterTopology.fromHazelcastInstance( hazelcastInstance, log ) );
        }
        catch ( Exception e )
        {
            log.info( "Failed to read cluster topology from Hazelcast. Continuing with empty (disconnected) topology. "
                    + "Connection will be reattempted on next polling attempt.", e );
            return new ClusterTopology( false, emptyMap(), emptySet() );
        }
    }

    @Override
    public synchronized void stop() throws Throwable
    {
        if ( hazelcastInstance != null )
        {
            hazelcastInstance.shutdown();
        }
    }

    @Override
    public void registerEdgeServer( AdvertisedSocketAddress address )
    {
        retry( ( hazelcastInstance ) -> hazelcastInstance.getSet( EDGE_SERVERS ).add( address.toString() ) );
    }

    private synchronized <T> T retry( Function<HazelcastInstance, T> hazelcastAccessor )
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
                return hazelcastAccessor.apply( hazelcastInstance );
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
