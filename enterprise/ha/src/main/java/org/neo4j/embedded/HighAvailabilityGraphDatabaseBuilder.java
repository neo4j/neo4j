/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.embedded;

import java.util.List;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.helpers.ArrayUtil;
import org.neo4j.kernel.ha.HaSettings;

abstract class HighAvailabilityGraphDatabaseBuilder<BUILDER extends GraphDatabaseBuilder<BUILDER,GRAPHDB>, GRAPHDB extends GraphDatabase>
        extends GraphDatabaseBuilder<BUILDER,GRAPHDB>
{
    HighAvailabilityGraphDatabaseBuilder( int memberId )
    {
        params.put( ClusterSettings.server_id.name(), String.valueOf( memberId ) );
    }

    /**
     * Set the address for binding the cluster listener to
     *
     * @param address the address to bind the cluster listener to, which must be the address of a local interface
     * @return this builder
     */
    public BUILDER bindClusterListenerTo( String address )
    {
        params.put( ClusterSettings.cluster_server.name(), address );
        return self();
    }

    /**
     * Set the address for binding the transaction listener to
     *
     * @param address the address to bind the transaction listener to, which must be the address of a local interface
     * @return this builder
     */
    public BUILDER bindTransactionListenerTo( String address )
    {
        params.put( HaSettings.ha_server.name(), address );
        return self();
    }

    /**
     * Set the addresses of other servers to contact when trying to connect to a cluster.
     *
     * @param addresses the list of server addresses to contact when trying to connect to a cluster
     * @return this builder
     */
    public BUILDER withInitialHostAddresses( String... addresses )
    {
        params.put( ClusterSettings.initial_hosts.name(), ArrayUtil.join( addresses, "," ) );
        return self();
    }

    /**
     * Set the addresses of other servers to contact when trying to connect to a cluster.
     *
     * @param addresses the list of server addresses to contact when trying to connect to a cluster
     * @return this builder
     */
    public BUILDER withInitialHostAddresses( List<String> addresses )
    {
        return withInitialHostAddresses( addresses.toArray( new String[addresses.size()] ) );
    }
}
