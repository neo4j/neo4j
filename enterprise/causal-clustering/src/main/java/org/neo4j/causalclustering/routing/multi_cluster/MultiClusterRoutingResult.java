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
package org.neo4j.causalclustering.routing.multi_cluster;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.neo4j.causalclustering.routing.RoutingResult;
import org.neo4j.causalclustering.routing.Endpoint;

/**
 * Simple struct containing the the result of multi-cluster routing procedure execution.
 */
public class MultiClusterRoutingResult implements RoutingResult
{
    private final Map<String,List<Endpoint>> routers;
    private final long timeToLiveMillis;

    public MultiClusterRoutingResult( Map<String,List<Endpoint>> routers, long timeToLiveMillis )
    {
        this.routers = routers;
        this.timeToLiveMillis = timeToLiveMillis;
    }

    public Map<String,List<Endpoint>> routers()
    {
        return routers;
    }

    public long ttlMillis()
    {
        return timeToLiveMillis;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        MultiClusterRoutingResult that = (MultiClusterRoutingResult) o;
        return timeToLiveMillis == that.timeToLiveMillis && Objects.equals( routers, that.routers );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( routers, timeToLiveMillis );
    }
}

