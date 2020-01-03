/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.procedure.builtin.routing;

import java.util.List;
import java.util.Objects;

import org.neo4j.configuration.helpers.SocketAddress;

/**
 * The outcome of applying a load balancing plugin, which will be used by client
 * software for scheduling work at the endpoints.
 */
public class RoutingResult
{
    private final List<SocketAddress> routeEndpoints;
    private final List<SocketAddress> writeEndpoints;
    private final List<SocketAddress> readEndpoints;
    private final long timeToLiveMillis;

    public RoutingResult( List<SocketAddress> routeEndpoints, List<SocketAddress> writeEndpoints,
            List<SocketAddress> readEndpoints, long timeToLiveMillis )
    {
        this.routeEndpoints = routeEndpoints;
        this.writeEndpoints = writeEndpoints;
        this.readEndpoints = readEndpoints;
        this.timeToLiveMillis = timeToLiveMillis;
    }

    public long ttlMillis()
    {
        return timeToLiveMillis;
    }

    public List<SocketAddress> routeEndpoints()
    {
        return routeEndpoints;
    }

    public List<SocketAddress> writeEndpoints()
    {
        return writeEndpoints;
    }

    public List<SocketAddress> readEndpoints()
    {
        return readEndpoints;
    }

    boolean containsNoEndpoints()
    {
        return routeEndpoints.isEmpty() && writeEndpoints.isEmpty() && readEndpoints.isEmpty();
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
        var that = (RoutingResult) o;
        return timeToLiveMillis == that.timeToLiveMillis &&
               Objects.equals( routeEndpoints, that.routeEndpoints ) &&
               Objects.equals( writeEndpoints, that.writeEndpoints ) &&
               Objects.equals( readEndpoints, that.readEndpoints );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( routeEndpoints, writeEndpoints, readEndpoints, timeToLiveMillis );
    }

    @Override
    public String toString()
    {
        return "RoutingResult{" +
               "routeEndpoints=" + routeEndpoints +
               ", writeEndpoints=" + writeEndpoints +
               ", readEndpoints=" + readEndpoints +
               ", timeToLiveMillis=" + timeToLiveMillis +
               '}';
    }
}
