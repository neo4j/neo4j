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
package org.neo4j.kernel.api.net;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link NetworkConnectionTracker} that keeps all given connections in a {@link ConcurrentHashMap}.
 */
public class DefaultNetworkConnectionTracker implements NetworkConnectionTracker
{
    private final NetworkConnectionIdGenerator idGenerator = new NetworkConnectionIdGenerator();
    private final Map<String,TrackedNetworkConnection> connectionsById = new ConcurrentHashMap<>();

    @Override
    public String newConnectionId( String connector )
    {
        return idGenerator.newConnectionId( connector );
    }

    @Override
    public void add( TrackedNetworkConnection connection )
    {
        TrackedNetworkConnection previousConnection = connectionsById.put( connection.id(), connection );
        if ( previousConnection != null )
        {
            throw new IllegalArgumentException( "Attempt to register a connection with an existing id " + connection.id() + ". " +
                                                "Existing connection: " + previousConnection + ", new connection: " + connection );
        }
    }

    @Override
    public void remove( TrackedNetworkConnection connection )
    {
        connectionsById.remove( connection.id() );
    }

    @Override
    public TrackedNetworkConnection get( String id )
    {
        return connectionsById.get( id );
    }

    @Override
    public List<TrackedNetworkConnection> activeConnections()
    {
        return new ArrayList<>( connectionsById.values() );
    }
}
