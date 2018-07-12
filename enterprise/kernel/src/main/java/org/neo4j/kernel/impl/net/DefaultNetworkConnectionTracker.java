/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.impl.net;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.api.net.NetworkConnectionIdGenerator;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.net.TrackedNetworkConnection;

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
