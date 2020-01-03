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

import java.util.List;

import static java.util.Collections.emptyList;

/**
 * Container for all established and active network connections to the database.
 */
public interface NetworkConnectionTracker
{
    String newConnectionId( String connector );

    void add( TrackedNetworkConnection connection );

    void remove( TrackedNetworkConnection connection );

    TrackedNetworkConnection get( String id );

    List<TrackedNetworkConnection> activeConnections();

    NetworkConnectionTracker NO_OP = new NetworkConnectionTracker()
    {
        private final NetworkConnectionIdGenerator idGenerator = new NetworkConnectionIdGenerator();

        @Override
        public String newConnectionId( String connector )
        {
            // need to generate a valid ID because it appears in logs, bolt messages, etc.
            return idGenerator.newConnectionId( connector );
        }

        @Override
        public void add( TrackedNetworkConnection connection )
        {
        }

        @Override
        public void remove( TrackedNetworkConnection connection )
        {
        }

        @Override
        public TrackedNetworkConnection get( String id )
        {
            return null;
        }

        @Override
        public List<TrackedNetworkConnection> activeConnections()
        {
            return emptyList();
        }
    };
}
