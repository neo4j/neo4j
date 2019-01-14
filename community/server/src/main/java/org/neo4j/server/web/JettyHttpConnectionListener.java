/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.server.web;

import org.eclipse.jetty.io.Connection;

import org.neo4j.kernel.api.net.NetworkConnectionTracker;

/**
 * Connection listener that notifies {@link NetworkConnectionTracker} about open and closed {@link JettyHttpConnection}s.
 * All other types of connections are ignored.
 */
public class JettyHttpConnectionListener implements Connection.Listener
{
    private final NetworkConnectionTracker connectionTracker;

    public JettyHttpConnectionListener( NetworkConnectionTracker connectionTracker )
    {
        this.connectionTracker = connectionTracker;
    }

    @Override
    public void onOpened( Connection connection )
    {
        if ( connection instanceof JettyHttpConnection )
        {
            connectionTracker.add( (JettyHttpConnection) connection );
        }
    }

    @Override
    public void onClosed( Connection connection )
    {
        if ( connection instanceof JettyHttpConnection )
        {
            connectionTracker.remove( (JettyHttpConnection) connection );
        }
    }
}
