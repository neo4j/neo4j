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
package org.neo4j.server.web;

import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;

import org.neo4j.kernel.api.net.NetworkConnectionTracker;

/**
 * Extension of the default Jetty {@link HttpConnectionFactory} which creates connections with additional properties.
 * Created connections also notify {@link NetworkConnectionTracker} when open or closed.
 */
public class JettyHttpConnectionFactory extends HttpConnectionFactory
{
    private final NetworkConnectionTracker connectionTracker;
    private final JettyHttpConnectionListener connectionListener;

    public JettyHttpConnectionFactory( NetworkConnectionTracker connectionTracker, HttpConfiguration configuration )
    {
        super( configuration );
        this.connectionTracker = connectionTracker;
        this.connectionListener = new JettyHttpConnectionListener( connectionTracker );
    }

    @Override
    public Connection newConnection( Connector connector, EndPoint endPoint )
    {
        JettyHttpConnection connection = createConnection( connector, endPoint );
        connection.addListener( connectionListener );
        return configure( connection, connector, endPoint );
    }

    private JettyHttpConnection createConnection( Connector connector, EndPoint endPoint )
    {
        String connectionId = connectionTracker.newConnectionId( connector.getName() );
        return new JettyHttpConnection( connectionId, getHttpConfiguration(), connector, endPoint,
                getHttpCompliance(), isRecordHttpComplianceViolations() );
    }
}
