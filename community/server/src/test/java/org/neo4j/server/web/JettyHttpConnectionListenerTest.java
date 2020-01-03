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
import org.junit.jupiter.api.Test;

import org.neo4j.kernel.api.net.NetworkConnectionTracker;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class JettyHttpConnectionListenerTest
{
    private final NetworkConnectionTracker connectionTracker = mock( NetworkConnectionTracker.class );
    private final JettyHttpConnectionListener listener = new JettyHttpConnectionListener( connectionTracker );

    @Test
    void shouldNotifyAboutOpenConnection()
    {
        JettyHttpConnection connection = mock( JettyHttpConnection.class );

        listener.onOpened( connection );

        verify( connectionTracker ).add( connection );
        verify( connectionTracker, never() ).remove( any() );
    }

    @Test
    void shouldNotifyAboutClosedConnection()
    {
        JettyHttpConnection connection = mock( JettyHttpConnection.class );

        listener.onClosed( connection );

        verify( connectionTracker, never() ).add( any() );
        verify( connectionTracker ).remove( connection );
    }

    @Test
    void shouldIgnoreOpenConnectionOfUnknownType()
    {
        Connection connection = mock( Connection.class );

        listener.onOpened( connection );

        verify( connectionTracker, never() ).add( any() );
        verify( connectionTracker, never() ).remove( any() );
    }

    @Test
    void shouldIgnoreClosedConnectionOfUnknownType()
    {
        Connection connection = mock( Connection.class );

        listener.onClosed( connection );

        verify( connectionTracker, never() ).add( any() );
        verify( connectionTracker, never() ).remove( any() );
    }
}
