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
package org.neo4j.causalclustering.net;

import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.ports.allocation.PortAuthority;

class ServerStateTestHelpers
{
    static void teardown( Server server )
    {
        server.stop();
        server.shutdown();
    }

    static Server createServer()
    {
        return new Server( channel ->
                           {
                           }, FormattedLogProvider.withDefaultLogLevel( Level.DEBUG ).toOutputStream( System.out ),
                           FormattedLogProvider.withDefaultLogLevel( Level.DEBUG ).toOutputStream( System.out ),
                           new ListenSocketAddress( "localhost", PortAuthority.allocatePort() ), "serverName" );
    }

    static void setEnableableState( Server server, EnableableState enableableState )
    {
        switch ( enableableState )
        {
        case Enabled:
            server.enable();
            return;
        case Disabled:
            server.disable();
            return;
        case Untouched:
            return;
        default:
            throw new IllegalStateException( "Not recognized state " + enableableState );
        }
    }

    static void setInitialState( Server server, LifeCycleState state ) throws Throwable
    {
        for ( LifeCycleState lifeCycleState : LifeCycleState.values() )
        {
            if ( lifeCycleState.compareTo( state ) <= 0 )
            {
                lifeCycleState.set( server );
            }
        }
    }

    enum LifeCycleState
    {
        Init
                {
                    @Override
                    void set( Lifecycle lifecycle ) throws Throwable
                    {
                        lifecycle.init();
                    }
                },
        Start
                {
                    @Override
                    void set( Lifecycle lifecycle ) throws Throwable
                    {
                        lifecycle.start();
                    }
                },
        Stop
                {
                    @Override
                    void set( Lifecycle lifecycle ) throws Throwable
                    {
                        lifecycle.stop();
                    }
                },
        Shutdown
                {
                    @Override
                    void set( Lifecycle lifecycle ) throws Throwable
                    {
                        lifecycle.shutdown();
                    }
                };

        abstract void set( Lifecycle lifecycle ) throws Throwable;
    }

    enum EnableableState
    {
        Untouched,
        Enabled,
        Disabled
    }
}
