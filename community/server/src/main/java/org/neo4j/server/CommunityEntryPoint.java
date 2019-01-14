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
package org.neo4j.server;

public class CommunityEntryPoint
{
    private static Bootstrapper bootstrapper;

    private CommunityEntryPoint()
    {
    }

    public static void main( String[] args )
    {
        int status = ServerBootstrapper.start( new CommunityBootstrapper(), args );
        if ( status != 0 )
        {
            System.exit( status );
        }
    }

    /**
     * Used by the windows service wrapper
     */
    @SuppressWarnings( "unused" )
    public static void start( String[] args )
    {
        bootstrapper = new BlockingBootstrapper( new CommunityBootstrapper() );
        System.exit( ServerBootstrapper.start( bootstrapper, args ) );
    }

    /**
     * Used by the windows service wrapper
     */
    @SuppressWarnings( "unused" )
    public static void stop( @SuppressWarnings( "UnusedParameters" ) String[] args )
    {
        if ( bootstrapper != null )
        {
            bootstrapper.stop();
        }
    }
}
