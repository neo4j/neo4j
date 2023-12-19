/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.server.enterprise;

import java.util.Collections;

import org.neo4j.server.BlockingBootstrapper;
import org.neo4j.server.Bootstrapper;
import org.neo4j.server.ServerBootstrapper;
import org.neo4j.server.ServerCommandLineArgs;

import static org.neo4j.commandline.Util.neo4jVersion;

public class ArbiterEntryPoint
{
    private static Bootstrapper bootstrapper;

    private ArbiterEntryPoint()
    {
    }

    public static void main( String[] argv )
    {
        ServerCommandLineArgs args = ServerCommandLineArgs.parse( argv );
        if ( args.version() )
        {
            System.out.println( "neo4j " + neo4jVersion() );
        }
        else
        {
            int status = new ArbiterBootstrapper().start( args.homeDir(), args.configFile(), Collections.emptyMap() );
            if ( status != 0 )
            {
                System.exit( status );
            }
        }
    }

    /**
     * Used by the windows service wrapper
     */
    @SuppressWarnings( "unused" )
    public static void start( String[] args )
    {
        bootstrapper = new BlockingBootstrapper( new ArbiterBootstrapper() );
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
