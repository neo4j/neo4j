/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.mjolnir.launcher;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.function.Functions;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.Args.Option;
import org.neo4j.helpers.Strings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.Converters;
import org.neo4j.kernel.lifecycle.LifeSupport;

public class Launcher
{

    private static String usage()
    {
        return Strings.lines(
                "Neo4j - Graph database server",
                "USAGE:",
                "         neo4j [--help] [--config <file>] [-c <name>=<value>]... [--config-options]" ,
                "WHERE:",
                "         -h --help           Show this help text",
                "         -C --config <file>  Path to configuration [default: ./neo4j.conf]",
                "         -c <name>=<value>   Override individual configuration parameters",
                "         --config-options    List all available config options and exit",
                "Examples:",
                "         neo4j -C /etc/neo4j.conf",
                "         neo4j -c dbms.datadir=/opt/neo4j" );
    }

    public static void main( String[] argv ) throws Throwable
    {
        Args args = Args.parse( argv );

        if ( args.has( "h" ) || args.has( "help" ) )
        {
            System.out.println( usage() );
        }
        else if ( args.has( "config-options" ) )
        {
            new ConfigOptions().describeTo( System.out );
        }
        else
        {
            LifeSupport life = new LifeSupport();
            shutdownOnVMShutdown( life );
            life.add( new HandAssembledNeo4j( loadConfig( args ) ) );
            life.start();
        }
    }

    private static void shutdownOnVMShutdown( final LifeSupport life )
    {
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                life.shutdown();
            }
        } );
    }

    static Config loadConfig( Args args ) throws IOException
    {
        // load config file
        File file;
        if ( args.has( "config" ) )
        {
            file = new File( args.get( "config" ) );
        }
        else if ( args.has( "C" ) )
        {
            file = new File( args.get( "C" ) );
        }
        else
        {
            file = new File( "./neo4j.conf" );
        }
        final Map<String,String> config = file.exists() ? MapUtil.load( file ) : new HashMap<String,String>();

        // re-set config options if -c is specified
        if ( args.has( "c" ) )
        {
            Collection<Option<String>> options = args.interpretOptionsWithMetadata( "c",
                    Converters.<String> mandatory(), Functions.<String> identity() );
            for ( Option<String> option : options )
            {
                String[] parts = option.value().split( "=" );
                config.put( parts[0], parts[1] );
            }
        }

        return new Config( config );
    }
}