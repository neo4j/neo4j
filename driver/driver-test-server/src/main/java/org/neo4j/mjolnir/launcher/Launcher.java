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

import org.docopt.Docopt;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifeSupport;

public class Launcher
{
    // Details on this: https://github.com/docopt/docopt.java
    public static final Docopt opt = new Docopt(
        "Neo4j - Graph database server\n" +
        "\n" +
        "Usage:\n" +
        "    neo4j [options]\n" +
        "    neo4j [options] [-c <name>=<value>]...\n" +
        "\n" +
        "Options:\n" +
        "    -h --help           Show this help text\n" +
        "    -C --config <file>  Path to configuration  [default: ./neo4j.conf]\n" +
        "    -c <name>=<value>   Override individual configuration parameters\n" +
        "    --config-options    List all available config options and exit\n" +
        "\n" +
        "Examples:\n" +
        "    neo4j -C /etc/neo4j.conf\n" +
        "    neo4j -c dbms.datadir=/opt/neo4j\n" +
        "\n");

    public static void main( String[] argv ) throws Throwable
    {
        Map<String,Object> options = opt.parse( argv );

        if ( options.get( "--config-options" ).equals( true ) )
        {
            new ConfigOptions().describeTo( System.out );
        }
        else
        {
            LifeSupport life = new LifeSupport();
            shutdownOnVMShutdown( life );
            life.add( new HandAssembledNeo4j( loadConfig( options ) ) );
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

    private static Config loadConfig( Map<String,Object> args ) throws IOException
    {
        File file = new File( (String) args.get( "--config" ) );
        final Map<String,String> config = file.exists() ? MapUtil.load( file ) : new HashMap<String,String>();

        for ( Map.Entry<String,Object> arg : args.entrySet() )
        {
            if ( arg.getKey().equals( "-c" ) )
            {
                List<String> values = (List<String>) arg.getValue();
                for ( String value : values )
                {
                    String[] parts = value.split( "=" );
                    config.put( parts[0], parts[1] );
                }
            }
        }

        return new Config( config );
    }
}