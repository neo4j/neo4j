/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.convert;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.ArrayUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.Converters;
import org.neo4j.server.configuration.ConfigLoader;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ConvertNonCoreEdgeStoreCli
{
    public static void main( String[] incomingArguments ) throws Throwable
    {
        Args args = Args.parse( incomingArguments );
        if ( ArrayUtil.isEmpty( incomingArguments ) )
        {
            printUsage( System.out );
            System.exit( 1 );
        }

        File homeDir = args.interpretOption( "home-dir", Converters.<File>mandatory(), File::new );
        String databaseName = args.interpretOption( "database", Converters.<String>mandatory(), s -> s );
        String configPath = args.interpretOption( "config", Converters.<String>mandatory(), s -> s );
        String clusterSeed = args.interpretOption( "cluster-seed", Converters.<String>mandatory(), s -> s );

        Config config = createConfig( homeDir, databaseName, configPath );

        new ConvertClassicStoreToCoreCommand( new ConversionVerifier() ).convert(
                config.get( DatabaseManagementSystemSettings.database_path ),
                config.get( GraphDatabaseSettings.record_format ),
                clusterSeed );
    }

    private static Config createConfig( File homeDir, String databaseName, String configPath )
    {
        return new ConfigLoader( settings() ).loadConfig( Optional.of( homeDir ),
                Optional.of( new File( configPath, "neo4j.conf" ) ) )
                .with( stringMap( DatabaseManagementSystemSettings.active_database.name(), databaseName ) );
    }

    private static List<Class<?>> settings()
    {
        List<Class<?>> settings = new ArrayList<>();
        settings.add( GraphDatabaseSettings.class );
        settings.add( DatabaseManagementSystemSettings.class );
        return settings;
    }

    private static void printUsage( PrintStream out )
    {
        out.println( "Neo4j Classic to Core Format Conversion Tool" );
        for ( String line : Args.splitLongLine( "The classic  to core conversion tool is used to convert a classic"
                + "Neo4j store into one which has a core friendly format.", 80 ) )
        {
            out.println( "\t" + line );
        }

        out.println( "Usage:" );
        out.println( "--home-dir <path-to-neo4j-directory>" );
        out.println( "--database <database-name>" );
        out.println( "--config <path-to-config-directory>" );
        out.println( "--cluster-seed <value returned from generate-cluster-seed command>" );
    }
}
