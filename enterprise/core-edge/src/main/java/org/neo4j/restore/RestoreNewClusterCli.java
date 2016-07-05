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
package org.neo4j.restore;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.coreedge.convert.ConversionVerifier;
import org.neo4j.coreedge.convert.ConvertClassicStoreToCoreCommand;
import org.neo4j.coreedge.convert.GenerateClusterSeedCommand;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.ArrayUtil;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.Converters;
import org.neo4j.logging.NullLog;
import org.neo4j.server.configuration.ConfigLoader;

import static org.neo4j.dbms.DatabaseManagementSystemSettings.database_path;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.record_format;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class RestoreNewClusterCli implements AdminCommand
{
    public static class Provider extends AdminCommand.Provider
    {
        public Provider()
        {
            super( "restore-new-cluster" );
        }

        @Override
        public Optional<String> arguments()
        {
            return Optional.of( "--from=<backup-directory> --database=<database-name> [--force]" );
        }

        @Override
        public String description()
        {
            return "Restores a database backed up using the neo4j-backup tool to be used as the first instance of a " +
                    "new cluster.";
        }

        @Override
        public AdminCommand create( Path homeDir, Path configDir )
        {
            return new RestoreNewClusterCli( homeDir, configDir );
        }
    }

    private final Path homeDir;
    private final Path configDir;

    public RestoreNewClusterCli( Path homeDir, Path configDir )
    {
        this.homeDir = homeDir;
        this.configDir = configDir;
    }

    @Override
    public void execute( String[] incomingArguments ) throws IncorrectUsage, CommandFailed
    {
        Args args = Args.parse( incomingArguments );
        if ( ArrayUtil.isEmpty( incomingArguments ) )
        {
            throw new IncorrectUsage( "mandatory arguments missing" );
        }

        String databaseName = args.interpretOption( "database", Converters.<String>mandatory(), s -> s );
        String fromPath = args.interpretOption( "from", Converters.<String>mandatory(), s -> s );
        boolean forceOverwrite = args.getBoolean( "force", Boolean.FALSE, true );

        try
        {
            Config config = loadNeo4jConfig( homeDir, configDir, databaseName );
            restoreDatabase( databaseName, fromPath, forceOverwrite, config );
            String seed = generateSeed( config );
            convertStore( config, seed );
            System.out.println( "Cluster Seed: " + seed );
        }
        catch ( IOException | TransactionFailureException e )
        {
            throw new RuntimeException( e );
        }
    }

    private static Config loadNeo4jConfig( Path homeDir, Path configDir, String databaseName )
    {
        ConfigLoader configLoader = new ConfigLoader( settings() );
        Config config = configLoader.loadConfig(
                Optional.of( homeDir.toFile() ),
                Optional.of( configDir.resolve( "neo4j.conf" ).toFile() ),
                NullLog.getInstance() );

        return config.with( stringMap( DatabaseManagementSystemSettings.active_database.name(), databaseName ) );
    }

    private static void convertStore( Config config, String seed ) throws IOException, TransactionFailureException
    {
        ConvertClassicStoreToCoreCommand convert = new ConvertClassicStoreToCoreCommand( new ConversionVerifier() );
        convert.convert( config.get( database_path ), config.get( record_format ), seed );
    }

    private static String generateSeed( Config config ) throws IOException
    {
        return new GenerateClusterSeedCommand().generate( config.get( database_path ) ).getConversionId();
    }

    private static void restoreDatabase( String databaseName, String fromPath, boolean forceOverwrite, Config config )
            throws IOException
    {
        new RestoreDatabaseCommand( new DefaultFileSystemAbstraction(),
                new File( fromPath ), config, databaseName, forceOverwrite ).execute();
    }

    private static List<Class<?>> settings()
    {
        List<Class<?>> settings = new ArrayList<>();
        settings.add( GraphDatabaseSettings.class );
        settings.add( DatabaseManagementSystemSettings.class );
        return settings;
    }
}
